package cn.scut.app.util.transfer.transformer.evaluation;

import static cn.scut.app.util.transfer.constant.EvalStageFields.EXPERIMENT_ID;
import static cn.scut.app.util.transfer.constant.EvalStageFields.EXPRESSION;
import static cn.scut.app.util.transfer.constant.EvalStageFields.GROUP_TRAIN;
import static cn.scut.app.util.transfer.constant.EvalStageFields.HEALTH_CHECKS;
import static cn.scut.app.util.transfer.constant.EvalStageFields.METRICS;
import static cn.scut.app.util.transfer.constant.EvalStageFields.NAME;
import static cn.scut.app.util.transfer.constant.TransferTaskFields.AMMO;
import static cn.scut.app.util.transfer.constant.TransferTaskFields.ARGUMENTS;
import static cn.scut.app.util.transfer.constant.TransferTaskFields.DEPENDS_ON;
import static cn.scut.app.util.transfer.constant.TransferTaskFields.DISABLE;
import static cn.scut.app.util.transfer.constant.TransferTaskFields.INHERIT;
import static cn.scut.app.util.transfer.constant.TransferTaskFields.SETTINGS;
import static cn.scut.app.util.transfer.constant.TransferTaskFields.TASK_TYPE;

import cn.scut.app.util.transfer.context.TransferContext;
import cn.scut.app.util.transfer.entity.output.layer.impl.TransferGroup;
import cn.scut.app.util.transfer.entity.output.layer.impl.TransferTask;
import cn.scut.app.util.transfer.transformer.Transformer;
import cn.scut.app.util.transfer.utils.TransferDevUtil;
import cn.scut.app.util.transfer.utils.TransferMetricsUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.vavr.control.Try;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import javax.xml.transform.TransformerException;

public class TrainEvalTransformer implements Transformer {

  private final String readMetricsTaskSuffix = "_metrics";

  @Override
  public TransferGroup transform(HashMap<String, Object> root,
      TransferContext context) {
    return Try.of(() -> {
      List<TransferTask> adapterTaskList = new ArrayList<>();
      this.evalTrainTransform(context, root)
          .forEach((key, value) -> adapterTaskList.add(new TransferTask(key, value)));
      TransferGroup adapterGroup = new TransferGroup(GROUP_TRAIN, adapterTaskList);
      adapterGroup.createRangeTask(GROUP_TRAIN);
      return adapterGroup;
    }).get();
  }

  private LinkedHashMap<String, Object> evalTrainTransform(TransferContext context,
      HashMap<String, Object> root)
      throws JsonProcessingException, TransformerException {
    LinkedHashMap<String, Object> readMetricsTaskList = this.readMetricsTransform(context, root);
    LinkedHashMap<String, Object> taskList            = new LinkedHashMap<>(readMetricsTaskList);
    LinkedHashMap<String, Object> healthCheckTaskList = this.healthCheckTransform(context, root);
    taskList.putAll(healthCheckTaskList);
    return taskList;
  }

  private LinkedHashMap<String, Object> readMetricsTransform(TransferContext context,
      HashMap<String, Object> root) throws TransformerException, JsonProcessingException {
    ArrayList<String> trainTaskIds = context.getTrainTaskIds();
    if (trainTaskIds.isEmpty()) {
      throw new TransformerException("train task is empty");
    }
    LinkedHashMap<String, Object> taskList           = new LinkedHashMap<>();
    String                        firstTrainTaskId   = trainTaskIds.get(0);
    String                        firstMetricsTaskId = firstTrainTaskId + readMetricsTaskSuffix;
    taskList.put(firstMetricsTaskId,
        this.readMetricsSettingsTransformForFirst(firstTrainTaskId, root));
    for (int count = 1; count < trainTaskIds.size(); count++) {
      String taskId = trainTaskIds.get(count);
      taskList.put(taskId + readMetricsTaskSuffix,
          this.readMetricsSettingsTransformForLast(firstMetricsTaskId, taskId));
    }
    return taskList;
  }

  private LinkedHashMap<String, Object> readMetricsSettingsTransformForFirst(String taskId,
      HashMap<String, Object> root) throws TransformerException, JsonProcessingException {
    ObjectNode settings = JsonNodeFactory.instance.objectNode();
    if (!root.containsKey(METRICS)) {
      throw new TransformerException(
          "Evaluation task with source 'train' loss key '" + METRICS + "'");
    }
    Object   metricsName = root.get(METRICS);
    JsonNode metrics     = this.analysisMetricsName(metricsName);
    settings.replace("metrics", metrics);
    String experimentId =
        "{{ ti.xcom_pull(task_ids=\"" + taskId + "\", key=\"experiment_id\" )}}";
    settings.replace("venv", JsonNodeFactory.instance.textNode("krylov_env.tar.gz"));
    settings.replace(EXPERIMENT_ID, JsonNodeFactory.instance.textNode(experimentId));
    settings.replace(AMMO, JsonNodeFactory.instance.textNode("flag_bit"));
    LinkedHashMap<String, Object> readTaskInstance = new LinkedHashMap<>();
    readTaskInstance.put(TASK_TYPE, "krylov_read_metrics");
    readTaskInstance.put(SETTINGS, settings);
    readTaskInstance.put(DISABLE, JsonNodeFactory.instance.booleanNode(false));
    readTaskInstance.put(DEPENDS_ON, JsonNodeFactory.instance.arrayNode());
    return readTaskInstance;
  }

  private LinkedHashMap<String, Object> readMetricsSettingsTransformForLast(String inheritId,
      String taskId) {
    LinkedHashMap<String, Object> readTaskInstance = new LinkedHashMap<>();
    readTaskInstance.put(TASK_TYPE, "krylov_read_metrics");
    ObjectNode settings = JsonNodeFactory.instance.objectNode();
    String experimentId =
        "{{ ti.xcom_pull(task_ids=\"" + taskId + "\", key=\"experiment_id\" )}}";
    settings.replace("experiment_id", JsonNodeFactory.instance.textNode(experimentId));
    readTaskInstance.put(INHERIT, inheritId);
    readTaskInstance.put(AMMO, "flag_bit");
    readTaskInstance.put(SETTINGS, settings);
    readTaskInstance.put(DISABLE, JsonNodeFactory.instance.booleanNode(false));
    readTaskInstance.put(DEPENDS_ON, JsonNodeFactory.instance.arrayNode());
    return readTaskInstance;
  }

  private LinkedHashMap<String, Object> healthCheckTransform(TransferContext context,
      HashMap<String, Object> root) throws JsonProcessingException, TransformerException {
    ArrayList<String>             trainTaskIds          = context.getTrainTaskIds();
    LinkedHashMap<String, Object> taskList              = new LinkedHashMap<>();
    String                        firstTrainTaskId      = trainTaskIds.get(0);
    String                        healthCheckTaskSuffix = "_health_check";
    String                        firstHealthTaskId     = firstTrainTaskId + healthCheckTaskSuffix;
    taskList.put(firstHealthTaskId,
        this.healthCheckSettingsTransformForFirst(firstTrainTaskId, root));
    for (int count = 1; count < trainTaskIds.size(); count++) {
      String taskId = trainTaskIds.get(count);
      taskList.put(taskId + healthCheckTaskSuffix,
          this.healthCheckSettingsTransformForLast(firstHealthTaskId, taskId));
    }
    return taskList;
  }

  private LinkedHashMap<String, Object> healthCheckSettingsTransformForFirst(String taskId,
      HashMap<String, Object> root) throws JsonProcessingException, TransformerException {
    LinkedHashMap<String, Object> hcTaskInstance = new LinkedHashMap<>();
    hcTaskInstance.put(TASK_TYPE, "python");
    ObjectNode settings = JsonNodeFactory.instance.objectNode();
    settings.replace("python_callable",
        JsonNodeFactory.instance.textNode(
            "submodules.ammo.util.ammo_util.experiment_health_check"));
    settings.replace("timeout",
        JsonNodeFactory.instance.textNode("${sys.timedelta(minutes=1)}"));
    hcTaskInstance.put(SETTINGS, settings);
    ObjectNode arguments = JsonNodeFactory.instance.objectNode();
    arguments.replace(EXPERIMENT_ID,
        JsonNodeFactory.instance.textNode(
            "{{ ti.xcom_pull(task_ids= \"" + taskId + "\", key=\"experiment_id\")}}"));
    ArrayNode tableMetricsList = JsonNodeFactory.instance.arrayNode();
    JsonNode metrics = TransferDevUtil.yamlMapper.readTree(
        TransferDevUtil.yamlMapper.writeValueAsString(root.get(METRICS)));
    if (!metrics.isArray()) {
      tableMetricsList.addAll(this.analysisDagMetricsNode(metrics));
    } else {
      for (JsonNode metric : metrics) {
        tableMetricsList.addAll(this.analysisDagMetricsNode(metric));
      }
    }
    arguments.replace(HEALTH_CHECKS, tableMetricsList);
    hcTaskInstance.put(ARGUMENTS, arguments);
    hcTaskInstance.put(DISABLE, JsonNodeFactory.instance.booleanNode(false));
    ArrayNode dependsOn = JsonNodeFactory.instance.arrayNode();
    dependsOn.add(taskId + readMetricsTaskSuffix);
    hcTaskInstance.put(DEPENDS_ON, dependsOn);
    return hcTaskInstance;
  }

  private LinkedHashMap<String, Object> healthCheckSettingsTransformForLast(String inheritId,
      String taskId) {
    LinkedHashMap<String, Object> hcTaskInstance = new LinkedHashMap<>();
    hcTaskInstance.put(INHERIT, JsonNodeFactory.instance.textNode(inheritId));
    hcTaskInstance.put(DISABLE, JsonNodeFactory.instance.booleanNode(false));
    hcTaskInstance.put(TASK_TYPE, "python");
    ObjectNode arguments = JsonNodeFactory.instance.objectNode();
    arguments.replace(EXPERIMENT_ID, JsonNodeFactory.instance.textNode(
        "{{ ti.xcom_pull(task_ids= \"" + taskId + "\", key=\"experiment_id\")}}"));
    hcTaskInstance.put(ARGUMENTS, arguments);
    ArrayNode dependsOn = JsonNodeFactory.instance.arrayNode();
    dependsOn.add(taskId + readMetricsTaskSuffix);
    hcTaskInstance.put(DEPENDS_ON, dependsOn);
    return hcTaskInstance;
  }

  private JsonNode analysisMetricsName(Object metricsObject)
      throws JsonProcessingException {
    return TransferMetricsUtil.analysisMetricsNames(TransferDevUtil.yamlMapper.readTree(
        TransferDevUtil.yamlMapper.writeValueAsString(metricsObject)));
  }

  private List<JsonNode> analysisDagMetricsNode(JsonNode metric)
      throws TransformerException, JsonProcessingException {
    if (!metric.has(NAME)) {
      throw new TransformerException(
          "Evaluation task with source 'train' in 'metrics' loss key '" + NAME + "'");
    }
    if (!metric.has(EXPRESSION)) {
      throw new TransformerException(
          "Evaluation task with source 'train' in 'metrics' loss key '" + EXPRESSION + "'");
    }
    return TransferMetricsUtil.analysisMetricsNode(metric);
  }
}

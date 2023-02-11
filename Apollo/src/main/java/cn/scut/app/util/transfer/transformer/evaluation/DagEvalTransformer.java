package cn.scut.app.util.transfer.transformer.evaluation;

import static cn.scut.app.util.transfer.constant.EvalStageFields.DAG_EVAL_HEALTH_CHECK;
import static cn.scut.app.util.transfer.constant.EvalStageFields.DAG_RUN_ID;
import static cn.scut.app.util.transfer.constant.EvalStageFields.EXPRESSION;
import static cn.scut.app.util.transfer.constant.EvalStageFields.GROUP_DAG_EVAL;
import static cn.scut.app.util.transfer.constant.EvalStageFields.HEALTH_CHECKS;
import static cn.scut.app.util.transfer.constant.EvalStageFields.METRICS;
import static cn.scut.app.util.transfer.constant.EvalStageFields.NAME;
import static cn.scut.app.util.transfer.constant.TransferTaskFields.ARGUMENTS;
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

public class DagEvalTransformer implements Transformer {

  @Override
  public TransferGroup transform(HashMap<String, Object> root,
      TransferContext context) {
    return Try.of(() -> {
      List<TransferTask> adapterTaskList = new ArrayList<>();
      this.dagEvalTransform(context, root)
          .forEach((key, value) -> adapterTaskList.add(new TransferTask(key, value)));
      TransferGroup adapterGroup = new TransferGroup(GROUP_DAG_EVAL, adapterTaskList);
      adapterGroup.createRangeTask(GROUP_DAG_EVAL);
      return adapterGroup;
    }).get();
  }

  private LinkedHashMap<String, Object> dagEvalTransform(TransferContext context,
      HashMap<String, Object> root) throws TransformerException, JsonProcessingException {
    LinkedHashMap<String, Object> taskList = new LinkedHashMap<>();
    taskList.put(DAG_EVAL_HEALTH_CHECK, this.healthCheckSettingsTransformForDag(context, root));
    return taskList;
  }

  private LinkedHashMap<String, Object> healthCheckSettingsTransformForDag(
      TransferContext context, HashMap<String, Object> root)
      throws TransformerException, JsonProcessingException {
    LinkedHashMap<String, Object> hcTaskInstance = new LinkedHashMap<>();
    hcTaskInstance.put(TASK_TYPE, "python");
    ObjectNode settings = JsonNodeFactory.instance.objectNode();
    settings.replace("python_callable",
        JsonNodeFactory.instance.textNode("submodules.ammo.util.ammo_util.model_health_check"));
    settings.replace("timeout",
        JsonNodeFactory.instance.textNode("'${sys.timedelta(minutes=3)}'"));
    hcTaskInstance.put(SETTINGS, settings);
    //hcTaskInstance.put(GROUP_ID, JsonNodeFactory.instance.textNode(GROUP_DAG_EVAL));
    ObjectNode arguments = JsonNodeFactory.instance.objectNode();
    // TODO: fill these parameters
    arguments.replace("project_name",
        JsonNodeFactory.instance.textNode(context.getTransferDagParams("project_name").toString()));
    arguments.replace("model_name", JsonNodeFactory.instance.textNode("model name"));
    arguments.replace(DAG_RUN_ID,
        JsonNodeFactory.instance.textNode("'{{run_id}}'"));
    ArrayNode tableMetricsList = JsonNodeFactory.instance.arrayNode();
    JsonNode metrics = TransferDevUtil.yamlMapper.readTree(
        TransferDevUtil.yamlMapper.writeValueAsString(root.get(METRICS)));
    if (!metrics.isArray()) {
      tableMetricsList.addAll(this.analysisMetricsNode(metrics));
    } else {
      for (JsonNode metric : metrics) {
        tableMetricsList.addAll(this.analysisMetricsNode(metric));
      }
    }
    arguments.replace(HEALTH_CHECKS, tableMetricsList);
    hcTaskInstance.put(ARGUMENTS, arguments);
    return hcTaskInstance;
  }

  private List<JsonNode> analysisMetricsNode(JsonNode metric)
      throws TransformerException, JsonProcessingException {
    if (!metric.has(NAME)) {
      throw new TransformerException(
          "Evaluation task with source 'dag' in 'metrics' loss key '" + NAME + "'");
    }
    if (!metric.has(EXPRESSION)) {
      throw new TransformerException(
          "Evaluation task with source 'dag' in 'metrics' loss key '" + EXPRESSION + "'");
    }
    return TransferMetricsUtil.analysisMetricsNode(metric);
  }
}

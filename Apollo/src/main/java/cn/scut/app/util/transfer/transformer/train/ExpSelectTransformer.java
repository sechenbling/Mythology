package cn.scut.app.util.transfer.transformer.train;

import static cn.scut.app.util.transfer.constant.TrainStageFields.CONDITION;
import static cn.scut.app.util.transfer.constant.TrainStageFields.CONDITIONS;
import static cn.scut.app.util.transfer.constant.TrainStageFields.CREATE_BY;
import static cn.scut.app.util.transfer.constant.TrainStageFields.DAG_RUN_ID;
import static cn.scut.app.util.transfer.constant.TrainStageFields.GROUP_EXP_SELECTION;
import static cn.scut.app.util.transfer.constant.TrainStageFields.MODEL_INSTANCE_NAME;
import static cn.scut.app.util.transfer.constant.TrainStageFields.MODEL_NAME;
import static cn.scut.app.util.transfer.constant.TrainStageFields.PROJECT_NAME;
import static cn.scut.app.util.transfer.constant.TrainStageFields.SORT;
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
import java.util.Map.Entry;

public class ExpSelectTransformer implements Transformer {

  private final String expSelectionSuffix = "_experiment_selection";

  @Override
  public TransferGroup transform(HashMap<String, Object> root, TransferContext context) {
    return Try.of(() -> {
      JsonNode exp = TransferDevUtil.yamlMapper.readTree(
          TransferDevUtil.yamlMapper.writeValueAsString(
              root.get("model_selection")));
      List<TransferTask> adapterTasks = this.expSelectTransform(exp, context);
      TransferGroup      adapterGroup = new TransferGroup(GROUP_EXP_SELECTION, adapterTasks);
      adapterGroup.createRangeTask(GROUP_EXP_SELECTION);
      return adapterGroup;
    }).get();
  }

  private List<TransferTask> expSelectTransform(JsonNode exp, TransferContext context)
      throws JsonProcessingException {
    List<TransferTask> expSelectTaskList = new ArrayList<>();
    for (Entry<String, ArrayList<String>> entry :
        context.getTransferModel().getModel2instance().entrySet()) {
      String       modelName      = entry.getKey();
      List<String> modelTaskNames = new ArrayList<>();
      boolean      isFirst        = true;
      String       firstTaskId    = "";
      for (String modelInstanceName : entry.getValue()) {
        if (isFirst) {
          firstTaskId = modelInstanceName + expSelectionSuffix;
          modelTaskNames.add(firstTaskId);
          expSelectTaskList.add(
              expSelectTransformForFirst(exp, firstTaskId, modelName, modelInstanceName, context));
          isFirst = false;
        } else {
          TransferTask adapterTask = expSelectTransformForLast(firstTaskId, modelInstanceName);
          modelTaskNames.add(adapterTask.getTaskId());
          expSelectTaskList.add(adapterTask);
        }
      }
      //expSelectTaskList.add(createAmmoModelTransform(modelName, modelTaskNames, context));
    }
    return expSelectTaskList;
  }

  private TransferTask expSelectTransformForFirst(JsonNode exp,
      String taskId, String modelName, String modelInstanceName,
      TransferContext context)
      throws JsonProcessingException {
    LinkedHashMap<String, Object> taskInstance = new LinkedHashMap<>();
    taskInstance.put(TASK_TYPE, "python");
    ObjectNode settings = JsonNodeFactory.instance.objectNode();
    settings.replace("python_callable",
        JsonNodeFactory.instance.textNode(
            "submodules.ammo.util.ammo_util.select_experiment"));
    settings.replace("timeout",
        JsonNodeFactory.instance.textNode("${sys.timedelta(minutes=1)}"));
    taskInstance.put(SETTINGS, settings);
    ObjectNode arguments = JsonNodeFactory.instance.objectNode();
    arguments.replace(CREATE_BY,
        JsonNodeFactory.instance.textNode(context.getTransferParams().getOwner()));
    arguments.replace(PROJECT_NAME,
        JsonNodeFactory.instance.textNode(context.getTransferParams().getDagId()));
    arguments.replace(DAG_RUN_ID, JsonNodeFactory.instance.textNode("'{{ run_id }}'"));
    arguments.replace(MODEL_NAME,
        JsonNodeFactory.instance.textNode(modelName));
    arguments.replace(MODEL_INSTANCE_NAME,
        JsonNodeFactory.instance.textNode(modelInstanceName));
    List<JsonNode> conditionList = TransferMetricsUtil.analysisMetricsList(
        exp.get(CONDITION));
    ArrayNode condition = JsonNodeFactory.instance.arrayNode();
    conditionList.forEach(condition::add);
    arguments.replace(CONDITIONS, condition);
    arguments.replace(SORT, exp.get(SORT));
    taskInstance.put(ARGUMENTS, arguments);
    taskInstance.put(DISABLE, false);
    taskInstance.put(DEPENDS_ON, new ArrayList<>());
    return new TransferTask(taskId, taskInstance);
  }

  private TransferTask expSelectTransformForLast(String firstTaskId,
      String modelInstanceName) {
    LinkedHashMap<String, Object> taskInstance = new LinkedHashMap<>();
    taskInstance.put(TASK_TYPE, "python");
    taskInstance.put(INHERIT, firstTaskId);
    ObjectNode arguments = JsonNodeFactory.instance.objectNode();
    arguments.replace(MODEL_INSTANCE_NAME,
        JsonNodeFactory.instance.textNode(modelInstanceName));
    taskInstance.put(ARGUMENTS, arguments);
    taskInstance.put(DEPENDS_ON, new ArrayList<>());
    return new TransferTask(modelInstanceName + expSelectionSuffix, taskInstance);
  }

  private TransferTask createAmmoModelTransform(String modelName, List<String> modelTaskNames,
      TransferContext context) {
    LinkedHashMap<String, Object> taskInstance = new LinkedHashMap<>();
    taskInstance.put(TASK_TYPE, "python");
    ObjectNode settings = JsonNodeFactory.instance.objectNode();
    settings.replace("timeout",
        JsonNodeFactory.instance.textNode("${sys.timedelta(minutes=1)}"));
    settings.replace("python_callable",
        JsonNodeFactory.instance.textNode(
            "submodules.ammo.util.ammo_util.create_model"));
    taskInstance.put(SETTINGS, settings);
    ObjectNode arguments = JsonNodeFactory.instance.objectNode();
    arguments.replace(CREATE_BY,
        JsonNodeFactory.instance.textNode(context.getTransferParams().getOwner()));
    arguments.replace(PROJECT_NAME,
        JsonNodeFactory.instance.textNode(context.getTransferParams().getDagId()));
    arguments.replace(MODEL_NAME,
        JsonNodeFactory.instance.textNode(modelName));
    arguments.replace(DAG_RUN_ID, JsonNodeFactory.instance.textNode("'{{ run_id }}'"));
    taskInstance.put(ARGUMENTS, arguments);
    // TODO: add stages in arguments
    taskInstance.put(DEPENDS_ON, modelTaskNames);
    return new TransferTask(modelName + "_create_ammo_model", taskInstance);
  }
}

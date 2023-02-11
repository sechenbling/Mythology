package cn.scut.app.util.transfer.transformer.train;

import static cn.scut.app.util.transfer.constant.TransferTaskFields.AMMO;
import static cn.scut.app.util.transfer.constant.TransferTaskFields.ARGUMENTS;
import static cn.scut.app.util.transfer.constant.TransferTaskFields.DEPENDS_ON;
import static cn.scut.app.util.transfer.constant.TransferTaskFields.DISABLE;
import static cn.scut.app.util.transfer.constant.TransferTaskFields.TASK_TYPE;
import static cn.scut.app.util.transfer.constant.AmmoFields.MODEL_INSTANCE;
import static cn.scut.app.util.transfer.constant.AmmoFields.MODEL_NAME;
import static cn.scut.app.util.transfer.constant.AmmoFields.PROJECT_NAME;
import static cn.scut.app.util.transfer.constant.AmmoFields.TRY_ID;
import static cn.scut.app.util.transfer.constant.TrainStageFields.DELIMITER;
import static cn.scut.app.util.transfer.constant.TrainStageFields.EXPERIMENT_NAME;
import static cn.scut.app.util.transfer.constant.TrainStageFields.GROUP_KRYLOV;
import static cn.scut.app.util.transfer.constant.TrainStageFields.RENDER;
import static cn.scut.app.util.transfer.constant.TrainStageFields.SETTINGS;
import static cn.scut.app.util.transfer.constant.TrainStageFields.SETTINGS_EMS_PARAMS;

import cn.scut.app.util.transfer.context.TransferContext;
import cn.scut.app.util.transfer.entity.output.layer.impl.TransferGroup;
import cn.scut.app.util.transfer.entity.output.layer.impl.TransferTask;
import cn.scut.app.util.transfer.transformer.Transformer;
import cn.scut.app.util.transfer.utils.TransferDevUtil;
import cn.scut.app.util.transfer.utils.TransferRenderUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.vavr.control.Try;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;
import javax.xml.transform.TransformerException;

public class KrylovTrainTransformer implements Transformer {

  private final String krylovRetrainSuffix = "_krylov_retrain";

  @Override
  public TransferGroup transform(HashMap<String, Object> root,
      TransferContext context) {
    return Try.of(() -> {
      JsonNode krylov = TransferDevUtil.yamlMapper.readTree(
          TransferDevUtil.yamlMapper.writeValueAsString(
              root.get(context.getTransferConfigs().getLevelOneField("train"))));
      List<TransferTask> adapterTaskList = new ArrayList<>();
      this.krylovTransform(context, krylov)
          .forEach((key, value) -> adapterTaskList.add(new TransferTask(key, value)));
      TransferGroup adapterGroup = new TransferGroup(GROUP_KRYLOV, adapterTaskList);
      context.setTrainTaskGroup(adapterGroup);
      adapterGroup.createRangeTask(GROUP_KRYLOV);
      return adapterGroup;
    }).get();
  }

  private LinkedHashMap<String, Object> krylovTransform(TransferContext context, JsonNode krylov)
      throws Exception {
    JsonNode settings = krylov.get(SETTINGS);
    if (!Optional.ofNullable(settings).isPresent()
        || settings == JsonNodeFactory.instance.nullNode()) {
      throw new TransformerException("Train task krylov loss key 'settings'");
    }
    if (krylov.get(RENDER) == null) {
      LinkedHashMap<String, Object> taskList = new LinkedHashMap<>();
      for (Map.Entry<String, ArrayList<String>> entry :
          context.getTransferModel().getModel2instance().entrySet()) {
        for (String entryValue : entry.getValue()) {
          ObjectNode ammo = JsonNodeFactory.instance.objectNode();
          ammo.replace(PROJECT_NAME, JsonNodeFactory.instance.textNode(
              context.getTransferDagParams("project_name").toString()));
          ammo.replace(MODEL_NAME, JsonNodeFactory.instance.textNode(entry.getKey()));
          ammo.replace(MODEL_INSTANCE, JsonNodeFactory.instance.textNode(entryValue));
          ammo.replace(TRY_ID, JsonNodeFactory.instance.numberNode(0));
          JsonNode settingsCp = settings.deepCopy();
          ((ObjectNode) settingsCp).replace(AMMO, ammo);
          String taskId = entryValue + krylovRetrainSuffix;
          LinkedHashMap<String, Object> taskInstance = this.krylovTaskInstanceFormat(taskId,
              settingsCp);
          taskList.put(taskId, taskInstance);
        }
      }
      return taskList;
    } else {
      // format tasks configs template
      String settingsTemplate = TransferDevUtil.yamlMapper
          .writeValueAsString(TransferDevUtil.yamlMapper.readTree(settings.toString()))
          .replaceAll("(\\s)(\"\\{\\{)((\\s)*(\\S)+(\\s)*)(}}\")", " {{$3}}");
      String delimiter = krylov.get(DELIMITER) == null ? "," : krylov.get(DELIMITER).toString();
      LinkedHashMap<String, ArrayList<HashMap<String, Object>>> modelRenders =
          this.krylovTransformRenderFormat(context, krylov.get(RENDER), delimiter);
      return this.krylovTransformRenderTasks(context, settingsTemplate, modelRenders);
    }
  }

  private LinkedHashMap<String, Object> krylovTaskInstanceFormat(String taskId, JsonNode settings) {
    LinkedHashMap<String, Object> taskInstance = new LinkedHashMap<>();
    taskInstance.put(DISABLE, JsonNodeFactory.instance.booleanNode(false));
    taskInstance.put(TASK_TYPE, "krylov_retrain");
    Map<String, JsonNode> configs = this.krylovTransformSettingsFormat(settings);
    if (Optional.ofNullable(configs.get(SETTINGS)).isPresent()) {
      JsonNode settingsNode = configs.get(SETTINGS);
      ((ObjectNode) settingsNode).replace(EXPERIMENT_NAME,
          JsonNodeFactory.instance.textNode(taskId));
      taskInstance.put(SETTINGS, settingsNode);
    } else {
      taskInstance.put(SETTINGS, JsonNodeFactory.instance.objectNode());
    }
    if (configs.get(ARGUMENTS) != null) {
      taskInstance.put(ARGUMENTS, configs.get(ARGUMENTS));
    }
    taskInstance.put(DEPENDS_ON, new ArrayList<>());
    return taskInstance;
  }

  private Map<String, JsonNode> krylovTransformSettingsFormat(
      JsonNode settingsNode) {
    HashMap<String, JsonNode> configs   = new HashMap<>();
    JsonNode                  settings  = settingsNode.deepCopy();
    JsonNode                  arguments = settings.get(SETTINGS_EMS_PARAMS);
    if (arguments != null && isNotNullJsonNode(arguments)) {
      configs.put(ARGUMENTS, arguments);
    } else {
      configs.put(ARGUMENTS, null);
    }
    ((ObjectNode) settings).remove(SETTINGS_EMS_PARAMS);
    if (isNotNullJsonNode(settings)) {
      configs.put(SETTINGS, settings);
    } else {
      configs.put(SETTINGS, null);
    }
    return configs;
  }

  private LinkedHashMap<String, ArrayList<HashMap<String, Object>>> krylovTransformRenderFormat(
      TransferContext context, JsonNode renderNode, String delimiter)
      throws Exception {
    LinkedHashMap<String, ArrayList<HashMap<String, Object>>> modelRenders = new LinkedHashMap<>();
    for (Map.Entry<String, ArrayList<String>> entry :
        context.getTransferModel().getModel2instance().entrySet()) {
      for (String modelName : entry.getValue()) {
        if (renderNode.get(modelName) == null) {
          throw new TransformerException("task render did not match task instance name");
        }
        ArrayList<HashMap<String, Object>> renderParamsList =
            TransferRenderUtil.render(renderNode.get(modelName), delimiter);
        modelRenders.put(modelName, renderParamsList);
      }
    }
    return modelRenders;
  }

  private LinkedHashMap<String, Object> krylovTransformRenderTasks(
      TransferContext context,
      String settingsTemplate,
      LinkedHashMap<String, ArrayList<HashMap<String, Object>>> modelRenders)
      throws JsonProcessingException {
    LinkedHashMap<String, Object> taskList = new LinkedHashMap<>();
    for (Map.Entry<String, ArrayList<HashMap<String, Object>>>
        modelRender : modelRenders.entrySet()) {
      String                             taskId          = modelRender.getKey();
      ArrayList<HashMap<String, Object>> modelRenderList = modelRender.getValue();
      int                                renderCount     = 0;
      boolean                            flag            = modelRenderList.size() == 1;
      for (HashMap<String, Object> modelRenderMap : modelRenderList) {
        ObjectNode ammo = JsonNodeFactory.instance.objectNode();
        ammo.replace(PROJECT_NAME, JsonNodeFactory.instance.textNode(
            context.getTransferDagParams(PROJECT_NAME).toString()));
        ammo.replace(MODEL_NAME, JsonNodeFactory.instance.textNode(
            context.getTransferModel().getInstance2model().get(taskId)));
        ammo.replace(MODEL_INSTANCE,
            JsonNodeFactory.instance.textNode(taskId));
        ammo.replace(TRY_ID,
            JsonNodeFactory.instance.numberNode(renderCount++));
        String settingsRender = TransferDevUtil.jinjava.render(settingsTemplate, modelRenderMap);
        String taskIdGenerate = flag ? taskId + krylovRetrainSuffix
            : taskId + "_tryid_" + renderCount + krylovRetrainSuffix;
        LinkedHashMap<String, Object> taskInstance = this.krylovTaskInstanceFormat(taskIdGenerate,
            TransferDevUtil.yamlMapper.readTree(settingsRender));
        ((ObjectNode) taskInstance.get(SETTINGS)).replace(AMMO, ammo);
        taskList.put(taskIdGenerate, taskInstance);
      }
    }
    return taskList;
  }

  private boolean isNotNullJsonNode(JsonNode jsonNode) {
    long count = StreamSupport.stream(
        Spliterators.spliteratorUnknownSize(jsonNode.fieldNames(), Spliterator.ORDERED),
        false).count();
    return count != 0;
  }

}

package cn.scut.app.util.transfer.navigator.impl;

import static cn.scut.app.util.transfer.constant.TransferConfigs.START;
import static cn.scut.app.util.transfer.constant.TrainStageFields.EXPERIMENT;
import static cn.scut.app.util.transfer.constant.TrainStageFields.MODEL_SELECTION;
import static cn.scut.app.util.transfer.constant.TrainStageFields.TYPE;

import cn.scut.app.util.transfer.context.TransferContext;
import cn.scut.app.util.transfer.entity.output.layer.impl.TransferGroup;
import cn.scut.app.util.transfer.entity.output.layer.impl.TransferStage;
import cn.scut.app.util.transfer.navigator.Navigator;
import cn.scut.app.util.transfer.transformer.Transformer;
import cn.scut.app.util.transfer.utils.TransferDevUtil;
import com.fasterxml.jackson.databind.JsonNode;
import io.vavr.control.Either;
import io.vavr.control.Try;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TrainNavigator implements Navigator {

  private final Map<String, List<String>>    layerOrder = new HashMap<>();
  private       HashMap<String, Transformer> strategyMap;

  public TrainNavigator() {
    strategyMap = new HashMap<>();
  }

  @Override
  public void setNavigatorStrategies(HashMap<String, Transformer> strategies) {
    strategyMap = new HashMap<>(strategies);
  }

  @Override
  public Either<Throwable, TransferStage> navigate(HashMap<String, Object> root,
      TransferContext context) {
    return Try.of(() -> this.trainTransform(root, context)).toEither();
  }

  private TransferStage trainTransform(
      HashMap<String, Object> root, TransferContext context) {
    return Try.of(() -> {
      JsonNode train = TransferDevUtil.yamlMapper.readTree(
          TransferDevUtil.yamlMapper.writeValueAsString(
              root.get(context.getTransferConfigs().getLevelOneField("train"))));
      String type =
          train.has(TYPE) ? train.get(TYPE).asText() : "unknown";
      Transformer         strategy       = this.strategyMap.get(type);
      TransferGroup       trainGroup     = strategy.transform(root, context);
      List<TransferGroup> trainGroupList = new ArrayList<>();
      trainGroupList.add(trainGroup);
      if (root.containsKey(MODEL_SELECTION)) {
        TransferGroup selectGroup = selectionTransform(root, context);
        trainGroupList.add(selectGroup);
        layerOrder.put(trainGroup.getLayerId(),
            Collections.singletonList(selectGroup.getLayerId()));
      }
      TransferStage adapterStage = new TransferStage(
          context.getTransferConfigs().getLevelOneField("train"), trainGroupList, layerOrder);
      adapterStage.createRangeTask(
          context.getTransferConfigs().getTransferStagesTasks().get("train").get(START)
              .split("_")[0]);
      return adapterStage;
    }).get();
  }

  private TransferGroup selectionTransform(
      HashMap<String, Object> root, TransferContext context) {
    Transformer strategy = this.strategyMap.get(EXPERIMENT);
    return strategy.transform(root, context);
  }
}

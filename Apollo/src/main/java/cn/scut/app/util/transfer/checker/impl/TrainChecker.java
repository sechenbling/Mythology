package cn.scut.app.util.transfer.checker.impl;

import static cn.scut.app.util.transfer.constant.TrainStageFields.CONDITION;
import static cn.scut.app.util.transfer.constant.TrainStageFields.MODEL_SELECTION;
import static cn.scut.app.util.transfer.constant.TrainStageFields.SETTINGS;
import static cn.scut.app.util.transfer.constant.TrainStageFields.SORT;
import static cn.scut.app.util.transfer.constant.TrainStageFields.TYPE;

import cn.scut.app.util.transfer.checker.Checker;
import cn.scut.app.util.transfer.context.TransferContext;
import cn.scut.app.util.transfer.utils.TransferDevUtil;
import com.fasterxml.jackson.databind.JsonNode;
import io.vavr.collection.List;
import io.vavr.control.Either;
import io.vavr.control.Try;
import java.util.HashMap;

public class TrainChecker implements Checker {

  @Override
  public Either<Throwable, Boolean> check(HashMap<String, Object> root, TransferContext context) {
    return Try.of(() -> {
      JsonNode rootNode = TransferDevUtil.yamlMapper.readTree(
          TransferDevUtil.yamlMapper.writeValueAsString(root));
      return trainCheck(rootNode, context) && selectCheck(rootNode);
    }).toEither();
  }

  private boolean trainCheck(JsonNode root, TransferContext context) throws NoSuchFieldException {
    JsonNode train = root.get(context.getTransferConfigs().getLevelOneField("train"));
    return this.trainFieldsCheck(train, List.of(TYPE, SETTINGS));
  }

  private boolean trainFieldsCheck(JsonNode task, List<String> fields)
      throws NoSuchFieldException {
    for (String field : fields) {
      if (!task.has(field)) {
        throw new NoSuchFieldException(
            "Yaml 'train' task field has no key: " + field);
      }
    }
    return true;
  }

  private boolean selectCheck(JsonNode root) throws NoSuchFieldException {
    if (!root.has(MODEL_SELECTION)) {
      return true;
    }
    JsonNode select = root.get(MODEL_SELECTION);
    return selectFieldsCheck(select, List.of(CONDITION, SORT));
  }


  private boolean selectFieldsCheck(JsonNode task, List<String> fields)
      throws NoSuchFieldException {
    for (String field : fields) {
      if (!task.has(field)) {
        throw new NoSuchFieldException(
            "Yaml 'model_selection' has no key: " + field);
      }
    }
    return true;
  }
}

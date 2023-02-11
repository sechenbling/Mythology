package cn.scut.app.util.transfer.checker.impl;

import static cn.scut.app.util.transfer.constant.EvalStageFields.LEVEL;
import static cn.scut.app.util.transfer.constant.EvalStageFields.METRICS;
import static cn.scut.app.util.transfer.constant.EvalStageFields.SOURCE;
import static cn.scut.app.util.transfer.constant.EvalStageFields.SOURCE_CUSTOMIZED;
import static cn.scut.app.util.transfer.constant.EvalStageFields.SOURCE_DAG;
import static cn.scut.app.util.transfer.constant.EvalStageFields.SOURCE_MODEL;
import static cn.scut.app.util.transfer.constant.EvalStageFields.SOURCE_TRAIN;
import static cn.scut.app.util.transfer.constant.EvalStageFields.TASKS;
import static io.vavr.API.$;
import static io.vavr.API.Case;
import static io.vavr.API.Match;

import cn.scut.app.util.transfer.checker.Checker;
import cn.scut.app.util.transfer.context.TransferContext;
import cn.scut.app.util.transfer.utils.TransferDevUtil;
import com.fasterxml.jackson.databind.JsonNode;
import io.vavr.collection.List;
import io.vavr.control.Either;
import io.vavr.control.Try;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Optional;

public class EvaluationChecker implements Checker {

  @Override
  public Either<Throwable, Boolean> check(HashMap<String, Object> root, TransferContext context) {
    return Try.of(() -> {
      JsonNode rootNode = TransferDevUtil.yamlMapper.readTree(
          TransferDevUtil.yamlMapper.writeValueAsString(root));
      return evalCheck(rootNode, context);
    }).toEither();
  }

  private boolean evalCheck(JsonNode root, TransferContext context)
      throws NoSuchFieldException {
    JsonNode eval = root.get(
        context.getTransferConfigs().getLevelOneField("evaluation"));
    Iterator<String> taskNamesIter = eval.fieldNames();
    while (taskNamesIter.hasNext()) {
      String  taskName = taskNamesIter.next();
      boolean taskFlag = this.evalCheckForEach(eval.get(taskName));
      if (!taskFlag) {
        return false;
      }
    }
    return true;
  }

  private boolean evalCheckForEach(JsonNode task)
      throws NoSuchFieldException {
    if (!task.has(SOURCE)) {
      throw new NoSuchFieldException("Yaml 'evaluation' task field has no key: 'source'");
    }
    String source = task.get("source").asText();
    Either<Throwable, Boolean> res = Match(source)
        .of(
            Case($(SOURCE_TRAIN), () -> Try.of(() -> this.evalCheckForTrain(task)).toEither()),
            Case($(SOURCE_CUSTOMIZED),
                () -> Try.of(() -> this.evalCheckForCustomized(task)).toEither()),
            Case($(SOURCE_DAG), () -> Try.of(() -> this.evalCheckForDagOrModel(task)).toEither()),
            Case($(SOURCE_MODEL), () -> Try.of(() -> this.evalCheckForDagOrModel(task)).toEither()),
            Case($(), () -> Either.right(null))
        );
    return Optional.ofNullable(
            res.getOrElseThrow(() -> new NoSuchFieldException(res.getLeft().getMessage())))
        .orElseThrow(() -> new NoSuchFieldException(
            "Yaml 'evaluation' task field has no 'source' type: " + source));
  }

  private boolean evalCheckForTrain(JsonNode task) throws NoSuchFieldException {
    return this.evalFieldsCheck(task, "train",
        List.of(METRICS));
  }

  private boolean evalCheckForCustomized(JsonNode task) throws NoSuchFieldException {
    return this.evalFieldsCheck(task, "customized",
        List.of(TASKS, LEVEL));
  }

  private boolean evalCheckForDagOrModel(JsonNode task) throws NoSuchFieldException {
    return this.evalFieldsCheck(task, task.get("source").asText(),
        List.of(METRICS));
  }

  private boolean evalFieldsCheck(JsonNode task, String taskType, List<String> fields)
      throws NoSuchFieldException {
    for (String field : fields) {
      if (!task.has(field)) {
        throw new NoSuchFieldException(
            "Yaml 'evaluation' task field for" + taskType + " has no key: " + field);
      }
    }
    return true;
  }
}

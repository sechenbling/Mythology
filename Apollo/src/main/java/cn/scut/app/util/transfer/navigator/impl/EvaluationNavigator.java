package cn.scut.app.util.transfer.navigator.impl;

import static cn.scut.app.util.transfer.constant.TransferConfigs.START;
import static cn.scut.app.util.transfer.constant.EvalStageFields.GROUP_TRAIN;
import static cn.scut.app.util.transfer.constant.EvalStageFields.LEVEL;
import static cn.scut.app.util.transfer.constant.EvalStageFields.SOURCE;
import static cn.scut.app.util.transfer.constant.EvalStageFields.SOURCE_CUSTOMIZED;
import static cn.scut.app.util.transfer.constant.EvalStageFields.SOURCE_DAG;
import static cn.scut.app.util.transfer.constant.EvalStageFields.SOURCE_MODEL;
import static cn.scut.app.util.transfer.constant.EvalStageFields.SOURCE_TRAIN;
import static io.vavr.API.$;
import static io.vavr.API.Case;
import static io.vavr.API.Match;
import static io.vavr.API.run;

import cn.scut.app.util.transfer.context.TransferContext;
import cn.scut.app.util.transfer.entity.output.layer.impl.TransferGroup;
import cn.scut.app.util.transfer.entity.output.layer.impl.TransferStage;
import cn.scut.app.util.transfer.entity.output.layer.impl.TransferTask;
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
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

public class EvaluationNavigator implements Navigator {

  private final Map<String, List<String>>    layerOrder = new HashMap<>();
  private       HashMap<String, Transformer> strategyMap;

  @Override
  public void setNavigatorStrategies(HashMap<String, Transformer> strategies) {
    strategyMap = new HashMap<>(strategies);
  }

  @Override
  public Either<Throwable, TransferStage> navigate(HashMap<String, Object> root,
      TransferContext context) {
    return Try.of(() -> this.evaluationTransform(root, context)).toEither();
  }

  private TransferStage evaluationTransform(
      HashMap<String, Object> root, TransferContext context) throws Exception {
    Object evaluation = root.get(context.getTransferConfigs()
        .getLevelOneField("evaluation"));
    JsonNode evaluationJsonNode = TransferDevUtil.yamlMapper.readTree(
        TransferDevUtil.yamlMapper.writeValueAsString(evaluation));
    ArrayList<JsonNode> train      = new ArrayList<>();
    ArrayList<JsonNode> customized = new ArrayList<>();
    ArrayList<JsonNode> dagEval    = new ArrayList<>();
    ArrayList<JsonNode> modelEval  = new ArrayList<>();
    StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(evaluationJsonNode.fieldNames(), Spliterator.ORDERED),
            false)
        .forEach(evalTaskName -> {
          JsonNode evalTaskNode = evaluationJsonNode.get(evalTaskName);
          String source =
              evalTaskNode.has(SOURCE) ? evalTaskNode.get(SOURCE).asText() : "unknown";
          Match(source).of(
              Case($(SOURCE_TRAIN), o -> run(() -> train.add(evalTaskNode))),
              Case($(SOURCE_CUSTOMIZED), o -> run(() -> customized.add(evalTaskNode))),
              Case($(SOURCE_DAG), o -> run(() -> dagEval.add(evalTaskNode))),
              Case($(SOURCE_MODEL), o -> run(() -> modelEval.add(evalTaskNode)))
          );
        });
    TransferGroup trainEvalGroup = this.evalTransformPartTrain(train, context);
    List<TransferGroup> cusEvalGroupList = this.evalTransformPartCus(
        customized, dagEval, modelEval, context);
    // TODO: customized connect order, now is train->[model,dag]->[model_eval,dag_eval]
    return this.evalTransferGroupCombine(context, trainEvalGroup, cusEvalGroupList);
  }

  private TransferGroup evalTransformPartTrain(ArrayList<JsonNode> train,
      TransferContext context) {
    List<TransferTask> taskList = new ArrayList<>();
    train.forEach(trainTaskNode -> {
      List<TransferTask> trainTasks =
          this.evalTransformForTrain(trainTaskNode, context);
      taskList.addAll(trainTasks);
    });
    return new TransferGroup(GROUP_TRAIN, taskList);
  }

  private List<TransferTask> evalTransformForTrain(JsonNode eval,
      TransferContext context) {
    Transformer             strategy = this.strategyMap.get("train");
    HashMap<String, Object> root     = TransferDevUtil.yaml.load(eval.toString());
    return strategy.transform(root, context).getAllTasks();
  }

  private List<TransferGroup> evalTransformPartCus(
      ArrayList<JsonNode> customized,
      ArrayList<JsonNode> dagEval,
      ArrayList<JsonNode> modelEval,
      TransferContext context) {
    List<TransferTask> cusModelTrainTaskList = new ArrayList<>();
    JsonNode dagEvalTaskNode = dagEval.size() >= 1
        ? dagEval.get(dagEval.size() - 1) : null;
    JsonNode modelEvalTaskNode = modelEval.size() >= 1
        ? modelEval.get(modelEval.size() - 1) : null;
    List<TransferGroup> adapterGroupList = new ArrayList<>();
    customized.forEach(cusTaskNode -> {
      TransferGroup cusTasksGroup = this.evalTransformForCustomized(cusTaskNode, context);
      String        level         = cusTaskNode.get(LEVEL).asText();
      if (level.equals(SOURCE_DAG)) {
        adapterGroupList.add(cusTasksGroup);
        if (Optional.ofNullable(dagEvalTaskNode).isPresent()) {
          TransferGroup cusTasksEvalGroup = this.evalTransformForDagCustomized(
              cusTasksGroup, dagEvalTaskNode, context);
          adapterGroupList.add(cusTasksEvalGroup);
          this.layerOrder.put(cusTasksGroup.getLayerId(),
              Collections.singletonList(cusTasksEvalGroup.getLayerId()));
        }
      } else if (level.equals(SOURCE_MODEL)) {
        cusModelTrainTaskList.addAll(cusTasksGroup.getAllTasks());
        if (Optional.ofNullable(modelEvalTaskNode).isPresent()) {
          cusModelTrainTaskList.addAll(
              this.evalTransformForModelCustomized(cusTasksGroup, modelEvalTaskNode, context)
                  .getAllTasks());
        }
      } else {
        throw new NoSuchElementException("Yaml 'evaluation' task field has no 'level': " + level);
      }
    });
    return adapterGroupList;
  }

  private TransferGroup evalTransformForCustomized(JsonNode eval,
      TransferContext context) {
    Transformer             strategy = this.strategyMap.get("customized");
    HashMap<String, Object> root     = TransferDevUtil.yaml.load(eval.toString());
    return strategy.transform(root, context);
  }

  private TransferGroup evalTransformForDagCustomized(
      TransferGroup cusTasksGroup, JsonNode dagEvalTaskNode,
      TransferContext context) {
    context.setDagEvalTasksGroup(cusTasksGroup);
    return this.evalTransformForTypeCustomized(
        dagEvalTaskNode, context, SOURCE_DAG);

  }

  private TransferGroup evalTransformForModelCustomized(
      TransferGroup cusTasksGroup, JsonNode modelEvalTaskNode,
      TransferContext context) {
    context.setModelEvalTasks(cusTasksGroup);
    return this.evalTransformForTypeCustomized(
        modelEvalTaskNode, context, SOURCE_MODEL);
  }

  private TransferGroup evalTransformForTypeCustomized(
      JsonNode evalTaskNode, TransferContext context, String sourceType) {
    Transformer transformer = this.strategyMap.get(sourceType);
    HashMap<String, Object> root = TransferDevUtil.yaml.load(
        evalTaskNode.toString());
    return transformer.transform(root, context);
  }

  private TransferStage evalTransferGroupCombine(
      TransferContext context,
      TransferGroup trainGroup,
      List<TransferGroup> cusGroupList
  ) {
    List<TransferGroup> evalGroupList = new ArrayList<>();
    evalGroupList.add(trainGroup);
    evalGroupList.addAll(cusGroupList);
    TransferStage adapterStage = new TransferStage(
        context.getTransferConfigs().getLevelOneField("evaluation"),
        evalGroupList, layerOrder);
    adapterStage.createRangeTask(
        context.getTransferConfigs().getTransferStagesTasks().get("evaluation").get(START)
            .split("_")[0]);
    return adapterStage;
  }
}

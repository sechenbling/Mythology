package cn.scut.app.util.transfer.navigator.impl;

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
import java.util.HashMap;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.logging.Logger;
import java.util.stream.StreamSupport;

public class PostNavigator implements Navigator {

  private HashMap<String, Transformer> strategyMap;

  public PostNavigator() {
    strategyMap = new HashMap<>();
  }

  @Override
  public void setNavigatorStrategies(HashMap<String, Transformer> strategies) {
    strategyMap = new HashMap<>(strategies);
  }

  @Override
  public Either<Throwable, TransferStage> navigate(HashMap<String, Object> root,
      TransferContext context) {
    return Try.of(() -> this.postTransform(root, context)).toEither();
  }

  private TransferStage postTransform(HashMap<String, Object> root,
      TransferContext context)
      throws Exception {
    List<TransferGroup> adapterGroupList = new ArrayList<>();
    Object postAnalysis = root.get(context.getTransferConfigs()
        .getLevelOneField("post"));
    JsonNode postNode = TransferDevUtil.yamlMapper.readTree(
        TransferDevUtil.yamlMapper.writeValueAsString(postAnalysis));
    StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(postNode.fieldNames(), Spliterator.ORDERED), false)
        .forEach(postMetricsName -> {
          TransferGroup adapterGroup = this.postTransformForPost(postNode.get(postMetricsName),
              context);
          adapterGroupList.add(adapterGroup);
          Logger.getGlobal().info(adapterGroup.getLayerId());
        });
    return new TransferStage(context.getTransferConfigs().getLevelOneField("post"), adapterGroupList);
  }

  private TransferGroup postTransformForPost(JsonNode post,
      TransferContext context) {
    String source = post.has("source") ? post.get("source").asText() : "unknown";
    //TaskContext context = new TaskContext("post", source);
    HashMap<String, Object> root     = TransferDevUtil.yaml.load(post.toString());
    Transformer             strategy = this.strategyMap.get(source);
    return strategy.transform(root, context);
  }
}

package cn.scut.app.util.transfer.transformer.meta;

import static cn.scut.app.util.transfer.constant.MetaStageFields.DAGS_DAG_ID;

import cn.scut.app.util.transfer.context.TransferContext;
import cn.scut.app.util.transfer.entity.output.TransferParams;
import cn.scut.app.util.transfer.utils.TransferDevUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.vavr.control.Try;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

public class MetaTransformer {

  public TransferParams transform(HashMap<String, Object> root,
      TransferContext context) {
    return Try.of(() -> new TransferParams(this.metaTransform(context, root))).get();
  }

  private LinkedHashMap<String, Object> metaTransform(TransferContext context,
      HashMap<String, Object> root)
      throws Exception {
    return this.metaTransformForMetadata(context, root.get("metadata"));
  }

  private LinkedHashMap<String, Object> metaTransformForMetadata(TransferContext context,
      Object metadata)
      throws Exception {
    JsonNode metadataNode = TransferDevUtil.yamlMapper.readTree(
        TransferDevUtil.yamlMapper.writeValueAsString(metadata));
    String projectName = metadataNode.get("project").asText();
    context.setTransferDagParams("project_name", projectName);
    LinkedHashMap<String, Object> transform = new LinkedHashMap<>();
    transform.put(DAGS_DAG_ID, projectName);
    JsonNode models = metadataNode.get("models");
    this.metaTransformForMetadataModelInstance(context, models);
    return transform;
  }

  private void metaTransformForMetadataModelInstance(TransferContext context, JsonNode models)
      throws JsonProcessingException {
    for (JsonNode model : models) {
      JsonNode         elemNode              = TransferDevUtil.yamlMapper.readTree(model.toString());
      Iterator<String> modelInstanceNameIter = elemNode.fieldNames();
      StreamSupport.stream(
              Spliterators.spliteratorUnknownSize(modelInstanceNameIter, Spliterator.ORDERED),
              false)
          .forEach(modelInstanceName -> {
            JsonNode          modelTaskNameIter = elemNode.get(modelInstanceName);
            ArrayList<String> modelTaskList     = new ArrayList<>();
            for (JsonNode modelTaskName : modelTaskNameIter) {
              String modelTask = modelTaskName.asText();
              modelTaskList.add(modelTask);
              context.getTransferModel().getInstance2model().put(modelTask, modelInstanceName);
            }
            context.getTransferModel().getModel2instance().put(modelInstanceName, modelTaskList);
          });
    }
  }
}

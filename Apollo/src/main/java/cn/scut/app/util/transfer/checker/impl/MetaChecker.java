package cn.scut.app.util.transfer.checker.impl;

import static cn.scut.app.util.transfer.constant.MetaStageFields.DADS_DOMAIN;
import static cn.scut.app.util.transfer.constant.MetaStageFields.DAGS_EMAIL;
import static cn.scut.app.util.transfer.constant.MetaStageFields.DAGS_OWNER;
import static cn.scut.app.util.transfer.constant.MetaStageFields.DAGS_SCHEDULER;
import static cn.scut.app.util.transfer.constant.MetaStageFields.DAGS_START_DATE;
import static cn.scut.app.util.transfer.constant.MetaStageFields.DAGS_TAGS;
import static cn.scut.app.util.transfer.constant.MetaStageFields.DAGS_VERSION;
import static cn.scut.app.util.transfer.constant.MetaStageFields.METADATA;
import static cn.scut.app.util.transfer.constant.MetaStageFields.METADATA_MODELS;
import static cn.scut.app.util.transfer.constant.MetaStageFields.METADATA_PROJECT;

import cn.scut.app.util.transfer.checker.Checker;
import cn.scut.app.util.transfer.context.TransferContext;
import cn.scut.app.util.transfer.utils.TransferDevUtil;
import com.fasterxml.jackson.databind.JsonNode;
import io.vavr.collection.List;
import io.vavr.control.Either;
import io.vavr.control.Try;
import java.util.HashMap;

public class MetaChecker implements Checker {

  @Override
  public Either<Throwable, Boolean> check(HashMap<String, Object> root, TransferContext context) {
    return Try.of(() -> {
      JsonNode rootNode = TransferDevUtil.yamlMapper.readTree(
          TransferDevUtil.yamlMapper.writeValueAsString(root));
      return metaCheck(rootNode) && dagParamsCheck(rootNode);
    }).toEither();
  }

  private boolean metaCheck(JsonNode root) throws NoSuchFieldException {
    if (!root.has(METADATA)) {
      throw new NoSuchFieldException("Yaml file has no key: '" + METADATA + "'");
    }
    JsonNode metadata = root.get(METADATA);
    return this.metaDataFieldsCheck(metadata, List.of(METADATA_PROJECT, METADATA_MODELS));
  }

  private boolean dagParamsCheck(JsonNode root) throws NoSuchFieldException {
    return this.metaDagParamsFieldsCheck(root,
        List.of(
            DAGS_SCHEDULER, DAGS_OWNER, DAGS_EMAIL,
            DAGS_TAGS, DAGS_VERSION, DAGS_START_DATE, DADS_DOMAIN));
  }

  private boolean metaDataFieldsCheck(JsonNode root, List<String> fields)
      throws NoSuchFieldException {
    for (String field : fields) {
      if (!root.has(field)) {
        throw new NoSuchFieldException(
            "Yaml field 'metadata' has no key: " + field);
      }
    }
    return true;
  }

  private boolean metaDagParamsFieldsCheck(JsonNode root, List<String> fields)
      throws NoSuchFieldException {
    for (String field : fields) {
      if (!root.has(field)) {
        throw new NoSuchFieldException(
            "Yaml file loss 'dag params' key: " + field);
      }
    }
    return true;
  }
}

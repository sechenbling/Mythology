package cn.scut.app.util.transfer.transformer.meta;

import static cn.scut.app.util.transfer.constant.TransferCallbackFields.DAG_ON_FAILURE;
import static cn.scut.app.util.transfer.constant.TransferCallbackFields.DAG_ON_FAILURE_CALLBACK;
import static cn.scut.app.util.transfer.constant.TransferCallbackFields.DAG_ON_SUCCESS;
import static cn.scut.app.util.transfer.constant.TransferCallbackFields.DAG_ON_SUCCESS_CALLBACK;
import static cn.scut.app.util.transfer.constant.TransferCallbackFields.END_ON_FAILURE;
import static cn.scut.app.util.transfer.constant.TransferCallbackFields.END_ON_FAILURE_CALLBACK;
import static cn.scut.app.util.transfer.constant.TransferCallbackFields.END_ON_RETRY;
import static cn.scut.app.util.transfer.constant.TransferCallbackFields.END_ON_RETRY_CALLBACK;
import static cn.scut.app.util.transfer.constant.TransferCallbackFields.END_ON_SUCCESS;
import static cn.scut.app.util.transfer.constant.TransferCallbackFields.END_ON_SUCCESS_CALLBACK;
import static cn.scut.app.util.transfer.constant.TransferCallbackFields.START_ON_FAILURE;
import static cn.scut.app.util.transfer.constant.TransferCallbackFields.START_ON_FAILURE_CALLBACK;
import static cn.scut.app.util.transfer.constant.TransferCallbackFields.START_ON_RETRY;
import static cn.scut.app.util.transfer.constant.TransferCallbackFields.START_ON_RETRY_CALLBACK;
import static cn.scut.app.util.transfer.constant.TransferCallbackFields.START_ON_SUCCESS;
import static cn.scut.app.util.transfer.constant.TransferCallbackFields.START_ON_SUCCESS_CALLBACK;
import static cn.scut.app.util.transfer.constant.MetaStageFields.DADS_DOMAIN;
import static cn.scut.app.util.transfer.constant.MetaStageFields.DAGS_AMMO_PARAMS;
import static cn.scut.app.util.transfer.constant.MetaStageFields.DAGS_AMMO_PARAMS_PROJECT_NAME;
import static cn.scut.app.util.transfer.constant.MetaStageFields.DAGS_CALLBACK;
import static cn.scut.app.util.transfer.constant.MetaStageFields.DAGS_EMAIL;
import static cn.scut.app.util.transfer.constant.MetaStageFields.DAGS_OWNER;
import static cn.scut.app.util.transfer.constant.MetaStageFields.DAGS_PARAMS;
import static cn.scut.app.util.transfer.constant.MetaStageFields.DAGS_SCHEDULER;
import static cn.scut.app.util.transfer.constant.MetaStageFields.DAGS_START_DATE;
import static cn.scut.app.util.transfer.constant.MetaStageFields.DAGS_TAGS;
import static cn.scut.app.util.transfer.constant.MetaStageFields.DAGS_VERSION;

import cn.scut.app.util.transfer.context.TransferContext;
import cn.scut.app.util.transfer.entity.output.TransferParams;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.vavr.control.Try;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class DagParamsTransformer {

  public TransferParams transform(HashMap<String, Object> root,
      TransferContext context) {
    return Try.of(() -> new TransferParams(dagParamsTransform(root, context))).get();
  }

  private LinkedHashMap<String, Object> dagParamsTransform(HashMap<String, Object> root,
      TransferContext context) {
    LinkedHashMap<String, Object> transform = new LinkedHashMap<>();
    transform.put(DAGS_SCHEDULER, root.getOrDefault(DAGS_SCHEDULER, "null"));
    context.setTransferDagParams(DAGS_SCHEDULER, transform.get(DAGS_SCHEDULER));
    transform.put(DAGS_OWNER, root.getOrDefault(DAGS_OWNER, "None"));
    context.setTransferDagParams(DAGS_OWNER, transform.get(DAGS_OWNER));
    ArrayList<String> tags = new ArrayList<>();
    tags.add("temp");
    transform.put(DAGS_TAGS, root.getOrDefault(DAGS_TAGS, tags));
    context.setTransferDagParams(DAGS_TAGS, transform.get(DAGS_TAGS));
    transform.put(DAGS_EMAIL, root.get(DAGS_EMAIL));
    transform.put(DAGS_VERSION, root.getOrDefault(DAGS_VERSION, "0.0.0"));
    transform.put(DAGS_START_DATE, "${sys.datetime(2023, 1, 1)}");
    transform.put(DAGS_CALLBACK, this.dagParamsCallbackTransform());
    transform.put(DAGS_AMMO_PARAMS, this.dagParamsAmmoParamsTransform(
        context.getTransferDagParams(DAGS_AMMO_PARAMS_PROJECT_NAME).toString()));
    transform.put(DADS_DOMAIN, root.getOrDefault(DADS_DOMAIN, "temp"));
    context.setTransferDagParams(DAGS_EMAIL, transform.get("email"));
    context.setTransferDagParams(DAGS_VERSION, transform.get("version"));
    if (root.containsKey(DAGS_PARAMS)) {
      transform.put(DAGS_PARAMS, root.get(DAGS_PARAMS));
    }
    return transform;
  }

  private ObjectNode dagParamsCallbackTransform() {
    ObjectNode callback = JsonNodeFactory.instance.objectNode();
    callback.put(START_ON_SUCCESS, START_ON_SUCCESS_CALLBACK);
    callback.put(START_ON_FAILURE, START_ON_FAILURE_CALLBACK);
    callback.put(START_ON_RETRY, START_ON_RETRY_CALLBACK);
    callback.put(END_ON_SUCCESS, END_ON_SUCCESS_CALLBACK);
    callback.put(END_ON_FAILURE, END_ON_FAILURE_CALLBACK);
    callback.put(END_ON_RETRY, END_ON_RETRY_CALLBACK);
    callback.put(DAG_ON_SUCCESS, DAG_ON_SUCCESS_CALLBACK);
    callback.put(DAG_ON_FAILURE, DAG_ON_FAILURE_CALLBACK);
    return callback;
  }

  private ObjectNode dagParamsAmmoParamsTransform(String projectName) {
    ObjectNode ammoParams = JsonNodeFactory.instance.objectNode();
    ammoParams.replace(DAGS_AMMO_PARAMS_PROJECT_NAME,
        JsonNodeFactory.instance.textNode(projectName));
    return ammoParams;
  }
}

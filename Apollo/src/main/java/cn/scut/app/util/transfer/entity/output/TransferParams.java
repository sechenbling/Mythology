package cn.scut.app.util.transfer.entity.output;

import static cn.scut.app.util.transfer.constant.MetaStageFields.DADS_DOMAIN;
import static cn.scut.app.util.transfer.constant.MetaStageFields.DAGS_CALLBACK;
import static cn.scut.app.util.transfer.constant.MetaStageFields.DAGS_DAG_ID;
import static cn.scut.app.util.transfer.constant.MetaStageFields.DAGS_EMAIL;
import static cn.scut.app.util.transfer.constant.MetaStageFields.DAGS_OWNER;
import static cn.scut.app.util.transfer.constant.MetaStageFields.DAGS_PARAMS;
import static cn.scut.app.util.transfer.constant.MetaStageFields.DAGS_SCHEDULER;
import static cn.scut.app.util.transfer.constant.MetaStageFields.DAGS_START_DATE;
import static cn.scut.app.util.transfer.constant.MetaStageFields.DAGS_TAGS;
import static cn.scut.app.util.transfer.constant.MetaStageFields.DAGS_VERSION;

import cn.scut.app.util.transfer.utils.TransferDevUtil;
import com.fasterxml.jackson.databind.JsonNode;
import io.vavr.control.Try;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

public class TransferParams {

  private final LinkedHashMap<String, Object> tasksParamsMap;
  private       String                        dagId;
  private       String                        owner;
  private       String                        email;
  private       String                        scheduler;
  private       Map<String, Object>           parameters;
  private       List<String>                  tags;
  private       String                        version;
  private       String                        startDate;
  private       Map<String, Object>           callback;
  private       String                        domain;

  public TransferParams(LinkedHashMap<String, Object> tasksParamsMap) {
    this.tasksParamsMap = tasksParamsMap;
    this.dagId          = (String) tasksParamsMap.get(DAGS_DAG_ID);
    this.owner          = (String) tasksParamsMap.get(DAGS_OWNER);
    this.email          = (String) tasksParamsMap.get(DAGS_EMAIL);
    this.scheduler      = (String) tasksParamsMap.get(DAGS_SCHEDULER);
    this.parameters     = this.transferObjectToMap(tasksParamsMap.get(DAGS_PARAMS));
    this.tags           = (List<String>) tasksParamsMap.get(DAGS_TAGS);
    this.version        = (String) tasksParamsMap.get(DAGS_VERSION);
    this.startDate      = (String) tasksParamsMap.get(DAGS_START_DATE);
    this.callback       = this.transferObjectToMap(tasksParamsMap.get(DAGS_CALLBACK));
    this.domain         = (String) tasksParamsMap.get(DADS_DOMAIN);
  }

  public LinkedHashMap<String, Object> getTasksParamsMap() {
    return tasksParamsMap;
  }

  public void addTasksParams(Map<String, Object> paramsMap) {
    this.tasksParamsMap.putAll(paramsMap);
    this.dagId      = (String) tasksParamsMap.get(DAGS_DAG_ID);
    this.owner      = (String) tasksParamsMap.get(DAGS_OWNER);
    this.email      = (String) tasksParamsMap.get(DAGS_EMAIL);
    this.scheduler  = (String) tasksParamsMap.get(DAGS_SCHEDULER);
    this.parameters = this.transferObjectToMap(tasksParamsMap.get(DAGS_PARAMS));
    this.tags       = (List<String>) tasksParamsMap.get(DAGS_TAGS);
    this.version    = (String) tasksParamsMap.get(DAGS_VERSION);
    this.startDate  = (String) tasksParamsMap.get(DAGS_START_DATE);
    this.callback   = this.transferObjectToMap(tasksParamsMap.get(DAGS_CALLBACK));
    this.domain     = (String) tasksParamsMap.get(DADS_DOMAIN);
  }

  public String getDagId() {
    return dagId;
  }

  public String getOwner() {
    return owner;
  }

  public String getEmail() {
    return email;
  }

  public String getScheduler() {
    return scheduler;
  }

  public Map<String, Object> getParameters() {
    return parameters;
  }

  public List<String> getTags() {
    return tags;
  }

  public String getVersion() {
    return version;
  }

  public String getStartDate() {
    return startDate;
  }

  public Map<String, Object> getCallback() {
    return callback;
  }

  public String getDomain() {
    return domain;
  }

  private Map<String, Object> transferObjectToMap(Object object) {
    JsonNode objectNode = Try.of(() -> TransferDevUtil.yamlMapper.readTree(
        TransferDevUtil.yamlMapper.writeValueAsString(object))).get();
    Map<String, Object> map = new LinkedHashMap<>();
    StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(objectNode.fieldNames(), Spliterator.ORDERED), false)
        .forEach(keyName -> map.put(keyName, objectNode.get(keyName)));
    return map;
  }
}

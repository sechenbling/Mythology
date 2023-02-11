package cn.scut.app.util.transfer.entity.output;


import static cn.scut.app.util.transfer.constant.MetaStageFields.DAGS_TASKS;

import cn.scut.app.util.transfer.entity.output.layer.impl.TransferDag;
import cn.scut.app.util.transfer.entity.output.layer.impl.TransferTask;
import cn.scut.app.util.transfer.utils.TransferDevUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;

@Getter
public class TransferYaml {

  LinkedHashMap<String, Object> adapterYamlContentMap;
  TransferParams     transferParams;
  TransferDag        adapterDag;
  TransferWorkflow   transferWorkflow;
  List<TransferTask> adapterTaskList;

  public TransferYaml(TransferParams transferParams, TransferDag adapterDag) {
    this.transferParams   = transferParams;
    this.adapterDag       = adapterDag;
    this.transferWorkflow = new TransferWorkflow(transferParams.getDagId(), adapterDag);
    this.adapterTaskList  = adapterDag.getAllTasks();
    adapterYamlContentMap = new LinkedHashMap<>();
    adapterYamlContentMap.putAll(transferParams.getTasksParamsMap());
    LinkedHashMap<String, Object> taskList = new LinkedHashMap<>();
    adapterTaskList.forEach(
        adapterTask -> taskList.put(adapterTask.getTaskId(), adapterTask.getTaskConfig()));
    adapterYamlContentMap.put(DAGS_TASKS, taskList);
  }

  public String getYamlContentString() throws JsonProcessingException {
    return TransferDevUtil.yamlMapper.writeValueAsString(adapterYamlContentMap);
  }

  public Map<String, Object> getYamlContentMap() {
    return this.adapterYamlContentMap;
  }

  public String getDagId() {
    return this.transferParams.getDagId();
  }

  public String getOwner() {
    return this.transferParams.getOwner();
  }

  public String getEmail() {
    return this.transferParams.getEmail();
  }

  public String getScheduler() {
    return this.transferParams.getScheduler();
  }

  public Map<String, Object> getParameters() {
    return this.transferParams.getParameters();
  }

  public List<String> getTags() {
    return this.transferParams.getTags();
  }

  public String getVersion() {
    return this.transferParams.getVersion();
  }

  public String getStartDate() {
    return this.transferParams.getStartDate();
  }

  public Map<String, Object> getCallback() {
    return this.transferParams.getCallback();
  }

  public String getDomain() {
    return this.transferParams.getDomain();
  }
}

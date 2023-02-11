package cn.scut.app.util.transfer.entity.output.layer.impl;

import static cn.scut.app.util.transfer.constant.TransferTaskFields.ARGUMENTS;
import static cn.scut.app.util.transfer.constant.TransferTaskFields.DEPENDS_ON;
import static cn.scut.app.util.transfer.constant.TransferTaskFields.INHERIT;
import static cn.scut.app.util.transfer.constant.TransferTaskFields.SETTINGS;
import static cn.scut.app.util.transfer.constant.TransferTaskFields.TASK_TYPE;

import cn.scut.app.util.transfer.entity.output.layer.TransferLayer;
import cn.scut.app.util.transfer.utils.TransferDevUtil;
import com.fasterxml.jackson.databind.JsonNode;
import io.vavr.control.Try;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;

@Getter
public class TransferTask implements TransferLayer {

  private final String       taskId;
  private final String       taskType;
  private final Object       settings;
  private final Object       arguments;
  private final String       inherit;
  private final List<String> dependsOn;
  private final Object       taskConfig;

  public TransferTask(String taskId, Object taskConfig) {
    this.taskId     = taskId;
    this.taskConfig = taskConfig;
    JsonNode configNode = Try.of(() -> TransferDevUtil.yamlMapper.readTree(
        TransferDevUtil.yamlMapper.writeValueAsString(taskConfig))).get();
    this.taskType  = Optional.ofNullable(configNode.get(TASK_TYPE))
        .map(JsonNode::asText).orElse(null);
    this.settings  = configNode.get(SETTINGS);
    this.arguments = configNode.get(ARGUMENTS);
    this.inherit   = Optional.ofNullable(configNode.get(INHERIT))
        .map(JsonNode::asText).orElse(null);
    this.dependsOn = Optional.ofNullable(configNode.get(DEPENDS_ON))
        .map(dependsOnNode -> {
              List<String> depends = new ArrayList<>();
              dependsOnNode.forEach(dependsOnElem -> depends.add(dependsOnElem.asText()));
              return depends;
            }
        ).orElse(new ArrayList<>());
  }

  @Override
  public List<TransferTask> getHeadTasks() {
    ArrayList<TransferTask> adapterTasks = new ArrayList<>();
    adapterTasks.add(this);
    return adapterTasks;
  }

  @Override
  public List<TransferTask> getTailTasks() {
    ArrayList<TransferTask> adapterTasks = new ArrayList<>();
    adapterTasks.add(this);
    return adapterTasks;
  }

  @Override
  public List<TransferTask> getAllTasks() {
    ArrayList<TransferTask> adapterTasks = new ArrayList<>();
    adapterTasks.add(this);
    return adapterTasks;
  }

  @Override
  public String getLayerId() {
    return taskId;
  }

  @Override
  public String toString() {
    Map<String, Object> taskInstance = new HashMap<>();
    taskInstance.put(this.taskId, this.taskConfig);
    return Try.of(() -> TransferDevUtil.yamlMapper.writeValueAsString(taskInstance)).get();
  }
}

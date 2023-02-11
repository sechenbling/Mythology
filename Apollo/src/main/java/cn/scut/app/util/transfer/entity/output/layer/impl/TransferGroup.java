package cn.scut.app.util.transfer.entity.output.layer.impl;

import cn.scut.app.util.transfer.entity.Dag;
import cn.scut.app.util.transfer.entity.output.layer.TransferLayer;
import java.util.List;
import java.util.Set;

public class TransferGroup implements TransferLayer {

  private final String            groupId;
  private final Dag<TransferTask> groupDag;

  public TransferGroup(String groupId, List<TransferTask> adapterTasks) {
    this.groupId  = groupId;
    this.groupDag = new Dag<>(adapterTasks);
  }

  @Override
  public List<TransferTask> getHeadTasks() {
    return groupDag.getHeadList();
  }

  public Set<String> getHeadTasksId() {
    return groupDag.getHeadTaskNames();
  }

  @Override
  public List<TransferTask> getTailTasks() {
    return groupDag.getTailList();
  }

  @Override
  public List<TransferTask> getAllTasks() {
    return groupDag.getTaskList();
  }

  @Override
  public String getLayerId() {
    return this.groupId;
  }

  public TransferTask getAdapterTask(String taskId) {
    for (TransferTask task : groupDag.getTaskList()) {
      if (task.getTaskId().equals(taskId)) {
        return task;
      }
    }
    return groupDag.getHeadList().get(0);
  }

  public List<TransferTask> createRangeTask(String id) {
    return this.groupDag.createRangeTask(id);
  }
}

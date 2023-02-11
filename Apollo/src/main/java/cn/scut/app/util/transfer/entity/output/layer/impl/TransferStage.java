package cn.scut.app.util.transfer.entity.output.layer.impl;

import cn.scut.app.util.transfer.entity.Dag;
import cn.scut.app.util.transfer.entity.output.layer.TransferLayer;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TransferStage implements TransferLayer {

  private final String             stageId;
  private final Dag<TransferGroup> stageDag;

  public TransferStage(String stageId, List<TransferGroup> stageDag) {
    this.stageId  = stageId;
    this.stageDag = new Dag<>(stageDag);
  }

  public TransferStage(String stageId, List<TransferGroup> stageDag,
      Map<String, List<String>> layerOrder) {
    this.stageId  = stageId;
    this.stageDag = new Dag<>(stageDag, layerOrder);
  }

  @Override
  public List<TransferTask> getHeadTasks() {
    return this.stageDag.getHeadList();
  }

  @Override
  public List<TransferTask> getTailTasks() {
    return this.stageDag.getTailList();
  }

  @Override
  public List<TransferTask> getAllTasks() {
    return this.stageDag.getTaskList();
  }

  @Override
  public String getLayerId() {
    return this.stageId;
  }

  public List<TransferTask> createRangeTask(String id) {
    return this.stageDag.createRangeTask(id);
  }

  public Set<String> getGroupName() {
    return stageDag.getLayerNodes();
  }

  public Map<String, List<String>> getGroupDependencies() {
    return stageDag.getLayerEdges();
  }

  public Map<String, TransferGroup> getGroupInfo() {
    return stageDag.getLayerMap();
  }
}

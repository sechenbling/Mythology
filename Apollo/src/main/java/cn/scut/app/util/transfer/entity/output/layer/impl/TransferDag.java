package cn.scut.app.util.transfer.entity.output.layer.impl;

import cn.scut.app.util.transfer.entity.Dag;
import cn.scut.app.util.transfer.entity.output.layer.TransferLayer;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TransferDag implements TransferLayer {

  private final String             dagId;
  private final Dag<TransferStage> dagDag;

  public TransferDag(String dagId, List<TransferStage> dagDag,
      Map<String, List<String>> layerOrder) {
    this.dagId  = dagId;
    this.dagDag = new Dag<>(dagDag, layerOrder);
  }

  @Override
  public List<TransferTask> getHeadTasks() {
    return this.dagDag.getHeadList();
  }

  @Override
  public List<TransferTask> getTailTasks() {
    return this.dagDag.getTailList();
  }

  @Override
  public List<TransferTask> getAllTasks() {
    return this.dagDag.getTaskList();
  }

  @Override
  public String getLayerId() {
    return this.dagId;
  }

  public Set<String> getStageName() {
    return dagDag.getLayerNodes();
  }

  public Map<String, List<String>> getStageDependencies() {
    return dagDag.getLayerEdges();
  }

  public Map<String, TransferStage> getStageInfo() {
    return dagDag.getLayerMap();
  }
}

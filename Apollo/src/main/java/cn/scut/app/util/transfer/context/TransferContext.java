package cn.scut.app.util.transfer.context;

import cn.scut.app.util.transfer.context.entity.TransferConfigs;
import cn.scut.app.util.transfer.context.entity.TransferModels;
import cn.scut.app.util.transfer.entity.output.TransferParams;
import cn.scut.app.util.transfer.entity.output.layer.impl.TransferDag;
import cn.scut.app.util.transfer.entity.output.layer.impl.TransferGroup;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import javax.xml.transform.TransformerException;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class TransferContext {

  private final LinkedHashMap<String, Object> transferDagParams;
  private final TransferConfigs   transferConfigs;
  private final TransferModels    transferModels;
  private final ArrayList<String> trainTaskIds;
  private final ArrayList<String>             dagEvalTaskIds;
  private final ArrayList<String> modelEvalTaskIds;
  private       TransferGroup     trainTaskGroup;
  private       TransferGroup     dagEvalTaskGroup;
  private       TransferGroup     modelEvalTaskGroup;
  private       TransferParams    transferParams;
  private       TransferDag       transferDag;

  public TransferContext() throws TransformerException {
    transferDagParams = new LinkedHashMap<>();
    transferConfigs = new TransferConfigs();
    transferModels  = new TransferModels();
    trainTaskIds    = new ArrayList<>();
    dagEvalTaskIds     = new ArrayList<>();
    modelEvalTaskIds   = new ArrayList<>();
    trainTaskGroup     = null;
    dagEvalTaskGroup   = null;
    modelEvalTaskGroup = null;
    transferParams     = null;
    transferDag         = null;
  }

  public TransferConfigs getTransferConfigs() {
    return transferConfigs;
  }

  public TransferModels getTransferModel() {
    return transferModels;
  }


  public ArrayList<String> getTrainTaskIds() {
    return this.trainTaskIds;
  }

  public void setTrainTaskGroup(TransferGroup trainGroup) {
    trainGroup.getAllTasks().forEach(transferTask -> this.trainTaskIds.add(transferTask.getTaskId()));
    this.trainTaskGroup = trainGroup;
  }

  public void setDagEvalTasksGroup(TransferGroup dagEvalTaskGroup) {
    dagEvalTaskGroup.getAllTasks()
        .forEach(transferTask -> this.dagEvalTaskIds.add(transferTask.getTaskId()));
    this.dagEvalTaskGroup = dagEvalTaskGroup;
  }

  public void setModelEvalTasks(TransferGroup modelEvalTaskGroup) {
    this.modelEvalTaskGroup = modelEvalTaskGroup;
  }

  public void setTransferDagParams(String key, Object value) {
    this.transferDagParams.put(key, value);
  }

  public Object getTransferDagParams(String key) {
    return transferDagParams.getOrDefault(key, new Object());
  }

  public TransferParams getTransferParams() {
    return transferParams;
  }

  public void setTransferParams(TransferParams transferParams) {
    this.transferParams = transferParams;
  }
}

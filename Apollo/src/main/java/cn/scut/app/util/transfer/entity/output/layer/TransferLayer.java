package cn.scut.app.util.transfer.entity.output.layer;

import cn.scut.app.util.transfer.entity.output.layer.impl.TransferTask;
import java.util.List;

public interface TransferLayer {

  List<TransferTask> getHeadTasks();

  List<TransferTask> getTailTasks();

  List<TransferTask> getAllTasks();

  String getLayerId();
}

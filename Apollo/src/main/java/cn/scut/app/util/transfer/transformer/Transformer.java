package cn.scut.app.util.transfer.transformer;

import cn.scut.app.util.transfer.context.TransferContext;
import cn.scut.app.util.transfer.entity.output.layer.impl.TransferGroup;
import java.util.HashMap;

public interface Transformer {

  /*
  TODO: fill your new train transform here,
    all you need to do is transform newTrain to a key-value format and put it into task_list,
    notes that:
    the key is the task id, while the value is a map that contains airflow configs of this task
    a simple example(Java 8):

    LinkedHashMap<String, Object> task_list=(LinkedHashMap<String, Object>) transform.get("tasks");
    Map<String,Object> configs=new LinkedHashMap<String,Object>();
    ...
    configs.put("task_type",task_type_object);
    configs.put("settings",settings_object);
    ...
    task_list.put("new task id", configs);
   */
  TransferGroup transform(HashMap<String, Object> root, TransferContext context);
}

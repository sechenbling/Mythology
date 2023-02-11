package cn.scut.app.util.transfer.constant;

public class TrainStageFields {

  // train meta yaml fields
  public static final String TYPE                = "type";
  public static final String SETTINGS            = "settings";
  public static final String SETTINGS_EMS_PARAMS = "ems_params";
  public static final String RENDER              = "render";
  public static final String DELIMITER           = "delimiter";
  public static final String EXPERIMENT_NAME     = "experiment_name";
  // train task fields group_id type
  public static final String GROUP_KRYLOV        = "train_krylov";

  public static final String MODEL_SELECTION     = "model_selection";
  // selection meta yaml fields
  public static final String CONDITION           = "condition";
  public static final String METRICS_NAME        = "metricsName";
  public static final String SORT                = "sort";
  // selection type
  public static final String EXPERIMENT          = "experiment";
  // selection dynamic dag yaml fields
  public static final String CREATE_BY           = "create_by";
  public static final String PROJECT_NAME        = "project_name";
  public static final String MODEL_NAME          = "model_name";
  public static final String DAG_RUN_ID          = "dag_run_id";
  public static final String MODEL_INSTANCE_NAME = "model_instance_name";
  public static final String CONDITIONS          = "conditions";
  public static final String GROUP_EXP_SELECTION = "exp_selection";
}

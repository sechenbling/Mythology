package cn.scut.app.util.transfer.constant;

public class EvalStageFields {

  // evaluation meta yaml fields
  public static final String SOURCE                = "source";
  public static final String LEVEL                 = "level";
  public static final String METRICS               = "metrics";
  public static final String NAME                  = "metricsName";
  public static final String METRICS_NAME          = "metricsName";
  public static final String EXPRESSION            = "expression";
  public static final String TEMPLATE              = "template";
  public static final String RENDER                = "render";
  public static final String TASKS                 = "tasks";
  public static final String DEFAULT               = "default";
  public static final String PLACEHOLDER           = "placeholder";
  public static final String DELIMITER             = "delimiter";
  // evaluation source type
  public static final String SOURCE_TRAIN          = "train";
  public static final String SOURCE_CUSTOMIZED     = "customized";
  public static final String SOURCE_DAG            = "dag";
  public static final String SOURCE_MODEL          = "model";
  //evaluation task fields
  public static final String EXPERIMENT_ID         = "experiment_id";
  public static final String DAG_RUN_ID            = "dag_run_id";
  public static final String HEALTH_CHECKS         = "health_checks";
  public static final String DAG_EVAL_HEALTH_CHECK = "dag_eval_health_check";
  // evaluation task fields group_id type
  public static final String GROUP_DAG_EVAL        = "eval_dag_eval";
  public static final String GROUP_MODEL_EVAL      = "eval_model_eval";
  public static final String GROUP_TRAIN           = "eval_train";
  public static final String GROUP_DAG             = "eval_dag";
  public static final String GROUP_MODEL           = "eval_model";
}

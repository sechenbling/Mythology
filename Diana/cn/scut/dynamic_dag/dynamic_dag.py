"""Dynamic DAG generator based on YAML configure for ads scheduler."""
import os
import copy
import yaml
import collections
from datetime import datetime, timedelta

from airflow.exceptions import AirflowException
from airflow.utils.task_group import TaskGroup
from airflow.utils.log.logging_mixin import LoggingMixin

from ebay.airflow.ads.models.dag import DAG
from ebay.airflow.ads.operators.bash import BashOperator
from ebay.airflow.ads.operators.python import PythonOperator

from ebay.airflow.ads.utils.constants import Constants
from ebay.airflow.ads.utils.common_tasks import parse_inherit
from ebay.airflow.ads.utils.ud_filters import ds_format_v2
from ebay.airflow.ads.utils.common_tasks import get_func_from_str
from ebay.airflow.ads.dynamic_dag.dynamic_dag_util import (
  DynamicDagUtil,
  global_etl_envs,
  dag_required_parameters,
  none_allowed_parameters,
  task_type_2_operator_map,
  none_scheduler_expressions,
)
from ebay.airflow.ads.dynamic_dag.dynamic_dag_operator_adaptor import \
  AdsOperatorAdapter
from ebay.airflow.ads.dynamic_dag.dynamic_dag_sensor import AdsFileSensor

DynamicDagUtil.read_dynamic_dag_config()


class AdsDynamicDAG(LoggingMixin):
  """
  Dynamic DAG Generator For Ads Domain.

  @param yaml_config: The definition of DAG.
  """
  ud_filters = {
    "ds_format_v2": ds_format_v2,
  }

  def __init__(self, yaml_config):
    conf = self._read_conf(yaml_config)

    # Airflow add permission view when DAG first load succeed while pass if
    # load failed, so here need to update permission to make sure
    # import-errors will be show on Web UI for users.
    self._update_role_permission(conf)

    check_pass, err_message = self._check_conf_prev(conf)
    if not check_pass:
      raise AirflowException(
          f"Check YAML config:{yaml_config} failed, error:{err_message}")

    self.conf = conf
    self.new_conf = copy.deepcopy(conf)

    self.dag_id = self._get_dag_id()
    self.tasks = {}
    self.task_ids = set()
    self.task_2_group = {}
    self.total_groups = []
    self.task_sensors = collections.defaultdict(list)
    self.task_sensor_group = {}

    self.default_etl_env = global_etl_envs["apollo-rno"]["b_ads_sre"]
    self.ads_file_sensor = AdsFileSensor()
    self.ads_operator_adapter = AdsOperatorAdapter()

    default_args = {
      "owner": conf.get("owner", "airflow"),
      "email": conf.get("email", ["yizshen@ebay.com"]),
      "email_on_failure": conf.get("email_on_failure", True),
      "email_on_retry": conf.get("email_on_retry", True),
      "depends_on_past": conf.get("depends_on_past", False),
      "retries": conf.get("retries", 0),
      "provide_context": True,
      "on_failure_callback": conf.get("on_failure_callback", None),
      "on_success_callback": conf.get("on_success_callback", None)
    }

    if conf.get("execution_timeout"):
      default_args["execution_timeout"] = \
        DynamicDagUtil.parameter_render({}, conf["execution_timeout"])

    if default_args["retries"] > 0 and conf.get("retry_delay"):
      default_args["retry_delay"] = timedelta(seconds=int(conf["retry_delay"]))

    # Parse DAG level placeholder to reduce the duplication of task arguments.
    if self.conf.get("parameters"):
      self.new_conf["parameters"] = DynamicDagUtil.parameter_self_render(
          self.conf["parameters"])

    # Parse DAG native dynamic parameters
    if self.conf.get("dynamic_params"):
      self.new_conf["dynamic_params"] = self._repl_value(self.new_conf,
                                                         self.conf[
                                                           "dynamic_params"])

    self.dag = DAG(self.dag_id,
                   description=conf.get("description",
                                        "Ads Pipeline " + self.dag_id),
                   default_args=default_args,
                   start_date=DynamicDagUtil.parameter_render({}, conf[
                     "start_date"]),
                   schedule_interval=conf["scheduler"],
                   catchup=conf.get("catchup", False),
                   max_active_runs=conf.get("max_active_runs", 1),
                   doc_md=conf.get("doc_md", __doc__),
                   tags=conf.get("tags", ["ads"]),
                   params=conf.get("dynamic_params", {}),
                   user_defined_filters=AdsDynamicDAG.ud_filters,
                   on_success_callback=get_func_from_str(
                       conf.get("callback", {}).get("dag_on_success", None)),
                   on_failure_callback=get_func_from_str(
                       conf.get("callback", {}).get("dag_on_failure", None)),
                   is_paused_upon_creation=conf.get("is_paused_upon_creation",
                                                    True))

    # pass dag context by with syntax
    with self.dag:
      # set start and end tasks to aggregate the whole DAG
      self.tasks["START"] = PythonOperator(
          task_id="start",
          python_callable=DynamicDagUtil.set_dynamic_dag_context,
          op_kwargs={"yaml_conf": self.conf},
          on_success_callback=get_func_from_str(
              conf.get("callback", {}).get("start_on_success", None)),
          on_failure_callback=get_func_from_str(
              conf.get("callback", {}).get("start_on_failure", None)),
          on_retry_callback=get_func_from_str(
              conf.get("callback", {}).get("start_on_retry", None)),
      )
      self.tasks["END"] = BashOperator(
          task_id="end",
          bash_command="echo end",
          on_success_callback=get_func_from_str(
              conf.get("callback", {}).get("end_on_success", None)),
          on_failure_callback=get_func_from_str(
              conf.get("callback", {}).get("end_on_failure", None)),
          on_retry_callback=get_func_from_str(
              conf.get("callback", {}).get("end_on_retry", None)))

      self._gene_tasks()

  def _read_conf(self, filename):
    if not filename.startswith("/"):
      filename = os.path.join(Constants.DAG_FOLDER, filename)

    with open(filename, 'r') as fd:
      yaml_conf = yaml.safe_load(fd)
      return yaml_conf

  def _update_role_permission(self, conf):
    from ebay.airflow.ads.utils.role_and_permission_util import \
      get_role_from_dag, insert_dag_permission_for_role
    role_2_permission = get_role_from_dag(set(conf.get("tags", ["ads"])),
                                          set([conf.get("owner", "airflow")]))

    for role_name in role_2_permission:
      insert_dag_permission_for_role(role_name, conf["dag_id"])

  def _check_conf_prev(self, conf):
    if not conf:
      return False, "Check conf failed, conf is empty!"

    for key in dag_required_parameters:
      if conf.get(key) is None and key not in none_allowed_parameters:
        return False, f"Check conf failed, DAG level parameters: {key} is missing"

    if conf["scheduler"] in none_scheduler_expressions:
      conf["scheduler"] = None

    if not isinstance(conf["tasks"], dict):
      return False, "Check conf failed, tasks is not a dict"

    return True, None

  def _get_dag_id(self):
    if self.conf.get("domain"):
      return f"{self.conf['domain']}_{self.conf['dag_id']}"
    return self.conf["dag_id"]

  def _repl_value(self, conf, input_param):
    if isinstance(input_param, dict):
      new_param = {}
      for key, value in input_param.items():
        new_value = self._repl_value(conf, value)
        new_param[key] = new_value
    elif isinstance(input_param, list):
      new_param = []
      for value in input_param:
        new_value = self._repl_value(conf, value)
        new_param.append(new_value)
    elif isinstance(input_param, str):
      new_param = DynamicDagUtil.parameter_render(conf.get("parameters", {}),
                                                  input_param)
    else:
      new_param = input_param

    return new_param

  def _process_sensor_dependency(self, task_id, parent_group=None):
    sensor_group_id = f"{task_id}_with_sensors"
    sensor_task_group = TaskGroup(
        group_id=sensor_group_id, parent_group=parent_group,
        tooltip=f"Group for {task_id} sensors")
    self.tasks[sensor_task_group.group_id] = sensor_task_group
    if self.tasks[task_id].task_group.is_root:
      self.tasks[task_id].task_group = sensor_task_group
    else:
      raise AirflowException(
          f"Task:{task_id} has other TaskGroup:{self.tasks[task_id].task_group.group_id}!")

    sensor_task_group.add(self.tasks[task_id])

    self.task_sensor_group[task_id] = sensor_task_group.group_id
    sensors = self.task_sensors[task_id]
    for sensor in sensors:
      self.tasks[task_id].set_upstream(sensor)
      if sensor.task_group.is_root:
        sensor.task_group = sensor_task_group
      else:
        raise AirflowException(
            f"Sensor:{sensor.task_id} has other TaskGroup:{sensor.task_group.group_id}!")
      sensor_task_group.add(sensor)

  def _parse_task(self, task_id, task_conf):
    """
    Create task object by configuration parameters.
    @param task_id: The task_id.
    @param task_conf: The original configure.
    @return:
    """
    new_task_conf = self.new_conf["tasks"][task_id]

    check_pass, err_message = DynamicDagUtil.check_task_conf(task_id,
                                                             new_task_conf)
    if not check_pass:
      raise AirflowException(
          f"Check task_id:{task_id} conf failed, error:{err_message}.")

    # Step1: Create file sensors by required_data_paths.
    if task_conf.get("required_data_paths"):
      for pos, path in enumerate(task_conf["required_data_paths"]):
        new_path = self._repl_value(self.new_conf, path)
        new_task_conf["required_data_paths"][pos] = new_path
        sensor = self.ads_file_sensor.get_sensor(
            task_id=f"required_data_path_{pos}_of_{task_id}",
            path=new_path
        )
        self.task_sensors[task_id].append(sensor)

    # Step2: Generate operator initialization parameters.
    if task_conf.get("settings"):
      for key, value in task_conf["settings"].items():
        new_value = self._repl_value(self.new_conf, value)
        new_task_conf["settings"][key] = new_value

    # Step3: Generate application command line arguments.
    if task_conf.get("arguments"):
      if isinstance(task_conf["arguments"], dict):
        for key, value in task_conf["arguments"].items():
          new_value = self._repl_value(self.new_conf, value)
          new_task_conf["arguments"][key] = new_value
      elif isinstance(task_conf["arguments"], list):
        for pos, value in enumerate(task_conf["arguments"]):
          new_value = self._repl_value(self.new_conf, value)
          new_task_conf["arguments"][pos] = new_value
      elif isinstance(task_conf["arguments"], str):
        new_task_conf["arguments"] = self._repl_value(self.new_conf,
                                                      task_conf["arguments"])
      else:
        raise AirflowException(
            f"Unsupported arguments format:{type(task_conf['arguments'])}")

  def _create_task(self, task_id, task_conf):
    task_type = task_conf["task_type"]
    # Step1: Modify parameters to adapt operator.
    self.ads_operator_adapter.adaptor(task_type, task_conf)

    # Step2: Instantiate operator.
    self.log.info(
        f"Instantiate operator:{task_type} with parameters:[{str(task_conf['settings'])}]")
    self.tasks[task_id] = task_type_2_operator_map[task_type](
        task_id=task_id, **task_conf["settings"])

    # Step3: Create TaskGroup if set group_id.
    if task_conf.get("group_id"):
      group_ids = task_conf["group_id"].split('.')
      # group_ids must have no conflict with task_ids
      conflict_task_ids = set(group_ids) & self.task_ids
      if conflict_task_ids:
        raise AirflowException(
            f"Group id:{';'.join(conflict_task_ids)} has conflict with task ids")

      parent_group_id = group_ids[0]
      # Default TaskGroup group_id will be prefixed by parent TaskGroup group_id.
      if not self.tasks.get(parent_group_id):
        self.log.info(f"Add task:{task_id} to TaskGroup:{parent_group_id}")
        parent_task_group = TaskGroup(group_id=parent_group_id)
        assert parent_group_id == parent_task_group.group_id
        self.tasks[parent_task_group.group_id] = parent_task_group
        self.total_groups.append(parent_task_group.group_id)
      else:
        parent_task_group = self.tasks[parent_group_id]

      for group_id in group_ids[1:]:
        group_id_with_prefix = parent_task_group.child_id(group_id)
        if not self.tasks.get(group_id_with_prefix):
          self.log.info(
              f"Add task:{task_id} to TaskGroup:{group_id_with_prefix}")
          task_group = TaskGroup(group_id=group_id,
                                 parent_group=parent_task_group)
          assert group_id_with_prefix == task_group.group_id
          self.tasks[task_group.group_id] = task_group
          self.total_groups.append(task_group.group_id)
        else:
          task_group = self.tasks[group_id_with_prefix]
        parent_task_group = task_group

      # Create TaskGroup including task and its sensors.
      if self.task_sensors.get(task_id):
        self._process_sensor_dependency(task_id, parent_group=parent_task_group)
      else:
        self.task_2_group[task_id] = parent_task_group
        if self.tasks[task_id].task_group.is_root:
          self.tasks[task_id].task_group = parent_task_group
        else:
          raise AirflowException(f"Task:{task_id} has other TaskGroup:"
                                 f"{self.tasks[task_id].task_group.group_id}!")
        parent_task_group.add(self.tasks[task_id])
    else:
      if self.task_sensors.get(task_id):
        self._process_sensor_dependency(task_id)

  def _process_task_group_join_id(self, task_group: TaskGroup,
      start_task_groups, end_task_groups):
    """
    If this TaskGroup has no upstream or downstream tasks, a dummy node called
    join_id will be created in Graph view to join the outgoing edges from this
    TaskGroup to reduce the total number of edges needed to be displayed.
    @param task_group: the TaskGroup to process
    @param start_task_groups: no upstream tasks task_groups
    @param end_task_groups: no downstream tasks task_groups
    @return: None
    """
    group_id = task_group.group_id

    upstream_tasks = set()
    for task in task_group.get_roots():
      upstream_tasks.update(task.upstream_task_ids)

    if len(upstream_tasks) == 0:
      task_group.set_upstream(self.tasks["START"])
      start_task_groups.add(group_id)

    downstream_tasks = set()
    for task in task_group.get_leaves():
      downstream_tasks.update(task.downstream_task_ids)

    if len(downstream_tasks) == 0:
      task_group.set_downstream(self.tasks["END"])
      end_task_groups.add(group_id)

  def _process_task_dependency(self, disabled_tasks):
    for task_id, task_conf in self.new_conf["tasks"].items():
      # replace task_id by group_id if task has sensors
      task_new_id = self.task_sensor_group.get(task_id, task_id)
      for up_task_id in task_conf["depends_on"]:
        if up_task_id in disabled_tasks:
          self.log.warning(
              f"The upstream task:{up_task_id} depended by task:{task_id} is disabled")
          continue

        up_task_new_id = self.task_sensor_group.get(up_task_id, up_task_id)
        self.tasks[task_new_id].set_upstream(self.tasks[up_task_new_id])

    # process start and end tasks
    start_task_groups = set()
    end_task_groups = set()
    for group_id in self.total_groups:
      task_group = self.tasks[group_id]
      self._process_task_group_join_id(task_group, start_task_groups,
                                       end_task_groups)

    for task_id, sensor_group_id in self.task_sensor_group.items():
      task_group = self.tasks[sensor_group_id]
      self._process_task_group_join_id(task_group, start_task_groups,
                                       end_task_groups)

    for task_id, task_conf in self.new_conf["tasks"].items():
      if len(self.tasks[task_id].upstream_task_ids) == 0:
        if task_id not in self.task_2_group:
          self.tasks[task_id].set_upstream(self.tasks["START"])
        elif self.task_2_group[task_id].group_id not in start_task_groups:
          self.tasks[task_id].set_upstream(self.tasks["START"])
      if len(self.tasks[task_id].downstream_task_ids) == 0:
        if task_id not in self.task_2_group:
          self.tasks[task_id].set_downstream(self.tasks["END"])
        elif self.task_2_group[task_id].group_id not in end_task_groups:
          self.tasks[task_id].set_downstream(self.tasks["END"])

  def _gene_tasks(self):
    """
    Create task object and process their dependency.
    @return:
    """
    disabled_tasks = set()
    if self.conf.get("disabled_tasks"):
      disabled_tasks.update(self.conf["disabled_tasks"])

    inherit_task_map = {}
    # Step1: Parse task config.
    for task_id, task_conf in self.conf["tasks"].items():
      if task_id in disabled_tasks or task_conf.get("disable", False):
        self.new_conf["tasks"].pop(task_id)
        disabled_tasks.add(task_id)
        self.log.info(f"Disable task:{task_id} by configure.")
        continue

      # Parse inherit later to avoid duplication of render work.
      # The inherited task could inherit disabled parent task.
      if task_conf.get("inherit"):
        inherit_task_id = task_conf["inherit"]
        inherit_task_map[task_id] = [inherit_task_id]
        self.log.info(f"Task:{task_id} inherits from task:{inherit_task_id}")
        continue

      self._parse_task(task_id, task_conf)

    # Step2: Process tasks inheritance.
    if inherit_task_map:
      sorted_tasks = DynamicDagUtil.parse_circle_dependency(inherit_task_map)
      for task_id in sorted_tasks:
        task_conf = self.conf["tasks"][task_id]
        inherit_task_id = task_conf["inherit"]
        if self.new_conf["tasks"].get(inherit_task_id):
          parent_conf = self.new_conf["tasks"][inherit_task_id]
        else:
          parent_conf = self.conf["tasks"][inherit_task_id]

        inherited_task_conf = parse_inherit(parent_conf, task_conf)
        if task_id in disabled_tasks or inherited_task_conf.get("disable",
                                                                False):
          self.log.info(f"Disable task:{task_id} from inheriting configure")
          self.new_conf["tasks"].pop(task_id)
          continue

        self.new_conf["tasks"][task_id] = copy.deepcopy(inherited_task_conf)
        self._parse_task(task_id, inherited_task_conf)

    # Step3: Create task object.
    self.task_ids.update(self.new_conf["tasks"].keys())
    for task_id, task_conf in self.new_conf["tasks"].items():
      self._create_task(task_id, task_conf)

    # Step4: Process task dependency
    self._process_task_dependency(disabled_tasks)

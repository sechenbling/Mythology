import re
import copy
import importlib
import logging
import collections
from datetime import timedelta
from typing import Dict

from airflow.exceptions import AirflowException

from cn.scut.utils.etl_env import get_all_etl_env
from cn.scut.utils.constants import Constants
from cn.scut.utils.confighandler import ConfigHandler
from cn.scut.utils.fidelius_secret import FideliusSecretHandler
from cn.scut.dynamic_dag.dynamic_dag_sys_parameter_parser import DynamicDAGSysParamParser
from cn.scut.dynamic_dag.dynamic_dag_context import DynamicDagContext


operator_default_timeout_conf = {}
sensor_default_time_conf = {}
global_etl_envs = get_all_etl_env()
task_type_2_operator_map = {}
task_type_2_adaptor = {}
dag_required_parameters = set()
none_allowed_parameters = set()

none_scheduler_expressions = set(["null", "none", "None", "NONE", "NULL", "Null"])


logger = logging.getLogger(__name__)


class DynamicDagUtil:
    """

    """
    PARAMETER_PATTERN = re.compile(r"\$\{(.*?)\}", re.MULTILINE)

    @classmethod
    def read_dynamic_dag_config(cls):
        """
        Read dynamic DAG configuration to init global variables.
        @return:
        """
        global operator_default_timeout_conf
        global sensor_default_time_conf
        global task_type_2_operator_map
        global task_type_2_adaptor

        conf = ConfigHandler.get_config(Constants.DYNAMIC_DAG_CONFIG_FILE, config_type="yaml")

        dag_required_parameters.update(conf["dag"]["required_parameters"].split(","))
        none_allowed_parameters.update(conf["dag"]["none_allowed_parameters"].split(","))

        for operator, operator_conf in conf["operators"].items():
            operator_default_timeout_conf[operator] = timedelta(seconds=int(operator_conf["timeout"]))
            module_path, class_name = operator_conf["import_lib"].rsplit('.', 1)
            task_type_2_operator_map[operator] = getattr(importlib.import_module(module_path), class_name)
            task_type_2_adaptor[operator] = operator_conf.get("import_adaptor", "empty_adaptor")

        for sensor, sensor_conf in conf["sensors"].items():
            sensor_default_time_conf[sensor] = {}
            for key, value in sensor_conf.items():
                sensor_default_time_conf[sensor][key] = int(value)

    @classmethod
    def check_task_conf(cls, task_id, task_conf):
        """
        Check configuration validation of task_id.
        @param task_id:
        @param task_conf:
        @return: True or False
        """
        for key in ["task_type", "depends_on"]:
            if task_conf.get(key) is None:
                return False, f"Check conf failed, task:{task_id} miss parameter:{key}"

        if task_conf["task_type"] not in task_type_2_operator_map:
            return False, f"Check conf failed, task:{task_id} task_type:{task_conf['task_type']} is unsupported"

        if not task_conf.get("settings"):
            if task_conf["task_type"] != "dummy":
                return False, f"Check conf failed, task:{task_id} is not dummy operator but has no settings"
        else:
            if not isinstance(task_conf["settings"], dict):
                return False, f"Check conf failed, " \
                              f"task:{task_id} settings is not a dict, type:{type(task_conf['settings'])}"

        if not isinstance(task_conf["depends_on"], list):
            return False, f"Check conf failed, task:{task_id} depends_on is not a list"

        return True, None

    @classmethod
    def parse_circle_dependency(cls, depend_map):
        """
        Find a sequence dependency without circle in it, otherwise raise an error.
        @param depend_map: A dict contains dependency, e.g.
            A: [B, C]
            B: [C, D]
            C: D
        @return: a sequence list, e.g.
            [D, C, B, A]
        """
        def dfs(key, dependency, visited, result_list):
            nonlocal no_circle
            visited[key] = 1
            for depend_key in dependency[key]:
                if depend_key not in visited:
                    continue
                if visited[depend_key] == 0:
                    error = dfs(depend_key, dependency, visited, result_list)
                    if not no_circle:
                        return error
                elif visited[depend_key] == 1:
                    no_circle = False
                    return depend_key

            visited[key] = 2
            result_list.append(key)

        if not depend_map or not isinstance(depend_map, dict):
            raise AirflowException(f"Unsupported dependency format:{type(depend_map)}")

        no_circle = True
        visited = {key: 0 for key in depend_map.keys()}
        sorted_depends = []
        error = None
        for key in depend_map.keys():
            if not no_circle:
                raise AirflowException(f"Key:{error} in dependency has circle, please check!")
            if visited[key] == 0:
                error = dfs(key, depend_map, visited, sorted_depends)

        return sorted_depends

    @classmethod
    def parameter_render(cls, param_conf: Dict, input_str: str):
        """
        Replace placeholders in input_str according to param_conf.
        @param param_conf: The parameters for lookup.
        @param input_str: The string to render.
        @return: The rendered string.
        """
        result = cls.PARAMETER_PATTERN.findall(input_str)
        for res in result:
            params = res.split(".", 1)
            if params[0] == "sys":
                value = DynamicDAGSysParamParser.parse(params[1])
                if value is None:
                    raise AirflowException(f"Invalid sys parameter: {params[1]}")
            elif params[0] == "secret":
                value = FideliusSecretHandler.get_secret(params[1])
            elif params[0] == "parameters":
                value = param_conf.get(params[1])
                if value is None:
                    raise AirflowException(f"Parameter key: {params[1]} not in configure")
            else:
                raise AirflowException(f"Unsupported DAG level parameter key: {params[0]}")

            input_str = input_str.replace("${" + res + "}", value) if isinstance(value, str) else value
        return input_str

    @classmethod
    def parameter_self_render(cls, param_conf: Dict):
        """
        @param param_conf: A dict of {key: value(contains placeholders)}
        @return: a new dict
        """
        new_param_conf = copy.deepcopy(param_conf)
        params_relation = collections.defaultdict(list)
        for key, param_value in param_conf.items():
            result = cls.PARAMETER_PATTERN.findall(param_value)
            for res in result:
                params = res.split(".")
                if params[0] == "sys":
                    value = DynamicDAGSysParamParser.parse(params[1])
                    if value is None:
                        raise AirflowException(f"Invalid sys parameter: {params[1]}")
                elif params[0] == "secret":
                    value = FideliusSecretHandler.get_secret(params[1])
                elif params[0] == "parameters":
                    params_relation[key].append(params[1])
                    continue
                else:
                    raise AirflowException(f"Unsupported DAG level parameter key: {params[0]}")

                param_value = param_value.replace("${" + res + "}", value) if isinstance(value, str) else value

            new_param_conf[key] = param_value

        if params_relation:
            sorted_parameters = cls.parse_circle_dependency(params_relation)
            for key in sorted_parameters:
                new_param_conf[key] = cls.parameter_render(new_param_conf, new_param_conf[key])

        return new_param_conf

    @classmethod
    def set_dynamic_dag_context(cls, yaml_conf, **kwargs):
        dynamic_dag_context = DynamicDagContext(yaml_conf)
        kwargs["ti"].xcom_push(key="dynamic_dag_context", value=dynamic_dag_context.encode())
        print("start")

    @classmethod
    def get_dynamic_dag_context(cls, context):
        encode_string = context['ti'].xcom_pull(task_ids='start', key='dynamic_dag_context')
        dynamic_conf = DynamicDagContext.decode(encode_string)
        return dynamic_conf['conf']

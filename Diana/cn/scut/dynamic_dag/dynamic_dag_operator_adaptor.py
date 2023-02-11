""""AdsOperatorAdapter is to implement the transformation from configure to operater instantiate"""
from airflow.exceptions import AirflowException
from airflow.utils.log.logging_mixin import LoggingMixin

from cn.scut.utils.common_tasks import get_func_from_str
from cn.scut.dynamic_dag.dynamic_dag_util import (
    operator_default_timeout_conf,
    global_etl_envs,
    task_type_2_adaptor)
from cn.scut.utils.enumerate import GitOperatorEnum


class AdsOperatorAdapterImpl(object):
    """
    AdsOperatorAdapter implementation
    """
    @classmethod
    def parse_etl_env(cls, settings_conf):
        cluster = settings_conf.pop("cluster")
        batch_account = settings_conf.pop("batch_account")
        etl_env = global_etl_envs[cluster].get(batch_account)
        if etl_env is None:
            raise AirflowException("Batch account: {} ticket cache of cluster: {} "
                                   "not existed on airflow".format(batch_account, cluster))

        settings_conf["etl_env"] = etl_env

    @classmethod
    def parse_argument(cls, conf, dict_2_array=True, keep_raw=False):
        arg_prefix = conf["settings"].pop("argument_prefix", "")
        if keep_raw:
            return conf["arguments"]
        if isinstance(conf["arguments"], dict):
            if dict_2_array:
                arg_joiner = conf["settings"].get("argument_joiner", " ")
                return [f"{arg_prefix}{k}{arg_joiner}{v}" for k, v in conf["arguments"].items()]
            else:
                return {f"{arg_prefix}{k}": v for k, v in conf["arguments"].items()}
        elif isinstance(conf["arguments"], list):
            return [f"{arg_prefix}{v}" for v in conf["arguments"]]
        elif isinstance(conf["arguments"], str):
            arg_type, arg_value = conf["arguments"].split(":", 1)
            if arg_type == "callable":
                return get_func_from_str(arg_value)()
            else:
                raise AirflowException("Unsupported arguments format:{}".format(conf["arguments"]))
        else:
            raise AirflowException("Unsupported arguments format:{}".format(type(conf["arguments"])))

    @classmethod
    def empty_adaptor(cls, conf):
        pass

    @classmethod
    def python_adaptor(cls, conf):
        function = get_func_from_str(conf["settings"]["python_callable"])
        conf["settings"]["python_callable"] = function
        if conf.get("arguments"):
            arguments = cls.parse_argument(conf, dict_2_array=False)
            if isinstance(arguments, dict):
                conf["settings"]["op_kwargs"] = arguments
            else:
                conf["settings"]["op_args"] = arguments

    @classmethod
    def hadoop_adaptor(cls, conf):
        cls.parse_etl_env(conf["settings"])

    @classmethod
    def spark2_adaptor(cls, conf):
        cls.parse_etl_env(conf["settings"])
        if conf.get("arguments"):
            arguments = cls.parse_argument(conf)
            conf["settings"]["app_args"] = arguments

    @classmethod
    def pyspark_adaptor(cls, conf):
        cls.parse_etl_env(conf["settings"])
        if conf.get("arguments"):
            arguments = cls.parse_argument(conf)
            conf["settings"]["arguments"] = arguments

        pyspark_conf = conf.pop("settings")
        conf["settings"] = {
            "etl_env": pyspark_conf.pop("etl_env"),
            "timeout": pyspark_conf.pop("timeout", None),
            "pyspark_conf": pyspark_conf
        }

    @classmethod
    def pykrylov_adapter(cls, conf):
        if conf.get("arguments"):
            arguments = cls.parse_argument(conf)
            conf["settings"]["job_params"] = " ".join(arguments)

        krylov_conf = conf.pop("settings")
        conf["settings"] = {
            "timeout": krylov_conf.pop("timeout", None),
            "krylov_conf": krylov_conf
        }

    @classmethod
    def krylov_retrain_adapter(cls, conf):
        if conf.get("arguments"):
            conf["settings"]["parameters"] = cls.parse_argument(conf, True, True)

        krylov_conf = conf.pop("settings")
        conf["settings"] = {
            "timeout": krylov_conf.pop("timeout", None),
            "krylov_conf": krylov_conf
        }

    @classmethod
    def git_sync_adapter(cls, conf):
        option = conf["settings"].get("option")
        if option:
            option_enum_name = f"clone_{option}"
            conf["settings"]["option"] = GitOperatorEnum[option_enum_name.upper()]

    @classmethod
    def krylov_read_metrics_adaptor(cls, conf):
        krylov_conf = conf.pop("settings")
        conf["settings"] = {
            "timeout": krylov_conf.pop("timeout", None),
            "krylov_conf": krylov_conf
        }

    @classmethod
    def shepherd_metrics_adaptor(cls, conf):
        if conf.get("arguments"):
            conf["settings"]["variables"] = cls.parse_argument(conf, True, True)

    @classmethod
    def darwin_model_create_adaptor(cls, conf):
        if conf.get("arguments"):
            conf["settings"]["parameters"] = cls.parse_argument(conf, True, True)

        darwin_conf = conf.pop("settings")
        conf["settings"] = {
            "timeout": darwin_conf.pop("timeout", None),
            "darwin_conf": darwin_conf
        }

    @classmethod
    def darwin_deploy_adaptor(cls, conf):
        if conf.get("arguments"):
            conf["settings"]["parameters"] = cls.parse_argument(conf, True, True)

        darwin_conf = conf.pop("settings")
        conf["settings"] = {
            "timeout": darwin_conf.pop("timeout", None),
            "darwin_conf": darwin_conf
        }


class AdsOperatorAdapter(LoggingMixin):
    """
    AdsOperatorAdapter
    """
    def __init__(self):
        for operator, adaptor_name in task_type_2_adaptor.items():
            if isinstance(adaptor_name, str):
                task_type_2_adaptor[operator] = getattr(AdsOperatorAdapterImpl, adaptor_name)

    def post_process(self, task_type, conf):
        if not conf.get("settings"):
            conf["settings"] = {}

        # process timeout
        execution_timeout = conf["settings"].pop("timeout", operator_default_timeout_conf[task_type])
        conf["settings"]["execution_timeout"] = execution_timeout

        # process callback setting
        for func_name in ["on_success_callback", "on_failure_callback", "on_retry_callback", "on_execute_callback"]:
            function_str = conf["settings"].get(func_name)
            if function_str:
                conf["settings"][func_name] = get_func_from_str(function_str)

    def adaptor(self, task_type, conf):
        if task_type not in task_type_2_adaptor:
            raise AirflowException("Unsupported operator type: {}".format(task_type))

        task_type_2_adaptor[task_type](conf)
        self.post_process(task_type, conf)

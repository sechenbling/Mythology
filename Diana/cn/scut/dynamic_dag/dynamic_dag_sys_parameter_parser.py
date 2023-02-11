import datetime
import os.path
import re
from datetime import timedelta

from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.exceptions import AirflowException

from cn.scut.utils.confighandler import ConfigHandler
from cn.scut.utils.constants import Constants

DATETIME_PATTERN = re.compile(r"datetime\((\d+),\s*(\d+),\s*(\d+)\)$")
EXECUTE_DATE_PATTERN = re.compile(r"execute_date\((.*?)\)\s?([+-]?)\s?(\d+d)?$")
TIMESTAMP_PATTERN = re.compile(r"execute_timestamp\((.*?)\)$")
TIMEDELTA_PATTERN = re.compile(r"timedelta\((.*?)\)$")
CONFIG_PATTERN = re.compile(r"config\((.*?)\)(.*?)$")
R_CONFIG_PATTERN = re.compile(r"r_config\(domain=(.*?),\s*name=(.*?)\)(.*?)$")


class DynamicDAGSysParamParserImpl:
    """
    Parse all system inner parameter implementation.
    """
    config_object_map = {}
    rivendell_config = ConfigHandler.get_config(config_name="rivendell_config", config_path="global/conf")

    @classmethod
    def parse_timedelta_of_seconds(cls, seconds):
        return timedelta(seconds=seconds)

    @classmethod
    def parse_timedelta_of_minutes(cls, minutes):
        return timedelta(minutes=minutes)

    @classmethod
    def parse_timedelta_of_hours(cls, hours):
        return timedelta(hours=hours)

    @classmethod
    def parse_timedelta_of_days(cls, days):
        return timedelta(days=days)

    @classmethod
    def parse_timedelta_of_weeks(cls, weeks):
        return timedelta(weeks=weeks)

    @classmethod
    def parse_execute_time(cls, input_str):
        result = EXECUTE_DATE_PATTERN.findall(input_str)
        if result and len(result) == 1 and len(result[0]) == 3:
            schema, symbol, dates = result[0]
            if not symbol and not dates:
                value = f"{{{{ ds | ds_format_v2('%Y-%m-%d', '{schema}') }}}}"
            else:
                days = dates.replace("d", "")
                if symbol == "-":
                    days = "-{}".format(days)

                value = f"{{{{ ds | ds_format_v2('%Y-%m-%d', '{schema}', {days}) }}}}"
            return value

    @classmethod
    def parse_datetime(cls, input_str):
        result = DATETIME_PATTERN.findall(input_str)
        if result and len(result) == 1 and len(result[0]) == 3:
            datetime_params = list(map(int, result[0]))
            return datetime.datetime(*datetime_params)

    @classmethod
    def parse_timestamp(cls, input_str):
        result = TIMESTAMP_PATTERN.findall(input_str)
        if result and len(result) == 1 and result[0]:
            value = f"{{{{ ts_nodash | ds_format_v2('%Y%m%dT%H%M%S', '{result[0]}') }}}}"
            return value

    @classmethod
    def parse_timedelta(cls, input_str):
        result = TIMEDELTA_PATTERN.findall(input_str)
        if result and len(result) == 1:
            schema, data = result[0].split("=")
            return getattr(DynamicDAGSysParamParserImpl, f"parse_timedelta_of_{schema}")(int(data))

    @classmethod
    def parse_config(cls, input_str):
        result = CONFIG_PATTERN.findall(input_str)
        if result and len(result) == 1:
            init_params = result[0][0].split(',')
            kwargs = {}
            for item in init_params:
                key, value = item.strip().split('=')
                kwargs[f"config_{key.strip()}"] = value.strip()

            # cache
            config_file = os.path.join(kwargs["config_path"], kwargs["config_name"])
            config = cls.config_object_map.get(config_file)
            if not config:
                config = ConfigHandler.get_config(**kwargs)
                cls.config_object_map[config_file] = config

            if result[0][1]:
                context_params = result[0][1].split('.')[1:]
                if kwargs.get("config_type", "ini") == 'ini':
                    if len(context_params) == 1:
                        return config.options(*context_params)
                    elif len(context_params) == 2:
                        return config.get(*context_params)
                    else:
                        raise AirflowException(f"Invalid configuration expression:{result[0][1]}")
                elif kwargs["config_type"] == "yaml":
                    value = config
                    for key in context_params:
                        value = value[key]
                    return value
                else:
                    raise AirflowException(f"Unsupported configuration type:{kwargs['config_type']}")
            else:
                return config

    @classmethod
    def parse_rivendell_config(cls, input_str):
        """
        Get config from ads scheduler service(rivendell)
        @param input_str:
        @return:
        """
        result = R_CONFIG_PATTERN.findall(input_str)
        if result and len(result) == 1:
            domain, dag_name = result[0][0], result[0][1]
            config_name = f"{domain}_{dag_name}_config.yaml"
            config_path = os.path.join(Constants.DAG_FOLDER, cls.rivendell_config.get("base", "local_config_path"), domain)
            config_file = os.path.join(config_path, config_name)
            config = cls.config_object_map.get(config_file)
            if not config:
                config = ConfigHandler.get_config(config_name=config_name, config_path=config_path, config_type="yaml")
                cls.config_object_map[config_file] = config

            if result[0][2]:
                context_params = result[0][2].split('.')[1:]
                value = config
                for key in context_params:
                    value = value[key]
                return value
            else:
                return config


class DynamicDAGSysParamParser(LoggingMixin):
    """
    Parse system parameters.
    """
    param_value_2_res = {
        "work_space": "{{ work_space.work_dir }}",
        "root_dir": Constants.DAG_FOLDER,
        "hive-site": Constants.HIVE_SITE_FILE,
    }

    param_prefix_2_res = {
        "datetime": DynamicDAGSysParamParserImpl.parse_datetime,
        "execute_date": DynamicDAGSysParamParserImpl.parse_execute_time,
        "execute_timestamp": DynamicDAGSysParamParserImpl.parse_timestamp,
        "timedelta": DynamicDAGSysParamParserImpl.parse_timedelta,
        "config": DynamicDAGSysParamParserImpl.parse_config,
        "r_config": DynamicDAGSysParamParserImpl.parse_rivendell_config,
    }

    @classmethod
    def parse(cls, param_value):
        param_result = cls.param_value_2_res.get(param_value)
        if param_result:
            return param_result

        for key in cls.param_prefix_2_res.keys():
            if param_value.startswith(key):
                param_result = cls.param_prefix_2_res[key](param_value)
                return param_result

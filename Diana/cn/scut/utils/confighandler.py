import os
import yaml
import configparser
import logging

from airflow.exceptions import AirflowException
from cn.scut.utils.constants import Constants
from cn.scut.utils.enumerate import ConfigType
from cn.scut.utils.common_tasks import parse_inherit

log = logging.getLogger(__name__)


class ConfigHandler(object):
    @classmethod
    def load_config(cls, config_file, type_enum):
        if type_enum == ConfigType.INI:
            config = configparser.ConfigParser()
            config.read(config_file)
            return config
        elif type_enum == ConfigType.YAML:
            with open(config_file) as fd:
                config = yaml.safe_load(fd)
                return config
        else:
            raise AirflowException(f"Unsupported configuration type:{type_enum.name}")

    @classmethod
    def get_config(cls, config_name, config_path=Constants.BASE_CONF_PATH, config_type="ini"):
        # convert absolute path
        if not config_path.startswith("/"):
            config_path = os.path.join(Constants.DAG_FOLDER, config_path)

        type_enum = ConfigType[config_type.upper()]

        # Load public config first, updates variables from cluster environment if any.
        pub_config_file = os.path.join(config_path, "public", config_name)
        cluster_config_file = os.path.join(config_path, Constants.CLUSTER_ENV, config_name)
        if not os.path.exists(pub_config_file) and not os.path.exists(cluster_config_file):
            raise FileExistsError(f"config_file:{config_name} has no public, neither cluster config")

        if os.path.exists(pub_config_file) and os.path.exists(cluster_config_file):
            log.info(f"Load config_file:{config_name} from public and {Constants.CLUSTER_ENV}")
            if type_enum == ConfigType.INI:
                config = configparser.ConfigParser()
                config.read([pub_config_file, cluster_config_file])
                return config
            elif type_enum == ConfigType.YAML:
                with open(pub_config_file) as fd:
                    pub_config = yaml.safe_load(fd)
                with open(cluster_config_file) as fd:
                    cluster_config = yaml.safe_load(fd)
                config = parse_inherit(pub_config, cluster_config)
                return config
            else:
                raise AirflowException(f"Unsupported configuration type:{config_type}")

        if os.path.exists(pub_config_file):
            return cls.load_config(pub_config_file, type_enum)

        return cls.load_config(cluster_config_file, type_enum)

    @classmethod
    def get_value(cls, config_name, stanza, key):
        config = cls.get_config(config_name)
        if config.has_section(stanza):
            if config.has_option(stanza, key):
                value = config.get(stanza, key)
                return value
            else:
                raise configparser.NoOptionError(key, stanza)
        else:
            raise configparser.NoSectionError(stanza)

    @classmethod
    def get_keys(cls, config_name, stanza):
        config = cls.get_config(config_name)
        if config.has_section(stanza):
            return config.options(stanza)
        else:
            return []

    @classmethod
    def has_stanza(cls, config_name, stanza):
        config = cls.get_config(config_name)
        return config.has_section(stanza)

import os
import configparser

from airflow.exceptions import AirflowException
from airflow import configuration


def get_conf_by_run_mode():
    dags_folder = configuration.conf.get("core", "dags_folder")
    run_config_file = os.path.join(dags_folder, "conf/public/run_config")
    run_config = configparser.ConfigParser()
    run_config.read(run_config_file)
    return run_config


def get_deploy_env(mode="cluster"):
    """
    Get cluster environment.

    @param mode: The run mode from configuration.
    @return: [prod, preprod]
    """
    if mode == "local":
        return "prod"

    deploy_env = os.environ.get("AIRFLOW_ENV")
    if not deploy_env:
        raise AirflowException("Env variable:AIRFLOW_ENV dost not exist.")
    return deploy_env.lower().rstrip("\n")


run_conf = get_conf_by_run_mode()
run_mode = run_conf.get("system", "run_mode")


class Constants(object):
    # public directory
    DAG_FOLDER = run_conf.get(run_mode, "DAG_FOLDER")
    DAG_LIBS = run_conf.get(run_mode, "DAG_LIBS")
    WORK_SPACE_BASE = run_conf.get(run_mode, "WORK_SPACE_BASE")
    BASE_LOG_DIR = run_conf.get(run_mode, "BASE_LOG_DIR")
    BASE_CONF_PATH = run_conf.get(run_mode, "BASE_CONF_PATH")
    FIDELIUS_SECRET_DIR = run_conf.get(run_mode, "FIDELIUS_SECRET_DIR")

    DAG_SYNC_FOLDER = f"{DAG_FOLDER}/git_sync"
    DAG_SUBMODULE_FOLDER = f"{DAG_FOLDER}/submodules"
    BACKUP_FOLDER = f"{DAG_FOLDER}/backup"

    BASH_SCRIPT_PATH = f"{DAG_LIBS}/ebay/airflow/ads/script"
    # K8S_ENV_NAMESPACE_FILE = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
    DYNAMIC_DAG_CONFIG_FILE = "dynamic_dag_config.yaml"

    # public variables
    CLUSTER_ENV = get_deploy_env(run_mode)
    GIT_SYNC_USERNAME = run_conf.get(run_mode, "GIT_SYNC_USERNAME")
    ROLE_2_DAG_PERMISSION = "role_dag_permission_conf"

    # hadoop public parameters
    HIVE_SITE_FILE = run_conf.get(run_mode, "HIVE_SITE_FILE")
    HADOOP_BATCH_ACCOUNTS = {
        "apollo-rno": ["b_ebayadvertising", "b_qa_ebayadvertising", "b_ads_sre", "b_ads_data", "b_merch"],
    }

    # krylov public parameters
    KRYLOV_CLIENT_HOME = f"{WORK_SPACE_BASE}/.krylov"
    KRYLOV_SERVICE_ACCOUNTS = set(["s_adsfd", "s_adsguidancekwreco", "s_adsguidance", "s_modelengine", "s_madsnap", "s_madccoe_dinv3"])

    # NuObject public parameters
    NUOBJECT_ACCOUNT = "nuobject-ads-infra"
    NUOBJECT_ENDPOINT = "https://ads-infra.nuobject.io"
    DEFAULT_VENV_BUCKET = "airflow-virtual-env"

    # Web Urls
    WEBSERVER_URL = os.getenv("AIRFLOW__WEBSERVER__BASE_URL")
    SLACK_APP_HOST = "https://slack.com/api/chat.postMessage"
    PAGER_DUTY_HOST = "https://events.pagerduty.com/v2/enqueue"
    BASE_PROXY_DICT = {"https": "http://httpproxy-tcop.vip.ebay.com:80", "http": "http://httpproxy-tcop.vip.ebay.com:80"}

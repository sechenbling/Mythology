import os
from airflow.models import Variable
from airflow.exceptions import AirflowException
from cn.scut.utils.constants import Constants


def get_airflow_variable(key, default, deserialize_json=False):
    v = default
    try:
        v = Variable.get(key, default, deserialize_json=deserialize_json)
    except Exception as e:
        print(f"Get Variable {key} failed")
    return v


def get_nuobject_conf(nuobj_key):
    s3_conf = get_airflow_variable(nuobj_key, None, deserialize_json=True)
    if s3_conf is None:
        raise AirflowException(f"Get Variable {nuobj_key} not set")

    mandatory_keys = ["endpoint", "access_key", "secret_key"]
    if set(mandatory_keys).issubset(s3_conf):
        return s3_conf
    else:
        raise AirflowException(f"Variable {nuobj_key} set not complete")


def get_webserver():
    return os.getenv("AIRFLOW__WEBSERVER__BASE_URL", get_airflow_variable("webserver", Constants.WEBSERVER_URL))


# def get_namespace():
#     if not os.path.exists(Constants.K8S_ENV_NAMESPACE_FILE):
#         return "Unknown"
#
#     with open(Constants.K8S_ENV_NAMESPACE_FILE) as fd:
#         namespace = fd.readlines()[0]
#     return namespace


def get_slack_enable():
    slack_enable = get_airflow_variable("SLACK_ENABLE", "True")
    return True if slack_enable.upper() == 'TRUE' else False


def get_pager_enable():
    pager_enable = get_airflow_variable("PAGER_ENABLE", "False")
    return True if pager_enable.upper() == 'TRUE' else False


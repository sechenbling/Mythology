import logging
import os
import shutil
import json
from datetime import datetime
from sqlalchemy import or_
from sqlalchemy.orm import Session

from airflow.providers.http.hooks.http import HttpHook
from airflow.utils.session import provide_session
from airflow.models import DagModel, DagRun
from airflow.utils.state import DagRunState
from airflow.utils.airflow_flask_app import get_airflow_app
from airflow.api.common.mark_tasks import set_dag_run_state_to_failed, set_dag_run_state_to_success

from cn.scut.utils.constants import Constants
from cn.scut.utils.nuobject import NuObjectClient
from cn.scut.utils.variables import get_nuobject_conf
from cn.scut.utils.confighandler import ConfigHandler
from cn.scut.utils.enumerate import DynamicDagState
from cn.scut.utils.url_util import get_dag_run_link, get_task_log
from cn.scut.dynamic_dag.dynamic_dag_util import (
    dag_required_parameters,
    none_allowed_parameters,
)


def get_nuobject_client():
    nuobject_conf = get_nuobject_conf(Constants.NUOBJECT_ACCOUNT)
    return NuObjectClient(**nuobject_conf)


class DynamicDagCRUDUtil:
    """
    Collection of crud operations for dynamic DAG.
    """
    rivendell_config = ConfigHandler.get_config(config_name="rivendell_config", config_path="global/conf")

    @classmethod
    def get_abstract_dag_id(cls, dagrun_conf):
        domain, dag_id = dagrun_conf['domain'], dagrun_conf['dagId']
        return f"{domain}_{dag_id}"

    @classmethod
    def get_local_yaml_path(cls, dagrun_conf):
        local_yaml_path = os.path.join(
            Constants.DAG_FOLDER, cls.rivendell_config.get("base", "local_yaml_path"), dagrun_conf["domain"])
        if not os.path.exists(local_yaml_path):
            os.makedirs(local_yaml_path)
        return local_yaml_path

    @classmethod
    def get_local_yaml_filename(cls, dagrun_conf):
        return f"{cls.get_abstract_dag_id(dagrun_conf)}.yaml"

    @classmethod
    def get_local_yaml_file(cls, dagrun_conf):
        config_path = cls.get_local_yaml_path(dagrun_conf)
        config_filename = cls.get_local_yaml_filename(dagrun_conf)
        return os.path.join(config_path, config_filename)

    @classmethod
    def get_local_config_path(cls, dagrun_conf):
        local_config_path = os.path.join(
            Constants.DAG_FOLDER, cls.rivendell_config.get("base", "local_config_path"),
            dagrun_conf["domain"], "public")
        if not os.path.exists(local_config_path):
            os.makedirs(local_config_path)
        return local_config_path

    @classmethod
    def get_local_config_filename(cls, dagrun_conf):
        return f"{cls.get_abstract_dag_id(dagrun_conf)}_config.yaml"

    @classmethod
    def get_local_config_file(cls, dagrun_conf):
        config_path = cls.get_local_config_path(dagrun_conf)
        config_filename = cls.get_local_config_filename(dagrun_conf)
        return os.path.join(config_path, config_filename)

    @classmethod
    def get_local_dag_path(cls, dagrun_conf):
        local_dag_path = os.path.join(
            Constants.DAG_FOLDER, cls.rivendell_config.get("base", "local_dag_path"), dagrun_conf["domain"])
        if not os.path.exists(local_dag_path):
            os.makedirs(local_dag_path)
        return local_dag_path

    @classmethod
    def get_local_dag_filename(cls, dagrun_conf):
        return f"{cls.get_abstract_dag_id(dagrun_conf)}.py"

    @classmethod
    def get_local_dag_file(cls, dagrun_conf):
        local_dag_path = cls.get_local_dag_path(dagrun_conf)
        local_dag_filename = cls.get_local_dag_filename(dagrun_conf)
        return os.path.join(local_dag_path, local_dag_filename)

    @classmethod
    def parse_s3_path(cls, config_path):
        """
        Parse NuObject path.
        @return:
        """
        _, _, bucket_name, object_name = config_path.split("/", 3)
        domain, name, file_name = object_name.split("/", 2)
        return bucket_name, object_name, domain, name, file_name

    @classmethod
    def get_yaml_from_nuobject(cls, dagrun_conf):
        """
        Get DAG yaml from NuObject.
        @param dagrun_conf: dagrun parameters.
        @return: The local file path of DAG yaml.
        """
        s3_client = get_nuobject_client()
        bucket_name, object_name, domain, name, file_name = cls.parse_s3_path(dagrun_conf["path"])

        local_yaml_file = cls.get_local_yaml_file(dagrun_conf)
        if os.path.exists(local_yaml_file):
            shutil.move(local_yaml_file, f"{local_yaml_file}.bk")

        local_yaml_file = s3_client.fget_object(bucket_name, object_name, local_yaml_file)
        return local_yaml_file

    @classmethod
    def get_dag_config_file(cls, dagrun_conf):
        """
        Get DAG config from parameter
        @param dagrun_conf:
        @return:
        """
        config_text = dagrun_conf.get("config")
        if config_text:
            local_config_file = cls.get_local_config_file(dagrun_conf)
            logging.info(f"create dag config file: {local_config_file}")
            if os.path.exists(local_config_file):
                shutil.move(local_config_file, f"{local_config_file}.bk")

            with open(local_config_file, "w") as fd:
                fd.write(config_text)
            return local_config_file

    @classmethod
    def send_to_rivendell_server(cls, data):
        """
        Send request to rivendell server to notify operation result.
        @param data: Request payload.
        @return: None
        """
        endpoint = f"dags/{data['option']}/{data['id']}/finish"
        http = HttpHook(method="POST", http_conn_id="rivendell_conn")
        response = http.run(endpoint=endpoint, headers={"Content-Type": "application/json"}, data=json.dumps(data))
        print(response)

    @classmethod
    def render_file(cls, local_config_file, local_dag_file):
        """
        Generate DAG file through template.
        @param local_config_file: The DAG configuration.
        @param local_dag_file: The DAG file.
        @return: The local file of DAG file after rendered.
        """
        from jinja2 import Environment, FileSystemLoader

        template_path = os.path.join(Constants.DAG_FOLDER, "global/conf/public")
        template_file = cls.rivendell_config.get("base", "template_file")
        environment = Environment(loader=FileSystemLoader(template_path))
        template = environment.get_template(template_file)
        content = template.render(config_file=local_config_file)
        with open(local_dag_file, "w") as fd:
            fd.write(content)
            print(f"Generate DAG file {local_dag_file} succeed.")
        return local_dag_file

    @classmethod
    @provide_session
    def check_duplicate_dag(cls, dag_id, dagrun_conf, session: Session = None):
        """
        Check new DAG whether exists.
        If the same DAG is dynamic DAG created by rivendell, just cover it,
        otherwise return conflict.
        @param dagrun_conf:
        @param session:
        @return:
        """
        dag_file = cls.get_local_dag_file(dagrun_conf)
        if os.path.exists(dag_file):
            return False

        dag_entity = session.query(DagModel).filter(DagModel.dag_id == dag_id).first()
        if dag_entity:
            return True
        return False

    @classmethod
    def push_response_status(cls, status: DynamicDagState, err_message: str, **kwargs):
        kwargs["ti"].xcom_push(key="status", value=status.value)
        kwargs["ti"].xcom_push(key="err_message", value=err_message)

    @classmethod
    def dag_process_success_callback(cls, context):
        response_data = context["dag_run"].conf
        option = response_data['option']
        response_data["dagRunUrl"] = get_dag_run_link(context)
        dag_name, domain = response_data["dagId"], response_data["domain"]
        import_error = context["ti"].xcom_pull(task_ids=f"{option}_sensor")
        if import_error:
            print(f"Got DAG import error: {import_error}")
            response_data["status"] = DynamicDagState[f"{option.upper()}_ERROR"].value
            response_data["errMessage"] = import_error
        else:
            response_data["status"] = DynamicDagState[f"{option.upper()}ED"].value

        print(f"Parse DAG<dag_name={dag_name}, domain={domain}> File done, "
              f"response to rivendell server: {response_data}")
        cls.send_to_rivendell_server(response_data)

    @classmethod
    def dag_task_failure_callback(cls, context):
        response_data = context["dag_run"].conf
        option = response_data['option']
        dag_name, domain = response_data["dagId"], response_data["domain"]
        response_data["dagRunUrl"] = get_dag_run_link(context)

        status = context["ti"].xcom_pull(task_ids=f"{option}", key="status")
        if status:
            err_message = context["ti"].xcom_pull(task_ids=f"{option}", key="err_message")
            response_data["status"] = status
            response_data["errMessage"] = err_message
        else:
            response_data["status"] = DynamicDagState[option.upper()].value
            response_data["errMessage"] = f"{option} DAG unknown error."

        print(f"{option} DAG<dag_name={dag_name}, domain={domain}> failed, "
              f"response to rivendell server: {response_data}")
        cls.send_to_rivendell_server(response_data)

    @classmethod
    def dag_sensor_failure_callback(cls, context):
        response_data = context["dag_run"].conf
        option = response_data['option']
        dag_name, domain = response_data["dagId"], response_data["domain"]

        response_data["dagRunUrl"] = get_dag_run_link(context)
        response_data["status"] = DynamicDagState[f"{option.upper()}_TIMEOUT"].value
        response_data["errMessage"] = f"{option} DAG timeout"

        print(f"{option} DAG<dag_name={dag_name}, domain={domain}> timeout, "
              f"response to rivendell server: {response_data}")
        cls.send_to_rivendell_server(response_data)

    @classmethod
    def get_run_id_timestamp(cls, run_id):
        timestamp_string = run_id.rsplit("+", 1)[-1]
        # return datetime.strptime(timestamp_string, "%Y-%m-%dT%H:%M:%S.%f")
        return datetime.strptime(timestamp_string, "%Y-%m-%d.%H:%M:%S.%f")

    @classmethod
    @provide_session
    def check_dag_sequence(cls, dag_id, session: Session = None, **kwargs):
        """
        Airflow will receive several same request because timeout or other reasons.
        We must make sure the latest task is the only one to be run to avoid data inconsistency.
        @param dag_id:
        @param session:
        @param kwargs:
        @return:
        """
        run_id = kwargs["dag_run"].run_id
        print(f"Start check sequence current DagRun:{run_id}.")

        active_dagruns = session.query(DagRun).filter(DagRun.dag_id == dag_id)\
            .filter(or_(DagRun.state == DagRunState.RUNNING, DagRun.state == DagRunState.QUEUED))\
            .order_by(DagRun.execution_date.desc()).all()

        if not active_dagruns:
            return True, None

        current_datetime = cls.get_run_id_timestamp(run_id)
        for dagrun_entity in active_dagruns:
            exist_datetime = cls.get_run_id_timestamp(dagrun_entity.run_id)
            # time sequence out of order, ignore current request, otherwise cancel older request
            if exist_datetime >= current_datetime:
                err_message = f"DagRun:{dagrun_entity.run_id} is newer but still {dagrun_entity.state}, " \
                              f"ignore current DagRun:{run_id}."
                print(err_message)
                return False, err_message
            else:
                set_dag_run_state_to_failed(dag=kwargs["dag"], run_id=dagrun_entity.run_id, commit=True)
                print(f"DagRun:{dagrun_entity.run_id} is older but still {dagrun_entity.state}, need to be canceled.")

        return True, None

    @classmethod
    def check_dag_config(cls, dag_id, conf):
        """

        @param dag_id:
        @param conf:
        @param response_data:
        @return:
        """
        response = {}
        if not conf:
            print(f"Check DAG:{dag_id} config failed, conf is empty!")
            response["status"] = DynamicDagState.IMPORT_ERROR.value
            response["errMessage"] = f"Check DAG:{dag_id} config failed, conf is empty!"
            return False, response

        for key in dag_required_parameters:
            if conf.get(key) is None and key not in none_allowed_parameters:
                print(f"Check config failed, DAG level required parameter:{key} is missing")
                response["status"] = DynamicDagState.IMPORT_ERROR.value
                response["errMessage"] = f"Check config failed, DAG level required parameter:{key} is missing"
                return False, response

        if not isinstance(conf["tasks"], dict):
            print(f"Check config failed, tasks is not a dict")
            response["status"] = DynamicDagState.IMPORT_ERROR.value
            response["errMessage"] = f"Check config failed, tasks is not a dict"
            return False, response

        return True, response

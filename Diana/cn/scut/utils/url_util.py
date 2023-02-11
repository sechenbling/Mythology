import sys
import os
import urllib

from cn.scut.utils.constants import Constants


def get_dag_run_link(context):
    """

    @param context:
    @return:
    """
    dag_id = context["dag"].dag_id
    run_id = context["dag_run"].run_id
    return f"{Constants.WEBSERVER_URL}/dags/{dag_id}/grid?dag_run_id={run_id}"


def get_task_log(task_id, context):
    """

    @param task_id:
    @param context:
    @return:
    """
    return context["ti"].log_url

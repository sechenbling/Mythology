import os
import logging
import json
import requests


from cn.scut.utils.config import Config
from cn.scut.utils.constants import Constants
from cn.scut.utils.variables import get_webserver
from cn.scut.utils.url_util import get_dag_run_link


LOG = logging.getLogger(__name__)
NAMESPACE = Constants.CLUSTER_ENV
WEBSERVER_URL = get_webserver()


# severity: info, warning, error, critical
def send_pager(event_action, details, summary, source="AdsScheduler", severity="critical"):
    pagerduty_token = Config().PAGET_DUTY_TOKEN
    payload = {
        "routing_key": pagerduty_token,
        "event_action": event_action,
        "payload": {
            "summary": summary,
            "source": source,
            "severity": severity,
            "custom_details": details
        }
    }

    response = requests.post(url=Constants.PAGER_DUTY_HOST, data=json.dumps(payload),
                             headers={"Content-Type": "application/json"}, proxies=Constants.BASE_PROXY_DICT)
    LOG.info('pager response. status: {}, res: {}'.format(response.status_code, response.text))
    return response.text


def get_success_pager_msg(context):
    dag_id = context['dag'].dag_id
    dag_run_link = get_dag_run_link(context)
    return {
        "execute_date": context['logical_date'],
        "ref_url": dag_run_link,
    }


def get_fail_pager_msg(context):
    dag_id = context['dag'].dag_id
    dag_run_link = get_dag_run_link(context)
    return {
        "execute_date": context['logical_date'],
        "ref_url": dag_run_link,
        "failed_task": context['ti'].task_id,
        "log_url": context['ti'].log_url,
    }


def pager_success_callback(context):
    dag_id = context['dag'].dag_id
    owner = context['dag'].owner
    send_pager(event_action="trigger",
               details=get_success_pager_msg(context),
               summary=f"[{NAMESPACE}][Airflow][DAG:{dag_id}][Owner:{owner}] Succeed")


def pager_failed_callback(context):
    dag_id = context['dag'].dag_id
    owner = context['dag'].owner
    send_pager(event_action="trigger",
               details=get_fail_pager_msg(context),
               summary=f"[{NAMESPACE}][Airflow][][DAG:{dag_id}][Owner:{owner}] Failed")


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    custom_details = {"test": "test"}
    send_pager(event_action='trigger',
               details=custom_details,
               summary='test for DQ')



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


def request_slack(channel, blocks, attachment=None, thread_ts=None):
    payload = {
        "channel": channel,
        "blocks": json.dumps(blocks),
        "attachments": json.dumps(attachment) if attachment else "",
        "icon_emoji": ":ghost:"
    }
    # to replay a parent message
    if thread_ts:
        payload['thread_ts'] = thread_ts

    slack_token = Config().SLACK_BOT_TOKEN
    response = requests.post(url=Constants.SLACK_APP_HOST, headers={
        'Authorization': f'Bearer {slack_token}'
    }, data=payload, proxies=Constants.BASE_PROXY_DICT)

    LOG.info(f'slack response. status: {response.status_code}, res: {response.text}')
    return response.text


def get_header(owner, dag_id):
    return {
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": f"[{NAMESPACE}][DAG:{dag_id}][Owner:{owner}]",
            "emoji": True
        }
    }


def get_exec_date(date):
    return {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": f"> *Execution Date*:\n {date}"
        }
    }


def get_ref_link(dag_id):
    return {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": "> *<" + WEBSERVER_URL + f"/dags/{dag_id}/grid>*"
        }
    }


def get_task_info(task):
    return {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": f"> *Failed Task*:\n> *[TaskId:{task.task_id}[RunId:{task.run_id}]*\n> *<{task.log_url}>*"
        }
    }


def get_fail_slack_msg(context):
    owner = context['dag'].owner
    dag_id = context['dag'].dag_id
    exec_date = context["logical_date"]
    task_id = context["ti"].task_id
    dag_run_link = get_dag_run_link(context)
    return [{
        "type": "section",
        "text": {"type": "mrkdwn", "text": f"*{dag_id}*\n> execution_date:{exec_date}\n"
                                           f"> owner:{owner}\n> failed task: {task_id}"},
        "accessory": {"type": "button", "text": {"type": "plain_text", "text": "Failed", "emoji": True},
                      "style": "danger", "url": dag_run_link}}
    ]


def get_success_slack_msg(context):
    owner = context['dag'].owner
    dag_id = context['dag'].dag_id
    exec_date = context["logical_date"]
    dag_run_link = get_dag_run_link(context)
    return [{
        "type": "section",
        "text": {"type": "mrkdwn", "text": f"*{dag_id}*\n> execution_date:{exec_date}\n> owner:{owner}"},
        "accessory": {"type": "button", "text": {"type": "plain_text", "text": "Success", "emoji": True},
                      "style": "primary", "url": dag_run_link}}
    ]


def slack_failed_callback(context):
    request_slack(channel="ads-scheduler-dev", blocks=get_fail_slack_msg(context))


def slack_success_callback(context):
    request_slack(channel="ads-scheduler-dev", blocks=get_success_slack_msg(context))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    blocks = [{"type": "section",
               "text": {"type": "mrkdwn", "text": "*pykrylov_demo*\n> execution_date:2022-08-31\n> owner:yizshen"},
               "accessory": {"type": "button", "text": {"type": "plain_text", "text": "Success", "emoji": True},
                             "style": "primary", "url": WEBSERVER_URL + "/dags/python_operator_demo/grid"}}]

    request_slack(channel='ads-scheduler-dev', blocks=blocks)


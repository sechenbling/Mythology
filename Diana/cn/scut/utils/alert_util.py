from cn.scut.utils.slack_util import slack_success_callback, slack_failed_callback
from cn.scut.utils.pager_util import pager_failed_callback
from cn.scut.utils.variables import get_slack_enable, get_pager_enable


def alert_failed_callback(context):
    if get_slack_enable():
        slack_failed_callback(context)
    if get_pager_enable():
        pager_failed_callback(context)


def alert_success_callback(context):
    if get_slack_enable():
        slack_success_callback(context)


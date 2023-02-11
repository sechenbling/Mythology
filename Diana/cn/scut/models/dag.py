from typing import Optional
import datetime
import functools
import logging
from sqlalchemy import or_
from sqlalchemy.orm import Session

from airflow.models.dag import DAG as AirflowDag
from airflow.utils.session import provide_session
from airflow.models import XCom, Log, DagRun, TaskFail, TaskInstance
from airflow.utils.state import DagRunState

from cn.scut.models.work_space import WorkSpace
from cn.scut.utils.alert_util import alert_success_callback, alert_failed_callback
from cn.scut.utils.role_and_permission_util import get_role_from_dag

logger = logging.getLogger(__name__)

@provide_session
def clear_expire_metadata(context, session: Session = None):
    """

    @return:
    """
    clean_conf = context["var"]["json"].get("clean_conf")
    age = clean_conf["dag_run"]["age"] if clean_conf else 30
    deadline = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=age)

    dag_id = context["dag"].dag_id
    dagruns = session.query(DagRun).filter(DagRun.dag_id == dag_id, DagRun.execution_date < deadline) \
        .filter(or_(DagRun.state == DagRunState.SUCCESS, DagRun.state == DagRunState.FAILED))
    clean_dagruns = dagruns.all()
    if not clean_dagruns:
        logger.info("There is no DagRun need to delete")
        return

    logger.info(f"There is {len(clean_dagruns)} DagRun need to delete")

    dag_run_ids = [dagrun.run_id for dagrun in clean_dagruns]
    for cls in [XCom, TaskInstance, TaskFail]:
        session.query(cls).filter(cls.dag_id == dag_id, cls.run_id.in_(dag_run_ids)) \
            .delete(synchronize_session=False)
    dagruns.delete(synchronize_session=False)


def on_finish_callback(context, finish_callback, state_callback, alert_callback):
    # clear work space
    WorkSpace(context).clear_work_dir()
    clear_expire_metadata(context)

    # call finish callback
    if finish_callback:
        finish_callback(context)
    # call success or failure callback
    if state_callback:
        state_callback(context)
    # call slack alert callback
    if alert_callback:
        alert_callback(context)


class DAG(AirflowDag):

    def __init__(self, *args, **kwargs):
        on_sc_callback = functools.partial(
            on_finish_callback,
            finish_callback=kwargs.get('on_finish_callback', None),
            state_callback=args[20] if len(args) >= 21 else kwargs.get('on_success_callback', None),
            alert_callback=alert_success_callback
        )
        on_fail_callback = functools.partial(
            on_finish_callback,
            finish_callback=kwargs.get('on_finish_callback', None),
            state_callback=args[21] if len(args) >= 22 else kwargs.get('on_failure_callback', None),
            alert_callback=alert_failed_callback
        )
        l_args = list(args)
        if len(args) < 21:
            kwargs['on_success_callback'] = on_sc_callback
        else:
            l_args[20] = on_sc_callback
        if len(args) < 22:
            kwargs['on_failure_callback'] = on_fail_callback
        else:
            l_args[21] = on_fail_callback

        # DAG level permission grant for custom roles
        if not kwargs.get("access_control"):
            arg_tags = set(kwargs.get("tags", []))
            arg_owners = set()
            if kwargs.get("default_args"):
                arg_owners.update([kwargs["default_args"].get("owners", "airflow")])

            kwargs["access_control"] = get_role_from_dag(arg_tags, arg_owners)

        super().__init__(*tuple(l_args), **kwargs)

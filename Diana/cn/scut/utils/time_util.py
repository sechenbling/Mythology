import datetime
import re

import dateutil.parser as parser


def parse_default_run_id(run_id):
    """
    Parse airflow generated run_id
    @param run_id: e.g. scheduled__2022-09-20T10:00:00+08:00
    @return:
        dag_id
        datetime object
    """
    dag_id, timestamp_str = run_id.rsplit("__", 1)
    date = parser.parse(timestamp_str)
    print(f"dag_id:{dag_id}, datetime:{date.isoformat()}")
    return dag_id, date


def parse_adsscheduler_run_id(run_id):
    """
    Parse adsscheduler service generated run_id
    @param run_id: adsscheduler_{domain}_{version}__2022-09-20T10:00:00+08:00
    @return:
        dag_id
        datetime object
    """
    dag_str, timestamp_str = run_id.rsplit("__", 1)
    group = re.findall(r"adsscheduler_\{(.*?)\}_\{(.*?)\}", dag_str)
    if group and group[0]:
        domain, version = group[0][0], group[0][1]
    date = parser.parse(timestamp_str)
    print(f"domain:{domain}, version:{version}, datetime:{date.isoformat()}")
    return domain, version, date


if __name__ == "__main__":
    #parse_default_run_id("scheduled__2022-09-20T10:00:00+08:00")
    parse_adsscheduler_run_id("adsscheduler_{guidance}_{v1.0.0}__2022-09-20T10:00:00+08:00")

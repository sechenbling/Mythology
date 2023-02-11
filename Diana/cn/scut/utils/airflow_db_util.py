import collections
import logging
from datetime import datetime, timedelta

from airflow.providers.mysql.hooks.mysql import MySqlHook

logger = logging.getLogger(__name__)
DatabaseHook = MySqlHook(mysql_conn_id="backend_mysql_conn", schema="airflow005")


def query_dag_view_permission(dag_id):
    """
    @param dag_id:
    @return:
    """
    dag_view_menu = f"select id, name from ab_view_menu where name = 'DAG:{dag_id}';"
    dag_view_menu_res = DatabaseHook.get_records(sql=dag_view_menu)
    if not dag_view_menu_res:
        logger.warning(f"No record in ab_view_menu table of dag_id:{dag_id}.")
        return

    view_id = dag_view_menu_res[0][0]

    permission_view = f"select id from ab_permission_view where view_menu_id = {view_id};"
    permission_view_res = DatabaseHook.get_records(sql=permission_view)
    if not permission_view_res:
        logger.warning(f"No record found in ab_permission_view by view_menu_ids of view_id:{view_id}")
        return

    return permission_view


def insert_dag_permission_for_role(dag_id, permission_ids, role_name):
    """

    @param dag_id:
    @param permission_ids:
    @param role_name:
    @return:
    """
    query_role_id = f"select id from ab_role where name = '{role_name}';"
    query_role_id_res = DatabaseHook.get_records(sql=query_role_id)
    if not query_role_id_res:
        logger.warning(f"Can't find record in ab_role table of role:{role_name}.")
        return

    role_id = query_role_id_res[0]

    query_permission_id_for_role = f"select permission_view_id from ab_permission_view_role where " \
                                   f"role_id = {role_id} and permission_view_id in ({','.join(map(str, permission_ids))});"
    query_permission_id_for_role_res = DatabaseHook.get_records(sql=query_permission_id_for_role)
    if query_permission_id_for_role_res and len(query_permission_id_for_role_res) == len(permission_ids):
        logger.warning(f"Permission of dag:{dag_id} has already in ab_permission_view_role table of role:{role_name}.")
        return

    insert_permission_ids = list(set(permission_ids) - set(query_permission_id_for_role_res))
    insert_rows = []
    for permission_view_id in insert_permission_ids:
        insert_rows.append((permission_view_id, role_id))
    DatabaseHook.insert_rows("ab_permission_view_role",
                             rows=insert_rows,
                             target_fields=("permission_view_id", "role_id"))

    logger.info(f"Insert {len(insert_rows)} rows into ab_permission_view_role for role:{role_name}")




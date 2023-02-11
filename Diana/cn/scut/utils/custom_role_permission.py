import sys

from cn.scut.utils.variables import get_airflow_variable


def get_role_on_condition(arg_tags, arg_owners):
    """
    @param arg_tags:
    @param arg_owners:
    @return:
    """
    role_2_permission = {}
    role_permission_conf = get_airflow_variable("role_dag_permission_conf", None, deserialize_json=True)
    for role, constraint in role_permission_conf.items():
        constraint_tags = set(constraint.get("tags", []))
        if constraint_tags and len(constraint_tags & set(arg_tags)) == 0:
            continue

        constraint_owners = set(constraint.get("owners", []))
        if constraint_owners and len(constraint_owners & set([arg_owners])) == 0:
            continue

        role_2_permission[role] = {"can_dag_read", "can_dag_edit"}

    return role_2_permission

import pdb
import logging
from sqlalchemy.orm import Session
from sqlalchemy import insert
from airflow.security import permissions
from airflow.models import DagModel
from airflow.www.fab_security.sqla.models import (
    Role,
    Resource,
    Permission,
    assoc_permission_role,
)

from cn.scut.utils.access_airflow_metadata import sql_alchemy_engine
from cn.scut.utils.variables import get_airflow_variable
from cn.scut.utils.constants import Constants


logger = logging.getLogger(__name__)


def get_role_from_dag(arg_tags, arg_owners):
    """
    Define custom Role by DAG arguments
    @param arg_tags:
    @param arg_owners:
    @return role_2_permission: {role_name: [permissions]}
    """
    role_2_permission_conf = get_airflow_variable(Constants.ROLE_2_DAG_PERMISSION, None, deserialize_json=True)
    role_2_permission = {}
    for role, constraint in role_2_permission_conf.items():
        constraint_tags = set(constraint.get("tags", []))
        if constraint_tags and len(constraint_tags & arg_tags) == 0:
            continue

        constraint_owners = set(constraint.get("owners", []))
        if constraint_owners and len(constraint_owners & arg_owners) == 0:
            continue

        role_2_permission[role] = permissions.DAG_ACTIONS
    return role_2_permission


def insert_dag_permission_for_role(role_name: str, dag_id: str, add_if_not_exist=True):
    with Session(sql_alchemy_engine) as session:
        role_entity = session.query(Role).filter(Role.name == role_name).first()
        resource_entity = session.query(Resource).filter(Resource.name == f'DAG:{dag_id}').first()
        if not resource_entity:
            logger.warning(f"DAG:{dag_id} has not added into metadata right now!")
            return

        permission_entities = session.query(Permission).filter(Permission.resource_id == resource_entity.id).all()
        if not permission_entities:
            logger.warning(f"DAG:{dag_id} has no permission right now!")
            return

        permission_view_ids = [e.id for e in permission_entities]
        permission_role_entities = session.query(assoc_permission_role)\
            .filter(assoc_permission_role.c.permission_view_id.in_(tuple(permission_view_ids)),
                    role_entity.id == assoc_permission_role.c.role_id).all()
        permission_role_ids = [e.permission_view_id for e in permission_role_entities]

        insert_permission_ids = list(set(permission_view_ids) - set(permission_role_ids))
        if add_if_not_exist and insert_permission_ids:
            add_rows = [{"permission_view_id": id, "role_id": role_entity.id} for id in insert_permission_ids]
            session.execute(insert(assoc_permission_role), add_rows)
            session.commit()


if __name__ == '__main__':
    # insert_dag_permission_for_role("SampleUser", "ads_dynamic_dag_basic_demo")
    insert_dag_permission_for_role("SampleUser", "clean_zombie_task")


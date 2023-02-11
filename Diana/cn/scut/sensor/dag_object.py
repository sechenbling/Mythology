from sqlalchemy.orm import Session
from typing import Any, Callable, Iterable, Optional, Union, Sequence

from airflow.sensors.base import BaseSensorOperator
from airflow.models import DagModel, ImportError
from airflow.models.serialized_dag import SerializedDagModel
from airflow.utils.context import Context
from airflow.sensors.base import PokeReturnValue
from airflow.utils.session import provide_session


class DagObjectSensor(BaseSensorOperator):
    """
    Check DAG File has been parsed or removed in Airflow.
    """
    template_fields: Sequence[str] = ("query_dag_id", "dag_file_loc", "dag_hashcode")

    def __init__(self, query_dag_id: str, dag_file_loc: str, option: str, dag_hashcode: str = None, **kwargs):
        super().__init__(**kwargs)
        self.query_dag_id = query_dag_id
        self.dag_file_loc = dag_file_loc
        self.option = option
        self.dag_hashcode = dag_hashcode

    @provide_session
    def _check_dag_add(self, session: Session = None):
        error_entity = session.query(ImportError).filter(ImportError.filename == self.dag_file_loc).first()
        if error_entity:
            print(f"Check DAG:{self.query_dag_id} has import error")
            return PokeReturnValue(is_done=True, xcom_value=error_entity.stacktrace)

        dag_entity = session.query(DagModel).filter(DagModel.dag_id == self.query_dag_id).first()
        if dag_entity:
            print(f"Check DAG:{self.query_dag_id} has dag entity")
            if not dag_entity.has_import_errors:
                print(f"Check DAG:{self.query_dag_id} has no import error")
                assert dag_entity.fileloc == self.dag_file_loc
                return True
            else:
                print(f"Check DAG:{self.query_dag_id} has import error but no error record")
                return PokeReturnValue(is_done=True, xcom_value=f"DAG has import error but no error stacktrace")

        print(f"Check DAG:{self.query_dag_id} has no record in dag metadata")
        return False

    @provide_session
    def _check_dag_update(self, session: Session = None):
        serial_dag = SerializedDagModel.get(dag_id=self.query_dag_id, session=session)
        if not serial_dag:
            return PokeReturnValue(is_done=True, xcom_value=f"DAG does not exist when update")

        if serial_dag.dag_hash != self.dag_hashcode:
            print(f"Check DAG:{self.query_dag_id} hashcode:{self.dag_hashcode} != {serial_dag.dag_hash} has updated")
            return True

        print(f"Check DAG:{self.query_dag_id} hashcode:{serial_dag.dag_hash} didn't change")
        return False

    @provide_session
    def _check_dag_delete(self, session: Session = None):
        dag_entity = session.query(DagModel).filter(DagModel.dag_id == self.query_dag_id).first()
        if not dag_entity:
            return True
        return False

    def poke(self, context: Context) -> Union[bool, PokeReturnValue]:
        if self.option == "add":
            return self._check_dag_add()
        elif self.option == "update":
            return self._check_dag_update()
        elif self.option == "delete":
            return self._check_dag_delete()



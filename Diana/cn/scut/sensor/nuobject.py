"""
## NuObjectSensor
"""
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Pattern, Sequence, Type

from airflow.exceptions import AirflowException
from airflow.triggers.temporal import TimeDeltaTrigger
from airflow.models.baseoperator import BaseOperator

from cn.scut.utils.constants import Constants
from cn.scut.utils.nuobject import NuObjectClient
from cn.scut.utils.variables import get_nuobject_conf


class NuObjectSensor(BaseOperator):

    template_fields: Sequence[str] = ('bucket_name', 'object_name')

    def __init__(self,
                 *,
                 bucket_name: str,
                 object_name: str,
                 poke_interval: float,
                 timeout: float,
                 **kwargs):
        super().__init__(execution_timeout=timedelta(seconds=timeout), **kwargs)
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.poke_interval = poke_interval

        self.nuobject_conf = get_nuobject_conf(Constants.NUOBJECT_ACCOUNT)
        self.s3_client = None

    def execute(self, context):
        self.s3_client = NuObjectClient(**self.nuobject_conf)
        response = self.s3_client.object_exists(self.bucket_name, self.object_name)
        if response is None:
            conf = {
                "bucket_name": self.bucket_name,
                "object_name": self.object_name,
                "poke_interval": self.poke_interval
            }
            self.defer(trigger=TimeDeltaTrigger(timedelta(seconds=self.poke_interval)),
                       method_name="execute_complete", kwargs=conf)

    def execute_complete(self, context, event=None, **kwargs):
        self.s3_client = NuObjectClient(**self.nuobject_conf)
        response = self.s3_client.object_exists(kwargs["bucket_name"], kwargs["object_name"])
        if response is None:
            self.defer(trigger=TimeDeltaTrigger(timedelta(seconds=kwargs["poke_interval"])),
                       method_name="execute_complete", kwargs=kwargs)

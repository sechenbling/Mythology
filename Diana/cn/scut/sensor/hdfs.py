"""
## HdfsSensor
"""
import os
from datetime import timedelta
from typing import Sequence

from airflow.exceptions import AirflowException
from airflow.hooks.subprocess import SubprocessHook
from airflow.triggers.temporal import TimeDeltaTrigger
from airflow.models.baseoperator import BaseOperator
from airflow.utils.operator_helpers import context_to_airflow_vars

from cn.scut.utils.etl_env import EtlEnv


class HdfsSensor(BaseOperator):

    template_fields: Sequence[str] = ('filepath',)

    def __init__(self,
                 *,
                 etl_env: EtlEnv,
                 file_path: str,
                 poke_interval: float,
                 timeout: float,
                 **kwargs):
        super().__init__(execution_timeout=timedelta(seconds=timeout), **kwargs)
        self.etl_env = etl_env
        self.filepath = file_path
        self.poke_interval = poke_interval
        self.bash_command = f"hadoop fs -test -e {file_path}"

    def get_env(self, context):
        """Builds the set of environment variables to be exposed for the bash command"""
        self.etl_env.setup_hadoop_env()
        env = os.environ.copy()

        airflow_context_vars = context_to_airflow_vars(context, in_env_var_format=True)
        self.log.debug(
            'Exporting the following env vars:\n%s',
            '\n'.join(f"{k}={v}" for k, v in airflow_context_vars.items()),
        )
        env.update(airflow_context_vars)
        return env

    def execute(self, context):
        env = self.get_env(context)
        command_conf = {
            "cmd": self.bash_command,
            "env": env,
            "poke_interval": self.poke_interval
        }

        result = SubprocessHook().run_command(
            command=['bash', '-c', self.bash_command],
            env=env,
            output_encoding='utf-8'
        )

        if result.exit_code != 0:
            self.defer(trigger=TimeDeltaTrigger(timedelta(seconds=self.poke_interval)),
                       method_name="execute_complete", kwargs=command_conf)

    def execute_complete(self, context, event=None, **kwargs):
        result = SubprocessHook().run_command(
            command=['bash', '-c', kwargs["cmd"]],
            env=kwargs["env"],
            output_encoding='utf-8'
        )

        if result.exit_code != 0:
            self.defer(trigger=TimeDeltaTrigger(timedelta(seconds=kwargs["poke_interval"])),
                       method_name="execute_complete", kwargs=kwargs)

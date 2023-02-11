"""AdsFileSensor is to instantiate sensor for testing required data path or monitoring some event"""

from airflow.exceptions import AirflowException
from airflow.sensors.filesystem import FileSensor
from airflow.utils.log.logging_mixin import LoggingMixin

from cn.scut.sensor.hdfs import HdfsSensor
from cn.scut.sensor.nuobject import NuObjectSensor
from cn.scut.utils.enumerate import DataPathStrategyEnum
from cn.scut.dynamic_dag.dynamic_dag_util import sensor_default_time_conf, global_etl_envs


class AdsFileSensorImpl(object):
    """
    AdsFileSensor implementation
    """
    default_etl_env = global_etl_envs["apollo-rno"]["b_ads_sre"]

    @classmethod
    def hdfs_sensor(cls, task_id, file_path, interval, timeout):
        file_path_real = "viewfs:{}".format(file_path)
        return HdfsSensor(
            task_id=task_id,
            etl_env=cls.default_etl_env,
            file_path=file_path_real,
            poke_interval=interval,
            timeout=timeout
        )

    @classmethod
    def nuobject_sensor(cls, task_id, file_path, interval, timeout):
        _, _, bucket_name, object_name = file_path.split("/", 3)
        return NuObjectSensor(
            task_id=task_id,
            bucket_name=bucket_name,
            object_name=object_name,
            poke_interval=interval,
            timeout=timeout
        )

    @classmethod
    def local_fs_sensor(cls, task_id, file_path, interval, timeout):
        mode = "reschedule" if timeout >= sensor_default_time_conf["localfs"]["timeout"] else "poke"
        interval = sensor_default_time_conf["localfs"][f"{mode}_interval"]
        soft_fail = True if DataPathStrategyEnum.WAIT_UNTIL_THEN_GO else False

        return FileSensor(
            task_id=task_id,
            fs_conn_id='local_fs_mnt_conn',
            soft_fail=soft_fail,
            poke_interval=interval,
            mode=mode,
            timeout=timeout,
            filepath=file_path,
        )


class AdsFileSensor(LoggingMixin):
    """
    AdsFileSensor
    """
    type_2_sensor_map = {
        "hdfs": AdsFileSensorImpl.hdfs_sensor,
        "nuobject": AdsFileSensorImpl.nuobject_sensor,
        "localfs": AdsFileSensorImpl.local_fs_sensor,
    }

    storage_type_2_sensor_type = {
        "viewfs": "hdfs",
        "s3": "nuobject",
        "localfs": "localfs",
    }

    fail_fast_timeout = 5

    def parse_sensor_strategy(self, strategy):
        if strategy == "wait_finished":
            return DataPathStrategyEnum.WAIT_FINISHED
        elif strategy == "wait_until_then_go":
            return DataPathStrategyEnum.WAIT_UNTIL_THEN_GO
        return DataPathStrategyEnum.FAIL_FAST

    def get_sensor(self, task_id, path):
        """path: {store_type}:/{file_path}+[strategy:{strategy}]+[timeout:{timeout}]"""
        path_fields = path.split("+")
        if len(path_fields) > 3:
            raise AirflowException(f"Invalid required data path: {path}")

        storage_type, file_path = path_fields[0].split(":", 1)
        sensor_type = AdsFileSensor.storage_type_2_sensor_type.get(storage_type)
        if sensor_type is None:
            raise AirflowException(f"Unsupported required data storage type: {storage_type}")

        strategy = DataPathStrategyEnum.FAIL_FAST
        timeout = 0
        for item in path_fields[1:]:
            key, value = item.split(":", 1)
            if key == "strategy":
                strategy = self.parse_sensor_strategy(value)
            elif key == "timeout":
                timeout = int(value)
            else:
                raise AirflowException(f"Unsupported required data path field {key}:{value}")

        interval = sensor_default_time_conf[sensor_type].get("interval")
        # timeout priority: fail_fast timeout > customized timeout < default timeout
        if strategy == DataPathStrategyEnum.FAIL_FAST:
            timeout = AdsFileSensor.fail_fast_timeout
        elif timeout == 0:
            timeout = sensor_default_time_conf[sensor_type]["timeout"]

        self.log.info("Set sensor {} of strategy:{} interval:{} timeout:{}".format(
            storage_type, strategy, interval, timeout))

        return AdsFileSensor.type_2_sensor_map[sensor_type](
                task_id, file_path, interval, timeout)

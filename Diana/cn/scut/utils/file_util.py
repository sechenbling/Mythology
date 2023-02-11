import os
import sys
import shutil
import logging
import yaml
import json

from airflow.exceptions import AirflowException

from cn.scut.utils.etl_env import EtlEnv
from cn.scut.utils.constants import Constants
from cn.scut.utils.nuobject import NuObjectClient, SUPPORTED_PACKAGE_TYPE
from cn.scut.utils.variables import get_nuobject_conf
from cn.scut.utils.common_tasks import exec_bash_command

log = logging.getLogger(__name__)


def read_oneline_file(filename):
    with open(filename) as f:
        context = f.read()
    return context.strip('\n')


def check_package_ext(package):
    """
    Check package extension validation.
    @param package: the package name.
    @return: True if valid otherwise False.
    """
    ext = os.path.splitext(package)
    if ext[-1] in SUPPORTED_PACKAGE_TYPE:
        if ext[-1] == ".gz" and os.path.splitext(ext[0])[-1] != ".tar":
            return False
    return True


def parse_nuobject_artifact(file_path, work_dir, save_dir):
    _, _, bucket_name, object_name = file_path.split("/", 3)
    if not check_package_ext(object_name):
        raise AirflowException(f"NuObject object:{file_path} is not a supported package.")

    if not os.path.exists(os.path.join(work_dir, object_name)):
        nuobject_conf = get_nuobject_conf(Constants.NUOBJECT_ACCOUNT)
        s3_client = NuObjectClient(**nuobject_conf)
        s3_client.fget_object_and_unpack(bucket_name, object_name, work_dir, save_dir)


def parse_hdfs_artifact(file_path, work_dir, save_dir):
    _, _, cluster, _ = file_path.split("/", 3)
    if cluster not in Constants.HADOOP_BATCH_ACCOUNTS:
        raise AirflowException(f"Unsupported cluster:{cluster} in HDFS:{file_path}")

    file_name = os.path.basename(file_path)
    if not check_package_ext(file_name):
        raise AirflowException(f"HDFS:{file_path} is not a supported package.")

    if not os.path.exists(os.path.join(work_dir, file_name)):
        etl_env = EtlEnv(batch_account="b_ads_sre", cluster="apollo-rno")
        etl_env.setup_hadoop_env()
        exec_bash_command(cmd=f'hadoop fs -get viewfs:{file_path} {work_dir}')
        artifact_path = os.path.join(work_dir, file_name)
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)
        shutil.unpack_archive(artifact_path, save_dir)


def parse_local_artifact(file_path, work_dir, save_dir):
    if not check_package_ext(file_path):
        raise AirflowException(f"Local file:{file_path} is not a supported package.")

    if not os.path.exists(file_path):
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)
        shutil.unpack_archive(file_path, save_dir)


def yaml_2_json(yaml_file, json_file):
    """

    @param yaml_file:
    @param json_file:
    @return:
    """
    with open(yaml_file, 'r') as fd:
        configuration = yaml.safe_load(fd)

    with open(json_file, 'w') as fd:
        json.dump(configuration, fd)

    output = json.dumps(json.load(open(json_file)), indent=2)
    print(output)


def json_2_yaml(json_file, yaml_file):
    """
    @param yaml_file:
    @param json_file:
    @return:
    """
    with open(json_file, 'r') as fd:
        configuration = json.load(fd)

    with open(yaml_file, 'w') as fd:
        yaml.dump(configuration, fd)

    with open(yaml_file, 'r') as fd:
        print(fd.read())


storage_type_2_parser = {
    "viewfs": parse_hdfs_artifact,
    "s3": parse_nuobject_artifact,
    "localfs": parse_local_artifact
}


def parse_artifact(input_path, work_dir, save_dir):
    """
    parse, download and unpack artifact from HDFS, NuObject or local fs.

    @param input_path: the artifact path, format: {store_type}:/{file_path}.
    @param work_dir: the local work space to download the artifact.
    @param save_dir: the local directory to save the unpacked artifact.
    """
    store_type, file_path = input_path.split(":", 1)
    if store_type not in storage_type_2_parser:
        raise AirflowException(f"Unsupported storage type:{store_type} of artifact path:{file_path}")

    return storage_type_2_parser[store_type](file_path, work_dir, save_dir)

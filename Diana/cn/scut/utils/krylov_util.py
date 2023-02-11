import os
from typing import Dict

from cn.scut.utils.constants import Constants


def init_account(username, namespace, cluster):
    os.environ["KRYLOV_CLIENT_HOME"] = Constants.KRYLOV_CLIENT_HOME

    import pykrylov as krylov
    krylov.use_account(
        account_name=username,
        namespace=namespace,
        yubikey_required=False if username in Constants.KRYLOV_SERVICE_ACCOUNTS else True
    )

    krylov.util.switch_krylov(cluster)


def package_model(model_save_path: str, tar_name: str, source_file_list: list = None):
    from cn.scut.utils.common_tasks import exec_bash_command
    if source_file_list:
        source_files = ' '.join(source_file_list)
        cmd = f"cd {model_save_path} && tar zcvf {tar_name} {source_files}"
    else:
        cmd = f"cd {model_save_path} && tar zcvf {tar_name} *"

    exec_bash_command(cmd)

    return os.path.join(model_save_path, tar_name)


def download_model(local_save_path: str, project_name: str, model_name: str, version: str):
    import pykrylov as krylov
    model_save_path = os.path.join(local_save_path, project_name, model_name)
    result = krylov.mms_v2.model.download_version(
        project_name, model_name, version, model_save_path, overwrites=True,
        chunk_size=67108864, show_progress_bar=False, timeout=120)

    return model_save_path


def download_model_by_version(krylov_conf: Dict, model_conf: Dict, local_save_path: str):
    init_account(**krylov_conf)
    return download_model(local_save_path, **model_conf)


def download_last_model(krylov_conf: Dict, model_conf: Dict, local_save_path: str):
    init_account(**krylov_conf)

    import pykrylov as krylov
    version_list = krylov.mms_v2.model.show_model(model_conf['project_name'], model_conf['model_name'])
    versions = [n['version'] for n in version_list]
    versions.sort(reverse=True)
    last_version = versions[0]

    return download_model(local_save_path, model_conf['project_name'], model_conf['model_name'], last_version)


def take_last(metrics_ele):
    return metrics_ele['timestamp']


def list_metric(experiment_id, metrics_name):
    import pykrylov.ems as ems
    metrics_info = ems.read_metric(experiment_id, metrics_name)
    metrics = metrics_info['metrics']
    metrics.sort(key=take_last, reverse=True)
    print(metrics)
    return metrics[0]['value']

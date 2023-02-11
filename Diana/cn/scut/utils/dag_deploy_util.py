import os
import shutil

from airflow.exceptions import AirflowException
from cn.scut.utils.constants import Constants


def backup_and_recreate_directory(target_path, recreate=True):
    # cover if exist
    if os.path.exists(target_path):
        print(f"{target_path} already exist, backup and delete.")
        shutil.move(target_path, Constants.BACKUP_FOLDER)

    if recreate:
        os.makedirs(target_path)


def backup_and_move(origin_path, deploy_path, target_path):
    if not os.path.exists(origin_path):
        print(f"{origin_path} not exist, please check.")
        return

    print(f"{origin_path} already exist, backup and move.")
    shutil.move(origin_path, Constants.BACKUP_FOLDER)
    shutil.move(deploy_path, target_path)


def delete_and_recovery(deploy_path, target_file):
    shutil.rmtree(f"{deploy_path}/{target_file}")
    shutil.move(f"{Constants.BACKUP_FOLDER}/{target_file}", deploy_path)


def clear_backup():
    if os.path.exists(Constants.BACKUP_FOLDER):
        shutil.rmtree(Constants.BACKUP_FOLDER)
    os.makedirs(f"{Constants.BACKUP_FOLDER}/submodules")


def rollback(new_files=[]):
    """
    Recover last deployment.
    @new_file: new files in this git sync need to delete
    @return:
    """
    for file in new_files:
        shutil.rmtree(f"{Constants.DAG_FOLDER}/{file}")

    for file in os.listdir(Constants.BACKUP_FOLDER):
        # recovery ads libs
        if file == "ebay":
            delete_and_recovery(Constants.DAG_LIBS, file)
        elif file == "submodules":
            for sub_module in os.listdir(f"{Constants.BACKUP_FOLDER}/{file}"):
                delete_and_recovery(f"{Constants.DAG_FOLDER}/{file}", sub_module)
        else:
            delete_and_recovery(Constants.DAG_FOLDER, file)

    print("Rollback succeed.")

    clear_backup()


def deploy(**kwargs):
    """
    Deploy dags to git_sync folder.
    @param kwargs: context of DAG Context
    @return:
    """
    repo_path = kwargs["ti"].xcom_pull(key="local_repo_path")
    if not repo_path:
        raise AirflowException("Get git_repository_dir failed!")

    clear_backup()

    new_files = []
    try:
        for path_name in os.listdir(repo_path):
            deploy_path = os.path.join(repo_path, path_name)
            # deploy ads-lib
            if path_name == "ebay":
                backup_and_move(f"{Constants.DAG_LIBS}/{path_name}", deploy_path, Constants.DAG_LIBS)
            # deploy conf
            elif path_name == "conf":
                backup_and_move(f"{Constants.DAG_FOLDER}/{path_name}", deploy_path, Constants.DAG_FOLDER)
            # deploy dag files
            elif path_name == "dags":
                for file in os.listdir(deploy_path):
                    if os.path.exists(f"{Constants.DAG_FOLDER}/{file}"):
                        shutil.move(f"{Constants.DAG_FOLDER}/{file}", Constants.BACKUP_FOLDER)
                    else:
                        new_files.append(file)
                    shutil.move(f"{deploy_path}/{file}", Constants.DAG_FOLDER)
            # deploy sub modules
            elif path_name == "submodules":
                for file in os.listdir(deploy_path):
                    if os.path.exists(f"{Constants.DAG_SUBMODULE_FOLDER}/{file}"):
                        shutil.move(f"{Constants.DAG_SUBMODULE_FOLDER}/{file}", f"{Constants.BACKUP_FOLDER}/submodules")
                    else:
                        new_files.append(f"submodules/{file}")
                    shutil.move(f"{deploy_path}/{file}", Constants.DAG_SUBMODULE_FOLDER)
            else:
                print("Unsupported path:{} under repository root!".format(path_name))
    except Exception as e:
        rollback(new_files)
        raise AirflowException(f"Deploy ads infra airflow failed, rollback!")

    # clear backup and report deploy succeed
    clear_backup()

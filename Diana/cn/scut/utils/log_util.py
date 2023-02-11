import os
from cn.scut.utils.constants import Constants


def get_task_exec_log_folder(context):
    from airflow.utils.log.log_reader import TaskLogReader
    tr = TaskLogReader()
    log_temp = tr.render_log_filename(context['ti'])
    return os.path.join(Constants.BASE_LOG_DIR, os.path.dirname(log_temp))


def get_appid_from_spark_log(fname):
    import re
    pattern = re.compile(r".*tracking URL:.*(application[_0-9]+)/?", re.MULTILINE)
    try:
        with open(fname, "r") as f:
            for line in f:
                results = pattern.findall(line)
                if results and len(results) > 0:
                    return results[0]
    except Exception as e:
        print(e)
        return None


def get_krylov_job_id_from_log(fname, regex_pattern):
    import re
    pattern = re.compile(regex_pattern, re.MULTILINE)
    try:
        with open(fname, "r") as f:
            for line in f:
                results = pattern.findall(line)
                if results and len(results) == 1:
                    if isinstance(results[0], str):
                        return results[0]
    except Exception as e:
        print(e)
    return


def remove_logs(log_files):
    if not log_files:
        pass
    if type(log_files) == str:
        logfiles = [log_files]
    for f in log_files:
        try:
            print(f"Delete file: {f}")
            os.remove(f)
        except Exception as e:
            print(e)

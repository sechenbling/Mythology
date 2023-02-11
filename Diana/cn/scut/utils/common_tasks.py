import copy
from airflow.exceptions import AirflowException
from airflow.hooks.subprocess import SubprocessHook


def exec_bash_command(cmd, env=None):
    result = SubprocessHook().run_command(command=['bash', '-c', cmd], env=env)
    if result.exit_code != 0:
        raise AirflowException("f'Bash command failed. The command returned a non-zero exit code {result.exit_code}.")
    return result.output


def get_func_from_str(func_name):
    if func_name is None:
        return func_name

    import importlib
    if callable(func_name):
        return func_name
    elif isinstance(func_name, str):
        if '.' in func_name:
            mod_name, function = func_name.rsplit('.', 1)
            mod = importlib.import_module(mod_name)
            func = getattr(mod, function)
            return func
        else:
            return eval(func_name)
    else:
        raise AirflowException(f"Cannot find function from {func_name}")


def parse_inherit(parent_conf, child_conf, first_level=True):
    """
    Inherit parent_conf and update the value of the same key in the child_conf.
    Note: Don't support inheritance of elements in list one by one.
    @param parent_conf: The inherited conf.
    @param child_conf: The conf to inherit.
    @param first_level: Set True if it is the first call.
    @return: The updated conf(new object).
    """
    new_parent_conf = copy.deepcopy(parent_conf) if first_level else parent_conf
    if isinstance(child_conf, dict):
        for key, value in child_conf.items():
            if key not in new_parent_conf:
                new_parent_conf[key] = value
                continue

            if isinstance(value, dict):
                parse_inherit(new_parent_conf[key], child_conf[key], first_level=False)
            elif isinstance(value, list):
                new_parent_conf[key] = child_conf[key]
            else:
                new_parent_conf[key] = value
    elif isinstance(child_conf, list):
        new_parent_conf = child_conf
    else:
        new_parent_conf = child_conf

    return new_parent_conf

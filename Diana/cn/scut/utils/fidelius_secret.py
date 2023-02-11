import os

from airflow.exceptions import AirflowException
from cn.scut.utils.constants import Constants
from cn.scut.utils.file_util import read_oneline_file


class FideliusSecretHandler(object):
    """
    Get secret by fidelius.
    """
    secret_dir = Constants.FIDELIUS_SECRET_DIR

    def __init__(self):
        self.secret_dict = {}

    def load_all(self):
        for secret_file in os.listdir(FideliusSecretHandler.secret_dir):
            if os.path.isfile(secret_file):
                secret_value = read_oneline_file(os.path.join(FideliusSecretHandler.secret_dir, secret_file))
            self.secret_dict[secret_file] = secret_value

    def get_secret_from_cache(self, secret):
        return self.secret_dict.get(secret)

    @classmethod
    def get_secret(cls, secret):
        secret_file = os.path.join(cls.secret_dir, secret)
        if os.path.exists(secret_file) and os.path.isfile(secret_file):
            return read_oneline_file(secret_file)
        raise AirflowException(f"Fidelius secret:{secret} not existed!")


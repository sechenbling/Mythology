import logging
import os


def get_all_etl_env():
    """ Instantiate all EtlEnv object in Constants"""
    from cn.scut.utils.constants import Constants
    etl_envs = {}
    for cluster, accounts in Constants.HADOOP_BATCH_ACCOUNTS.items():
        etl_envs.setdefault(cluster, {})
        for account in accounts:
            etl_envs[cluster][account] = EtlEnv(batch_account=account, cluster=cluster)
    return etl_envs


class EtlEnv:
    """
    Setup Hadoop/Spark/Hive/Yarn environment variables
    """
    def __init__(self, batch_account, cluster):
        self.batch_account = batch_account
        self.cluster = cluster
        self.spark_version = "V2_3_1"
        self.ticket_file_path = "/tmp/krb5cc_" + self.batch_account

    def __init_cache_ticket(self, ):
        logging.info("ticket cache location is {}".format(self.ticket_file_path))
        os.environ["KRB5CCNAME"] = self.ticket_file_path

    def setup_hadoop_env(self):
        self.__init_cache_ticket()
        os.environ["HADOOP_HOME"] = self.hadoop_home
        os.environ["HADOOP_CONF_DIR"] = os.path.join(self.base_conf_path, "hadoop/conf")
        os.environ["PATH"] += os.pathsep + os.environ["HADOOP_HOME"] + "/bin"

    def setup_spark_env(self, version):
        self.setup_hadoop_env()
        self.setup_hive_env()
        self.setup_yarn_env()
        self.spark_version = version
        os.environ["SPARK_HOME"] = self.spark_home
        os.environ["PATH"] += os.pathsep + os.environ["SPARK_HOME"] + "/bin"

    def setup_hive_env(self):
        os.environ["HIVE_HOME"] = self.hive_home
        os.environ["HIVE_CONF_DIR"] = os.path.join(self.base_conf_path, "hive/conf")
        os.environ["PATH"] += os.pathsep + os.environ["HIVE_HOME"] + "/bin"

    def setup_yarn_env(self):
        os.environ["YARN_CONF_DIR"] = os.path.join(self.base_conf_path, "hadoop/conf")

    @property
    def base_path(self):
        return "/apache/releases/" + self.cluster

    @property
    def base_conf_path(self):
        return "/apache/confs/" + self.cluster

    @property
    def hadoop_home(self):
        return self.base_path + "/hadoop"

    @property
    def spark_home(self):
        if self.spark_version == 'V3_1_1':
            return self.base_path + "/panda-client/spark3.1.1"
        return self.base_path + "/panda-client"

    @property
    def hive_home(self):
        return self.base_path + "/hive"

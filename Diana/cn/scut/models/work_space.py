import os
import shutil
import logging
from typing import Any
from airflow.utils.log.logging_mixin import LoggingMixin

from cn.scut.utils.constants import Constants

logger = logging.getLogger("cn.scut.models.work_space.WorkSpace")


class WorkSpace(LoggingMixin):
    def __init__(self, context: Any):
        super().__init__()
        self.dag_id = context['dag'].dag_id
        self.run_id = context['run_id'].replace(':', '').replace('-', '')
        self.__init_shared_dir()

    @property
    def dag_shared_dir(self):
        return Constants.WORK_SPACE_BASE + "/" + self.dag_id + "/shared"

    def __init_shared_dir(self):
        if self.dag_shared_dir and not os.path.exists(self.dag_shared_dir):
            logger.info("dag shared dir does not exist. create. {}".format(self.dag_shared_dir))
            os.makedirs(self.dag_shared_dir)

    @property
    def work_dir(self):
        return Constants.WORK_SPACE_BASE + "/" + self.dag_id + "/" + self.run_id

    def init_work_dir(self):
        if not os.path.exists(self.work_dir):
            logger.info("work space does not exist. create. {}".format(self.work_dir))
            os.makedirs(self.work_dir)

    def clear_work_dir(self):
        if os.path.exists(self.work_dir):
            logger.info("remove work space. {}".format(self.work_dir))
            shutil.rmtree(self.work_dir)

from cn.scut.utils.confighandler import ConfigHandler
from cn.scut.utils.file_util import read_oneline_file


class Config(object):
    @property
    def SLACK_BOT_TOKEN(self):
        filename = ConfigHandler.get_value('secret_config', 'slack', 'bot_token')
        return read_oneline_file(filename)

    @property
    def PAGET_DUTY_TOKEN(self):
        filename = ConfigHandler.get_value('secret_config', 'pager_duty', 'routing_key')
        return read_oneline_file(filename)

    @property
    def GIT_SYNC_TOKEN(self):
        filename = ConfigHandler.get_value('secret_config', 'git', 'sync_token')
        return read_oneline_file(filename)

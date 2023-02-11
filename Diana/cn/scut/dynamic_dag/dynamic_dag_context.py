import logging
import json
from json import JSONEncoder

logger = logging.getLogger(__name__)


class DynamicDagContextEncoder(JSONEncoder):
    def default(self, o):
        return o.__dict__


class DynamicDagContext(object):
    def __init__(self, yaml_conf):
        super().__init__()
        self.conf = yaml_conf

    def encode(self):
        return json.dumps(self, indent=4, cls=DynamicDagContextEncoder)

    @staticmethod
    def decode(encode_string):
        return json.loads(encode_string)

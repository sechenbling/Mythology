import os
from typing import Dict

from submodules.ammo.util import ammo_util
import json


def get_model_entity_info(darwin_conf: Dict, context):
    if 'ammo' in darwin_conf:
        project_name = darwin_conf["ammo"]["project_name"]
        model_name = darwin_conf["ammo"]["model_name"]
        response = ammo_util.get_model_info(project_name, model_name, context['run_id'])
        result = json.loads(response.text)['result']
        darwin_conf['model_instances'] = result['instances']
        darwin_conf['ml_app_version'] = result['mlAppVersion']

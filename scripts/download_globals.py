import sys
import os
from pathlib import Path
DIR = Path(__file__).absolute().parent.parent

# Add the aezpz directory to the Python path
sys.path.insert(0, str(DIR))

import aezpz
from aezpz.schema import ResourceType
import json

api = aezpz.load_config('auth.json')
global_resource_types = {}

def add_resource_type(resources, resource_type):
    for r in resources:
        uuid = r['meta:altId'].lstrip('_')
        global_resource_types[uuid] = resource_type + ' ' + r['$id']

add_resource_type(api.registry._paginate(container='global', resource=ResourceType.SCHEMA), 'schemas')
add_resource_type(api.registry._paginate(container='global', resource=ResourceType.CLASS), 'classes')
add_resource_type(api.registry._paginate(container='global', resource=ResourceType.FIELD_GROUP), 'fieldgroups')
add_resource_type(api.registry._paginate(container='global', resource=ResourceType.DATA_TYPE), 'datatypes')
add_resource_type(api.registry._paginate(container='global', resource=ResourceType.BEHAVIOR), 'behaviors')

f = DIR / 'aezpz/globals.json'
json.dump(global_resource_types, f.open('w'), indent='  ')
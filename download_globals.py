import aezpz
import pandas as pd
import json
from pathlib import Path

api = aezpz.load_config('auth.json')
global_resource_types = {}

def add_resource_type(resources, resource_type):
    for r in resources:
        global_resource_types[r.uuid] = resource_type + ' ' + r.ref

add_resource_type(api.schemas.findall(container='global'), 'schemas')
add_resource_type(api.classes.findall(container='global'), 'classes')
add_resource_type(api.field_groups.findall(container='global'), 'fieldgroups')
add_resource_type(api.data_types.findall(container='global'), 'datatypes')
add_resource_type(api.behaviors.findall(container='global'), 'behaviors')

f = Path(__file__).absolute().parent / 'aezpz/globals.json'
json.dump(global_resource_types, f.open('w'), indent='  ')
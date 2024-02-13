from __future__ import annotations
from functools import cached_property
from typing import Any, Optional, Literal, TYPE_CHECKING
import json
from pathlib import Path

SCRIPT_DIR = Path(__file__).absolute().parent
GLOBAL_RESOURCES = {
    k: v.split(' ')
    for k,v in json.loads((SCRIPT_DIR / 'globals.json').read_text()).items()
}

if TYPE_CHECKING:
    from .api import Api

RESOURCE_MAP = {
    'mixins': 'fieldgroups',
    'schemas': 'schemas',
    'classes': 'classes',
    'datatypes': 'datatypes',
    'data': 'behaviors',
}

def init_resource_class(resource, api: RequestHandler, body: dict):
    RESOURCE_CLASS = {
        'schemas': Schema,
        'fieldgroups': FieldGroup,
        'classes': Class,
        'datatypes': DataType,
        'behaviors': Behavior,
    }
    return RESOURCE_CLASS[resource](api, body)

def init_resource_from_ref(api, ref, resource: ResourceType = None) -> Resource:
    if isinstance(api, RequestHandler):
        api = api.api
    ref = SchemaRef(ref)
    if ref.resource is None and resource is None:
        raise Exception('Unable to determine resource type from id')
    if ref.resource is not None:
        if resource is not None:
            assert resource == ref.resource
        resource = ref.resource
    RESOURCE_CLASS = {
        'schemas': Schema,
        'fieldgroups': FieldGroup,
        'classes': Class,
        'datatypes': DataType,
        'behaviors': Behavior,
    }
    request_handler = RequestHandler(api, resource=resource, container=ref.container)
    return RESOURCE_CLASS[resource](request_handler, {
        '$id': ref.ref,
    })

ResourceType = Literal['fieldgroups','schemas','behaviors','classes','datatypes']
Container = Literal['global','tenant']

class SchemaRef:
    container: Container
    resource: ResourceType
    tenant: Optional[str]
    uuid: str
    id: str
    ref: str

    def __init__(self, ref):
        if ref.startswith('http://') or ref.startswith('https://'):
            ref = ref.split('://', 1)[-1]
            split = ref.split('/')
            if split[0] == 'ns.adobe.com':
                split = split[1:]
        elif ref.startswith('_'):
            split = ref[1:].split('.')
        else:
            raise Exception(f'unable to parse ref: "{ref}"')
        assert len(split) > 1

        uuid = '.'.join(split)
        if uuid in GLOBAL_RESOURCES:
            self.resource, self.ref = GLOBAL_RESOURCES[uuid]
            self.container = 'global'
            self.tenant = None
            self.uuid = uuid
            self.id = '_' + uuid
        elif split[1] in RESOURCE_MAP:
            assert len(split) == 3
            self.container = 'tenant'
            self.tenant = split[0]
            self.resource = RESOURCE_MAP[split[1]]
            self.uuid = split[2]
            self.id = '_' + uuid
            self.ref = 'https://ns.adobe.com/' + '/'.join(split)
        else:
            raise Exception(f'unable to parse ref: "{ref}"')

class RequestHandler:
    api: Api
    container: Container
    resource: ResourceType

    def __init__(self, api: Api, container: Container, resource: ResourceType):
        assert container in ('global','tenant'), f'unknown container: "{container}"'
        assert resource in ('fieldgroups','schemas','behaviors','classes','datatypes'), f'unknown resource: "{resource}"'
        self.api = api
        self.container = container
        self.resource = resource

    def request(self, method, id=None, xed=None, xed_version=1, headers={}, **kwargs) -> list:
        # Set Accept Header
        if xed is None:
            headers['Accept'] = f'application/vnd.adobe.xed+json'
        else:
            headers['Accept'] = f'application/vnd.adobe.xed-{xed}+json'
        
        if xed_version is not None:
            headers['Accept'] += f'; version={xed_version}'

        path = f'/data/foundation/schemaregistry/{self.container}/{self.resource}'
        if id is not None:
            path += '/' + id

        return self.api.request(method, path, headers, **kwargs)

    def paginate(self, xed=None, **kwargs):
        records = []
        params = {}
        if len(kwargs):
            params['property'] = ','.join(
                k + '==' + v
                for k,v in kwargs.items()
            )
        more = True
        while more:
            r = self.request('GET', xed=xed, xed_version=None, params=params)
            records += r['results']
            if r['_page'].get('next') is not None:
                params['start'] = r['_page']['next']
            else:
                more = False
        return records

class ResourceCollection:
    """ Responsible for handling requests for a collection of resources (get, search, create) """
    api: Api
    resources: list[ResourceType]

    def __init__(self, api: Api, resources: list[ResourceType] = ['schemas','fieldgroups','classes','datatypes','behaviors']):
        self.api = api
        self.resources = resources
    
    def get(self, id, full:bool=False) -> Resource:
        resource_type = self.resources[0] if len(self.resources) == 1 else None
        resource = init_resource_from_ref(id, resource=resource_type)
        resource.get(full=full)
        return resource

    def find(self,
             container: Optional[Container] = None,
             full: bool = True,
             **params) -> Resource:
        resources = self.findall(container=container, full=full, **params)
        if len(resources) == 0:
            raise Exception(f'Could not find resource')
        if len(resources) > 1:
            raise Exception(f'Multiple resources match the parameters')
        return resources[0]

    def findall(self,
               container: Optional[Container] = None,
               full: bool = False,
               **params
            ) -> list[Resource]:
        containers = ['tenant','global']
        if container is not None:
            assert container in ('tenant','global')
            containers = [ container ]
        results = []
        for resource in self.resources:
            for container in containers:
                request_handler = RequestHandler(self.api, container, resource)
                for record in request_handler.paginate(xed=None if full else 'id', **params):
                    results.append(init_resource_class(resource, request_handler, record))
        return results

class Resource:
    api: RequestHandler
    body: dict
    type: Literal['schema','class','field_group','data_type','behavior'] = None

    def __init__(self, api: RequestHandler, body):
        self.api = api
        self.body = body
        ref = SchemaRef(body['$id'])
        self.id = ref.id
        self.uuid = ref.uuid
        self.ref = ref.ref
        self.tenant = ref.tenant
        self.container = ref.container
        self.type = self.__class__.type
    
    @property
    def version(self):
        if 'version' not in self.body:
            self.get(full=False)
        return self.body['version']
    
    @property
    def title(self):
        if 'title' not in self.body:
            self.get(full=False)
        return self.body['title']

    @title.setter
    def title(self, value):
        self.api.request('PATCH', id=self.id, json=[{
            'op': 'replace',
            'path': '/title',
            'value': value,
        }])
        self.body['title'] = value

    @property
    def description(self):
        if 'description' not in self.body:
            self.get(full=False)
        return self.body['description']

    @description.setter
    def description(self, value):
        self.api.request('PATCH', id=self.id, json=[{
            'op': 'replace',
            'path': '/description',
            'value': value,
        }])
        self.body['description'] = value
    
    @property
    def properties(self):
        if 'properties' not in self.body:
            self.get(full=True)
        self.body.setdefault('properties', {})
        return self.body['properties']

    @property
    def definitions(self):
        if 'allOf' not in self.body:
            self.get(full=False)
        self.body.setdefault('allOf', [])
        properties = {}
        for record in self.body['allOf']:
            definition = record
            if record.get('$ref','').startswith('#'):
                assert 'properties' not in record, 'unexpected "properties" and "$ref" definition'
                assert record['$ref'].startswith('#/definitions/'), 'unexpected non-definitions reference'
                field = record['$ref'][len('#/definitions/'):]
                assert '/' not in field, 'unexpected nested definition reference'
                definition = self.body.get('definitions',{}).get(field)
                assert definition is not None, 'reference to missing definition'
                assert 'properties' in definition, 'expected definition to be an object'

            for key,val in definition.get('properties',{}).items():
                assert key not in properties, 'unhandled merging of definitions'
                properties[key] = val
        return properties

    @property
    def extends(self) -> list[Resource]:
        if 'meta:extends' not in self.body:
            self.get(full=False)
        self.body.setdefault('meta:extends', [])
        extends = []
        for ref in self.body['meta:extends']:
            extends.append(init_resource_from_ref(self.api, ref))
        return extends
    
    def get(self, full=True):
        r = self.api.request('GET', id=self.id, xed='full' if full else None)
        self.body.update(**r)
        return self
    
    def delete(self):
        self.api.request('DELETE', id=self.id)

    def __repr__(self):
        return '<{class_name} {uuid}{title}{version}>'.format(
            class_name=self.__class__.__name__,
            uuid=self.uuid,
            title=f' title="{self.title}"' if 'title' in self.body else '',
            version=f' version="{self.version}"' if 'version' in self.body else '',
        )

class SchemaCollection(ResourceCollection):
    def __init__(self, api: Api):
        super().__init__(api, resources=['schemas'])
    
    def get(self, id) -> Schema:
        return super().get(id)
    
    def find(self, container:Optional[Container] = None, full:bool=True, **params) -> Schema:
        return super().find(container, full, **params)
    
    def findall(self, container:Optional[Container] = None, full:bool=False, **params) -> list[Schema]:
        return super().findall(container, full, **params)
    
    def create(
            self,
            title: str,
            parent: Class,
            description: str='',
            field_groups: list[FieldGroup] = [],
        ) -> Schema:
        assert isinstance(parent, Class), 'Must inherit from a class'
        for field_group in field_groups:
            assert isinstance(field_group, FieldGroup)

        request_handler = RequestHandler(self.api, 'tenant', 'schemas')
        body = request_handler.request('POST', json={
            'type': 'object',
            'title': title,
            'description': description,
            'allOf': [
                { '$ref': ref.ref }
                for ref in ([parent] + field_groups)
            ]
        })
        return Schema(request_handler, body)


class Schema(Resource):
    type: Literal['schema'] = 'schema'

    @property
    def parent(self) -> Class:
        if 'meta:class' not in self.body:
            self.get()
        return init_resource_from_ref(self.api, self.body['meta:class'], 'classes')

    @property
    def behavior(self) -> Behavior:
        behaviors = [r for r in self.extends if r.type == 'behavior']
        assert len(behaviors) == 1
        return behaviors[0]

    @property
    def field_groups(self) -> list[FieldGroup]:
        return [resource for resource in self.extends if resource.type == 'field_group']

    def add_field_group(self, field_group: FieldGroup):
        assert isinstance(field_group, FieldGroup)
        r = self.api.request('PATCH', id=self.id, json=[
            { 'op': 'add', 'path': '/allOf/-', 'value': {'$ref': field_group.id} }
        ])
        return self(**r)

class ClassCollection(ResourceCollection):
    def __init__(self, api: Api):
        super().__init__(api, resources=['classes'])
    
    def get(self, id) -> Class:
        return super().get(id)
    
    def find(self, container:Optional[Container] = None, full:bool=True, **params) -> Class:
        return super().find(container, full, **params)
    
    def findall(self, container:Optional[Container] = None, full:bool=False, **params) -> list[Class]:
        return super().findall(container, full, **params)

    # TODO: also allow direct definitions of fields
    # TODO: make behavior default to "adhoc" if field_groups have been defined
    def create(
            self,
            title: str,
            behavior: Behavior,
            description: str = '',
            field_groups: list[FieldGroup] = [],
        ):
        assert isinstance(behavior, Behavior), 'Must inherit from a behavior'
        for field_group in field_groups:
            assert isinstance(field_group, FieldGroup)
        request_hander = RequestHandler(self.api, 'tenant', 'classes')
        body = request_hander.request('POST', json={
            'type': 'object',
            'title': title,
            'description': description,
            'allOf': [
                { '$ref': ref.ref }
                for ref in ([behavior] + field_groups)
            ]
        })
        return Class(request_hander, body)

class Class(Resource):
    type: Literal['class'] = 'class'
    
    @property
    def behavior(self) -> Behavior:
        behaviors = [r for r in self.extends if r.type == 'behavior']
        assert len(behaviors) == 1
        return behaviors[0]

    @property
    def field_groups(self) -> list[FieldGroup]:
        return [resource for resource in self.extends if resource.type == 'field_group']

class FieldGroupCollection(ResourceCollection):
    def __init__(self, api: Api):
        super().__init__(api, resources=['fieldgroups'])

    def get(self, id) -> FieldGroup:
        return super().get(id)
    
    def find(self, container:Optional[Container] = None, full:bool=True, **params) -> FieldGroup:
        return super().find(container, full, **params)
    
    def findall(self, container:Optional[Container] = None, full:bool=False, **params) -> list[FieldGroup]:
        return super().findall(container, full, **params)
    
    def create(self,
               title: str,
               description: str = '',
               fields: dict[str, dict] = {},
               extends: list[Class] = [],
               ):
        for r in extends:
            assert isinstance(r, Class)
        request_hander = RequestHandler(self.api, 'tenant', 'fieldgroups')
        body = request_hander.request('POST', json={
            'type': 'object',
            'title': title,
            'description': description,
            'meta:intendedToExtend': [ ctx.ref for ctx in extends ],
            'allOf': [{
                'properties': fields,
            }]
        })
        return FieldGroup(request_hander, body)
        

class FieldGroup(Resource):
    type: Literal['field_group'] = 'field_group'

    @property
    def extends(self):
        if 'meta:intendedToExtend' not in self.body:
            self.get(full=False)
        self.body.setdefault('meta:intendedToExtend', [])
        return [init_resource_from_ref(ref) for ref in self.body['meta:intendedToExtend']]

class DataTypeCollection(ResourceCollection):
    def __init__(self, api: Api):
        super().__init__(api, resources=['datatypes'])

    def get(self, id) -> DataType:
        return super().get(id)
    
    def find(self, container:Optional[Container] = None, full:bool=True, **params) -> DataType:
        return super().find(container, full, **params)
    
    def findall(self, container:Optional[Container] = None, full:bool=False, **params) -> list[DataType]:
        return super().findall(container, full, **params)
    
    def create(self,
            title: str,
            description: str = '',
            properties: dict[str, dict] = {},
            ):
        request_hander = RequestHandler(self.api, 'tenant', 'datatypes')
        body = request_hander.request('POST', json={
            'type': 'object',
            'title': title,
            'description': description,
            'properties': properties,
        })
        return DataType(request_hander, body)

class DataType(Resource):
    type: Literal['data_type'] = 'data_type'


class BehaviorCollection(ResourceCollection):
    adhoc: Behavior
    record: Behavior
    time_series: Behavior

    def __init__(self, api: Api):
        super().__init__(api, resources=['behaviors'])
        self.adhoc = init_resource_from_ref(self.api, 'https://ns.adobe.com/xdm/data/adhoc', 'behaviors')
        self.record = init_resource_from_ref(self.api, 'https://ns.adobe.com/xdm/data/record', 'behaviors')
        self.time_series = init_resource_from_ref(self.api, 'https://ns.adobe.com/xdm/data/time-series', 'behaviors')

    def get(self, id) -> Behavior:
        return super().get(id)
    
    def find(self, container:Optional[Container] = None, full:bool=True, **params) -> Behavior:
        return super().find(container, full, **params)
    
    def findall(self, container:Optional[Container] = None, full:bool=False, **params) -> list[Behavior]:
        return super().findall(container, full, **params)

class Behavior(Resource):
    type: Literal['behavior'] = 'behavior'
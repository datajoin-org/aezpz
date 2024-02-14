from __future__ import annotations
from functools import cached_property
from typing import Any, Optional, Literal, TYPE_CHECKING
import json
from pathlib import Path
from enum import Enum

if TYPE_CHECKING:
    from .api import Api

Container = Literal['global','tenant']
class ResourceType(Enum):
    DATA_TYPE = 0
    FIELD_GROUP = 1
    SCHEMA = 2
    CLASS = 3
    BEHAVIOR = 4

    @property
    def _class(self):
        return [DataType,FieldGroup,Schema,Class,Behavior][self.value]

    @property
    def path(self) -> str:
        return [
            'datatypes',
            'fieldgroups',
            'schemas',
            'classes',
            'behaviors',
        ][self.value]

    @staticmethod
    def from_name(val):
        return ({
            'mixins': ResourceType.FIELD_GROUP,
            'fieldgroups': ResourceType.FIELD_GROUP,
            'schemas': ResourceType.SCHEMA,
            'classes': ResourceType.CLASS,
            'datatypes': ResourceType.DATA_TYPE,
            'data': ResourceType.BEHAVIOR,
            'behaviors': ResourceType.BEHAVIOR,
        }).get(val)

SCRIPT_DIR = Path(__file__).absolute().parent
GLOBAL_RESOURCES = {
    k: v.split(' ')
    for k,v in json.loads((SCRIPT_DIR / 'globals.json').read_text()).items()
}

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
            resource, self.ref = GLOBAL_RESOURCES[uuid]
            self.resource = ResourceType.from_name(resource)
            assert self.resource is not None
            self.container = 'global'
            self.tenant = None
            self.uuid = uuid
            self.id = '_' + uuid
        elif len(split) == 3 and ResourceType.from_name(split[1]):
            self.container = 'tenant'
            self.tenant = split[0]
            self.resource = ResourceType.from_name(split[1])
            self.uuid = split[2]
            self.id = '_' + uuid
            self.ref = 'https://ns.adobe.com/' + '/'.join(split)
        else:
            raise Exception(f'unable to parse ref: "{ref}"')
    
    def init(self, api: Api):
        return self.resource._class(api, self)
        

def get_accept_header(xed=None, xed_version=1):
    if xed is None:
        accept_header = 'application/vnd.adobe.xed+json'
    else:
        accept_header = f'application/vnd.adobe.xed-{xed}+json'

    if xed_version is not None:
        accept_header += f'; version={xed_version}'

    return { 'Accept': accept_header }

def get_resource_path(container: Container, resource: ResourceType, id=None):
    assert container in ('global','tenant'), f'unknown container: "{container}"'
    assert isinstance(resource, ResourceType), f'unknown resource: "{resource}"'
    path = f'/data/foundation/schemaregistry/{container}/{resource.path}'
    if id is not None:
        path += '/' + id
    return path

class ResourceCollection:
    """ Responsible for handling requests for a collection of resources (get, search, create) """
    api: Api
    resources: list[ResourceType]

    def __init__(self, api: Api, resources: list[ResourceType] = []):
        self.api = api
        if len(resources) == 0:
            resources = [
                ResourceType.DATA_TYPE,
                ResourceType.FIELD_GROUP,
                ResourceType.SCHEMA,
                ResourceType.CLASS,
                ResourceType.BEHAVIOR,
            ]
        self.resources = resources
    
    def get(self, ref) -> Resource:
        ref = SchemaRef(ref)
        assert ref.resource in self.resources
        return ref.init(self.api)

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

    def _paginate(self, container, resource, full: bool = False, query: dict = {}):
        records = []
        params = {}
        if len(query):
            params['property'] = ','.join(
                k + '==' + v
                for k,v in query.items()
            )
        more = True
        while more:
            r = self.api.request('GET',
                                 path=get_resource_path(container, resource),
                                 headers=get_accept_header(
                                    xed='full' if full else None,
                                    xed_version=None
                                 ),
                                 params=params)
            records += r['results']
            if r['_page'].get('next') is not None:
                params['start'] = r['_page']['next']
            else:
                more = False
        return records

    def findall(self,
               container: Optional[Container] = None,
               full: bool = False,
               **query
            ) -> list[Resource]:
        containers = ['tenant','global']
        if container is not None:
            assert container in ('tenant','global')
            containers = [ container ]
        results = []
        for resource in self.resources:
            for container in containers:
                for record in self._paginate(container, resource, full, query=query):
                    results.append(resource._class(self.api, record))
        return results
    
    def _create(self, body) -> Resource:
        assert len(self.resources) == 1
        r = self.api.request('POST',
            path=get_resource_path(
                container='tenant',
                resource=self.resources[0],
            ),
            headers=get_accept_header(),
            json=body
        )
        return self.resources[0]._class(self.api, r)

class Resource:
    api: Api
    body: dict
    type: ResourceType

    def __init__(self, api: Api, body):
        if type(body) is str:
            ref = SchemaRef(body)
            body = { '$id': ref.ref }
        elif isinstance(body, SchemaRef):
            ref = body
            body = { '$id': ref.ref }
        elif type(body) is dict:
            ref = SchemaRef(body['$id'])
        else:
            raise TypeError(f'Unexpected body type: {body}')

        self.api = api
        self.body = body
        self.id = ref.id
        self.uuid = ref.uuid
        self.ref = ref.ref
        self.tenant = ref.tenant
        self.container = ref.container
        self.type = self.__class__.type
        assert self.type == ref.resource, 'Mismatched resource type'
    
    def request(self, method, full:bool=False, json=None):
        r = self.api.request(
            method=method,
            path=get_resource_path(
                container=self.container,
                resource=self.type,
                id=self.id,
            ),
            headers=get_accept_header(
                xed='full' if full else None
            ),
            json=json,
        )
        if r is not None:
            assert r['$id'] == self.ref
            assert r['meta:altId'] == self.id
            self.body.update(**r)
        return self
    
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
        self.request('PATCH', json=[{
            'op': 'replace',
            'path': '/title',
            'value': value,
        }])

    @property
    def description(self):
        if 'description' not in self.body:
            self.get(full=False)
        return self.body['description']

    @description.setter
    def description(self, value):
        self.request('PATCH', json=[{
            'op': 'replace',
            'path': '/description',
            'value': value,
        }])
    
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
            extends.append(SchemaRef(ref).init(self.api))
        return extends
    
    def get(self, full=True):
        return self.request('GET', full=full)
    
    def delete(self):
        self.request('DELETE')

    def __repr__(self):
        return '<{class_name} {uuid}{title}{version}>'.format(
            class_name=self.__class__.__name__,
            uuid=self.uuid,
            title=f' title="{self.title}"' if 'title' in self.body else '',
            version=f' version="{self.version}"' if 'version' in self.body else '',
        )

class SchemaCollection(ResourceCollection):
    def __init__(self, api: Api):
        super().__init__(api, resources=[ResourceType.SCHEMA])
    
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
        return self._create({
            'type': 'object',
            'title': title,
            'description': description,
            'allOf': [
                { '$ref': ref.ref }
                for ref in ([parent] + field_groups)
            ]
        })


class Schema(Resource):
    type = ResourceType.SCHEMA

    @property
    def parent(self) -> Class:
        if 'meta:class' not in self.body:
            self.get()
        return Class(self.api, self.body['meta:class'])

    @property
    def behavior(self) -> Behavior:
        behaviors = [r for r in self.extends if r.type == ResourceType.BEHAVIOR]
        assert len(behaviors) == 1
        return behaviors[0]

    @property
    def field_groups(self) -> list[FieldGroup]:
        return [resource for resource in self.extends if resource.type == ResourceType.FIELD_GROUP]

    def add_field_group(self, field_group: FieldGroup):
        assert isinstance(field_group, FieldGroup)
        r = self.request('PATCH', json=[
            { 'op': 'add', 'path': '/allOf/-', 'value': {'$ref': field_group.id} }
        ])
        return self(**r)

class ClassCollection(ResourceCollection):
    def __init__(self, api: Api):
        super().__init__(api, resources=[ResourceType.CLASS])
    
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
        ) -> Class:
        assert isinstance(behavior, Behavior), 'Must inherit from a behavior'
        for field_group in field_groups:
            assert isinstance(field_group, FieldGroup)
        return self._create({
            'type': 'object',
            'title': title,
            'description': description,
            'allOf': [
                { '$ref': ref.ref }
                for ref in ([behavior] + field_groups)
            ]
        })

class Class(Resource):
    type = ResourceType.CLASS
    
    @property
    def behavior(self) -> Behavior:
        behaviors = [r for r in self.extends if r.type == ResourceType.BEHAVIOR]
        assert len(behaviors) == 1
        return behaviors[0]

    @property
    def field_groups(self) -> list[FieldGroup]:
        return [resource for resource in self.extends if resource.type == ResourceType.FIELD_GROUP]

class FieldGroupCollection(ResourceCollection):
    def __init__(self, api: Api):
        super().__init__(api, resources=[ResourceType.FIELD_GROUP])

    def get(self, id) -> FieldGroup:
        return super().get(id)
    
    def find(self, container:Optional[Container] = None, full:bool=True, **params) -> FieldGroup:
        return super().find(container, full, **params)
    
    def findall(self, container:Optional[Container] = None, full:bool=False, **params) -> list[FieldGroup]:
        return super().findall(container, full, **params)
    
    def create(self,
               title: str,
               description: str = '',
               properties: dict[str, dict] = {},
               extends: list[Class] = [],
               ) -> FieldGroup:
        for r in extends:
            assert isinstance(r, Class)
        return self._create({
            'type': 'object',
            'title': title,
            'description': description,
            'meta:intendedToExtend': [ ctx.ref for ctx in extends ],
            'allOf': [{
                'properties': properties,
            }]
        })
        

class FieldGroup(Resource):
    type = ResourceType.FIELD_GROUP

    @property
    def extends(self):
        if 'meta:intendedToExtend' not in self.body:
            self.get(full=False)
        self.body.setdefault('meta:intendedToExtend', [])
        return [SchemaRef(ref).init(self.api) for ref in self.body['meta:intendedToExtend']]

class DataTypeCollection(ResourceCollection):
    def __init__(self, api: Api):
        super().__init__(api, resources=[ResourceType.DATA_TYPE])

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
            ) -> DataType:
        return self._create({
            'type': 'object',
            'title': title,
            'description': description,
            'properties': properties,
        })

class DataType(Resource):
    type = ResourceType.DATA_TYPE


class BehaviorCollection(ResourceCollection):
    adhoc: Behavior
    record: Behavior
    time_series: Behavior

    def __init__(self, api: Api):
        super().__init__(api, resources=[ResourceType.BEHAVIOR])
        self.adhoc = Behavior(self.api, 'https://ns.adobe.com/xdm/data/adhoc')
        self.record = Behavior(self.api, 'https://ns.adobe.com/xdm/data/record')
        self.time_series = Behavior(self.api, 'https://ns.adobe.com/xdm/data/time-series')

    def get(self, id) -> Behavior:
        return super().get(id)
    
    def find(self, container:Optional[Container] = None, full:bool=True, **params) -> Behavior:
        return super().find(container, full, **params)
    
    def findall(self, container:Optional[Container] = None, full:bool=False, **params) -> list[Behavior]:
        return super().findall(container, full, **params)

class Behavior(Resource):
    type = ResourceType.BEHAVIOR
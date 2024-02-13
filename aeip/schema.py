from __future__ import annotations
from functools import cached_property
from typing import Any, Optional, Literal, TYPE_CHECKING

if TYPE_CHECKING:
    from .api import Api

RESOURCE_MAP = {
    'mixins': 'fieldgroups',
    'schemas': 'schemas',
    'classes': 'classes',
    'datatypes': 'datatypes',
}

# There is a bug with adobe's api where some of these references can only be retrieved
# by their $id and not their altId
WEIRD_REFS = [
    ['https://ns.adobe.com/b2b/marketo/marketo-web-url', '_b2b.marketo.marketo-web-url', 'fieldgroups'],
    ['http://ns.adobe.com/adobecloud/core/1.0',          '_adobecloud.core.1.0',         'datatypes'],
    ['http://www.iptc.org/episode',                      '_www.iptc.org.episode',        'datatypes'],
    ['http://www.iptc.org/creator',                      '_www.iptc.org.creator',        'datatypes'],
    ['http://www.iptc.org/series',                       '_www.iptc.org.series',         'datatypes'],
    ['http://www.iptc.org/season',                       '_www.iptc.org.season',         'datatypes'],
    ['http://www.iptc.org/rating',                       '_www.iptc.org.rating',         'datatypes'],
    ['http://schema.org/GeoShape',                       '_schema.org.GeoShape',         'datatypes'],
    ['http://schema.org/GeoCoordinates',                 '_schema.org.GeoCoordinates',   'datatypes'],
    ['http://schema.org/GeoCircle',                      '_schema.org.GeoCircle',        'datatypes'],
    ['https://id3.org/id3v2.4/audio',                    '_id3.org.id3v2.4.audio',       'datatypes']
]

def init_resource_class(resource, api: RequestHandler, body: dict):
    RESOURCE_CLASS = {
        'schemas': Schema,
        'fieldgroups': FieldGroup,
        'classes': Classes,
        'datatypes': DataType,
        'behaviors': Behavior,
    }
    return RESOURCE_CLASS[resource](api, body)

ResourceType = Literal['fieldgroups','schemas','behaviors','classes','datatypes']
Container = Literal['global','tenant']

class SchemaRef:
    container: Container
    resource: Optional[ResourceType]
    tenant: Optional[str]
    uuid: str
    id: str
    ref: str

    def __init__(self, ref):
        split = [None, None]
        if ref.startswith('https://ns.adobe.com/'):
            split = ref[len('https://ns.adobe.com/'):].split('/')
        elif ref.startswith('_'):
            split = ref[1:].split('.')
        assert len(split) > 1

        if split[0] in ('xdm','experience'):
            self.container = 'global' 
            self.tenant = None
            self.resource = None
            if '.'.join(split[:2]) == 'xdm.data':
                self.resource = 'behaviors'
            self.uuid = '.'.join(split)
        elif split[1] in RESOURCE_MAP:
            assert len(split) == 3
            self.container = 'tenant'
            self.tenant = split[0]
            self.resource = RESOURCE_MAP[split[1]]
            self.uuid = split[2]
        else:
            for weird in WEIRD_REFS:
                if weird[0] == ref or weird[1] == ref:
                    self.container = 'global'
                    self.tenant = None
                    self.ref = weird[0]
                    self.id = weird[1]
                    self.resource = weird[2]
                    self.uuid = self.id[1:]
                    return
            raise Exception(f'Unable to parse reference: "{ref}"')

        self.id = '_' + '.'.join(split)
        self.ref = 'https://ns.adobe.com/' + '/'.join(split)

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
        ref = SchemaRef(id)
        resource = ref.resource
        if resource is None:
            if len(self.resources) > 1:
                # TODO: iterate through resources and see which one
                # doesn't throw a 404 error to determine resource type
                raise Exception('Unable to determine resource type from id')
            else:
                resource = self.resources[0]
        elif resource not in self.resources:
            raise Exception(f"Expected a resource of type ({','.join(self.resources)}) got {ref.resource}")
        request_handler = RequestHandler(self.api, ref.container, resource)
        body = request_handler.request('GET', id=ref.id, xed='full' if full else None)
        return init_resource_class(resource, request_handler, body)

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

    def __init__(self, api: RequestHandler, body):
        self.api = api
        self.body = body
        ref = SchemaRef(body['$id'])
        self.id = ref.id
        self.uuid = ref.uuid
        self.ref = ref.ref
        self.tenant = ref.tenant
        self.container = ref.container
    
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
            definition = record.get('properties', {})
            if record.get('$ref','').startswith('#'):
                assert 'properties' not in record, 'unexpected "properties" and "$ref" definition'
                assert record['$ref'].startswith('#/definitions/'), 'unexpected non-definitions reference'
                field = record['$ref'][len('#/definitions/'):]
                assert '/' not in field, 'unexpected nested definition reference'
                definition = self.body.get('definitions',{}).get(field)
                assert definition is not None, 'reference to missing definition'
            for key,val in definition:
                assert key not in properties, 'unhandled merging of definitions'
                properties[key] = val
        return properties
    
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
            extends: Classes,
            description: str='',
            field_groups: list[FieldGroup] = [],
        ) -> Schema:
        assert isinstance(extends, Classes), 'Must inherit from a class'
        for field_group in field_groups:
            assert isinstance(field_group, FieldGroup)

        request_hander = RequestHandler(self.api, 'tenant', 'schemas')
        body = request_hander.request('POST', json={
            'type': 'object',
            'title': title,
            'description': description,
            'allOf': [
                { '$ref': ref.ref }
                for ref in ([extends] + field_groups)
            ]
        })
        return Schema(request_hander, body)


class Schema(Resource):

    def extends(self):
        return self.api.api.classes.get(self.body['meta:class'])

    def field_groups(self):
        field_groups = []
        for ref in self.body['allOf']:
            if SchemaRef(ref['$ref']).resource == 'fieldgroups':
                field_group = self.api.api.field_groups.get(ref['$ref'])
                field_groups.append(field_group)
        return field_groups

    def add_field_group(self, field_group: FieldGroup):
        assert isinstance(field_group, FieldGroup)
        r = self.api.request('PATCH', id=self.id, json=[
            { 'op': 'add', 'path': '/allOf/-', 'value': {'$ref': field_group.id} }
        ])
        return self(**r)

class ClassCollection(ResourceCollection):
    def __init__(self, api: Api):
        super().__init__(api, resources=['classes'])
    
    def get(self, id) -> Classes:
        return super().get(id)
    
    def find(self, container:Optional[Container] = None, full:bool=True, **params) -> Classes:
        return super().find(container, full, **params)
    
    def findall(self, container:Optional[Container] = None, full:bool=False, **params) -> list[Classes]:
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
        return Classes(request_hander, body)

class Classes(Resource):
    pass

class FieldGroupCollection(ResourceCollection):
    def __init__(self, api: Api):
        super().__init__(api, resources=['fieldgroups'])

    def get(self, id) -> FieldGroup:
        return super().get(id)
    
    def find(self, container:Optional[Container] = None, full:bool=True, **params) -> FieldGroup:
        return super().find(container, full, **params)
    
    def findall(self, container:Optional[Container] = None, full:bool=False, **params) -> list[FieldGroup]:
        return super().findall(container, full, **params)

class FieldGroup(Resource):
    @classmethod
    def props(
            cls,
            tenant: str,
            title: str,
            description: str = '',
            fields: dict[str, dict] = {},
            extends: list[Classes] = [],
        ):
        return {
            'title': title,
            'description': description,
            'meta:intendedToExtend': [ ctx.id for ctx in extends ],
            'allOf': [{
                'properties': {
                    tenant: {
                        'type': 'object',
                        'properties': fields,
                    }
                }
            }]
        }

class DataTypeCollection(ResourceCollection):
    def __init__(self, api: Api):
        super().__init__(api, resources=['datatypes'])

    def get(self, id) -> DataType:
        return super().get(id)
    
    def find(self, container:Optional[Container] = None, full:bool=True, **params) -> DataType:
        return super().find(container, full, **params)
    
    def findall(self, container:Optional[Container] = None, full:bool=False, **params) -> list[DataType]:
        return super().findall(container, full, **params)

class DataType(Resource):
    pass

class BehaviorCollection(ResourceCollection):
    def __init__(self, api: Api):
        super().__init__(api, resources=['behaviors'])
    
    @cached_property
    def adhoc(self):
        return Behavior(
            RequestHandler(self.api, container='global', resource='behaviors'),
            {
                '$id': 'https://ns.adobe.com/xdm/data/adhoc',
                'title': 'Ad Hoc Schema'
            }
        )
    
    @cached_property
    def record(self):
        return Behavior(
            RequestHandler(self.api, container='global', resource='behaviors'),
            {
                '$id': 'https://ns.adobe.com/xdm/data/record',
                'title': 'Record Schema'
            }
        )

    @cached_property
    def time_series(self):
        return Behavior(
            RequestHandler(self.api, container='global', resource='behaviors'),
            {
                '$id': 'https://ns.adobe.com/xdm/data/time-series',
                'title': 'Time-series Schema'
            }
        )

    def get(self, id) -> Behavior:
        return super().get(id)
    
    def find(self, container:Optional[Container] = None, full:bool=True, **params) -> Behavior:
        return super().find(container, full, **params)
    
    def findall(self, container:Optional[Container] = None, full:bool=False, **params) -> list[Behavior]:
        return super().findall(container, full, **params)

class Behavior(Resource):
    pass
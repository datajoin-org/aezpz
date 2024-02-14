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
    """ Base class for all resource collections. 
    Can be used directly through the `registry` attribute of the API Instance to
    retrieve resources of any type

    Methods:
        get: Retrieves a resource based on the provided reference.
        find: Finds a resource based on the specified parameters.
        findall: Finds all resources based on the specified parameters.

    Examples:
        >>> api.registry.findall()
        [<Class xdm.classes.summarymetrics>, <Schema 7a5416d13571>, ...]
    """
    api: Api
    container: Optional[Container]
    resources: list[ResourceType]

    def __init__(self, api: Api, container:Optional[Container]=None, resources:list[ResourceType]=[]):
        self.api = api
        if len(resources) == 0:
            resources = [
                ResourceType.DATA_TYPE,
                ResourceType.FIELD_GROUP,
                ResourceType.SCHEMA,
                ResourceType.CLASS,
                ResourceType.BEHAVIOR,
            ]
        assert container is None or container in ('global','tenant')
        for resource in resources:
            assert isinstance(resource, ResourceType)
        self.resources = resources
        self.container = container
    
    @cached_property
    def containers(self) -> list[Container]:
        containers = ['tenant','global']
        if self.container is not None:
            containers = [ self.container ]
        return containers
    
    def get(self, ref: str) -> Resource:
        """
        Retrieves a resource based on the provided reference.

        Args:
            ref: The `$id` or `meta:altId` of the reference to retrieve.

        Returns:
            Resource: The retrieved resource.
        """
        ref = SchemaRef(ref)
        assert ref.resource in self.resources
        return ref.init(self.api)

    def find(self,
             full: bool = True,
             **params) -> Resource:
        """
        Finds a resource based on the specified parameters.

        Args:
            full: If True will use `vnd.adobe.xed-full+json` accept header.
            **params: Additional parameters for filtering the resources.

        Returns:
            Resource: The found resource.

        Raises:
            Exception: If no resource is found or multiple resources match the parameters.
        
        Examples:
            >>> api.registry.find(title='My Schema')
            <Schema 7a5416d13571 title="My Schema" version="1.0">
        """
        resources = self.findall(full=full, **params)
        if len(resources) == 0:
            raise Exception(f'Could not find resource')
        if len(resources) > 1:
            raise Exception(f'Multiple resources match the parameters')
        return resources[0]

    def _paginate(self, container, resource, full: bool = False, query: dict = {}) -> list[dict]:
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
               full: bool = False,
               **query
            ) -> list[Resource]:
        """
        Finds all resources based on the specified parameters.

        Args:
            full: If True will use `vnd.adobe.xed-full+json` accept header. Defaults to True.
            **query: Additional query parameters for filtering the resources.

        Returns:
            List[Resource]: The list of found resources.
        
        Examples:
            >>> api.registry.findall()
            [<Class xdm.classes.summarymetrics>, <Schema 7a5416d13571>, ...]
        """
        results = []
        for resource in self.resources:
            for container in self.containers:
                for record in self._paginate(container, resource, full, query=query):
                    results.append(resource._class(self.api, record))
        return results
    
    def _create(self, body) -> Resource:
        if self.container == 'global':
            raise Exception('cannot create global resource')
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

class SchemaCollection(ResourceCollection):
    """ Collection of Schema resources.
    
    Initialized through `api.schemas`, `api.global_schemas`, or `api.tenant_schemas`.
    
    Methods:
        get: Retrieves a schema based on the provided reference.
        find: Finds a schema based on the specified parameters.
        findall: Finds all schemas based on the specified parameters.
        create: Creates a new schema.
    
    Examples:
        Get a schema by reference
        >>> api.schemas.get('_mytenant.schemas.7a5416d13572')
        <Schema 7a5416d13572 title="My Schema" version="1.0">

        Find a schema by title
        >>> api.tenant_schemas.find(title='My Schema')
        <Schema 7a5416d13572 title="My Schema" version="1.0">

        List all global and tenant schemas
        >>> api.schemas.findall()
        [<Schema xdm.schemas.computed-attributes>, <Schema 7a5416d13572>, ...]

        List all global schemas
        >>> api.global_schemas.findall()
        [<Schema xdm.schemas.computed-attributes>, <Schema xdm.schemas.consentidmap>, ...]

        List all tenant schemas
        >>> api.tenant_schemas.findall()
        [<Schema 7a5416d13572>, <Schema 7a5416d13571>, ...]
    """
    def __init__(self, api: Api, container:Optional[Container]=None):
        super().__init__(api, container=container, resources=[ResourceType.SCHEMA])
    
    def get(self, id) -> Schema:
        return super().get(id)
    
    def find(self, full:bool=True, **params) -> Schema:
        return super().find(full, **params)
    
    def findall(self, full:bool=False, **params) -> list[Schema]:
        return super().findall(full, **params)
    
    def create(
            self,
            title: str,
            parent: Class,
            description: str='',
            field_groups: list[FieldGroup] = [],
        ) -> Schema:
        """
        Create a new schema.

        Args:
            title: The title of the schema.
            parent: The parent class that the schema inherits from.
            description: The description of the schema.
            field_groups: The list of field groups for the schema.

        Returns:
            Schema: The created schema.
        
        Examples:
            >>> schema = api.schemas.create(
            ...     title='My Schema',
            ...     parent=api.classes.get('_xdm.context.profile'),
            ...     description='My test schema',
            ...     field_groups=[api.field_groups.get('_mytenant.mixins.f7d78220431')]
            ... )
        """
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


class ClassCollection(ResourceCollection):
    """ Collection of Class resources.
    
    Initialized through `api.classes`, `api.global_classes`, or `api.tenant_classes`.
    
    Methods:
        get: Retrieves a class based on the provided reference.
        find: Finds a class based on the specified parameters.
        findall: Finds all classes based on the specified parameters.
        create: Creates a new class.
    
    Examples:
        Get a class by reference
        >>> api.classes.get('_mytenant.classes.7a5416d13572')
        <Class 7a5416d13572 title="My Class" version="1.0">

        Find a class by title
        >>> api.tenant_classes.find(title='My Class')
        <Class 7a5416d13572 title="My Class" version="1.0">

        List all global and tenant classes
        >>> api.classes.findall()
        [<Class xdm.context.profile>, <Class 7a5416d13572>, ...]

        List all global classes
        >>> api.global_classes.findall()
        [<Class xdm.context.profile>, <Class xdm.classes.conversion>, ...]

        List all tenant classes
        >>> api.tenant_classes.findall()
        [<Class 7a5416d13572>, <Class 7a5416d13571>, ...]
    """
    def __init__(self, api: Api, container:Optional[Container]=None):
        super().__init__(api, container=container, resources=[ResourceType.CLASS])
    
    def get(self, id) -> Class:
        return super().get(id)
    
    def find(self, full:bool=True, **params) -> Class:
        return super().find(full, **params)
    
    def findall(self, full:bool=False, **params) -> list[Class]:
        return super().findall(full, **params)

    # TODO: also allow direct definitions of fields
    # TODO: make behavior default to "adhoc" if field_groups have been defined
    def create(
            self,
            title: str,
            behavior: Behavior,
            description: str = '',
            field_groups: list[FieldGroup] = [],
        ) -> Class:
        """
        Create a new class.

        Args:
            title: The title of the class.
            behavior: The behavior of the class.
            description: The description of the class.
            field_groups: The list of field groups for the class.

        Returns:
            Class: The created class.
        
        Examples:
            >>> my_class = api.classes.create(
            ...     title='My Class',
            ...     behavior=api.behaviors.adhoc,
            ...     description='My test class',
            ...     field_groups=[api.field_groups.get('_mytenant.mixins.f7d78220431')]
            ... )
        """
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

class FieldGroupCollection(ResourceCollection):
    """ Collection of FieldGroup resources.
    
    Initialized through `api.field_groups`, `api.global_field_groups`, or `api.tenant_field_groups`.
    
    Methods:
        get: Retrieves a field group based on the provided reference.
        find: Finds a field group based on the specified parameters.
        findall: Finds all field groups based on the specified parameters.
        create: Creates a new field group.
    
    Examples:
        Get a field group by reference
        >>> api.field_groups.get('_mytenant.field_groups.7a5416d13572')
        <FieldGroup 7a5416d13572 title="My Field Group" version="1.0">

        Find a field group by title
        >>> api.tenant_field_groups.find(title='My Field Group')
        <FieldGroup 7a5416d13572 title="My Field Group" version="1.0">

        List all global and tenant field groups
        >>> api.field_groups.findall()
        [<FieldGroup xdm.context.identitymap>, <FieldGroup 7a5416d13572>, ...]

        List all global field groups
        >>> api.global_field_groups.findall()
        [<FieldGroup xdm.context.identitymap>, <FieldGroup xdm.mixins.current-weather>, ...]

        List all tenant field groups
        >>> api.tenant_field_groups.findall()
        [<FieldGroup 7a5416d13572>, <FieldGroup 7a5416d13571>, ...]
    """
    def __init__(self, api: Api, container:Optional[Container]=None):
        super().__init__(api, container=container, resources=[ResourceType.FIELD_GROUP])

    def get(self, id) -> FieldGroup:
        return super().get(id)
    
    def find(self, full:bool=True, **params) -> FieldGroup:
        return super().find(full, **params)
    
    def findall(self, full:bool=False, **params) -> list[FieldGroup]:
        return super().findall(full, **params)
    
    def create(self,
               title: str,
               description: str = '',
               properties: dict[str, dict] = {},
               intendedToExtend: list[Resource] = [],
               ) -> FieldGroup:
        """
        Create a new field group.

        Args:
            title: The title of the field group.
            description: The description of the field group.
            properties: The properties of the field group.
            extends: The resources this field group intends to extend.

        Returns:
            FieldGroup: The created field group.
        
        Examples:
            >>> field_group = api.field_groups.create(
            ...     title='My Field Group',
            ...     description='My test field group',
            ...     properties={
            ...         '_mytenant': {
            ...             'type': 'object',
            ...             'properties': {
            ...                 'is_super_star': {'type': 'boolean'},
            ...             }
            ...         },
            ...     }
            ...     extends=[api.classes.get('_xdm.context.profile')],
            ... )
        """
        for r in intendedToExtend:
            assert isinstance(r, Resource)
        return self._create({
            'type': 'object',
            'title': title,
            'description': description,
            'meta:intendedToExtend': [ ctx.ref for ctx in intendedToExtend ],
            'allOf': [{
                'properties': properties,
            }]
        })

class DataTypeCollection(ResourceCollection):
    """ Collection of DataType resources.
    
    Initialized through `api.data_types`, `api.global_data_types`, or `api.tenant_data_types`.
    
    Methods:
        get: Retrieves a data type based on the provided reference.
        find: Finds a data type based on the specified parameters.
        findall: Finds all data types based on the specified parameters.
        create: Creates a new data type.
    
    Examples:
        Get a data type by reference
        >>> api.data_types.get('_mytenant.data_types.7a5416d13572')
        <DataType 7a5416d13572 title="My Data Type" version="1.0">

        Find a data type by title
        >>> api.tenant_data_types.find(title='My Data Type')
        <DataType 7a5416d13572 title="My Data Type" version="1.0">

        List all global and tenant data types
        >>> api.data_types.findall()
        [<DataType xdm.context.person>, <DataType 7a5416d13572>, ...]

        List all global data types
        >>> api.global_data_types.findall()
        [<DataType xdm.context.person>, <DataType xdm.context.person-name>, ...]

        List all tenant data types
        >>> api.tenant_data_types.findall()
        [<DataType 7a5416d13572>, <DataType 7a5416d13571>, ...]
    """
    def __init__(self, api: Api, container:Optional[Container]=None):
        super().__init__(api, container=container, resources=[ResourceType.DATA_TYPE])

    def get(self, id) -> DataType:
        return super().get(id)
    
    def find(self, full:bool=True, **params) -> DataType:
        return super().find(full, **params)
    
    def findall(self, full:bool=False, **params) -> list[DataType]:
        return super().findall(full, **params)
    
    def create(self,
               title: str,
               description: str = '',
               properties: dict[str, dict] = {},
               ) -> DataType:
        """
        Create a new data type.

        Args:
            title (str): The title of the data type.
            description (str, optional): The description of the data type. Defaults to ''.
            properties (dict[str, dict], optional): The properties of the data type. Defaults to {}.

        Returns:
            DataType: The created data type.

        Examples:
            >>> data_type = api.data_types.create(
            ...     title='My Data Type',
            ...     description='My test data type',
            ...     properties={
            ...         '_mytenant': {
            ...             'type': 'object',
            ...             'properties': {
            ...                 'is_super_star': {'type': 'boolean'},
            ...             }
            ...         },
            ...     }
            ... )
        """
        return self._create({
            'type': 'object',
            'title': title,
            'description': description,
            'properties': properties,
        })


class BehaviorCollection(ResourceCollection):
    """ Collection of Behavior resources.
    
    Initialized through `api.behaviors`.
    
    Attributes:
        adhoc: The adhoc behavior.
        record: The record behavior.
        time_series: The time series behavior.
    
    Examples:
        >>> api.behaviors.adhoc
        <Behavior xdm.data.adhoc>

        >>> api.behaviors.record
        <Behavior xdm.data.record>

        >>> api.behaviors.time_series
        <Behavior xdm.data.time-series>
    """

    adhoc: Behavior
    record: Behavior
    time_series: Behavior

    def __init__(self, api: Api, container:Optional[Container]=None):
        super().__init__(api, container=container, resources=[ResourceType.BEHAVIOR])
        self.adhoc = Behavior(self.api, 'https://ns.adobe.com/xdm/data/adhoc')
        self.record = Behavior(self.api, 'https://ns.adobe.com/xdm/data/record')
        self.time_series = Behavior(self.api, 'https://ns.adobe.com/xdm/data/time-series')

    def get(self, id) -> Behavior:
        return super().get(id)
    
    def find(self, full:bool=True, **params) -> Behavior:
        return super().find(full, **params)
    
    def findall(self, full:bool=False, **params) -> list[Behavior]:
        return super().findall(full, **params)

class Resource:
    """ Base class for all resources.
    
    Attributes:
        body: The raw body of the resource.
        id: The `meta:altId` of the resource.
        ref: The `$id` of the resource.
        uuid: The unique identifier part of the id or ref.
        container: The container of the resource either "global" or "tenant".
        tenant: The tenant name used in the resource id. Available only for tenant resources.

        version: The version of the resource.
        title: The title of the resource.
        description: The description of the resource.
        extends: The list of resources that the resource extends.
    
    Methods:
        get: Refreshes the data to be in sync with the server.
        delete: Deletes the resource.
    
    Examples:
        >>> schema = api.schemas.get('_mytenant.schemas.7a5416d13572')
        <Schema 7a5416d13572 title="My Schema" version="1.0">

        Update the title of a schema (will send a PATCH request to the server)
        >>> schema.title = 'My New Schema'

        Update the description of a schema
        >>> schema.description = 'My new test schema'

        Get attribute from the raw response body
        >>> schema.body['meta:altId']
        '_mytenant.schemas.7a5416d13572'
        
        Delete the schema
        >>> schema.delete()
    """

    api: Api
    body: dict
    type: ResourceType

    id: str
    ref: str
    uuid: str
    tenant: Optional[str]
    container: Container

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
    def version(self) -> str:
        if 'version' not in self.body:
            self.get(full=False)
        return self.body['version']
    
    @property
    def title(self) -> str:
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
    def description(self) -> str:
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
    def properties(self) -> dict[str, dict]:
        if 'properties' not in self.body:
            self.get(full=True)
        self.body.setdefault('properties', {})
        return self.body['properties']

    @property
    def definitions(self) -> dict[str, dict]:
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

class Schema(Resource):
    """ A schema resource.

    Attributes:
        parent: The parent class that the schema inherits from.
        behavior: The behavior of the schema.
        field_groups: The list of field groups used in the schema.
    """

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

class Class(Resource):
    """ A class resource.

    Attributes:
        behavior: The behavior of the class.
        field_groups: The list of field groups used in the class.
    """
    type = ResourceType.CLASS
    
    @property
    def behavior(self) -> Behavior:
        behaviors = [r for r in self.extends if r.type == ResourceType.BEHAVIOR]
        assert len(behaviors) == 1
        return behaviors[0]

    @property
    def field_groups(self) -> list[FieldGroup]:
        return [resource for resource in self.extends if resource.type == ResourceType.FIELD_GROUP]

class FieldGroup(Resource):
    """ A field group resource.

    Attributes:
        intendedToExtend: The resources this field group intends to extend.
    """
    type = ResourceType.FIELD_GROUP

    @property
    def intendedToExtend(self):
        if 'meta:intendedToExtend' not in self.body:
            self.get(full=False)
        self.body.setdefault('meta:intendedToExtend', [])
        return [SchemaRef(ref).init(self.api) for ref in self.body['meta:intendedToExtend']]

class DataType(Resource):
    """ A data type resource.
    """
    type = ResourceType.DATA_TYPE

class Behavior(Resource):
    """ A behavior resource.
    """
    type = ResourceType.BEHAVIOR
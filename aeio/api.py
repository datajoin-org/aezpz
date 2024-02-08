from __future__ import annotations
from typing import Literal, Optional, Any
import requests
import json
from pathlib import Path
from .schema import SchemaResourceTemplate

class Api:
    base_url: str
    headers: dict[str, str]
    verbose: bool

    def __init__(self, config_file, verbose=True, sandbox='prod'):
        self.headers = { 'x-sandbox-name': sandbox }
        self.verbose = verbose
        self.base_url = 'https://platform.adobe.io'
        self.load_config_file(config_file)

    @property
    def sandbox(self):
        return self.headers['x-sandbox-name']

    @sandbox.setter
    def sandbox(self, value: str):
        assert isinstance(value, str)
        self.headers['x-sandbox-name'] = value
    
    def load_config_file(self, config_file):
        auth = json.load(Path(config_file).open('r'))
        self.headers['x-api-key'] = auth['CLIENT_ID']
        self.headers['x-gw-ims-org-id'] = auth['ORG_ID']
        self.authenticate(
            client_id=auth['CLIENT_ID'],
            client_secret=auth['CLIENT_SECRETS'][0],
            scopes=auth['SCOPES'],
        )
    
    def authenticate(self, client_id, client_secret, scopes):
        r = requests.post('https://ims-na1.adobelogin.com/ims/token/v2', params={
            'grant_type': 'client_credentials',
            'client_id': client_id,
            'client_secret': client_secret,
            'scope': ','.join(scopes)
        })
        r.raise_for_status()
        self.headers['Authorization'] = 'Bearer ' + r.json()['access_token']

    def request(self, method, path, headers={}, **kwargs):
        assert 'Authorization' in self.headers, 'need to load_config first'
        r = requests.request(
            method=method,
            url=self.base_url+path,
            headers={
                **self.headers,
                **headers,
            },
            **kwargs,
        )
        if self.verbose:
            print(r.status_code, r.request.method, r.request.path_url)
        if not r.ok:
            try:
                error = r.json()
                if 'title' in error:
                    print(error['title'])
                if 'detail' in error:
                    print(error['detail'])
            except:
                print(r.text)
        r.raise_for_status()
        if len(r.content):
            return r.json()
    
    def ref(self, id: str):
        return SchemaResourceTemplate.from_ref(self, id)
    
    @property
    def get_schemas(self, **kwargs):
        return SchemaResourceTemplate(
            api=self,
            namespace='schema',
            container='tenant',
        ).all(**kwargs) + \
        SchemaResourceTemplate(
            api=self,
            namespace='schema',
            container='global',
        ).all(**kwargs)

# SCHEMA_REGISTRY_PATH = '/data/foundation/schemaregistry/{container}/{resource}'

# class SchemaRegistryResource(Resource):
#     id: str = Field(alias='$id', repr=False)
#     altId: str = Field(alias='meta:altId')
#     version: str
#     title: str
#     description: str
#     properties: Optional[dict[str, dict]] = Field(repr=False, default={})
#     extends: Optional[list[str]] = Field(alias='meta:extends', repr=False, default=[])
#     tenant: Optional[str] = Field(alias='meta:tenantNamespace', default=None)

#     @classmethod
#     def props(cls, **kwargs):
#         raise NotImplementedError()
    
#     @classmethod
#     def request(cls, method, id=None, xed=None, xed_version=1, use_global=None, headers={}, **kwargs):
#         # Set Accept Header
#         if xed is None:
#             headers['Accept'] = f'application/vnd.adobe.xed+json'
#         else:
#             headers['Accept'] = f'application/vnd.adobe.xed-{xed}+json'
        
#         if xed_version is not None:
#             headers['Accept'] += f'; version={xed_version}'

#         # Configure path
#         container = 'tenant' if not use_global else 'global'
#         resource = cls._resource.default
#         if id is not None and id.startswith('_'):
#             tenant = id.split('.')[0]
#             if tenant in ('_xdm','_experience'):
#                 container = 'global'
#         path = SCHEMA_REGISTRY_PATH.format(container=container, resource=resource)
#         if id is not None:
#             path += '/' + id
        
#         return super().request(method, path, headers, **kwargs)

#     @classmethod
#     def all(cls, use_global=None, **kwargs):
#         records = []
#         params = {}
#         if len(kwargs):
#             params['property'] = ','.join(
#                 k + '==' + v
#                 for k,v in kwargs.items()
#             )
#         use_global_iter = [True, False] if use_global is None else [use_global]
#         for use_global in use_global_iter:
#             more = True
#             while more:
#                 r = cls.request('GET', xed_version=None, use_global=use_global, params=params)
#                 for item in r['results']:
#                     records.append(cls(**item))
#                 if r['_page'].get('next') is not None:
#                     params['start'] = r['_page']['next']
#                 else:
#                     more = False
#         return records

#     @classmethod
#     def get(cls, id):
#         r = cls.request('GET', id=id, xed='full')
#         r.setdefault('meta:tenantNamespace', id.split('.')[0])
#         return cls(**r)

#     @classmethod
#     def create(cls, **kwargs):
#         body = cls.props(**kwargs)
#         r = cls.request('POST', json=body)
#         return cls(**r)

#     def delete(self):
#         self.request('DELETE',id=self.altId)

# class Schema(SchemaRegistryResource):
#     _resource = 'schemas'

#     @classmethod
#     def props(
#             cls,
#             title: str,
#             extends: Classes,
#             description: str = '',
#             field_groups: list[FieldGroup] = [],
#         ):
#         assert isinstance(extends, Classes), 'Must inherit from one class'
#         for field_group in field_groups:
#             assert isinstance(field_group, FieldGroup)
#         return {
#             'type': 'object',
#             'title': title,
#             'description': description,
#             'allOf': [
#                 { '$ref': ref.id }
#                 for ref in ([extends] + field_groups)
#             ]
#         }

#     def add_field_group(self, field_group: FieldGroup):
#         assert isinstance(field_group, FieldGroup)
#         r = self.request('PATCH', id=self.altId, json=[
#             { 'op': 'add', 'path': '/allOf/-', 'value': {'$ref': field_group.id} }
#         ])
#         return self(**r)
        

# class Classes(SchemaRegistryResource):
#     _resource = 'classes'

#     @classmethod
#     def props(
#             cls,
#             title: str,
#             description: str = '',
#             extends: list[Classes] = [],
#             field_groups: list[FieldGroup] = [],
#         ):
#         if type(extends) is not list:
#             extends = [ extends ]
#         for ctx in extends:
#             assert isinstance(ctx, Classes)
#         for field_group in field_groups:
#             assert isinstance(field_group, FieldGroup)
#         return {
#             'type': 'object',
#             'title': title,
#             'description': description,
#             'allOf': [
#                 { '$ref': ref.id }
#                 for ref in (extends + field_groups)
#             ]
#         }

# # class Definition(BaseModel):
# #     type: Literal['integer','string','boolean','object','long','short','byte','date','datetime','map']
# #     xdmType: str = Field(alias='meta:xdmType')
# #     xdmField: Optional[str] = Field(alias='meta:xdmField', default=None)
# #     title: Optional[str] = None
# #     description: Optional[str] = None
# #     format: Optional[str] = None
# #     properties: Optional[dict[str, Definition]] = None
# #     pattern: Optional[str] = None
# #     maxLength: Optional[int] = None
# #     enum: Optional[list[str]] = []
# #     minimum: Optional[int] = None
# #     maximum: Optional[int] = None
# #     default: Optional[Any] = None
# #     examples: Optional[list[str]] = []
# #     required: Optional[list[str]] = None

# class FieldGroup(SchemaRegistryResource):
#     _resource = 'fieldgroups'
#     extends: list[str] = Field(alias='meta:intendedToExtend',repr=False)

#     @classmethod
#     def props(
#             cls,
#             tenant: str,
#             title: str,
#             description: str = '',
#             fields: dict[str, dict] = {},
#             extends: list[Classes] = [],
#         ):
#         return {
#             'title': title,
#             'description': description,
#             'meta:intendedToExtend': [ ctx.id for ctx in extends ],
#             'allOf': [{
#                 'properties': {
#                     tenant: {
#                         'type': 'object',
#                         'properties': fields,
#                     }
#                 }
#             }]
#         }
        

# class DataType(SchemaRegistryResource):
#     _resource = 'datatypes'
#     type: Literal['object','array']
#     properties: dict[str, dict]




# # class Context:
# #     BASE_PATH = '/data/foundation/schemaregistry/tenant/classes'

# # class Schema(ApiResource):
# #     BASE_PATH = '/data/foundation/schemaregistry/tenant/schemas'
# #     title: str
# #     extends: Context
# #     description: str = ''
# #     field_groups: list[FieldGroup] = []
    
# #     @classmethod
# #     def create(
# #         cls,
# #         title: str,
# #         extends: Context,
# #         description: str='',
# #         field_groups: list[FieldGroup]=[]
# #     ):
# #         cls.request(method='POST', json={
# #             'type': 'object',
# #             'title': title,
# #             'description': description,
# #             'allOf': [
# #                 {
# #                     '$ref': Context.id,
# #                 }
# #             ]
# #         })

# # class DataSet(ApiResource):
# #     BASE_PATH = '/data/foundation/catalog/dataSets'

# #     def __init__(self, schema: Schema):
# #         pass

# # class Batch(ApiResource):
# #     BASE_PATH = '/data/foundation/import/batches'

# #     def __init__(self, dataset: DataSet):
# #         pass

# if __name__ == '__main__':
#     load_config('/Users/ben/Documents/datajoin/notepad/aep-api/auth.json')
#     # schema = Schema.get('_amcevangelists.schemas.beeb478a1ecd7aa0ed4aa28aa19aca98275097c5962e448b')
#     # schema = Schema.create(title='Datajoin ID Map Schema v3', extends=Context.get('_xdm.context.profile'))
#     # fields = FieldGroup.get('_amcevangelists.mixins.e359501b687dc1268bc8ec3885b2527e6d7a68fc9c2813a')
#     # schema = Schema.create(title='Datajoin ID Map Schema')
#     # schema = Schema.get('_amcevangelists.schemas.45b5bd0fe8637fc9200050492c191661efd2bafe4d63c53d')
#     # mySchema = Schema.model_validate('_amcevangelists.schemas.50d343a5c3b0ef7b974a5f1d2d9039f8967145e13e0cd40b')
#     # print(schema)
#     print('hi')
#     # schema.delete()

#     # profile = Context.get('_xdm.context.profile')
#     # field_group = FieldGroup.create(
#     #     extends=[profile],
#     #     tenant="_amcevangelists",
#     #     title="Datajoin Identity v2",
#     #     description="Customer identity attributes",
#     #     fields={
#     #         'ECID': {'type': 'string'},
#     #         'emailAddress': {'type': 'string'},
#     #     }
#     # )
#     # schema = Schema.create(
#     #     title="Datajoin Identity v2",
#     #     extends=profile,
#     #     field_groups=[field_group]
#     # )
#     schemas = list(Schema.list_())
#     print('hi')

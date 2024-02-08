from __future__ import annotations
from typing import Any, Optional, Literal, TYPE_CHECKING

if TYPE_CHECKING:
    from .api import Api

class SchemaResourceTemplate:
    RESOURCE_MAP = {
        'mixins': 'fieldgroups',
        'schemas': 'schemas',
        'data': 'behaviors',
        'classes': 'classes',
        'context': 'classes',
        'datatypes': 'datatypes',
    }
    api: Api
    namespace: Literal['mixins','schemas','data','classes','context','datatypes']
    container: Literal['global','tenant']
    tenant: Optional[str]
    uuid: Optional[str]

    def __init__(self,
                 api: Api,
                 namespace: Literal['mixins','schemas','data','classes','context','datatypes'],
                 container: Literal['global', 'tenant'],
                 tenant: Optional[str],
                 uuid: Optional[str],
                ):
        self.api = api
        self.namespace = namespace
        self.container = container
        self.tenant = tenant
        self.uuid = uuid
        self.namespace = namespace
        assert self.namespace in self.RESOURCE_MAP
        assert self.container in ('global', 'tenant')

        if namespace == 'context':
            assert self.container == 'global', \
                'I assumed the "context" namespace could only be used for global'
        if tenant is not None:
            assert (tenant in ('xdm','experience')) == (container == 'global'), \
                'I assumed global containers are only for "xdm" or "experience" tenants'
        
    
    @classmethod
    def from_ref(cls, api: Api, id: str):
        if id.startswith('https://ns.adobe.com/'):
            split = id[len('https://ns.adobe.com/'):].split('/')
        elif id.startswith('_'):
            split = id[1:].split('.')
        assert len(split) == 3

        tenant = split[0]
        container = 'global' if tenant in ('xdm','experience') else 'tenant'
        namespace = split[1]
        assert namespace in cls.RESOURCE_MAP, f'unknown resource type: {namespace}'
        return cls(
            api=api,
            container=container,
            namespace=namespace,
            tenant=tenant,
            uuid=split[2],
        )
    
    @property
    def id(self):
        assert self.uuid is not None
        return f'_{self.tenant}.{self.namespace}.{self.uuid}'
    
    @property
    def ref(self):
        assert self.uuid is not None
        return f'https://ns.adobe.com/{self.tenant}/{self.namespace}/{self.uuid}'

    @property
    def resource(self):
        return self.RESOURCE_MAP[self.namespace]
    
    def request(self, method, xed=None, xed_version=1, headers={}, **kwargs):
        # Set Accept Header
        if xed is None:
            headers['Accept'] = f'application/vnd.adobe.xed+json'
        else:
            headers['Accept'] = f'application/vnd.adobe.xed-{xed}+json'
        
        if xed_version is not None:
            headers['Accept'] += f'; version={xed_version}'

        path = f'/data/foundation/schemaregistry/{self.container}/{self.resource}'
        if self.uuid is not None:
            path += '/' + self.id
        
        return self.api.request(method, path, headers, **kwargs)

    def all(self, xed=None, **kwargs):
        assert self.uuid is None
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

    def get(self, xed=None):
        assert self.uuid is not None
        return self.request('GET', xed=xed)

    def create(self, body):
        assert self.uuid is None
        return self.request('POST', json=body)
    
    def delete(self):
        assert self.uuid is not None
        return self.request('DELETE')
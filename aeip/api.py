from __future__ import annotations
import requests
import json
from pathlib import Path
from . import schema

class Api:
    base_url: str
    headers: dict[str, str]
    verbose: bool

    def __init__(self, config_file, verbose=True, sandbox='prod'):
        self.headers = { 'x-sandbox-name': sandbox }
        self.verbose = verbose
        self.base_url = 'https://platform.adobe.io'
        self.load_config_file(config_file)
        self.schemas = schema.SchemaCollection(self)
        self.classes = schema.ClassCollection(self)
        self.field_groups = schema.FieldGroupCollection(self)
        self.behaviors = schema.BehaviorCollection(self)
        self.schema_registry = schema.ResourceCollection(self)

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


def load_config(config_file) -> Api:
    return Api(config_file)

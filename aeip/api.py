"""Provide several sample math calculations.

This module allows the user to make mathematical calculations.

Examples:
    >>> from calculator import calculations
    >>> calculations.add(2, 4)
    6.0
    >>> calculations.multiply(2.0, 4.0)
    8.0
    >>> from calculator.calculations import divide
    >>> divide(4.0, 2)
    2.0

The module contains the following functions:

- `add(a, b)` - Returns the sum of two numbers.
- `subtract(a, b)` - Returns the difference of two numbers.
- `multiply(a, b)` - Returns the product of two numbers.
- `divide(a, b)` - Returns the quotient of two numbers.
"""

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
        self.registry = schema.ResourceCollection(self)
        self.global_registry = schema.ResourceCollection(self, container='global')
        self.tenant_registry = schema.ResourceCollection(self, container='tenant')
        self.schemas = schema.SchemaCollection(self)
        self.global_schemas = schema.SchemaCollection(self, container='global')
        self.tenant_schemas = schema.SchemaCollection(self, container='tenant')
        self.classes = schema.ClassCollection(self)
        self.global_classes = schema.ClassCollection(self, container='global')
        self.tenant_classes = schema.ClassCollection(self, container='tenant')
        self.field_groups = schema.FieldGroupCollection(self)
        self.global_field_groups = schema.FieldGroupCollection(self, container='global')
        self.tenant_field_groups = schema.FieldGroupCollection(self, container='tenant')
        self.data_types = schema.DataTypeCollection(self)
        self.global_data_types = schema.DataTypeCollection(self, container='global')
        self.tenant_data_types = schema.DataTypeCollection(self, container='tenant')
        self.behaviors = schema.BehaviorCollection(self)
    
    def ref(self, ref):
        return self.registry.get(ref)

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


def load_config(config_file: str) -> Api:
    """ Initialize the api from a config file

    Examples:
        >>> import aeip
        >>> api = aeip.load_config("auth.json")
    
    Args:
        config_file: The filepath of your json config file that you downloaded from AEP
    
    Returns:
        The initialized api interface
    """
    return Api(config_file)

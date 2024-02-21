from __future__ import annotations
import requests
import json
from pathlib import Path
from . import schema, datasets
from typing import Optional

def load_config(config_file: str, verbose: bool=True, sandbox: str='prod') -> Api:
    """ Initialize the api from a config file

    Examples:
        >>> import aezpz
        >>> api = aezpz.load_config("auth.json")
    
    Args:
        config_file: The filepath of your json config file that you downloaded from AEP
        verbose: Whether to print the status code of every request. Defaults to True
        sandbox: The name of the sandbox to use. Defaults to 'prod'
    
    Returns:
        The initialized api interface
    """
    return Api(config_file, verbose=verbose, sandbox=sandbox)

class Api:
    """The main interface to the Adobe XDM API
    
    Attributes:
        base_url: The base url of the Adobe XDM API. Defaults to 'https://platform.adobe.io'
        headers: The default headers to be sent with every request.
        verbose: Whether to print the status code of every request. Defaults to True
        sandbox: The name of the sandbox to use. Defaults to 'prod'
        
        registry: A collection of all resources in all containers
        global_registry: A collection of all resources in the global container
        tenant_registry: A collection of all resources in the tenant container
        schemas: A collection of all schemas in the tenant container
        global_schemas: A collection of all schemas in the global container
        tenant_schemas: A collection of all schemas in the tenant container
        classes: A collection of all classes in the tenant container
        global_classes: A collection of all classes in the global container
        tenant_classes: A collection of all classes in the tenant container
        field_groups: A collection of all field groups in the tenant container
        global_field_groups: A collection of all field groups in the global container
        tenant_field_groups: A collection of all field groups in the tenant container
        data_types: A collection of all data types in the tenant container
        global_data_types: A collection of all data types in the global container
        tenant_data_types: A collection of all data types in the tenant container
        behaviors: A collection of all behaviors in the tenant container
    
    Methods:
        ref: Retrieves the value associated with the given reference.
        request: The underlying method for all requests to the api.
    
    Examples:
        Set the sandbox to 'stage'
        >>> api.sandbox = 'stage'
        >>> api.headers
        { 'x-sandbox-name': 'stage' }
    """

    base_url: str
    sandbox: str
    verbose: bool
    _access_token: str
    _config: dict

    registry: schema.ResourceCollection
    global_registry: schema.ResourceCollection
    tenant_registry: schema.ResourceCollection
    schemas: schema.SchemaCollection
    global_schemas: schema.SchemaCollection
    tenant_schemas: schema.SchemaCollection
    classes: schema.ClassCollection
    global_classes: schema.ClassCollection
    tenant_classes: schema.ClassCollection
    field_groups: schema.FieldGroupCollection
    global_field_groups: schema.FieldGroupCollection
    tenant_field_groups: schema.FieldGroupCollection
    data_types: schema.DataTypeCollection
    global_data_types: schema.DataTypeCollection
    tenant_data_types: schema.DataTypeCollection
    behaviors: schema.BehaviorCollection
    datasets: datasets.DatasetCollection
    batches: datasets.BatchCollection

    def __init__(self, config_file, verbose=True, sandbox='prod'):
        self.sandbox = sandbox
        self.verbose = verbose
        self.base_url = 'https://platform.adobe.io'
        self._config = self.load_config_file(config_file)
        self._access_token = self.authenticate()
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
        
        self.datasets = datasets.DatasetCollection(self)
        self.batches = datasets.BatchCollection(self)
    
    def ref(self, ref: str) -> schema.Resource:
        """
        Retrieves the value associated with the given reference.

        Args:
            ref: The `$id` or `meta:altId` of the reference to retrieve.

        Returns:
            The value associated with the reference.
        
        Examples:
            >>> api.ref('https://ns.adobe.com/xdm/context/profile')
            <Class xdm.context.profile>

            >>> api.ref('_mytenant.schemas.7a5416d135713dae7957')
            <Schema 7a5416d135713dae7957>
        """
        return self.registry.get(ref)

    @property
    def headers(self) -> dict:
        assert getattr(self, 'config', None) and getattr(self, 'access_token', None), 'need to authenticate first'
        return {
            'x-sandbox-name': self.sandbox,
            'x-api-key': self._config['CLIENT_ID'],
            'x-gw-ims-org-id': self._config['ORG_ID'],
            'Authorization': 'Bearer ' + self._access_token,
        }

    @property
    def me(self) -> str:
        return self._config['ACCOUNT_ID']

    def load_config_file(self, config_file) -> dict:
        config = json.load(Path(config_file).open('r'))
        return {
            'CLIENT_ID': config['CLIENT_ID'],
            'ORG_ID': config['ORG_ID'],
            'CLIENT_SECRET': config['CLIENT_SECRETS'][0],
            'SCOPES': config['SCOPES'],
            'ORG_ID': config['ORG_ID'],
            'ACCOUNT_ID': config['TECHNICAL_ACCOUNT_ID'],
        }
    
    def authenticate(self) -> str:
        r = requests.post('https://ims-na1.adobelogin.com/ims/token/v2', params={
            'grant_type': 'client_credentials',
            'client_id': self._config['CLIENT_ID'],
            'client_secret': self._config['CLIENT_SECRET'],
            'scope': ','.join(self._config['SCOPES'])
        })
        r.raise_for_status()
        return r.json()['access_token']

    def request(self, method, path, headers={}, **kwargs) -> Optional[dict]:
        """
        The underlying method for all requests to the api. Wraps the requests library.

        Args:
            method (str): The HTTP method to use for the request (e.g., 'GET', 'POST', 'PUT', 'DELETE').
            path (str): The path of the resource to request.
            headers (dict, optional): Additional headers to include in the request. Defaults to an empty dictionary.
            **kwargs: Additional keyword arguments to pass to the underlying requests library.

        Returns:
            dict: The JSON response from the server, if any.

        Raises:
            requests.exceptions.HTTPError: If the response status code indicates an error.
            
        Examples:
            >>> api.request('GET', '/data/foundation/schemaregistry/global/behaviors', headers={'Accept': 'application/vnd.adobe.xed-id+json'})
            { "results": [{ "$id": "https://ns.adobe.com/xdm/data/time-series" }, ...], "_page": { "count": 3 } }
        """
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

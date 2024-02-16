from __future__ import annotations
import datetime
from typing import Any, Union, Optional, Literal, TYPE_CHECKING
if TYPE_CHECKING:
    from .api import Api
    from .schema import Schema

def parse_id_list(ids: list[str]) -> str:
    assert len(ids) == 1, f"Expected a list of length 1, but got {ids}"
    assert ids[0].startswith('@/dataSets/'), f"Expected a list of dataset ids, but got {ids}"
    return ids[0][len('@/dataSets/'):]

def form_dataset_body(**kwargs):
    body = {}

    def file_description():
        if 'fileDescription' not in body:
            body['fileDescription'] = {'format': ''}
        return body['fileDescription']

    def normalize_str_list(value):
        if isinstance(value, str):
            return [value]
        assert isinstance(value, list)
        for v in value:
            assert isinstance(v, str)
        return value
    
    for k, v in kwargs.items():
        if v is not None:
            if k == 'name':
                assert isinstance(v, str)
                body['name'] = v
            elif k == 'description':
                assert isinstance(v, str)
                body['description'] = v
            elif k == 'tags':
                assert isinstance(tags, dict)
                tags = {k: normalize_str_list(v) for k, v in tags.items()}
                body['tags'] = tags
            elif k == 'enableErrorDiagnostics':
                assert isinstance(v, bool)
                body['enableErrorDiagnostics'] = v
            elif k == 'observableSchema':
                assert isinstance(v, dict)
                body['observableSchema'] = v
            elif k == 'format':
                assert isinstance(format, str)
                file_description()['format'] = format
            elif k == 'delimiters':
                file_description()['delimiters'] = normalize_str_list(v)
            elif k == 'quotes':
                file_description()['quotes'] = normalize_str_list(v)
            elif k == 'escapes':
                file_description()['escapes'] = normalize_str_list(v)
            elif k == 'nullMarkers':
                file_description()['nullMarkers'] = normalize_str_list(v)
            elif k == 'header':
                assert isinstance(v, bool)
                file_description()['header'] = v
            elif k == 'charset':
                assert isinstance(v, str)
                file_description()['charset'] = v
            elif k == 'schema':
                assert isinstance(v, Schema)
                body['schema'] = v.ref
            elif k == 'dataSourceId':
                assert isinstance(v, int)
                body['dataSourceId'] = v
    return body

class DatasetCollection:
    """ Collection of Datasets.
    
    Initialized through `api.datasets`.
    
    Methods:
        get: Find an existing dataset by id.
        find: Find an existing dataset by name.
        list: List all datasets.
        create: Create a new dataset.
    """
    api: Api

    def __init__(self, api: Api):
        self.api = api
    
    def get(self, id) -> DataSet:
        """ Get a dataset by id.
        
        Examples:
            >>> api.datasets.get('65cfb1ca9e91f22')
            <DataSet 65cfb1ca9e91f22>
        """
        return DataSet(api=self.api, id=id)
    
    def find(self, **kwargs) -> DataSet:
        """ Find a dataset by properties
        
        Examples:
            >>> api.datasets.find(name='My Dataset')
            <DataSet 65cfb1ca9e91f22 name="My Dataset" version="1.0.0">
        """
        datasets = self.list(**kwargs)
        assert len(datasets) == 1, f"Expected to find exactly one dataset, but found {len(datasets)}"
        return datasets[0]
    
    def list(self, **kwargs) -> list[DataSet]:
        """ List all datasets that match the given properties.
        
        Examples:
            >>> api.datasets.list(name="untitled")
            [<DataSet 65cfb1ca9e91f22 name="untitled" version="1.0.0">, ...]
        """
        params = kwargs
        datasets = []
        while True:
            r = self.api.request('GET', '/data/foundation/catalog/dataSets', params=params)
            for k, v in r.items():
                datasets.append(DataSet(self.api, k, v))
            if len(r) == 100:
                params['start'] = params.get('start',0) + 100
            else:
                break
        return datasets

    def create(self,
               name: str = None,
               description: str = None,
               tags: dict[str, Union[str, list[str]]] = None,
               enableErrorDiagnostics: bool = None,
               observableSchema: dict = None,
               format: Literal['','csv','text','json','parquet','sequencefile','avro'] = None,
               delimiters: Union[str, list[str]] = None,
               quotes: Union[str, list[str]] = None,
               escapes: Union[str, list[str]] = None,
               nullMarkers: Union[str, list[str]] = None,
               header: bool = None,
               charset: Literal['US-ASCII','UTF-8','ISO-8859-1',''] = None,
               schema: Schema = None,
               dataSourceId: int = None,
              ) -> DataSet:
        """ Create a new dataset.
        
        Args:
            name: A descriptive, human-readable name for this dataset.
            description: A longer text description of the dataset.
            tags: Tags are values associated with a particular object, 
                these are generally used by external systems for marking an object 
                in a way that it understands. Normally tags are not used for 
                internal Catalog business logic
            enableErrorDiagnostics: This field provides the ability to opt in to 
                generating diagnostic files for the errors while ingesting data.
            observableSchema: observableSchema stores a JSON Schema object that is a 
                valid subset (including the null case) of the XDM model specified by schemaRef.
            format: The file format for all dataSetFiles associated with this view. 
                Required for CSV upload workflows, but optional in all other cases.
            delimiters: Characters used to separate fields for the file format.
            quotes: Quote characters used for the file format.
            escapes: Escape characters used for the file format.
            nullMarkers: Null markers used for the file format.
            header: Whether the file format has a header.
            charset: The character encoding of the files..
            schema: The schema of the dataset.
            dataSourceId: The ID of the datasource. It must be unique, along with its name.
        
        Examples:
            >>> api.datasets.create()
            <DataSet 65cfb1ca9e91f22 name="untitled" version="1.0.0">
        """
        body = form_dataset_body(
            name=name,
            description=description,
            tags=tags,
            enableErrorDiagnostics=enableErrorDiagnostics,
            observableSchema=observableSchema,
            format=format,
            delimiters=delimiters,
            quotes=quotes,
            escapes=escapes,
            nullMarkers=nullMarkers,
            header=header,
            charset=charset,
            schema=schema,
            dataSourceId=dataSourceId,
        )
        r = self.api.request('POST', '/data/foundation/catalog/dataSets', json=body)
        return DataSet(self.api, parse_id_list(r))
        

class DataSet:
    api: Api
    id: str
    _body: dict

    def __init__(self, api: Api, id: str, body: dict = None):
        self.api = api
        self.id = id
        self._body = body

    def request(self, method: Literal['GET', 'POST', 'PUT', 'DELETE'], json: Optional[dict[str, Any]] = None):
        return self.api.request(method, f'/data/foundation/catalog/dataSets/{self.id}', json=json)

    @property
    def body(self):
        if self._body is None:
            self.get()
        return self._body

    @property
    def name(self):
        return self.body.get('name')

    @property
    def description(self):
        return self.body.get('description')

    @property
    def tags(self):
        return self.body.get('tags')

    @property
    def enableErrorDiagnostics(self):
        return self.body.get('enableErrorDiagnostics')
    
    @property
    def observableSchema(self):
        return self.body.get('observableSchema')

    @property
    def format(self):
        return self.body.get('fileDescription', {}).get('format')
    
    @property
    def delimiters(self):
        return self.body.get('fileDescription', {}).get('delimiters')
    
    @property
    def quotes(self):
        return self.body.get('fileDescription', {}).get('quotes')
    
    @property
    def escapes(self):
        return self.body.get('fileDescription', {}).get('escapes')
    
    @property
    def nullMarkers(self):
        return self.body.get('fileDescription', {}).get('nullMarkers')
    
    @property
    def header(self):
        return self.body.get('fileDescription', {}).get('header')
    
    @property
    def charset(self):
        return self.body.get('fileDescription', {}).get('charset')
    
    @property
    def schema(self):
        if 'schemaRef' not in self.body:
            return None
        return self.api.schemas.get(self.body.get('schemaRef')['id'])
    
    @property
    def dataSourceId(self):
        return self.body.get('dataSourceId')
    
    @property
    def version(self):
        return self.body.get('version')
    
    @property
    def created(self):
        return datetime.datetime.fromtimestamp(self.body.get('created') / 1000)

    @property
    def updated(self):
        return datetime.datetime.fromtimestamp(self.body.get('updated') / 1000)
        
    def get(self):
        r = self.request('GET')
        assert self.id in r
        if self._body is None:
            self._body = {}
        self._body.update(**r[self.id])
        return self
    
    def update(self,
                name: str = None,
                description: str = None,
                tags: dict[str, Union[str, list[str]]] = None,
                enableErrorDiagnostics: bool = None,
                observableSchema: dict = None,
                format: Literal['','csv','text','json','parquet','sequencefile','avro'] = None,
                delimiters: Union[str, list[str]] = None,
                quotes: Union[str, list[str]] = None,
                escapes: Union[str, list[str]] = None,
                nullMarkers: Union[str, list[str]] = None,
                header: bool = None,
                charset: Literal['US-ASCII','UTF-8','ISO-8859-1',''] = None,
                schema: Schema = None,
                dataSourceId: int = None,
            ):
        """ Update the dataset.
        
        Args:
            name: A descriptive, human-readable name for this dataset.
            description: A longer text description of the dataset.
            tags: Tags are values associated with a particular object, 
                these are generally used by external systems for marking an object 
                in a way that it understands. Normally tags are not used for 
                internal Catalog business logic
            enableErrorDiagnostics: This field provides the ability to opt in to 
                generating diagnostic files for the errors while ingesting data.
            observableSchema: observableSchema stores a JSON Schema object that is a 
                valid subset (including the null case) of the XDM model specified by schemaRef.
            format: The file format for all dataSetFiles associated with this view. 
                Required for CSV upload workflows, but optional in all other cases.
            delimiters: Characters used to separate fields for the file format.
            quotes: Quote characters used for the file format.
            escapes: Escape characters used for the file format.
            nullMarkers: Null markers used for the file format.
            header: Whether the file format has a header.
            charset: The character encoding of the files..
            schema: The schema of the dataset.
            dataSourceId: The ID of the datasource. It must be unique, along with its name.
        """
        body = form_dataset_body(
            name=name,
            description=description,
            tags=tags,
            enableErrorDiagnostics=enableErrorDiagnostics,
            observableSchema=observableSchema,
            format=format,
            delimiters=delimiters,
            quotes=quotes,
            escapes=escapes,
            nullMarkers=nullMarkers,
            header=header,
            charset=charset,
            schema=schema,
            dataSourceId=dataSourceId,
        )
        self.request('PATCH', json=body)
        self.get()
        return self
    
    def delete(self):
        self.request('DELETE')
        return self

    def __repr__(self):
        return '<{class_name} {id}{name}{version}>'.format(
            class_name=self.__class__.__name__,
            id=self.id,
            name=f' name="{self.name}"' if self._body is not None else '',
            version=f' version="{self.version}"' if self._body is not None else '',
        )
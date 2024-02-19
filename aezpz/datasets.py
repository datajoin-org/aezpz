from __future__ import annotations
import datetime
from typing import Any, Union, Optional, Literal, TYPE_CHECKING
from pathlib import Path
if TYPE_CHECKING:
    from .schema import Schema
    from .api import Api

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
                body['schemaRef'] = { 'id': v.ref, 'contentType': 'application/vnd.adobe.xed+json;version=1' }
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
               schema: Schema,
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
               dataSourceId: int = None,
              ) -> DataSet:
        """ Create a new dataset.
        
        Args:
            schema: The schema of the dataset.
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
    def name(self) -> str:
        return self.body.get('name')

    @property
    def description(self) -> str:
        return self.body.get('description')

    @property
    def tags(self) -> dict[str, list[str]]:
        return self.body.get('tags')

    @property
    def enableErrorDiagnostics(self) -> bool:
        return self.body.get('enableErrorDiagnostics')
    
    @property
    def observableSchema(self) -> dict:
        return self.body.get('observableSchema')

    @property
    def format(self) -> Literal['','csv','text','json','parquet','sequencefile','avro']:
        return self.body.get('fileDescription', {}).get('format')
    
    @property
    def delimiters(self) -> list[str]:
        return self.body.get('fileDescription', {}).get('delimiters', [])
    
    @property
    def quotes(self) -> list[str]:
        return self.body.get('fileDescription', {}).get('quotes', [])
    
    @property
    def escapes(self) -> list[str]:
        return self.body.get('fileDescription', {}).get('escapes', [])
    
    @property
    def nullMarkers(self) -> list[str]:
        return self.body.get('fileDescription', {}).get('nullMarkers', [])
    
    @property
    def header(self) -> bool:
        return self.body.get('fileDescription', {}).get('header')
    
    @property
    def charset(self) -> Literal['US-ASCII','UTF-8','ISO-8859-1','']:
        return self.body.get('fileDescription', {}).get('charset', '')
    
    @property
    def schema(self):
        if 'schemaRef' not in self.body:
            return None
        return self.api.schemas.get(self.body.get('schemaRef')['id'])
    
    @property
    def dataSourceId(self) -> int:
        return self.body.get('dataSourceId')
    
    @property
    def version(self) -> str:
        return self.body.get('version')
    
    @property
    def created(self) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(self.body.get('created') / 1000)

    @property
    def updated(self) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(self.body.get('updated') / 1000)

    def batches(self) -> list[Batch]:
        return self.api.batches.list(dataSet=self.id)
    
    def upload(self,
               filepath: str,
               format: Literal['json','jsonl','parquet','csv'],
               replace: Union[bool, list[Batch]] = []
            ) -> Batch:
        batch = self.api.batches.create(dataset=self, format=format, replace=replace)
        batch.upload(filepath)
        batch.complete()
        return self

    def get(self) -> DataSet:
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
            ) -> DataSet:
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
    
    def delete(self) -> DataSet:
        self.request('DELETE')
        return self

    def __repr__(self):
        return '<{class_name} {id}{name}{version}>'.format(
            class_name=self.__class__.__name__,
            id=self.id,
            name=f' name="{self.name}"' if self._body is not None else '',
            version=f' version="{self.version}"' if self._body is not None else '',
        )

class BatchCollection:
    api: Api

    def __init__(self, api: Api):
        self.api = api
    
    def get(self, id) -> Batch:
        return Batch(api=self.api, id=id)

    def create(self,
               dataset: DataSet,
               format: Literal['json','jsonl','parquet','csv'],
               replace: Union[bool, list[Batch]] = []
            ) -> Batch:
        assert isinstance(dataset, DataSet), f"Expected dataset to be a DataSet object, but got {dataset}"
        assert format in ('json','jsonl','parquet','csv','avro'), f"Expected format to be one of 'json','jsonl','parquet','csv', but got {format}"
        assert isinstance(replace, bool) or isinstance(replace, list), f"Expected replace to be a list of Batch objects or a boolean, but got {replace}"
        if isinstance(replace, bool):
            replace = dataset.batches() if replace else []
        for replace_batch in replace:
            assert isinstance(replace_batch, Batch), f"Expected replace to be a list of Batch objects, but got {replace_batch}"
        
        body = {
            'datasetId': dataset.id,
            'inputFormat': {'format': format},
        }
        if format == 'jsonl':
            body['inputFormat']['format'] = 'json'
            body['inputFormat']['isMultiLineJson'] = True
        if len(replace) > 0:
            body['replay'] = {
                'reason': 'replace',
                'predecessors': [batch.id for batch in replace],
            }

        r = self.api.request('POST', '/data/foundation/import/batches', json=body)
        return Batch(self.api, r['id'], r)

    def list(self, **kwargs) -> list[DataSet]:
        params = kwargs
        batches = []
        while True:
            r = self.api.request('GET', '/data/foundation/catalog/batches', params=params)
            for k, v in r.items():
                batches.append(Batch(self.api, k, v))
            if len(r) == 100:
                params['start'] = params.get('start',0) + 100
            else:
                break
        return batches

class Batch:
    api: Api
    id: str
    _body: dict

    def __init__(self, api: Api, id: str, body: dict = None):
        self.api = api
        self.id = id
        self._body = body
    
    @property
    def status(self):
        return self.body.get('status')
    
    @property
    def created(self):
        return datetime.datetime.fromtimestamp(self.body.get('created') / 1000)

    @property
    def updated(self):
        return datetime.datetime.fromtimestamp(self.body.get('updated') / 1000)

    @property
    def version(self):
        return self.body.get('version')
    
    @property
    def dataset(self):
        related_datasets = [
            row['id'] 
            for row in self.body['relatedObjects']
            if row['type'] == 'dataSet'
        ]
        assert len(related_datasets) == 1, 'Expected exactly one related dataset'
        return self.api.datasets.get(related_datasets[0])

    def get(self, aggregate: bool = False):
        params = {}
        if aggregate:
            params['aggregate'] = 'true'
        r = self.catalog_request('GET', params=params)
        assert self.id in r
        if self._body is None:
            self._body = {}
        self._body.update(**r[self.id])
        return self
    
    def import_request(self, method: Literal['GET', 'POST', 'PUT', 'DELETE'], **kwargs):
        return self.api.request(method, f'/data/foundation/import/batches/{self.id}', **kwargs)

    def catalog_request(self, method: Literal['GET', 'POST', 'PUT', 'DELETE'], **kwargs):
        return self.api.request(method, f'/data/foundation/catalog/batches/{self.id}', **kwargs)

    def complete(self):
        self.import_request('POST', params={'action': 'COMPLETE'})
    
    def upload(self, filepath: str):
        batch_file = BatchFile(self.api, self.dataset, self, filepath)
        batch_file.upload()
    
    def delete(self):
        action = 'ABORT' if self.status in ('loading','processing','queued','retrying','staging') else 'REVERT'
        self.import_request('POST', params={'action': action})

    @property
    def body(self):
        if self._body is None:
            self.get()
        return self._body

    def __repr__(self):
        return '<{class_name} {id}{status}{version}>'.format(
            class_name=self.__class__.__name__,
            id=self.id,
            status=f' status="{self.status}"' if self._body is not None else '',
            version=f' version="{self.version}"' if self._body is not None else '',
        )

class BatchFile:
    api: Api
    dataset: DataSet
    batch: Batch
    file: Path

    def __init__(self, api: Api, dataset: DataSet, batch: Batch, filepath: str):
        assert isinstance(dataset, DataSet)
        assert isinstance(batch, Batch)
        self.api = api
        self.dataset = dataset
        self.batch = batch
        self.file = Path(filepath)
        assert self.file.exists() and self.file.is_file(), f"File {self.file} does not exist or is not a file"
        assert self.file.suffix in ('.csv', '.json', '.jsonl', '.parquet'), f"File {self.file} is not in a supported format"
    
    def request(self, method: Literal['GET', 'POST', 'PUT', 'DELETE'], **kwargs):
        return self.api.request(method, f'/data/foundation/import/batches/{self.batch.id}/datasets/{self.dataset.id}/files/{self.file.name}', **kwargs)
    
    def initialize(self):
        self.request('POST', params={'action': 'INITIALIZE'})
    
    def complete(self):
        self.request('POST', params={'action': 'COMPLETE'})
    
    def upload(self):
        self.request('PUT',
                     data=self.file.open('rb'),
                     headers={
                        'Content-Type': 'application/octet-stream',
                    })
## Creating a Schema

To create a schema, you'll need to call the `schemas.create` method and pass the `title` and `description` of the schema that you want to create.

```python
import aezpz

# Load the credentials from the credentials file
api = aezpz.load_config('path/to/credentials.json')
```

Every schema needs to inherit from a class. The class that you inherit from will determine the type of schema that you are creating. For example, if you want to create a schema that represents a customer profile, you would inherit from the `_xdm.context.profile` class.

```python
# Load the profile class
profile_class = api.classes.get('_xdm.context.profile')
```

Next in order to add fields to the schema, we'll need to create a field group
to contain those fields. A field group is a way to organize fields within a schema, and allow those fields to be reused in other schemas.

```python
# Create a field group
field_group = api.field_groups.create(
    intendedToExtend=[profile_class],
    title='My Field Group',
    description='This is a field group that I created',
    properties={
        '_mytenant': {
            'type': 'object',
            'properties': {
                'is_super_star': {'type': 'boolean'},
            }
        },
    }
)
```

Finally, we can create the schema by calling the `schemas.create` method and passing the `title`, `description`, and `fieldGroup` of the schema that you want to create.

```python
# Create the schema
schema = api.schemas.create(
    title='My Schema',
    description='This is a schema that I created',
    parent=profile_class,
    field_groups=[field_group]
)
```

To list all of the fields that our newly created schema contains, we can access
the `properties` attribute of the schema.

```python
from pprint import pprint
pprint(schema.properties)
#  {'_amcevangelists': {'meta:xdmType': 'object',
#                       'properties': {'is_super_star': {'meta:xdmType': 'boolean',
#                                                        'type': 'boolean'}},
#                       'type': 'object'},
#   '_id': {'description': 'A unique identifier for the record.',
#           'format': 'uri-reference',
#           'meta:xdmField': '@id',
#           'meta:xdmType': 'string',
#           'title': 'Identifier',
#           'type': 'string'},
#   '_repo': {'meta:xdmType': 'object',
#             'meta:xedConverted': True,
#             'properties': {'createDate': {'description': 'The server date and '
#                                                          'time when the '
#                                                          'resource was created '
#                                                          'in the repository, '
#                                                          'such as when an asset '
#                                                          'file is first '
#                                                          'uploaded or a '
#                                                          'directory is created '
#                                                          'by the server as the '
#                                                          'parent of a new '
#                                                          'asset. The date time '
#                                                          'property should '
#                                                          'conform to ISO 8601 '
#                                                          'standard. An example '
#                                                          'form is '
#                                                          '"2004-10-23T12:00:00-06:00".',
#                                           'examples': ['2004-10-23T12:00:00-06:00'],
#                                           'format': 'date-time',
#                                           'meta:immutable': True,
#                                           'meta:usereditable': False,
#                                           'meta:xdmField': 'repo:createDate',
#                                           'meta:xdmType': 'date-time',
#                                           'type': 'string'},
#                            'modifyDate': {'description': 'The server date and '
#                                                          'time when the '
#                                                          'resource was last '
#                                                          'modified in the '
#                                                          'repository, such as '
#                                                          'when a new version of '
#                                                          'an asset is uploaded '
#                                                          "or a directory's "
#                                                          'child resource is '
#                                                          'added or removed. The '
#                                                          'date time property '
#                                                          'should conform to ISO '
#                                                          '8601 standard. An '
#                                                          'example form is '
#                                                          '"2004-10-23T12:00:00-06:00".',
#                                           'examples': ['2004-10-23T12:00:00-06:00'],
#                                           'format': 'date-time',
#                                           'meta:usereditable': False,
#                                           'meta:xdmField': 'repo:modifyDate',
#                                           'meta:xdmType': 'date-time',
#                                           'type': 'string'}},
#             'type': 'object'},
#   'createdByBatchID': {'description': 'The dataset files in Catalog which has '
#                                       'been originating the creation of the '
#                                       'record.',
#                        'format': 'uri-reference',
#                        'meta:xdmField': 'xdm:createdByBatchID',
#                        'meta:xdmType': 'string',
#                        'title': 'Created by batch identifier',
#                        'type': 'string'},
#   'modifiedByBatchID': {'description': 'The last dataset files in Catalog which '
#                                        'has modified the record. At creation '
#                                        'time, `modifiedByBatchID` is set as '
#                                        '`createdByBatchID`.',
#                         'format': 'uri-reference',
#                         'meta:xdmField': 'xdm:modifiedByBatchID',
#                         'meta:xdmType': 'string',
#                         'title': 'Modified by batch identifier',
#                         'type': 'string'},
#   'personID': {'description': 'Unique identifier of Person/Profile fragment.',
#                'meta:xdmField': 'xdm:personID',
#                'meta:xdmType': 'string',
#                'title': 'Person ID',
#                'type': 'string'},
#   'repositoryCreatedBy': {'description': 'User ID of who created the record.',
#                           'meta:xdmField': 'xdm:repositoryCreatedBy',
#                           'meta:xdmType': 'string',
#                           'title': 'Created by user identifier',
#                           'type': 'string'},
#   'repositoryLastModifiedBy': {'description': 'User ID of who last modified the '
#                                               'record. At creation time, '
#                                               '`modifiedByUser` is set as '
#                                               '`createdByUser`.',
#                                'meta:xdmField': 'xdm:repositoryLastModifiedBy',
#                                'meta:xdmType': 'string',
#                                'title': 'Modified by user identifier',
#                                'type': 'string'}}
```

To remove the resources that we just created, we can call `schema.delete` and `field_group.delete`.

```python
# Delete the schema
schema.delete()

# Delete the field_group
field_group.delete()
```
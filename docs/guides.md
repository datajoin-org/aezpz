## Adding a Field to a Schema

First you'll need to load the schema that you want to add a field to. You can do this by calling the `schemas.get` method and passing the `id` of the schema that you want to load.

```python
import aezpz

# Load the credentials from the credentials file
api = aezpz.load_config('path/to/credentials.json')

# Load the schema by id
schema = api.schemas.get('_mytenant.schemas.7a5416d13571')
```

Alternatively, you can use the `schemas.find` method to find the schema by title.

??? note

    When searching for a schema by title, it is good to specify the container with `tenant_schemas` or `global_schemas` so that less requests are made to the server.

    Use `tenant_schemas` for schemas that were created by your organization and `global_schemas` for schemas that are "built-in" to the platform and provided by Adobe.

```python
# Load the schema by title
schema = api.tenant_schemas.find(title='My Schema')
```


Once you have the schema loaded, you can add a field to the schema by calling the `fields.create` method on the schema object.

!!! warning "Not Implemented"

    This feature is not implemented yet. The below example will not work in the
    current version of the library.

```python
# Add a field to the schema
field = schema.fields.create(
    title='My Field',
    description='This is a field that I added to the schema',
    type='string'
)
```
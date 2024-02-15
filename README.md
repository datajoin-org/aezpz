# Adobe Experience Platform API made easy peasy

[![PyPI version](https://badge.fury.io/py/aezpz.svg)](https://badge.fury.io/py/aezpz)

[Documentation](https://datajoin-org.github.io/aezpz/)

The Adobe Experience Platform API is a RESTful API that uses OAuth 2.0 for authentication. To use the API you'll need to create a project in the Adobe Developer Console and create OAuth Server-to-Server credentials for your project.

This library makes it easy to authenticate with the Adobe Experience Platform API and make requests to the API.

### Installation
```bash
pip install aezpz
```

### Usage
```python
import aezpz

# Load the credentials from the credentials file
api = aezpz.load_config('path/to/credentials.json')

# Make a request to the API
api.schemas.find(title='my_schema')
```

### Credentials
1. Sign in to the Adobe Developer Console with your Adobe Experience Platform account [https://developer.adobe.com/console](https://developer.adobe.com/console)

2. Create a new project or use an existing project

3. Add the Experience Platform API to your project

4. Create OAuth Server-to-Server credentials for your project

5. On the Credentials page click the "Download JSON" button to download the credentials file

![Screenshot of credentials page with highlighted download json button](https://github.com/datajoin-org/aezpz/raw/main/docs/images/download-creds-screenshot.png)
The Adobe Experience Platform API is a RESTful API that
can be used to interact with the Adobe Experience Platform.

This site contains the project documentation for the
`aezpz` project that makes it easy to authenticate and 
make requests to the Adobe Experience Platform API.

## Table Of Contents

1. [Tutorials](tutorials.md)
2. [How-To Guides](how-to-guides.md)
3. [Reference](reference.md)
4. [Explanation](explanation.md)

## Getting Started

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

![Screenshot of credentials page with highlighted download json button](/images/download-creds-screenshot.png)
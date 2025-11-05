# Basic Authentication Guide for RSQueue

## Overview
RSQueue supports optional Basic Authentication to secure your queue service. When enabled, all API requests must include valid credentials.

## Configuration

### Environment Variables
Set the following environment variables to enable authentication:

- `AUTH_USER` - The username for Basic Authentication
- `AUTH_PASSWORD` - The password for Basic Authentication

**Note:** Both variables must be set to enable authentication. If either is missing or empty, authentication will be disabled.

## Running with Authentication

### Local Development
```bash
# Without authentication (default)
cargo run

# With authentication
AUTH_USER=admin AUTH_PASSWORD=secret123 cargo run
```

### Docker
```bash
# Without authentication
docker run -p 4000:4000 rsqueue

# With authentication
docker run -p 4000:4000 \
  -e AUTH_USER=admin \
  -e AUTH_PASSWORD=secret123 \
  rsqueue
```

### Docker Compose Example
```yaml
version: '3.8'
services:
  rsqueue:
    image: rsqueue:latest
    ports:
      - "4000:4000"
    environment:
      - AUTH_USER=admin
      - AUTH_PASSWORD=${RSQUEUE_PASSWORD:-changeme}
    volumes:
      - ./queue_data:/app/queue_specs
```

## Making Authenticated Requests

### Using curl
```bash
# With Basic Auth flag
curl -u admin:secret123 http://localhost:4000/health

# With Authorization header
curl -H "Authorization: Basic YWRtaW46c2VjcmV0MTIz" http://localhost:4000/health
```

### Using HTTP clients
Most HTTP clients support Basic Authentication natively:

**JavaScript (fetch)**
```javascript
const response = await fetch('http://localhost:4000/health', {
  headers: {
    'Authorization': 'Basic ' + btoa('admin:secret123')
  }
});
```

**Python (requests)**
```python
import requests
from requests.auth import HTTPBasicAuth

response = requests.get(
    'http://localhost:4000/health',
    auth=HTTPBasicAuth('admin', 'secret123')
)
```

**Go**
```go
client := &http.Client{}
req, _ := http.NewRequest("GET", "http://localhost:4000/health", nil)
req.SetBasicAuth("admin", "secret123")
resp, _ := client.Do(req)
```

## Security Considerations

1. **Use HTTPS in Production**: Basic Authentication transmits credentials in base64 encoding (not encryption). Always use HTTPS in production.

2. **Strong Passwords**: Use strong, randomly generated passwords.

3. **Environment Variables**: Never commit credentials to version control. Use environment files or secrets management systems.

4. **Rate Limiting**: Consider implementing rate limiting to prevent brute force attacks.

## Testing Authentication

### Test without credentials (should return 401)
```bash
curl -I http://localhost:4000/health
# HTTP/1.1 401 Unauthorized
# WWW-Authenticate: Basic realm="RSQueue"
```

### Test with wrong credentials (should return 401)
```bash
curl -I -u admin:wrongpass http://localhost:4000/health
# HTTP/1.1 401 Unauthorized
```

### Test with correct credentials (should return 200)
```bash
curl -I -u admin:secret123 http://localhost:4000/health
# HTTP/1.1 200 OK
```

## Disabling Authentication
To disable authentication, simply don't set the environment variables or set them to empty values:

```bash
# Any of these will disable auth:
docker run -p 4000:4000 rsqueue
docker run -p 4000:4000 -e AUTH_USER="" -e AUTH_PASSWORD="" rsqueue
```
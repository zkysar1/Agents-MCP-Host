# Agents-MCP-Host API Documentation

This document provides detailed specifications for all API endpoints available in the Agents-MCP-Host application.

## Base URL
```
http://localhost:8080
```

## Authentication
Currently, no authentication is required for any endpoints. Future versions may implement API key authentication via the `ZAK_AGENT_KEY` environment variable.

## Endpoints

### 1. Health Check

**Endpoint:** `GET /health`

**Description:** Returns system health metrics and resource utilization information.

**Request:**
```bash
curl http://localhost:8080/health
```

**Response (200 OK):**
```json
{
  "status": "healthy",
  "timestamp": 1754487723794,
  "version": "1.0.0",
  "service": "Agents-MCP-Host Host API",
  "uptime": 12078,
  "memory": {
    "usedMB": 14,
    "freeMB": 111,
    "totalMB": 126,
    "maxMB": 1970,
    "usagePercent": 0.73
  },
  "threads": 6,
  "processors": 4
}
```

**Response Fields:**
- `status`: Always "healthy" if the service is running
- `timestamp`: Current time in milliseconds since epoch
- `version`: Application version
- `service`: Service name
- `uptime`: Time since service started in milliseconds
- `memory`: Memory usage statistics
  - `usedMB`: Used memory in megabytes
  - `freeMB`: Free memory in megabytes
  - `totalMB`: Total allocated memory in megabytes
  - `maxMB`: Maximum available memory in megabytes
  - `usagePercent`: Percentage of max memory used
- `threads`: Number of active threads
- `processors`: Number of available processors

### 2. Host Status

**Endpoint:** `GET /host/v1/status`

**Description:** Returns host configuration and runtime status information.

**Request:**
```bash
curl http://localhost:8080/host/v1/status
```

**Response (200 OK):**
```json
{
  "status": "running",
  "version": "1.0.0",
  "timestamp": 1754487741463,
  "uptime": 29736,
  "zakAgentKey": "not configured",
  "instanceId": "local-instance",
  "publicIpAddress": "127.0.0.1",
  "awsRegion": "us-east-2"
}
```

**Response Fields:**
- `status`: Service status (always "running" if responsive)
- `version`: Application version
- `timestamp`: Current time in milliseconds since epoch
- `uptime`: Time since service started in milliseconds
- `zakAgentKey`: "configured" or "not configured" (never shows actual key)
- `instanceId`: Instance identifier (default: "local-instance")
- `publicIpAddress`: Public IP address (default: "127.0.0.1")
- `awsRegion`: AWS region from environment variable (default: "us-east-2")

### 3. Conversation API

**Endpoint:** `POST /host/v1/conversations`

**Description:** OpenAI-compatible chat completions endpoint. When `OPENAI_API_KEY` is configured, forwards requests to OpenAI's GPT-4o-mini model. Without the API key, returns a fallback message.

**Headers:**
- `Content-Type: application/json` (required)

**Request Body:**
```json
{
  "messages": [
    {
      "role": "system",
      "content": "You are a helpful assistant"
    },
    {
      "role": "user",
      "content": "Hello, how are you?"
    }
  ]
}
```

**Request Fields:**
- `messages` (required): Array of message objects
  - `role`: Message role ("system", "user", or "assistant")
  - `content`: Message content string

**Example Request:**
```bash
curl -X POST http://localhost:8080/host/v1/conversations \
  -H "Content-Type: application/json" \
  -d '{
    "messages": [
      {
        "role": "user",
        "content": "Hello, how are you?"
      }
    ]
  }'
```

**Response (200 OK):**
```json
{
  "id": "conv-33c227bd",
  "object": "chat.completion",
  "created": 1754487751,
  "model": "hardcoded-v1",
  "choices": [
    {
      "index": 0,
      "message": {
        "role": "assistant",
        "content": "Hard coded response placeholder to add LLM response later"
      },
      "finish_reason": "stop"
    }
  ],
  "usage": {
    "prompt_tokens": 0,
    "completion_tokens": 0,
    "total_tokens": 0
  }
}
```

**Response Fields:**
- `id`: Unique conversation identifier
- `object`: Always "chat.completion"
- `created`: Unix timestamp
- `model`: Model identifier ("gpt-4o-mini-2024-07-18" with OpenAI, "fallback-v1" without)
- `choices`: Array of completion choices
  - `index`: Choice index (starts at 0)
  - `message`: Response message object
    - `role`: Always "assistant"
    - `content`: AI-generated response or fallback message
  - `finish_reason`: Completion reason
- `usage`: Token usage statistics
  - `prompt_tokens`: Input token count
  - `completion_tokens`: Output token count
  - `total_tokens`: Total token count

**Error Response (400 Bad Request):**
```json
{
  "error": {
    "message": "Request must include 'messages' array",
    "type": "invalid_request_error",
    "code": 400
  }
}
```

**Common Error Messages:**
- "Request must include 'messages' array" - Missing messages field
- "Messages array cannot be empty" - Empty messages array
- "No user message found in conversation" - No user role message in array

**Error Response (500 Internal Server Error):**
```json
{
  "error": {
    "message": "Internal server error: [error details]",
    "type": "invalid_request_error",
    "code": 500
  }
}
```

## Error Handling

### 404 Not Found
Any request to an undefined endpoint returns:
```json
{
  "error": "Resource not found",
  "path": "/undefined/path",
  "method": "GET"
}
```

## CORS Configuration

The API server has CORS enabled with the following settings:
- Allowed Origins: `*` (all origins)
- Allowed Methods: `OPTIONS`, `GET`, `POST`, `PUT`
- Allowed Headers: `Content-Type`, `X-API-KEY`, `origin`, `Access-Control-Allow-Origin`

## Implementation Notes

### Adding New Endpoints

To add a new endpoint:

1. Create a new Verticle class in `src/main/java/AgentsMCPHost/hostAPI/`
2. Implement the static `setRouter(Router parentRouter)` method
3. Add the router configuration in `HostAPIVerticle.java`
4. Deploy the verticle in `Driver.java`
5. Add a completion flag in `Driver.java`

### Future Enhancements

1. ~~**LLM Integration**~~: ✅ Implemented with OpenAI GPT-4o-mini
2. **Authentication**: Implement API key validation using `ZAK_AGENT_KEY`
3. **Streaming**: Add Server-Sent Events (SSE) support for streaming responses
4. **Conversation History**: Implement conversation storage and retrieval
5. **Rate Limiting**: Add request rate limiting per client
6. **Metrics**: Add prometheus-compatible metrics endpoint
7. **WebSocket**: Add WebSocket support for real-time communication
8. **Multiple LLM Providers**: Support for Anthropic, Google, and other providers
9. **Caching**: Cache frequently requested responses
10. **Custom System Prompts**: Allow configuration of system prompts per endpoint

## Testing

### Manual Testing with curl

```bash
# Test health endpoint
curl http://localhost:8080/health | jq

# Test status endpoint
curl http://localhost:8080/host/v1/status | jq

# Test conversation endpoint
curl -X POST http://localhost:8080/host/v1/conversations \
  -H "Content-Type: application/json" \
  -d '{"messages":[{"role":"user","content":"Test message"}]}' | jq

# Test error handling
curl -X POST http://localhost:8080/host/v1/conversations \
  -H "Content-Type: application/json" \
  -d '{"invalid":"data"}' | jq
```

### Integration Testing

Use the test suite:
```bash
./gradlew test
```

## Performance Considerations

- The server uses Vert.x's event-driven, non-blocking architecture
- Worker pool size: 4 threads
- Event loop pool size: 1 thread
- All I/O operations are asynchronous
- JSON processing uses Jackson for efficiency

## Monitoring

- Check `/health` endpoint for system metrics
- Monitor log files in `logs/` directory
- Use `logLevel` in Driver.java to control logging verbosity (0-4)
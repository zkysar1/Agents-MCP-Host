# System Architecture

## 📋 Overview

Agents-MCP-Host is built on the **Vert.x** reactive framework, providing a non-blocking, event-driven architecture that efficiently handles concurrent API requests. The system integrates with OpenAI's API to provide intelligent chat responses.

## 🏗️ High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Client Applications                   │
│                   (Web, Mobile, CLI, Services)               │
└─────────────────┬───────────────────────────────────────────┘
                  │ HTTP/REST
                  ▼
┌─────────────────────────────────────────────────────────────┐
│                    Agents-MCP-Host API Server                      │
│  ┌─────────────────────────────────────────────────────┐    │
│  │                  HostAPIVerticle                     │    │
│  │                    (Port 8080)                       │    │
│  └──────┬────────────────┬────────────────┬────────────┘    │
│         │                │                │                  │
│    ┌────▼────┐    ┌─────▼─────┐    ┌────▼──────┐          │
│    │ Health  │    │  Status   │    │Conversation│          │
│    │Verticle │    │ Verticle  │    │ Verticle  │          │
│    └─────────┘    └───────────┘    └────┬──────┘          │
│                                          │                  │
│                                    ┌─────▼─────┐            │
│                                    │   LLM     │            │
│                                    │  Service  │            │
│                                    └─────┬─────┘            │
└──────────────────────────────────────────┼──────────────────┘
                                          │ HTTPS
                                          ▼
                              ┌────────────────────┐
                              │   OpenAI API       │
                              │  (GPT-4o-mini)     │
                              └────────────────────┘
```

## 🔧 Core Components

### 1. **Driver** (`Driver.java`)
- **Role**: Application entry point and orchestrator
- **Responsibilities**:
  - Initializes Vert.x with configured thread pools
  - Deploys all verticles in correct order
  - Initializes services (LLM integration)
  - Manages application lifecycle

### 2. **HostAPIVerticle** 
- **Role**: Main HTTP server
- **Port**: 8080
- **Features**:
  - CORS configuration for web clients
  - Request routing to endpoint verticles
  - 404 handling for undefined routes
  - Static method router pattern

### 3. **Endpoint Verticles**

#### HealthVerticle (`/health`)
- Returns system health metrics
- Memory usage, uptime, thread count
- No authentication required
- Used for monitoring/load balancers

#### StatusVerticle (`/host/v1/status`)
- Server configuration information
- Shows API key configuration status
- Environment details (AWS region, instance ID)

#### ConversationVerticle (`/host/v1/conversations`)
- OpenAI-compatible chat completions
- Validates message format
- Integrates with LLM Service
- Handles async responses

### 4. **LlmAPIService** (Singleton Service)
- **Not a verticle** - shared service instance
- WebClient for async HTTP calls
- OpenAI API integration
- Error handling and retry logic
- Response parsing and forwarding

### 5. **LoggerVerticle**
- Centralized asynchronous logging
- Buffers and batch writes (30-second intervals)
- Daily log rotation
- CSV format with timestamps
- Event bus integration

## 🔄 Request Flow

### Chat API Request Journey:

1. **Client Request** → HTTP POST to `/host/v1/conversations`
2. **HostAPIVerticle** → Routes to ConversationVerticle
3. **ConversationVerticle** → Validates request format
4. **LlmAPIService Called** → Prepares OpenAI request
5. **WebClient** → Async HTTPS call to OpenAI
6. **OpenAI Response** → Parsed and validated
7. **Response Returned** → Client receives JSON response

### Async Processing:
```java
// Non-blocking chain
validateRequest()
  .compose(messages -> llmService.chatCompletion(messages))
  .onSuccess(response -> ctx.response().end(response))
  .onFailure(error -> sendError(ctx, error));
```

## 📦 Technology Stack

### Core Framework
- **Vert.x 4.5.7**: Reactive, event-driven framework
- **Java 21**: Required JDK version
- **Gradle 8.8**: Build automation

### Key Libraries
```
io.vertx:vertx-web         → HTTP server and routing
io.vertx:vertx-web-client  → Async HTTP client for OpenAI
jackson:2.15.3             → JSON processing
flexmark:0.64.8            → Markdown processing
```

### Thread Model
- **Event Loop**: 1 thread (handles all I/O)
- **Worker Pool**: 4 threads (blocking operations)
- **Non-blocking**: All API calls are async

## 📁 Project Structure

```
Agents-MCP-Host/
├── src/main/java/AgentsMCPHost/
│   ├── Driver.java                 # Application entry point
│   ├── LoggerVerticle.java         # Async logging service
│   ├── services/
│   │   └── LlmAPIService.java      # OpenAI integration
│   └── hostAPI/
│       ├── HostAPIVerticle.java    # Main HTTP server
│       ├── ConversationVerticle.java # Chat endpoint
│       ├── HealthVerticle.java      # Health check
│       └── StatusVerticle.java      # Status endpoint
├── data/
│   └── agent/
│       └── logs/                   # Application logs
│           └── current.csv         # Active log file
└── build/
    └── libs/
        └── Agents-MCP-Host-1.0.0-fat.jar  # Executable JAR
```

## 🔐 Security Considerations

### API Key Management
- Never logged or exposed in responses
- Accessed via environment variables only
- Status endpoint shows "configured" not value

### Input Validation
- Message format validation
- Request size limits (via BodyHandler)
- Error messages don't leak sensitive info

### CORS Configuration
- Configured for web client access
- Allows all origins (customize for production)
- Specific HTTP methods allowed

## 🚀 Scaling Considerations

### Current Design (Single Instance)
- Handles ~100-200 concurrent requests
- Limited by OpenAI rate limits
- Single event loop for all I/O

### Future Scaling Options

#### Vertical Scaling
- Increase worker pool size
- Add more event loop threads
- Increase JVM heap size

#### Horizontal Scaling
- Deploy multiple instances
- Load balancer (nginx/HAProxy)
- Shared state via Redis/Hazelcast

#### Caching Layer
- Cache frequent responses
- Redis for distributed cache
- TTL based on content type

## 🔌 Extension Points

### Adding New Endpoints

1. Create new Verticle:
```java
public class CustomVerticle extends AbstractVerticle {
  public static void setRouter(Router router) {
    router.get("/custom").handler(ctx -> {
      // Handle request
    });
  }
}
```

2. Register in HostAPIVerticle:
```java
CustomVerticle.setRouter(mainRouter);
```

3. Deploy in Driver:
```java
vertx.deployVerticle(new CustomVerticle());
```

### Adding New LLM Providers

1. Create provider interface:
```java
interface LLMProvider {
  Future<JsonObject> complete(JsonArray messages);
}
```

2. Implement for each provider:
```java
class AnthropicProvider implements LLMProvider { }
class GoogleProvider implements LLMProvider { }
```

3. Configure in LlmAPIService

## 📊 Monitoring & Observability

### Health Metrics (`/health`)
- Memory usage (used/free/max)
- Thread count
- Uptime
- Processor count

### Logging
- Structured CSV format
- Severity levels (0-4)
- Category-based filtering
- Event correlation via sequence numbers

### Future Enhancements
- Prometheus metrics endpoint
- OpenTelemetry integration
- Distributed tracing
- Request/response logging

## 🔄 Event Bus Communication

### Message Format
```
"message,level,source,category,type"
```

### Key Addresses
- `log` - Logging messages
- `saveAllDataToFiles_OnTermination` - Graceful shutdown

### Publishing Events
```java
vertx.eventBus().publish("log", 
  "Event description,2,Component,Category,Type");
```

## 🛠️ Configuration

### Environment Variables
- `OPENAI_API_KEY` - Required for LLM
- `ZAK_AGENT_KEY` - Future auth
- `AWS_REGION` - AWS services
- `DATA_PATH` - Data directory

### Startup Configuration
```java
VertxOptions:
  - workerPoolSize: 4
  - eventLoopPoolSize: 1
  
HttpServerOptions:
  - port: 8080
  - ssl: false (local dev)
```

## 🔮 Future Architecture Goals

1. **MCP Integration**: Full Model Context Protocol support
2. **Multi-Provider**: Support for multiple LLM providers
3. **Streaming**: Server-sent events for real-time responses
4. **WebSocket**: Persistent connections for chat sessions
5. **Authentication**: API key validation and rate limiting
6. **Persistence**: Conversation history storage
7. **Clustering**: Vert.x cluster mode for HA

---

This architecture provides a solid foundation for a scalable, maintainable AI-powered API server while keeping the codebase simple and focused.
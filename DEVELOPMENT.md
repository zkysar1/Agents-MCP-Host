# Development Guide

This guide covers everything you need to develop, test, and contribute to Agents-MCP-Host.

## 🧪 Testing Requirements

| Test Type | Requires OPENAI_API_KEY | Description |
|-----------|------------------------|-------------|
| `./test-mcp-full.sh` | ❌ No | Comprehensive MCP infrastructure test |
| `./test-openai.sh` | ✅ **Yes** | Full OpenAI integration test |
| `./test-mcp-endpoints.sh` | ❌ No | Basic tool detection and routing |
| MCP Status endpoints | ❌ No | `/host/v1/mcp/status`, `/host/v1/tools`, `/host/v1/clients` |
| Tool execution | ❌ No | All 13 MCP tools work without API key |
| General chat messages | ✅ **Yes** | Requires API key for non-tool AI responses |

## 🛠️ Development Setup

### Prerequisites

1. **Java Development Kit (JDK) 21**
   ```bash
   java --version  # Should show 21.x.x
   ```

2. **IDE Setup** (Recommended: IntelliJ IDEA)
   - Install IntelliJ IDEA Community Edition
   - Import as Gradle project
   - Set Project SDK to Java 21
   - Enable annotation processing

## 📁 Project Structure

```
Agents-MCP-Host/
├── src/
│   ├── main/
│   │   └── java/AgentsMCPHost/
│   │       ├── Driver.java              # Entry point & orchestration
│   │       ├── LoggerVerticle.java      # Async logging service
│   │       ├── services/                # Shared services
│   │       │   └── LlmAPIService.java   # LLM integration (OpenAI)
│   │       ├── hostAPI/                 # HTTP endpoint verticles
│   │       │   ├── HostAPIVerticle.java # Main HTTP server
│   │       │   ├── ConversationVerticle.java # Unified chat endpoint
│   │       │   ├── HealthVerticle.java  # Health monitoring
│   │       │   └── StatusVerticle.java  # Status information
│   │       └── mcp/                     # Full MCP Implementation
│   │           ├── servers/             # MCP Servers (Worker Verticles)
│   │           │   ├── CalculatorServerVerticle.java  # Port 8081
│   │           │   ├── WeatherServerVerticle.java     # Port 8082
│   │           │   ├── DatabaseServerVerticle.java    # Port 8083
│   │           │   └── FileSystemServerVerticle.java  # Port 8084
│   │           ├── clients/             # MCP Clients (Standard Verticles)
│   │           │   ├── DualServerClientVerticle.java
│   │           │   ├── SingleServerClientVerticle.java
│   │           │   ├── FileSystemClientVerticle.java
│   │           │   └── LocalServerClientVerticle.java  # NEW: Stdio client
│   │           ├── host/                # Host Management
│   │           │   └── McpHostManagerVerticle.java      # UPDATED: Config support
│   │           ├── transport/           # Transport Layer
│   │           │   ├── VertxStreamableHttpTransport.java
│   │           │   └── VertxStdioTransport.java        # NEW: Stdio transport
│   │           └── config/              # Configuration Management (NEW)
│   │               └── McpConfigLoader.java            # Config loader
│   ├── test/
│   │   ├── java/                        # Test classes
│   │   └── resources/                   # Test configurations
│   └── resources/                       # Application resources (NEW)
│       └── mcp-config.json              # MCP configuration file
├── build.gradle.kts                     # Build configuration
├── settings.gradle.kts                  # Project settings
├── README.md                            # Main documentation
├── DEVELOPMENT.md                       # This file
├── MCP-IMPLEMENTATION.md                # MCP architecture details
├── CLAUDE.md                           # AI agent context
├── test-mcp-endpoints.sh              # Integration test script
└── gradlew / gradlew.bat               # Gradle wrapper scripts
```

## 🔨 Build Commands

### Basic Operations

```bash
# Clean build artifacts
./gradlew clean

# Compile Java source
./gradlew compileJava

# Run tests
./gradlew test

# Build everything (compile + test)
./gradlew build

# Run the application
./gradlew run

# Build executable JAR
./gradlew shadowJar
```

### Advanced Commands

```bash
# Run with debug output
./gradlew run --debug

# Run specific test class
./gradlew test --tests "AgentsMCPHost.DriverTest"

# Run with custom JVM options
./gradlew run -Dvertx.logger-delegate-factory-class-name=io.vertx.core.logging.SLF4JLogDelegateFactory

# Check dependencies
./gradlew dependencies

# Update Gradle wrapper
./gradlew wrapper --gradle-version=8.8
```

## 🧪 Testing

### Running Tests

```bash
# Run all tests
./gradlew test

# Run with detailed output
./gradlew test --info

# Run specific test
./gradlew test --tests "*ConversationTest"

# Generate test report
./gradlew test jacocoTestReport
```

### Writing Tests

Example test structure:
```java
package AgentsMCPHost;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
class ConversationTest {
    
    @Test
    void testChatEndpoint(Vertx vertx, VertxTestContext ctx) {
        // Test implementation
        ctx.completeNow();
    }
}
```

### Manual Testing

Use the included test scripts:
```bash
# Test MCP endpoints
chmod +x test-mcp-endpoints.sh
./test-mcp-endpoints.sh

# Test OpenAI integration (if available)
chmod +x test-openai.sh
OPENAI_API_KEY=your-key ./test-openai.sh
```

Or test manually:
```bash
# Start server
OPENAI_API_KEY=$(wslvar OPENAI_API_KEY) ./gradlew run

# Test unified conversation endpoint (auto-detects tool needs)

# General message - uses OpenAI if API key set, else fallback
curl -X POST http://localhost:8080/host/v1/conversations \
  -H "Content-Type: application/json" \
  -d '{"messages":[{"role":"user","content":"Hello"}]}'

# Tool message - auto-detects 'calculate' and uses MCP (no API key needed)
curl -X POST http://localhost:8080/host/v1/conversations \
  -H "Content-Type: application/json" \
  -d '{"messages":[{"role":"user","content":"Calculate 10 plus 20"}]}'

# Tool trigger keywords: calculate, weather, database, file, query, list
```

## 🎯 Common Development Tasks

### Configuring MCP Servers

1. **Edit Configuration File** (`src/main/resources/mcp-config.json`):
```json
{
  "mcpServers": {
    "httpServers": {
      "myserver": {
        "enabled": true,
        "port": 8085,
        "description": "My custom server"
      }
    },
    "localServers": {
      "python-server": {
        "enabled": true,
        "command": "python",
        "args": ["-m", "mcp_server"],
        "environment": {"PYTHONPATH": "./lib"}
      }
    }
  },
  "toolNaming": {
    "usePrefixing": true,
    "prefixSeparator": "__"
  }
}
```

2. **Override via Environment Variables**:
```bash
export MCP_CONFIG_PATH=/custom/path/config.json
export MCP_CALCULATOR_PORT=9081
export MCP_USE_PREFIXING=true
```

### Adding a New API Endpoint

1. **Create the Verticle** (`src/main/java/AgentsMCPHost/hostAPI/MyEndpointVerticle.java`):
```java
package AgentsMCPHost.hostAPI;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

public class MyEndpointVerticle extends AbstractVerticle {
    
    @Override
    public void start(Promise<Void> startPromise) {
        startPromise.complete();
    }
    
    public static void setRouter(Router parentRouter) {
        parentRouter.get("/my-endpoint").handler(MyEndpointVerticle::handleRequest);
    }
    
    private static void handleRequest(RoutingContext ctx) {
        JsonObject response = new JsonObject()
            .put("status", "success")
            .put("message", "Hello from new endpoint");
            
        ctx.response()
            .putHeader("Content-Type", "application/json")
            .setStatusCode(200)
            .end(response.encode());
    }
}
```

2. **Register in HostAPIVerticle** (`HostAPIVerticle.java`):
```java
// Add with other verticle registrations
MyEndpointVerticle.setRouter(mainRouter);
```

3. **Deploy in Driver** (`Driver.java`):
```java
// Add deployment method
private void setMyEndpointVerticle() {
    vertx.deployVerticle(new MyEndpointVerticle(), res -> {
        if (res.succeeded()) {
            // Log success
        }
    });
}

// Call in doIt() method
setMyEndpointVerticle();
```

### Adding a New MCP Tool

1. **For HTTP Servers - Add to Server Verticle**:
```java
// In your server verticle
private final JsonArray tools = new JsonArray()
    .add(new JsonObject()
        .put("name", "myTool")
        .put("description", "My custom tool")
        .put("inputSchema", new JsonObject()
            .put("type", "object")
            .put("properties", new JsonObject()
                .put("param", new JsonObject().put("type", "string")))
            .put("required", new JsonArray().add("param"))));

// Tool will automatically be prefixed as serverName__myTool
```

2. **For Local Stdio Servers - Define in External Process**:
```python
# In your Python MCP server
tools = [
    {
        "name": "myTool",
        "description": "My tool",
        "inputSchema": {...}
    }
]
# Will be discovered and prefixed automatically
```

### Understanding Tool Naming

All tools now use the `serverName__toolName` pattern:
```java
// Original tool name from server
String originalName = "add";

// Prefixed name used in system
String prefixedName = "calculator__add";

// The system handles both:
// - calculator__add (prefixed)
// - add (will search for any server with this tool)
```

### Adding a New LLM Provider

1. **Create Provider Interface**:
```java
public interface LLMProvider {
    Future<JsonObject> complete(JsonArray messages);
    boolean isAvailable();
    String getName();
}
```

2. **Implement Provider**:
```java
public class AnthropicProvider implements LLMProvider {
    // Implementation
}
```

3. **Update LlmAPIService** to support multiple providers.

### Modifying the Logging System

The LoggerVerticle handles all logging via the event bus:
```java
// Send log message
vertx.eventBus().publish("log", 
    "Message,Level,Class,Category,Type");

// Levels: 0=error, 1=info, 2=detail, 3=debug, 4=data
```

## 🐛 Debugging

### Enable Debug Logging

Set log level in `Driver.java`:
```java
public static int logLevel = 4; // Maximum verbosity
```

### Working with Transport Layers

1. **HTTP/SSE Transport** (for remote servers):
```java
VertxStreamableHttpTransport transport = 
    new VertxStreamableHttpTransport(vertx, "localhost", 8081);
transport.initialize()
    .compose(v -> transport.listTools())
    .onSuccess(tools -> {/* process tools */});
```

2. **Stdio Transport** (for local processes):
```java
VertxStdioTransport transport = 
    new VertxStdioTransport(vertx, "python", args, env);
transport.start()
    .compose(v -> transport.initialize())
    .compose(v -> transport.listTools())
    .onSuccess(tools -> {/* process tools */});
```

### Common Issues

| Issue | Solution |
|-------|----------|
| Port 8080 in use | `lsof -ti:8080 \| xargs kill -9` |
| Class not found | `./gradlew clean compileJava` |
| Out of memory | Increase heap: `JAVA_OPTS="-Xmx2g" ./gradlew run` |
| Gradle daemon issues | `./gradlew --stop` then retry |
| Config not loading | Check path: `src/main/resources/mcp-config.json` |
| Local server fails | Check command exists, add to PATH |
| Tool not found | Verify tool name uses `server__tool` pattern |
| Lambda compilation error | Make variables final before lambda use |

### IntelliJ IDEA Debugging

1. Create Run Configuration:
   - Type: Application
   - Main class: `AgentsMCPHost.Driver`
   - Module: Agents-MCP-Host.main
   - Environment: `OPENAI_API_KEY=your-key`

2. Set breakpoints and run in debug mode

## 📦 Dependencies

### Core Dependencies (build.gradle.kts)

```kotlin
dependencies {
    // MCP SDK (simplified implementation)
    implementation("io.modelcontextprotocol.sdk:mcp:0.11.0")
    
    // Vert.x framework
    implementation("io.vertx:vertx-web:4.5.7")
    implementation("io.vertx:vertx-web-client:4.5.7")
    implementation("io.vertx:vertx-config:4.5.7")
    
    // JSON processing
    implementation("com.fasterxml.jackson.core:jackson-databind:2.15.3")
    
    // Testing
    testImplementation("io.vertx:vertx-junit5")
    testImplementation("org.junit.jupiter:junit-jupiter:5.9.1")
    testImplementation("org.mockito:mockito-core:5.11.0")
}
```

### Adding New Dependencies

1. Add to `build.gradle.kts`:
```kotlin
implementation("group:artifact:version")
```

2. Refresh Gradle:
```bash
./gradlew --refresh-dependencies
```

## 🚀 Deployment

### Building for Production

```bash
# Build fat JAR
./gradlew shadowJar

# Output: build/libs/Agents-MCP-Host-1.0.0-fat.jar
```

### Running in Production

```bash
# With environment variables
OPENAI_API_KEY=xxx java -jar build/libs/Agents-MCP-Host-1.0.0-fat.jar

# With JVM tuning
java -Xmx1g -Xms512m \
     -Dvertx.logger-delegate-factory-class-name=io.vertx.core.logging.SLF4JLogDelegateFactory \
     -jar build/libs/Agents-MCP-Host-1.0.0-fat.jar
```

### Docker Deployment

Create `Dockerfile`:
```dockerfile
FROM eclipse-temurin:21-jre-alpine
COPY build/libs/Agents-MCP-Host-1.0.0-fat.jar app.jar
EXPOSE 8080
ENV OPENAI_API_KEY=""
ENTRYPOINT ["java", "-jar", "/app.jar"]
```

Build and run:
```bash
docker build -t zak-agent .
docker run -p 8080:8080 -e OPENAI_API_KEY=xxx zak-agent
```

## 🔄 Git Workflow

### Branch Strategy

- `main` - Stable production code
- `develop` - Integration branch
- `feature/*` - New features
- `bugfix/*` - Bug fixes
- `release/*` - Release preparation

### Commit Messages

Follow conventional commits:
```
feat: Add new chat endpoint
fix: Correct token counting logic
docs: Update API documentation
refactor: Simplify error handling
test: Add conversation tests
chore: Update dependencies
```

### Pull Request Process

1. Create feature branch
2. Make changes with tests
3. Run `./gradlew build`
4. Push and create PR
5. Address review comments
6. Merge after approval

## 📊 Performance Tuning

### JVM Options

```bash
# Heap size
-Xmx2g -Xms1g

# GC tuning
-XX:+UseG1GC
-XX:MaxGCPauseMillis=200

# Vert.x options
-Dvertx.disableFileCaching=false
-Dvertx.threadChecks=false
```

### Vert.x Configuration

In `Driver.java`:
```java
VertxOptions options = new VertxOptions()
    .setWorkerPoolSize(8)      // Increase for CPU-bound work
    .setEventLoopPoolSize(2)    // Usually 2x CPU cores
    .setMaxEventLoopExecuteTime(2000000000); // 2 seconds
```

## 🔐 Security Considerations

### API Key Management

- Never commit API keys
- Use environment variables
- Add `.env` to `.gitignore`
- Rotate keys regularly

### Input Validation

Always validate incoming data:
```java
if (requestBody == null || !requestBody.containsKey("required_field")) {
    sendError(ctx, 400, "Missing required field");
    return;
}
```

### CORS Configuration

Restrict origins in production:
```java
CorsHandler.create()
    .addOrigin("https://yourdomain.com")
    .allowedMethod(HttpMethod.POST);
```

## 📝 Code Style Guidelines

### Java Conventions

- Use 2-space indentation
- Opening braces on same line
- Descriptive variable names
- JavaDoc for public methods

### Example:
```java
/**
 * Processes chat completion request.
 * @param messages The conversation messages
 * @return Future containing the response
 */
public Future<JsonObject> processChat(JsonArray messages) {
  if (messages == null || messages.isEmpty()) {
    return Future.failedFuture("Messages cannot be empty");
  }
  // Implementation
}
```

## 🤝 Contributing

1. Fork the repository
2. Create feature branch
3. Write tests for new features
4. Ensure all tests pass
5. Submit pull request

### Code Review Checklist

- [ ] Tests included
- [ ] Documentation updated
- [ ] No hardcoded values
- [ ] Error handling complete
- [ ] Logging appropriate
- [ ] Performance considered

## 📚 Resources

### Documentation
- [Vert.x Documentation](https://vertx.io/docs/)
- [OpenAI API Reference](https://platform.openai.com/docs)
- [Gradle User Guide](https://docs.gradle.org/current/userguide/userguide.html)

### Tools
- [Postman](https://www.postman.com/) - API testing
- [JVisualVM](https://visualvm.github.io/) - JVM monitoring
- [Apache JMeter](https://jmeter.apache.org/) - Load testing

---

**Questions?** Open an issue on GitHub or check existing issues for solutions.
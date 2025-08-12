# MCP Developer Guide

This guide provides detailed instructions for developers working with the Model Context Protocol (MCP) implementation in Agents-MCP-Host.

## Recent Updates (January 2025)

- **Stdio Transport Support** - Run local MCP servers as separate processes
- **Tool Name Prefixing** - All tools use `serverName__toolName` pattern
- **Configuration-Driven Architecture** - Flexible JSON-based configuration
- **Enhanced Tool Routing** - Smart routing for prefixed and unprefixed names

## Understanding MCP Architecture

### The Three MCP Roles

1. **MCP Server** - Exposes tools and resources
2. **MCP Client** - Connects to servers and discovers tools
3. **MCP Host** - Orchestrates clients and routes tool calls

In our implementation:
- **Servers** run as worker verticles on ports 8081-8084
- **Clients** run as standard verticles with async HTTP
- **Host** manages everything via McpHostManagerVerticle

## Configuration Management

All MCP settings are now managed via `src/main/resources/mcp-config.json`:

```json
{
  "mcpServers": {
    "httpServers": {
      "calculator": {"enabled": true, "port": 8081}
    },
    "localServers": {
      "python-server": {
        "enabled": false,
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

### Environment Variable Overrides

```bash
export MCP_CONFIG_PATH=/custom/config.json
export MCP_CALCULATOR_PORT=9081
export MCP_USE_PREFIXING=true
```

## Adding a New MCP Server

### Option 1: HTTP-Based Server

#### Step 1: Create the Server Verticle

```java
package AgentsMCPHost.mcp.servers;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

public class MyNewServerVerticle extends AbstractVerticle {
    
    private static final int PORT = 8085; // Choose unique port
    private static final String MCP_PROTOCOL_VERSION = "2024-11-05";
    
    // Define your tools
    private final JsonArray tools = new JsonArray()
        .add(new JsonObject()
            .put("name", "myTool")
            .put("description", "My custom tool")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("param1", new JsonObject().put("type", "string")))
                .put("required", new JsonArray().add("param1"))));
    
    @Override
    public void start(Promise<Void> startPromise) {
        Router router = Router.router(vertx);
        setRouter(router);
        
        // Start HTTP server
        vertx.createHttpServer()
            .requestHandler(router)
            .listen(PORT)
            .onSuccess(server -> {
                System.out.println("MyNew MCP Server started on port " + PORT);
                
                // Notify host
                vertx.eventBus().publish("mcp.server.ready", new JsonObject()
                    .put("server", "mynew")
                    .put("port", PORT)
                    .put("tools", tools.size()));
                    
                startPromise.complete();
            })
            .onFailure(startPromise::fail);
    }
    
    // Static router configuration (following repo pattern)
    public static void setRouter(Router router) {
        router.route().handler(BodyHandler.create());
        
        // CORS headers
        router.route().handler(ctx -> {
            ctx.response()
                .putHeader("Access-Control-Allow-Origin", "http://localhost:8080")
                .putHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
                .putHeader("Access-Control-Allow-Headers", "Content-Type, MCP-Protocol-Version, Mcp-Session-Id");
            
            if (ctx.request().method().name().equals("OPTIONS")) {
                ctx.response().setStatusCode(204).end();
            } else {
                ctx.next();
            }
        });
        
        // MCP endpoints
        router.post("/").handler(MyNewServerVerticle::handleMcpRequest);
        router.get("/").handler(MyNewServerVerticle::handleSseStream);
    }
    
    // Implement MCP protocol handlers...
}
```

#### Step 2: Add to Configuration

```json
// In mcp-config.json
"httpServers": {
  "mynewserver": {
    "enabled": true,
    "port": 8085,
    "description": "My new server"
  }
}
```

#### Step 3: Update McpHostManagerVerticle

```java
// In deployMcpInfrastructure()
if (httpServers.getJsonObject("mynewserver", new JsonObject()).getBoolean("enabled", false)) {
    serverDeployments.add(deployVerticle(new MyNewServerVerticle(), workerOpts, "MyNewServer"));
}
```

### Option 2: Local Stdio-Based Server

#### Step 1: Create MCP-Compatible Executable

```python
# my_mcp_server.py
import json
import sys

def handle_initialize(params):
    return {"protocolVersion": "2024-11-05"}

def handle_tools_list(params):
    return {
        "tools": [
            {
                "name": "myTool",
                "description": "My custom tool",
                "inputSchema": {...}
            }
        ]
    }

# Main JSON-RPC loop
while True:
    line = sys.stdin.readline()
    request = json.loads(line)
    # Handle request and write response to stdout
```

#### Step 2: Add to Configuration

```json
// In mcp-config.json
"localServers": {
  "my-python-server": {
    "enabled": true,
    "command": "python",
    "args": ["my_mcp_server.py"],
    "environment": {"PYTHONPATH": "./"}
  }
}
```

The LocalServerClientVerticle will automatically manage it!

## Tool Naming Convention

All tools MUST follow the `serverName__toolName` pattern:

```java
// Server defines tool
String originalName = "add";

// System automatically prefixes it
String prefixedName = "calculator__add";

// Both names work for routing
// - calculator__add (explicit)
// - add (searches all servers)
```

### Implementing Tool Prefixing

```java
// In client when discovering tools
for (Object toolObj : tools) {
    JsonObject tool = (JsonObject) toolObj;
    String originalName = tool.getString("name");
    String prefixedName = serverName + "__" + originalName;
    
    tool.put("name", prefixedName);
    tool.put("_originalName", originalName);
    tool.put("_server", serverName);
}
```

## Creating a New Client Configuration

### Step 1: Create the Client Verticle

```java
package AgentsMCPHost.mcp.clients;

import AgentsMCPHost.mcp.transport.VertxStreamableHttpTransport;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;

public class MyClientVerticle extends AbstractVerticle {
    
    private VertxStreamableHttpTransport transport;
    private static final String CLIENT_ID = "myclient";
    private static final int SERVER_PORT = 8085;
    
    @Override
    public void start(Promise<Void> startPromise) {
        // Initialize transport
        transport = new VertxStreamableHttpTransport(vertx, "localhost", SERVER_PORT);
        
        // Connect to server
        connectToServer()
            .onSuccess(v -> {
                // Register event bus consumers
                vertx.eventBus().consumer("mcp.client." + CLIENT_ID + ".call", this::handleToolCall);
                vertx.eventBus().consumer("mcp.client." + CLIENT_ID + ".status", this::handleStatusRequest);
                
                // Notify host
                publishClientReady();
                
                startPromise.complete();
            })
            .onFailure(startPromise::fail);
    }
    
    private Future<Void> connectToServer() {
        return transport.initialize()
            .compose(init -> transport.listTools())
            .map(tools -> {
                // Publish discovered tools
                vertx.eventBus().publish("mcp.tools.discovered", new JsonObject()
                    .put("client", CLIENT_ID)
                    .put("server", "mynew")
                    .put("tools", tools));
                
                // Start SSE stream
                transport.startSseStream(this::handleSseEvent);
                
                return null;
            });
    }
    
    // Implement handlers...
}
```

### Step 2: Add to Host Manager

```java
// In McpHostManagerVerticle
List<Future<String>> clientDeployments = Arrays.asList(
    deployVerticle(new DualServerClientVerticle(), null, "DualServerClient"),
    deployVerticle(new SingleServerClientVerticle(), null, "SingleServerClient"),
    deployVerticle(new FileSystemClientVerticle(), null, "FileSystemClient"),
    deployVerticle(new MyClientVerticle(), null, "MyClient") // Add here
);
```

## Working with Transport Layers

### HTTP/SSE Transport (VertxStreamableHttpTransport)

The transport adapter provides these key methods:

```java
// Initialize connection
Future<JsonObject> initialize()
Future<JsonObject> initializeWithParams(JsonObject params)

// Tool operations
Future<JsonArray> listTools()
Future<JsonObject> callTool(String toolName, JsonObject arguments)

// Resource operations (for FileSystem-like servers)
Future<JsonArray> listResources()
Future<JsonArray> listRoots()

// SSE streaming
Future<Void> startSseStream(Consumer<JsonObject> handler)
```

### Stdio Transport (VertxStdioTransport)

```java
// Create and start stdio transport
VertxStdioTransport transport = new VertxStdioTransport(
    vertx, 
    "python",                    // command
    List.of("-m", "mcp_server"), // args
    Map.of("ENV_VAR", "value")   // environment
);

// Start the process
transport.start()
    .compose(v -> transport.initialize())
    .compose(v -> transport.listTools())
    .onSuccess(tools -> {
        // Process discovered tools
    });

// Call a tool
transport.callTool("myTool", arguments)
    .onSuccess(result -> {
        // Handle result
    });

// Shutdown gracefully
transport.shutdown();
```

### Example Tool Call Flow

```java
// In client verticle
private void handleToolCall(Message<JsonObject> msg) {
    JsonObject request = msg.body();
    String toolName = request.getString("tool");
    JsonObject arguments = request.getJsonObject("arguments");
    
    transport.callTool(toolName, arguments)
        .onSuccess(result -> {
            result.put("_client", CLIENT_ID);
            msg.reply(result);
        })
        .onFailure(err -> {
            msg.fail(500, err.getMessage());
        });
}
```

## Event Bus Communication

### Key Event Bus Addresses

```java
// Tool discovery
"mcp.tools.discovered"     // Clients publish when tools found
"mcp.tools.aggregated"     // Host publishes aggregated view

// Client/Server status
"mcp.client.ready"         // Client announces readiness
"mcp.server.ready"         // Server announces readiness

// Tool routing (with prefixing)
"mcp.host.route"           // Route tool call (handles prefixed names)
"mcp.client.{id}.call"     // Call tool on specific client

// Status queries
"mcp.host.status"          // Get system status
"mcp.host.tools"           // Get all tools (with prefixed names)

// SSE events
"mcp.sse.{clientId}"       // SSE events from client

// Notifications (NEW)
"mcp.notification.{server}" // Server notifications from stdio servers
```

### Example Event Bus Message

```java
// Publishing tool discovery
vertx.eventBus().publish("mcp.tools.discovered", new JsonObject()
    .put("client", "myclient")
    .put("server", "myserver")
    .put("tools", new JsonArray()
        .add(new JsonObject()
            .put("name", "myTool")
            .put("description", "My tool"))));

// Requesting tool execution
vertx.eventBus().request("mcp.host.route", new JsonObject()
    .put("tool", "myTool")
    .put("arguments", new JsonObject()
        .put("param1", "value1")))
    .onSuccess(reply -> {
        JsonObject result = (JsonObject) reply.body();
        // Handle result
    });
```

## Implementing MCP Workflows

### 1. Basic Tool Invocation

```java
private Object executeTool(String toolName, JsonObject arguments) {
    switch (toolName) {
        case "myTool":
            String param = arguments.getString("param1");
            // Execute tool logic
            return doSomething(param);
        default:
            throw new IllegalArgumentException("Unknown tool: " + toolName);
    }
}
```

### 2. Sampling Workflow (Large Result Sets)

```java
private Object executeTool(String toolName, JsonObject arguments) {
    JsonArray results = performQuery(arguments);
    
    if (results.size() > THRESHOLD) {
        return new JsonObject()
            .put("requiresSampling", true)
            .put("resultCount", results.size())
            .put("preview", results.copy().subList(0, 10))
            .put("message", "Large result set - sampling recommended");
    }
    
    return results;
}
```

### 3. Resources/Roots Workflow

```java
// During initialization
private void handleInitialize(RoutingContext ctx, JsonObject params) {
    if (params.containsKey("roots")) {
        JsonArray roots = params.getJsonArray("roots");
        updateAllowedRoots(roots);
    }
    // ... rest of initialization
}

// Path validation
private boolean isPathAllowed(String path) {
    Path normalizedPath = Paths.get(path).normalize();
    for (String root : allowedRoots) {
        if (normalizedPath.startsWith(Paths.get(root))) {
            return true;
        }
    }
    return false;
}
```

## Testing Your MCP Components

### Unit Testing a Server

```java
@Test
public void testToolExecution() {
    // Deploy server
    vertx.deployVerticle(new MyServerVerticle(), testContext.succeeding(id -> {
        // Send tool call
        WebClient client = WebClient.create(vertx);
        
        JsonObject request = new JsonObject()
            .put("jsonrpc", "2.0")
            .put("method", "tools/call")
            .put("id", "test-1")
            .put("params", new JsonObject()
                .put("name", "myTool")
                .put("arguments", new JsonObject()
                    .put("param1", "test")));
        
        client.post(8085, "localhost", "/")
            .sendJsonObject(request, testContext.succeeding(response -> {
                JsonObject body = response.bodyAsJsonObject();
                testContext.verify(() -> {
                    assertTrue(body.containsKey("result"));
                    testContext.completeNow();
                });
            }));
    }));
}
```

### Integration Testing

```bash
# Add to test-mcp-full.sh
test_endpoint "My New Tool Test" "POST" "$CONV_URL" \
    '{"messages":[{"role":"user","content":"Use my new tool"}]}'
```

## Debugging Tips

### Enable Debug Logging

```java
// In Driver.java
public static int logLevel = 4; // Set to 4 for debug
```

### Monitor Event Bus

```java
// Add debug consumer
vertx.eventBus().consumer("mcp.*", msg -> {
    System.out.println("Event Bus: " + msg.address() + " -> " + msg.body());
});
```

### Check Port Availability

```bash
# Check if port is in use
lsof -i :8085

# Kill process using port
kill -9 $(lsof -t -i:8085)
```

### Common Issues and Solutions

| Issue | Cause | Solution |
|-------|-------|----------|
| Connection refused | Server not started | Ensure 2-second delay after server deployment |
| Tool not found | Discovery failed | Check tool uses `server__tool` pattern |
| SSE not working | Wrong Accept header | Ensure "Accept: text/event-stream" |
| Blocking event loop | Sync operations in standard verticle | Use worker verticle or executeBlocking |
| Config not loading | Wrong path | Check `src/main/resources/mcp-config.json` |
| Local server fails | Command not found | Ensure command is in PATH |
| Lambda compilation | Non-final variable | Make variable final before lambda |
| Tool name conflicts | Multiple same names | Use prefixed names consistently |

## Best Practices

### DO's
- ✅ Use worker verticles for servers (blocking operations)
- ✅ Use standard verticles for clients (async operations)
- ✅ Always use Vert.x Future for async operations
- ✅ Handle both success and failure cases
- ✅ Validate all inputs
- ✅ Follow existing patterns for consistency
- ✅ Use event bus for internal communication
- ✅ Add comprehensive error messages

### DON'Ts
- ❌ Don't block the event loop
- ❌ Don't use CompletableFuture (use Vert.x Future)
- ❌ Don't forget CORS headers
- ❌ Don't hardcode values (use constants)
- ❌ Don't skip error handling
- ❌ Don't mix sync and async patterns

## Advanced Topics

### Custom Transport Implementation

If you need a different transport (e.g., WebSocket):

```java
public class WebSocketTransport {
    // Implement MCP protocol over WebSocket
    // Follow VertxStreamableHttpTransport pattern
}
```

### Tool Composition

Chain multiple tools together:

```java
private Future<JsonObject> compositeToolCall() {
    return callTool("tool1", args1)
        .compose(result1 -> callTool("tool2", args2))
        .compose(result2 -> callTool("tool3", args3));
}
```

### Dynamic Tool Registration

Add/remove tools at runtime:

```java
// Add tool
tools.add(newToolDefinition);
vertx.eventBus().publish("mcp.tools.updated", tools);

// Remove tool
tools.removeIf(t -> t.getString("name").equals("oldTool"));
vertx.eventBus().publish("mcp.tools.updated", tools);
```

## Resources

- [MCP Specification](https://modelcontextprotocol.io)
- [Vert.x Documentation](https://vertx.io/docs/)
- [JSON-RPC 2.0 Spec](https://www.jsonrpc.org/specification)
- Project Examples:
  - Calculator Server: `src/main/java/AgentsMCPHost/mcp/servers/CalculatorServerVerticle.java`
  - DualClient: `src/main/java/AgentsMCPHost/mcp/clients/DualServerClientVerticle.java`
  - Transport: `src/main/java/AgentsMCPHost/mcp/transport/VertxStreamableHttpTransport.java`
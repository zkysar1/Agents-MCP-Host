# Full Model Context Protocol (MCP) Implementation

## Overview

This is a complete implementation of the Model Context Protocol (MCP) specification using the official MCP SDK v0.11.0. The system implements all three MCP roles (servers, clients, host) with proper architectural separation and supports multiple workflow patterns including tool invocation, sampling, and resources/roots management.

### Recent Enhancements (January 2025)

Inspired by analysis of SST OpenCode's implementation, we've added:
- **Stdio Transport Support** - Run local MCP servers as separate processes
- **Tool Name Prefixing** - `serverName__toolName` pattern for clear tool identification
- **Configuration-Driven Architecture** - Flexible JSON-based configuration system
- **Enhanced Tool Routing** - Support for both prefixed and unprefixed tool names

## Architecture

### MCP Component Hierarchy

```
┌────────────────────────────────────────────────────────────────┐
│                    Host Application (Port 8080)                 │
│  ┌────────────────────────────────────────────────────────┐    │
│  │                McpHostManagerVerticle                  │    │
│  │         (Orchestrates all MCP components)              │    │
│  └────────────────┬──────────────┬────────────────────────┘    │
│                   │              │                              │
│         Event Bus Messages    HTTP/SSE                          │
│                   │              │                              │
│  ┌────────────────▼──────────────▼────────────────────────┐    │
│  │                    MCP Clients                          │    │
│  │  ┌─────────────────────────────────────────────────┐   │    │
│  │  │ DualServerClient  │ SingleServerClient │ FileSystemClient││
│  │  │ (Calculator+Weather) │ (Database only) │ (FileSystem)   ││
│  │  └─────────────────────────────────────────────────┘   │    │
│  └──────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
                              │
                    HTTP/SSE Transport
                              │
┌────────────────────────────────────────────────────────────────┐
│                        MCP Servers                              │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌────────┐│
│  │ Calculator   │ │ Weather      │ │ Database     │ │FileSystem│
│  │ Port 8081    │ │ Port 8082    │ │ Port 8083    │ │Port 8084││
│  │ 4 tools      │ │ 2 tools      │ │ 4 tools      │ │4 tools  ││
│  └──────────────┘ └──────────────┘ └──────────────┘ └────────┘│
└────────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. MCP Servers (Worker Verticles)

Each server runs as a worker verticle on its own port, implementing the MCP protocol with HTTP/SSE transport.

#### CalculatorServerVerticle (Port 8081)
- **Tools**: `add`, `subtract`, `multiply`, `divide`
- **Pattern**: Simple tool invocation
- **Location**: `src/main/java/AgentsMCPHost/mcp/servers/CalculatorServerVerticle.java`

#### WeatherServerVerticle (Port 8082)
- **Tools**: `weather`, `forecast`
- **Pattern**: Tool invocation with optional elicitation
- **Location**: `src/main/java/AgentsMCPHost/mcp/servers/WeatherServerVerticle.java`

#### DatabaseServerVerticle (Port 8083)
- **Tools**: `query`, `insert`, `update`, `delete`
- **Pattern**: Tool invocation with sampling workflow for large result sets
- **Features**: Triggers sampling when results exceed threshold
- **Location**: `src/main/java/AgentsMCPHost/mcp/servers/DatabaseServerVerticle.java`

#### FileSystemServerVerticle (Port 8084)
- **Tools**: `list`, `read`, `write`, `delete`
- **Pattern**: Resources and roots workflows
- **Features**: 
  - Configurable allowed roots for security
  - Resource exposure for file access
  - Sandbox directory at `/tmp/mcp-sandbox`
- **Location**: `src/main/java/AgentsMCPHost/mcp/servers/FileSystemServerVerticle.java`

### 2. MCP Clients (Standard Verticles)

Clients run as standard verticles using async HTTP operations to connect to servers.

#### DualServerClientVerticle
- **Connects to**: Calculator (8081) + Weather (8082)
- **Tools available**: 6 tools (4 math + 2 weather)
- **Client ID**: `dual`
- **Purpose**: Demonstrates multi-server client configuration
- **Location**: `src/main/java/AgentsMCPHost/mcp/clients/DualServerClientVerticle.java`

#### SingleServerClientVerticle
- **Connects to**: Database (8083) only
- **Tools available**: 4 database tools
- **Client ID**: `single-db`
- **Purpose**: Demonstrates single-server client with sampling support
- **Location**: `src/main/java/AgentsMCPHost/mcp/clients/SingleServerClientVerticle.java`

#### FileSystemClientVerticle
- **Connects to**: FileSystem (8084) only
- **Tools available**: 4 file operation tools
- **Client ID**: `filesystem`
- **Purpose**: Demonstrates resources/roots workflows
- **Features**:
  - Configurable allowed roots
  - Resource management
  - Path validation
- **Location**: `src/main/java/AgentsMCPHost/mcp/clients/FileSystemClientVerticle.java`

#### LocalServerClientVerticle (NEW)
- **Connects to**: Local stdio-based MCP servers
- **Tools available**: Dynamically discovered from local servers
- **Client ID**: `local`
- **Purpose**: Manages local process-based MCP servers
- **Features**:
  - Spawns local processes via ProcessBuilder
  - Communicates via stdin/stdout
  - Supports any MCP-compatible executable
  - Configuration-driven server management
- **Location**: `src/main/java/AgentsMCPHost/mcp/clients/LocalServerClientVerticle.java`

### 3. Host Management

#### McpHostManagerVerticle
- **Role**: Central orchestrator for all MCP components
- **Responsibilities**:
  - Deploy all servers and clients in correct order
  - Aggregate tools from all clients
  - Route tool calls to appropriate clients
  - Provide status and monitoring endpoints
  - Handle SSE events from clients
- **Location**: `src/main/java/AgentsMCPHost/mcp/host/McpHostManagerVerticle.java`

### 4. Transport Layer

#### VertxStreamableHttpTransport
- **Purpose**: Adapter between Vert.x WebClient and MCP protocol for HTTP/SSE
- **Features**:
  - HTTP POST for JSON-RPC messages
  - HTTP GET for SSE streams
  - Session management
  - Protocol version negotiation
- **Methods**:
  - `initialize()` / `initializeWithParams()` - Protocol negotiation
  - `listTools()` - Discover available tools
  - `callTool()` - Execute tool with arguments
  - `listResources()` - Get available resources
  - `listRoots()` - Get allowed file system roots
  - `startSseStream()` - Establish SSE connection
- **Location**: `src/main/java/AgentsMCPHost/mcp/transport/VertxStreamableHttpTransport.java`

#### VertxStdioTransport (NEW)
- **Purpose**: Adapter for stdio-based communication with local MCP servers
- **Features**:
  - Process management via ProcessBuilder
  - JSON-RPC over stdin/stdout
  - Buffered I/O for efficient communication
  - Error stream monitoring
  - Graceful shutdown handling
- **Methods**:
  - `start()` - Launch the local process
  - `initialize()` - MCP session initialization
  - `listTools()` - Discover tools from local server
  - `callTool()` - Execute tool via stdio
  - `shutdown()` - Graceful process termination
- **Location**: `src/main/java/AgentsMCPHost/mcp/transport/VertxStdioTransport.java`

### 5. Configuration Management (NEW)

#### McpConfigLoader
- **Purpose**: Centralized configuration management for MCP system
- **Features**:
  - JSON configuration file loading
  - Environment variable overrides
  - Default configuration fallback
  - Server enable/disable control
  - Port configuration
  - Tool naming preferences
- **Configuration File**: `src/main/resources/mcp-config.json`
- **Location**: `src/main/java/AgentsMCPHost/mcp/config/McpConfigLoader.java`

## Communication Patterns

### Tool Naming Convention

All tools now follow the `serverName__toolName` pattern:
- **Example**: `calculator__add`, `weather__forecast`, `database__query`
- **Benefits**: Clear origin identification, no naming conflicts
- **Compatibility**: System supports both prefixed and unprefixed names
- **Configuration**: Controlled via `toolNaming` section in config

### Event Bus Messages

The system uses Vert.x event bus for internal communication:

```
Event Bus Addresses:
- mcp.tools.discovered     - Clients publish discovered tools
- mcp.client.ready         - Clients announce readiness
- mcp.server.ready         - Servers announce readiness
- mcp.host.route           - Route tool calls to clients
- mcp.host.status          - Request system status
- mcp.host.tools           - Request tools list
- mcp.client.{id}.call     - Call tool on specific client
- mcp.client.{id}.status   - Get client status
- mcp.sse.{id}            - SSE events from client
- mcp.sampling.required    - Trigger sampling workflow
- mcp.notification.{server} - Server notifications (NEW)
```

### HTTP/SSE Protocol

#### JSON-RPC Request Format
```json
{
  "jsonrpc": "2.0",
  "method": "tools/call",
  "id": "unique-id",
  "params": {
    "name": "add",
    "arguments": {
      "a": 10,
      "b": 20
    }
  }
}
```

#### SSE Event Format
```
event: tool-update
data: {"type":"tool-added","tool":"newTool"}

event: ping
data: {"type":"ping"}
```

## MCP Workflows

### 1. Tool Invocation Workflow
Basic pattern for calling tools and getting results:
1. Client discovers tools via `tools/list`
2. Host routes tool call to appropriate client
3. Client forwards to server via HTTP
4. Server executes tool and returns result
5. Result flows back through client to host

### 2. Sampling Workflow (Database Server)
For handling large result sets:
1. Tool returns result with `requiresSampling: true`
2. Client notifies host of sampling requirement
3. Host can request LLM summarization
4. Summarized result returned to user

### 3. Resources/Roots Workflow (FileSystem Server)
For secure file access:
1. Client sends allowed roots during initialization
2. Server validates all paths against allowed roots
3. Resources are exposed with unique IDs
4. Client can subscribe to resource updates

### 4. Elicitation Workflow (Planned for Weather Server)
For requesting missing parameters:
1. Tool detects missing required parameters
2. Server sends prompt request via SSE
3. Host collects user input
4. Tool re-executed with complete parameters

## Deployment Architecture

### Configuration-Driven Deployment

The system now reads configuration from `mcp-config.json`:
1. **Configuration Loading** - McpConfigLoader reads JSON config
2. **Selective Server Deployment** - Only enabled servers are started
3. **Dynamic Client Configuration** - Clients deployed based on config
4. **Local Server Support** - Optional stdio-based servers

### Verticle Deployment Order
1. **Configuration Loading** (first step)
2. **MCP Servers** (deployed as worker verticles if enabled)
   - 2-second delay after deployment for HTTP server startup
3. **MCP Clients** (deployed after servers are ready)
   - Connect to servers and discover tools
   - Local client deployed if local servers configured
4. **Host Manager** (orchestrates everything)
   - Aggregates tools and manages routing

### Port Allocation
| Component | Port | Type | Configurable |
|-----------|------|------|-------------|
| Host API | 8080 | HTTP/REST | No (hardcoded) |
| Calculator Server | 8081 | MCP/HTTP | Yes |
| Weather Server | 8082 | MCP/HTTP | Yes |
| Database Server | 8083 | MCP/HTTP | Yes |
| FileSystem Server | 8084 | MCP/HTTP | Yes |
| Local Servers | N/A | stdio | N/A |

## Status and Monitoring

### Endpoints
- `GET /host/v1/mcp/status` - Complete system status
- `GET /host/v1/clients` - List all clients and their tools
- `GET /host/v1/tools` - List all available tools

### Status Response Example
```json
{
  "ready": true,
  "servers": {
    "count": 4,
    "list": [...]
  },
  "clients": {
    "count": 3,
    "list": [...]
  },
  "tools": {
    "count": 13,
    "names": ["add", "subtract", "multiply", ...]
  }
}
```

## Testing

### Comprehensive Test Script
```bash
./test-mcp-full.sh
```

### Individual Component Tests
```bash
# Test MCP status
curl http://localhost:8080/host/v1/mcp/status

# Test tool execution
curl -X POST http://localhost:8080/host/v1/conversations \
  -H "Content-Type: application/json" \
  -d '{"messages":[{"role":"user","content":"Calculate 25 plus 17"}]}'

# List all tools
curl http://localhost:8080/host/v1/tools
```

## Development Guidelines

### Adding a New MCP Server

#### HTTP Server
1. Create server verticle extending `AbstractVerticle`
2. Implement MCP protocol endpoints (initialize, tools/list, tools/call)
3. Add configuration to `mcp-config.json`:
   ```json
   "myserver": {
     "enabled": true,
     "port": 8085,
     "description": "My custom server"
   }
   ```
4. Update McpHostManagerVerticle to deploy when enabled

#### Local Stdio Server
1. Create MCP-compatible executable
2. Add to `localServers` section in config:
   ```json
   "my-local-server": {
     "enabled": true,
     "command": "python",
     "args": ["my_server.py"],
     "environment": {"VAR": "value"}
   }
   ```
3. LocalServerClient will automatically manage it

### Adding a New Client Configuration
1. Create client verticle extending `AbstractVerticle`
2. Choose transport:
   - Use `VertxStreamableHttpTransport` for HTTP/SSE servers
   - Use `VertxStdioTransport` for local stdio servers
3. Implement tool discovery with prefixing:
   ```java
   String prefixedName = serverName + "__" + originalName;
   ```
4. Register with unique client ID
5. Add configuration support
6. Update McpHostManagerVerticle deployment

### Best Practices
- Use worker verticles for servers (blocking operations)
- Use standard verticles for clients (async operations)
- Always use Vert.x Future for async operations
- Handle both success and failure cases
- Use event bus for internal communication
- Validate all inputs and paths
- Follow existing patterns for consistency

## Troubleshooting

### Common Issues

1. **Connection Refused Errors**
   - Ensure servers start before clients
   - Check port availability
   - Verify localhost binding

2. **Tool Not Found**
   - Check tool discovery in client
   - Verify event bus addressing
   - Ensure proper tool routing

3. **SSE Stream Issues**
   - Check Accept header is "text/event-stream"
   - Verify chunked encoding support
   - Monitor connection timeouts

4. **Compilation Errors**
   - Ensure MCP SDK 0.11.0 is available
   - Check snapshot repository configuration
   - Verify Java 21 compatibility

## Future Enhancements

1. **Elicitation Workflow** - Complete implementation in WeatherServer
2. **Remote Tool Servers** - Support for external MCP servers
3. **WebSocket Transport** - Alternative to HTTP/SSE
4. **Tool Composition** - Chaining multiple tools
5. **Authentication** - Secure server-client connections
6. **Persistence** - Store tool results and sessions
7. **Metrics** - Prometheus/Grafana integration
8. **Tool Discovery UI** - Web interface for tool exploration

Zak old notes, do not delete: 
"""
# Simplified MCP Implementation

## Overview

This implementation demonstrates Model Context Protocol (MCP) concepts using a simplified approach built on Vert.x event bus messaging. This allows the system to compile and run without requiring the full MCP SDK while still showcasing tool orchestration capabilities.

## Architecture

### Core Components

1. **SimplifiedMcpDemo** (`src/main/java/AgentsMCPHost/mcp/SimplifiedMcpDemo.java`)
   - Registers tool handlers on the Vert.x event bus
   - Implements four demonstration tools:
     - Calculator: Basic arithmetic operations
     - Weather: Mock weather data
     - Database: Mock database queries
     - FileSystem: Mock file operations

2. **EnhancedConversationVerticle** (`src/main/java/AgentsMCPHost/hostAPI/EnhancedConversationVerticle.java`)
   - Enhanced conversation endpoint with tool detection
   - Routes tool requests through event bus
   - Falls back to standard LLM when no tools needed

### Communication Flow

```
User Request -> EnhancedConversationVerticle
                        |
                   Tool Detection
                        |
              [Tool Needed?] -- No --> Standard LLM Response
                        |
                       Yes
                        |
                Event Bus Request
                        |
               SimplifiedMcpDemo
                        |
                  Tool Handler
                        |
                 Tool Response
                        |
              Format & Return to User
```

## Available Endpoints

### Health Check
```bash
GET http://localhost:8080/health
```

### Standard Conversation (No Tools)
```bash
POST http://localhost:8080/host/v1/conversations
Content-Type: application/json

{
  "messages": [
    {"role": "user", "content": "Hello"}
  ]
}
```

### Enhanced Conversation (With Tool Support)
```bash
POST http://localhost:8080/host/v1/enhanced-conversations
Content-Type: application/json

{
  "messages": [
    {"role": "user", "content": "Can you calculate 10 plus 20?"}
  ]
}
```

## Tool Triggers

The system detects tool needs based on keywords in user messages:

| Tool | Trigger Keywords |
|------|-----------------|
| Calculator | calculate, add, subtract, multiply, divide |
| Weather | weather, temperature, forecast |
| Database | database, query, users |
| FileSystem | file, save, read |

## Testing

### Running the Server
```bash
# Build the JAR
./gradlew shadowJar

# Run the server
java -jar build/libs/Agents-MCP-Host-1.0.0-fat.jar
```

### Automated Testing
```bash
# Run the test suite
./test-mcp-endpoints.sh
```

### Manual Testing Examples

#### Calculator Tool
```bash
curl -X POST http://localhost:8080/host/v1/enhanced-conversations \
  -H "Content-Type: application/json" \
  -d '{"messages":[{"role":"user","content":"Calculate 10 plus 20"}]}'
```

#### Weather Tool
```bash
curl -X POST http://localhost:8080/host/v1/enhanced-conversations \
  -H "Content-Type: application/json" \
  -d '{"messages":[{"role":"user","content":"What is the weather?"}]}'
```

## Design Decisions

### Why Simplified Implementation?

1. **Compilation Issues**: The full MCP SDK (v0.11.0) has complex type requirements that were causing compilation issues
2. **Low Cognitive Load**: Direct event bus messaging is simpler to understand and debug
3. **Vert.x Native**: Leverages Vert.x's built-in event bus for async communication
4. **Easy to Extend**: Adding new tools just requires registering a new event bus consumer

### Why Remove FutureAdapter?

The FutureAdapter utility was created to bridge CompletableFuture (used by MCP SDK) with Vert.x Future. Since our simplified implementation uses only Vert.x patterns, the adapter was unnecessary and removed to:
- Eliminate type safety warnings
- Reduce complexity
- Follow YAGNI principle

## Future Enhancements

When ready to integrate the full MCP SDK:

1. **Add MCP SDK Types**: Import proper Tool, CallToolResult, etc. from the SDK
2. **Implement MCP Transport**: Add WebSocket or stdio transport layers
3. **Add Resource Management**: Implement MCP resource providers
4. **Enable Sampling**: Add LLM sampling capabilities through MCP
5. **Implement Roots**: Add file system roots for tool access

## Benefits of Current Approach

1. **Working Implementation**: System compiles and runs without errors
2. **Tool Orchestration**: Demonstrates MCP concepts effectively
3. **Type Safety**: No @SuppressWarnings or unsafe casts
4. **Testable**: Easy to test individual tools via event bus
5. **Extensible**: Simple to add new tools or modify existing ones
6. **Production Ready**: Can be deployed and used immediately

## Limitations

1. **Mock Data**: Tools return simulated data, not real results
2. **No MCP Protocol**: Doesn't implement actual MCP wire protocol
3. **No Remote Tools**: Tools must be in-process, not remote servers
4. **Basic Tool Detection**: Simple keyword matching, not semantic understanding

## Migration Path

To migrate to full MCP SDK:

1. Keep SimplifiedMcpDemo as fallback
2. Create new MCP server implementations alongside
3. Gradually replace event bus calls with MCP protocol calls
4. Maintain backward compatibility during transition

# OLD Zak Thoughts - keep these so I have reference.  
- This official python libray is very low code about how to make mcp clients and mcp server!!
  - https://chatgpt.com/c/688e5b3b-f1e0-8323-a1d7-f62aee23e123
  - gpt said: The confusion stems from conflating the roles of the host application, the client, and the server. MCP itself is just a protocol for exchanging JSON‑RPC messages about what tools, resources and prompts exist and how to call them. It does not prescribe how to pick a tool or how to manage agent state; those behaviours live in the host application and its LLM.

## The Three Roles
The confusion stems from conflating the **roles** of the host application, the client, and the server.  MCP itself is just a protocol for exchanging JSON‑RPC messages about *what* tools, resources and prompts exist and *how* to call them.  It does **not** prescribe how to pick a tool or how to manage agent state; those behaviours live in the host application and its LLM.
1. **MCP server:** A server exposes tools, resources and prompts. Each tool has an input schema and an implementation (e.g. “search flights” or “add two numbers”). When the server receives a tools/call request, it executes the tool and returns the result. The server never decides which tool to call; it just provides capabilities.
2. **MCP client:** A client maintains a single connection to a server. It can ask the server “what tools do you have?” (tools/list) and “call this tool with these arguments” (tools/call). Clients do not define tools—that’s the server’s job.
3. **Host application (agent):** The host (e.g. Claude Desktop, your own agent code) owns the language model and provides the user interface. It uses one or more MCP clients to discover available tools from servers, then uses the model to decide which tool to invoke. The host calls the tool through the client, receives the result from the server, and uses its model to craft a final answer. The decision logic and agent “backstory” live in the host, not in the client or server.

## Typical MCP workflows
The protocol defines how messages are exchanged, but the workflow depends on what the host application does.  Some common patterns are:

1. **Tool invocation (the most common workflow):**

   * User asks a question in the host UI.
   * The host’s model looks at the available tools (via `tools/list`) and decides to call one.
   * The host sends a `tools/call` request via its client to the appropriate server.
   * The server executes the tool and returns structured results.
   * The host feeds those results back into the model to craft a response for the user.

2. **Sampling:**

   * A server may need the host’s model to perform an internal analysis (e.g. summarise 47 flight options).
   * The server sends a `sampling/create_message` request to the client.
   * The client asks the user for permission, then uses its own model to produce the requested completion and returns it to the server.
   * The server uses that completion as part of its tool or prompt implementation.

3. **Elicitation:**

   * A server can prompt the client to obtain more information from the user (e.g. “Which city do you want to depart from?”).
   * The client displays the question to the user, collects the answer, and forwards it back to the server.
   * The server continues processing with the provided information.

4. **Roots & resources:**

   * Clients can send a list of filesystem roots to servers to constrain their access (e.g. tell a file-system server it may only operate in a given directory).
   * Servers expose resources (files, API data) that the host can read; the host decides which resources to fetch and how to use them.

These workflows highlight that **MCP itself does not contain the agent logic**.  It specifies how the client and server exchange JSON‑RPC messages: `tools/list`, `tools/call`, `resources/read`, sampling, etc.  The host application is responsible for orchestrating them and for using an LLM to choose when to call a tool and how to present the final answer.

## Sarting prompt
```
# Onboard

You are given the following context:
<context>
Our Java 21 vert.x development task today is today is to make a proof of concept for a big idea - a full-spectrum custom built Model Message Protocol (MCP)!  You will need to go all out with designing the architecture behind this idea to make it as least cognitive load as possible (make it easy to understand), because I had a really hard time understanding it.  I think I get it now, but it is very nuanced, and I may have missed something that is needed to consider during implementation.  To get you started, here I give you the high-level business requirements I am considering for this project.
This repo needs to be refactored to officially play all three roles to make up an MCP, but they also need to be kept separate from architecture and design perspective, because I may break them out later into separate applications altogether.  Here are the 3 roles and my implementation notes:
<threeRoles>
1. **MCP server:** A server exposes tools, resources and prompts. Each tool has an input schema and an implementation (e.g. “search flights” or “add two numbers”). When the server receives a tools/call request, it executes the tool and returns the result. The server never decides which tool to call; it just provides capabilities.
- Implementation notes:
  - This repo already has part of Host application (agent)!  Upon reviewing, you will notice that it already hosts an conversations api, and it calls openai directly.  So it already covers the “owns the language model and provides the user interface” part.  
  - It still needs a lot of work though, because somehow, we have to refactor to handle It using one or more MCP clients to discover available tools from servers, then uses the model to decide which tool to invoke. The host calls the tool through the client, receives the result from the server, and uses its model to craft a final answer.
2. **MCP client:** A client maintains a single connection to a server. It can ask the server “what tools do you have?” (tools/list) and “call this tool with these arguments” (tools/call). Clients do not define tools—that’s the server’s job.
- Implementation notes:
3. **Host application (agent):** The host (e.g. Claude Desktop, your own agent code) owns the language model and provides the user interface. It uses one or more MCP clients to discover available tools from servers, then uses the model to decide which tool to invoke. The host calls the tool through the client, receives the result from the server, and uses its model to craft a final answer. The decision logic and agent “backstory” live in the host, not in the client or server.
- Implementation notes:
</threeRoles>
Now, we do have some help and I have done a lot of research and you must use: https://github.com/modelcontextprotocol/java-sdk.  It is very low level code, and, as you fully read it as part of your tasks, you will quickly notice that they use the same language as you and you will know how to use it right away in this implementation.  Take your time studying this library, how to use it, and how it helps this project!
We must also implement all of these workflows, which I think works out well because we need 4 servers anyway?!: 
<fourWorkflows>
## Typical MCP workflows
The protocol defines how messages are exchanged, but the workflow depends on what the host application does.  Some common patterns are:

1. **Tool invocation (the most common workflow):**

   * User asks a question in the host UI.
   * The host’s model looks at the available tools (via `tools/list`) and decides to call one.
   * The host sends a `tools/call` request via its client to the appropriate server.
   * The server executes the tool and returns structured results.
   * The host feeds those results back into the model to craft a response for the user.

2. **Sampling:**

   * A server may need the host’s model to perform an internal analysis (e.g. summarise 47 flight options).
   * The server sends a `sampling/create_message` request to the client.
   * The client asks the user for permission, then uses its own model to produce the requested completion and returns it to the server.
   * The server uses that completion as part of its tool or prompt implementation.

3. **Elicitation:**

   * A server can prompt the client to obtain more information from the user (e.g. “Which city do you want to depart from?”).
   * The client displays the question to the user, collects the answer, and forwards it back to the server.
   * The server continues processing with the provided information.

4. **Roots & resources:**

   * Clients can send a list of filesystem roots to servers to constrain their access (e.g. tell a file-system server it may only operate in a given directory).
   * Servers expose resources (files, API data) that the host can read; the host decides which resources to fetch and how to use them.

These workflows highlight that **MCP itself does not contain the agent logic**.  It specifies how the client and server exchange JSON‑RPC messages: `tools/list`, `tools/call`, `resources/read`, sampling, etc.  The host application is responsible for orchestrating them and for using an LLM to choose when to call a tool and how to present the final answer.
</fourWorkflows>
Here is where you can do a full deep dive on MCP And learn more than me on how to implement this: https://modelcontextprotocol.io/overview (explore this entire webpage in full).  You can use the same examples as them; keep it simple to start.  Like add two number and weather, and done.  Although I think we need at least 4 servers and 3 clients to fully test proper pattern for the host.   For example, we can have two servers on one client, then one client per server for third, and then one extra server on its own client so we can test the full pattern! You’ll have to document questions for test cases you think through them.
You will need to use your web search tools, and many other tools.  GO all out with the tools you use and do not hold back your effort toward your dreams on this project!

</context>

## Instructions

"AI models are geniuses who start from scratch on every task." – Noam Brown

Your job is to "onboard" yourself to the current task.

Do this by:
- Using ultrathink
- Exploring the codebase
- Asking me questions if needed
- You can take as long as you need, and you will be required to deeply ultrathink about each decomposed task!  You must get every detail right for this work.  

The goal is to get you fully prepared to start working on the task.

Take as long as you need to get yourself ready. Overdoing it is better than underdoing it.

Record everything in a .claude/tasks/[TASK_ID]/onboarding.md file. This file will be used to onboard you to the task in a new session if needed, so make sure it's comprehensive.
```
"""
# Agents-MCP-Host: Full Model Context Protocol (MCP) Implementation with OpenAI Integration

[![Java 21](https://img.shields.io/badge/Java-21-blue.svg)](https://openjdk.org/projects/jdk/21/)
[![MCP SDK](https://img.shields.io/badge/MCP%20SDK-v0.11.0-green.svg)](https://github.com/modelcontextprotocol/java-sdk)
[![Vert.x](https://img.shields.io/badge/Vert.x-4.5.7-purple.svg)](https://vertx.io/)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## 🎯 What's New (January 2025)

- **🚀 Stdio Transport Support** - Run local MCP servers as separate processes
- **🏷️ Tool Name Prefixing** - Clear `serverName__toolName` pattern for all tools
- **⚙️ Configuration-Driven Architecture** - Flexible JSON-based server configuration
- **🔌 Local Server Support** - Spawn and manage local MCP servers via stdio
- **🎨 Enhanced Tool Routing** - Smart routing with prefixed and unprefixed name support

## 🚀 Quick Start for New Developers

### Directory Location
```bash
# Windows (WSL)
win_home=$(wslpath -u "$(wslvar USERPROFILE)")
cd $win_home/OneDrive/Zak/SmartNPCs/MCPThink/Agents-MCP-Host/

# Linux/Mac
cd ~/Agents-MCP-Host/
```

### Prerequisites
- **Java 21+** - Check: `java --version`
- **Gradle 8.8** - Included via wrapper (`./gradlew`)
- **OpenAI API Key** (optional) - For LLM responses when tools aren't needed

### Build & Run (30 seconds)
```bash
# Build the JAR
./gradlew shadowJar

# Run the server (starts MCP infrastructure automatically)
java -jar build/libs/Agents-MCP-Host-1.0.0-fat.jar

# Or run directly with Gradle
./gradlew run
```

### Test the Full MCP System
```bash
# Run comprehensive MCP tests
./test-mcp-full.sh

# Or test individual components:

# Check MCP infrastructure status
curl http://localhost:8080/host/v1/mcp/status

# List all available tools (14+ tools with server prefixing)
curl http://localhost:8080/host/v1/tools
# Tools now use serverName__toolName pattern (e.g., calculator__add)

# Test unified conversation endpoint (auto-detects MCP tools)
curl -X POST http://localhost:8080/host/v1/conversations \
  -H "Content-Type: application/json" \
  -d '{"messages":[{"role":"user","content":"Calculate 10 plus 20"}]}'
```

## 🏗️ Architecture Overview

### Full MCP Implementation

This system implements the complete Model Context Protocol (MCP) specification with proper separation of servers, clients, and host orchestration.

#### MCP Servers (Worker Verticles on configurable ports)
1. **CalculatorServerVerticle** (Port 8081) - Mathematical operations
2. **WeatherServerVerticle** (Port 8082) - Weather data and forecasts
3. **DatabaseServerVerticle** (Port 8083) - Database operations with sampling
4. **FileSystemServerVerticle** (Port 8084) - File operations with roots/resources
5. **Local Process Servers** (Via stdio) - Any MCP-compatible executable

#### MCP Clients (Standard Verticles)
1. **DualServerClientVerticle** - Connects to Calculator + Weather
2. **SingleServerClientVerticle** - Connects to Database only
3. **FileSystemClientVerticle** - Connects to FileSystem with permissions
4. **LocalServerClientVerticle** - Manages local stdio-based servers

#### Transport Layers
1. **VertxStreamableHttpTransport** - HTTP/SSE transport for remote servers
2. **VertxStdioTransport** - Stdio transport for local process servers

#### Host Components
1. **McpHostManagerVerticle** - Orchestrates all MCP components with configuration
2. **McpConfigLoader** - Loads and manages MCP configuration
3. **HostAPIVerticle** - Main HTTP server on port 8080
4. **ConversationVerticle** - Unified chat endpoint with auto tool detection

### Tech Stack
- **Java 21** - Language
- **Vert.x 4.5.7** - Reactive framework with event bus
- **MCP SDK 0.11.0** - Official Model Context Protocol SDK
- **OpenAI API** - LLM integration for non-tool queries
- **HTTP/SSE** - Streamable HTTP transport with Server-Sent Events
- **Gradle 8.8** - Build system

## ⚙️ Configuration

The system now uses a flexible configuration file at `src/main/resources/mcp-config.json`:

```json
{
  "mcpServers": {
    "httpServers": {
      "calculator": { "enabled": true, "port": 8081 },
      "weather": { "enabled": true, "port": 8082 },
      "database": { "enabled": true, "port": 8083 },
      "filesystem": { "enabled": true, "port": 8084 }
    },
    "localServers": {
      "example-stdio": {
        "enabled": false,
        "command": "python",
        "args": ["-m", "mcp_server"],
        "environment": { "PYTHONPATH": "./lib" }
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
export MCP_CONFIG_PATH=/path/to/custom-config.json
export MCP_CALCULATOR_PORT=9081
export MCP_USE_PREFIXING=true
```

## 📁 Project Structure

```
Agents-MCP-Host/
├── build.gradle.kts           # Build configuration with MCP SDK
├── settings.gradle.kts         # Project settings
├── gradlew                     # Gradle wrapper (Unix)
├── gradlew.bat                # Gradle wrapper (Windows)
├── README.md                  # This file
├── MCP-IMPLEMENTATION.md      # Full MCP architecture documentation
├── DEVELOPMENT.md             # Development guide
├── CLAUDE.md                  # AI agent context
├── test-mcp-full.sh          # Comprehensive MCP test script
├── test-mcp-endpoints.sh     # Basic endpoint tests
├── src/
│   ├── main/
│   │   └── java/
│   │       └── AgentsMCPHost/
│   │           ├── Driver.java              # Main entry point
│   │           ├── hostAPI/
│   │           │   ├── HostAPIVerticle.java
│   │           │   ├── ConversationVerticle.java
│   │           │   ├── EnhancedConversationVerticle.java
│   │           │   ├── HealthVerticle.java
│   │           │   ├── StatusVerticle.java
│   │           │   └── LoggerVerticle.java
│   │           ├── services/
│   │           │   └── LlmAPIService.java   # OpenAI integration
│   │           └── mcp/
│   │               ├── servers/
│   │               │   ├── CalculatorServerVerticle.java
│   │               │   ├── WeatherServerVerticle.java
│   │               │   ├── DatabaseServerVerticle.java
│   │               │   └── FileSystemServerVerticle.java
│   │               ├── clients/
│   │               │   ├── DualServerClientVerticle.java
│   │               │   ├── SingleServerClientVerticle.java
│   │               │   ├── FileSystemClientVerticle.java
│   │               │   └── LocalServerClientVerticle.java  # NEW: Stdio client
│   │               ├── host/
│   │               │   └── McpHostManagerVerticle.java
│   │               ├── transport/
│   │               │   ├── VertxStreamableHttpTransport.java
│   │               │   └── VertxStdioTransport.java        # NEW: Stdio transport
│   │               └── config/
│   │                   └── McpConfigLoader.java             # NEW: Config loader
│   └── resources/
│       └── mcp-config.json                                  # NEW: Configuration
│   └── test/
│       └── java/
│           └── AgentsMCPHost/
│               └── TestMainVerticle.java
└── build/
    └── libs/
        └── Agents-MCP-Host-1.0.0-fat.jar  # Executable JAR
```

## 🔌 API Endpoints

### Core Endpoints
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | System health and metrics |
| `/host/v1/status` | GET | Server configuration info |
| `/host/v1/conversations` | POST | Unified chat endpoint with auto tool detection |

### MCP Management Endpoints
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/host/v1/mcp/status` | GET | Full MCP infrastructure status |
| `/host/v1/clients` | GET | List all MCP clients and their tools |
| `/host/v1/tools` | GET | List all available tools across servers |

## 🛠️ MCP Tools Available

### 14+ Tools Distributed Across Multiple Servers

| Server | Port/Type | Tools (Prefixed Names) | Client Access |
|--------|-----------|------------------------|---------------|
| **Calculator** | 8081 | `calculator__add`, `calculator__subtract`, `calculator__multiply`, `calculator__divide` | DualServerClient |
| **Weather** | 8082 | `weather__weather`, `weather__forecast` | DualServerClient |
| **Database** | 8083 | `database__query`, `database__insert`, `database__update`, `database__delete` | SingleServerClient |
| **FileSystem** | 8084 | `filesystem__list`, `filesystem__read`, `filesystem__write`, `filesystem__delete` | FileSystemClient |
| **Local Servers** | stdio | Custom tools via stdio transport | LocalServerClient |

### Tool Naming Pattern
- All tools use `serverName__toolName` pattern (inspired by OpenCode)
- Supports both prefixed and unprefixed names for backward compatibility
- Clear origin identification for each tool
- Configurable via `toolNaming` section in config

### Tool Routing
- Smart routing automatically finds tools by prefixed or original names
- Multiple clients can access the same tool if connected to that server
- FileSystem tools include roots/resources security workflows
- Local servers discovered dynamically on startup

## 🔧 Configuration

### Environment Variables

| Variable | Description | Required | Default |
|----------|-------------|----------|---------|
| `OPENAI_API_KEY` | OpenAI API key for LLM | No | None (fallback mode) |
| `ZAK_AGENT_KEY` | Future authentication | No | None |
| `AWS_REGION` | AWS region | No | us-east-2 |
| `DATA_PATH` | Data directory | No | ./data |

### Setting OpenAI Key (Optional)

```bash
# Linux/Mac
export OPENAI_API_KEY=sk-your-key-here

# Windows WSL (from Windows env var)
export OPENAI_API_KEY=$(wslvar OPENAI_API_KEY)

# Permanent (add to ~/.bashrc)
echo 'export OPENAI_API_KEY=sk-your-key-here' >> ~/.bashrc
```

## 🧪 Testing

### Automated Testing

**Note:** The unified endpoint intelligently routes requests:
- Messages with tool keywords → MCP mock tools (no API key needed)
- Other messages → OpenAI API (requires OPENAI_API_KEY)
- No API key → Fallback response

```bash
# Test with OpenAI API key (full functionality)
OPENAI_API_KEY=your-key ./test-openai.sh

# Test without API key (mock tools still work)
./test-mcp-endpoints.sh

# Run unit tests
./gradlew test
```

### Manual Testing
```bash
# Start server
java -jar build/libs/Agents-MCP-Host-1.0.0-fat.jar

# In another terminal, test endpoints
curl http://localhost:8080/health
```

## 📚 Documentation

- **[MCP-IMPLEMENTATION.md](MCP-IMPLEMENTATION.md)** - Simplified MCP architecture and design decisions
- **[DEVELOPMENT.md](DEVELOPMENT.md)** - Technical details for developers
- **[CLAUDE.md](CLAUDE.md)** - Context for AI agents working on this project

## 🐛 Troubleshooting

| Problem | Solution |
|---------|----------|
| Port 8080 in use | Change port or kill process: `lsof -i :8080` |
| No LLM responses | Set OPENAI_API_KEY environment variable |
| Compilation errors | Ensure Java 21: `java --version` |
| Gradle issues | Use wrapper: `./gradlew` not `gradle` |
| MCP tools not working | Check enhanced endpoint: `/host/v1/enhanced-conversations` |

## 🚦 Development Workflow

1. **Clone/Navigate to Project**
   ```bash
   cd /mnt/c/Users/zkysa/OneDrive/Zak/SmartNPCs/MCPThink/Agents-MCP-Host/
   ```

2. **Make Changes**
   - Edit Java files in `src/main/java/AgentsMCPHost/`
   - Follow Vert.x patterns (verticles, event bus)

3. **Build & Test**
   ```bash
   ./gradlew clean compileJava
   ./gradlew shadowJar
   ./test-mcp-endpoints.sh
   ```

4. **Run Server**
   ```bash
   java -jar build/libs/Agents-MCP-Host-1.0.0-fat.jar
   ```

## 🎯 Current Status

### ✅ Working Features
- HTTP server with CORS support
- Health and status endpoints
- OpenAI-compatible conversation API
- Simplified MCP tool orchestration
- Event bus based tool communication
- Fallback responses without API key

### 🚧 Future Enhancements
- Full MCP SDK integration
- WebSocket transport for MCP
- Resource management
- Persistent conversation storage
- Authentication system
- Rate limiting

## 🤝 Contributing

See [DEVELOPMENT.md](DEVELOPMENT.md) for:
- Code style guidelines
- Vert.x best practices
- Testing requirements
- PR process

## 📝 Notes for AI Agents

If you're an AI agent working on this project, start with:
1. Read [CLAUDE.md](CLAUDE.md) for full context
2. Review [MCP-IMPLEMENTATION.md](MCP-IMPLEMENTATION.md) for architecture
3. Check [DEVELOPMENT.md](DEVELOPMENT.md) for technical details
4. Run `./gradlew compileJava` to verify setup
5. Test with `./test-mcp-endpoints.sh`

## 🔗 Resources

- **Vert.x Documentation**: [vertx.io/docs](https://vertx.io/docs)
- **Model Context Protocol**: [modelcontextprotocol.io](https://modelcontextprotocol.io)
- **OpenAI API**: [platform.openai.com](https://platform.openai.com)
- **Java 21 Features**: [openjdk.org/projects/jdk/21](https://openjdk.org/projects/jdk/21)

---

**Version**: 1.0.0  
**Java Version**: 21  
**Vert.x Version**: 4.5.7  
**Default Port**: 8080  
**Build Date**: January 2025
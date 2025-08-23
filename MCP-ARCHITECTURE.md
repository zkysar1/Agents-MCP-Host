# MCP-First Architecture

## 🎯 Overview

This codebase implements a **Model Context Protocol (MCP) Host** with a clean, modular architecture designed for low cognitive load and easy extensibility. The system is organized around MCP as the core, with pluggable tool domains that can be added or removed without affecting other components.

## 📁 Directory Structure

```
AgentsMCPHost/
├── mcp/                          # Everything MCP-related
│   ├── core/                     # Shared MCP infrastructure
│   │   ├── McpHostManager.java  # Orchestrates all MCP components
│   │   ├── config/              # Configuration management
│   │   ├── transport/           # MCP transport implementations
│   │   └── orchestration/       # Tool orchestration strategies
│   │
│   └── servers/                  # Tool domains (pluggable)
│       ├── oracle/               # Oracle database tools
│       │   ├── servers/         # MCP server implementations
│       │   ├── clients/         # MCP client implementations
│       │   ├── orchestration/   # Domain-specific orchestration
│       │   └── utils/           # Domain-specific utilities
│       │
│       └── examples/             # Reference implementations
│           ├── calculator/      # Math operations example
│           ├── weather/         # Weather service example
│           ├── database/        # Generic database example
│           └── filesystem/      # File operations example
│
├── api/                          # HTTP API layer
│   ├── HostAPI.java             # Main HTTP router
│   ├── Conversation.java        # Chat endpoint with auto-tool detection
│   ├── Health.java              # Health check endpoint
│   └── Status.java              # System status endpoint
│
├── llm/                          # LLM integrations
│   └── LlmAPIService.java       # Provider-agnostic LLM service
│
├── logging/                      # Logging infrastructure
│   └── Logger.java              # Centralized logging
│
└── Driver.java                   # Application entry point
```

## 🔌 Adding a New Tool Domain

Adding a new tool domain (e.g., DynamoDB, Elasticsearch, Stripe) is straightforward:

### 1. Create Domain Package
```
mcp/servers/yourdomain/
├── servers/                      # Your MCP servers
│   └── YourDomainServer.java
├── clients/                      # Your MCP clients  
│   └── YourDomainClient.java
├── orchestration/                # Optional: complex workflows
└── utils/                        # Optional: domain utilities
```

### 2. Update Configuration
Add your servers to `/src/main/resources/mcp-config.json`:
```json
{
  "mcpServers": {
    "httpServers": {
      "yourdomain": {
        "enabled": true,
        "port": 8090,
        "description": "Your domain description"
      }
    }
  },
  "clientConfigurations": {
    "yourdomain": {
      "enabled": true,
      "connects": ["yourdomain"],
      "description": "Your domain client"
    }
  }
}
```

### 3. Deploy in Driver.java
The McpHostManager will automatically detect and deploy your components based on the configuration.

## 🗑️ Removing a Tool Domain

To remove a tool domain:
1. Delete the domain directory: `rm -rf mcp/servers/yourdomain/`
2. Set `"enabled": false` in `mcp-config.json`
3. Rebuild

That's it! No other code changes needed.

## 🏗️ Architecture Principles

### MCP-First Design
- Everything revolves around MCP protocol
- Tool domains are plugins to the MCP infrastructure
- Transport and orchestration are shared across all domains

### Low Cognitive Load
- Predictable structure: `/mcp/servers/[domain]/`
- Self-contained domains with no cross-dependencies
- Clear separation between infrastructure and implementations

### Provider Agnostic
- LLM service supports multiple providers (not just OpenAI)
- Transport layer supports HTTP/SSE and stdio
- Configuration-driven deployment

## 🔄 Request Flow

```
1. HTTP Request → api/Conversation
2. Auto-detects if tools needed
3. If tools needed → mcp/core/McpHostManager
4. Routes to appropriate domain → mcp/servers/[domain]/
5. Domain processes request
6. Response flows back through MCP infrastructure
7. If no tools needed → llm/LlmAPIService → Direct LLM response
```

## 🚀 Quick Start

```bash
# Build
./gradlew shadowJar

# Run
java -jar build/libs/Agents-MCP-Host-1.0.0-fat.jar

# Test
curl http://localhost:8080/health
```

## 🧪 Testing

```bash
# Test specific domain
./test-oracle.sh        # Test Oracle tools

# Test all domains
./test-all.sh          # Comprehensive test suite
```

## 📝 Configuration

All MCP configuration is in `/src/main/resources/mcp-config.json`:
- Enable/disable tool domains
- Configure ports and connections
- Set transport options
- Define tool naming patterns

## 🎯 Current Tool Domains

### Oracle (`/mcp/servers/oracle/`)
- **Purpose**: Enterprise database operations
- **Servers**: OracleServer, OracleMetadataServer, OracleToolsServer
- **Clients**: OracleClient, OracleToolsClient
- **Features**: SQL generation, metadata navigation, query optimization

### Examples (`/mcp/servers/examples/`)
- **Calculator**: Basic math operations
- **Weather**: Weather information service
- **Database**: Generic database operations
- **FileSystem**: File I/O operations

## 🔮 Future Domains (Easy to Add)

- **DynamoDB**: AWS NoSQL operations
- **Elasticsearch**: Search and analytics
- **Stripe**: Payment processing
- **Slack**: Team communication
- **GitHub**: Repository management
- **Kubernetes**: Container orchestration

Each domain would follow the same pattern: create package, add config, deploy.

## 🤝 Contributing

When adding new features:
1. Follow the existing package structure
2. Keep domains self-contained
3. Use the shared MCP infrastructure
4. Update configuration, not code
5. Test with domain-specific test scripts

## 📚 Key Files

- `Driver.java`: Entry point and component deployment
- `McpHostManager.java`: MCP orchestration hub
- `Conversation.java`: Unified chat endpoint with auto-tool detection
- `mcp-config.json`: All MCP configuration
- `LlmAPIService.java`: Provider-agnostic LLM integration

## 🎓 For New Developers

1. **Start here**: Read `Driver.java` to see what gets deployed
2. **Understand flow**: Follow a request through `Conversation.java`
3. **Find Oracle code**: Look in `/mcp/servers/oracle/`
4. **Add new tools**: Create `/mcp/servers/yourdomain/`
5. **Configure**: Edit `mcp-config.json`

The architecture is designed to be self-documenting through its structure. If you understand one domain, you understand them all.
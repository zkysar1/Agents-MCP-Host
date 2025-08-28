# MCP Host - Intelligent Oracle Database Query System

An event-driven Model Context Protocol (MCP) host that orchestrates AI-powered database interactions, providing natural language query processing for Oracle databases.

## Key Features

- **Multi-stage Query Processing**: Specialized AI servers handle different aspects of query understanding and execution
- **Real-time Streaming**: Server-Sent Events (SSE) for progressive response delivery
- **Oracle Integration**: Robust connection pooling and safe query execution
- **Dynamic Strategy Generation**: Adaptive execution plans based on query complexity
- **Extensible Architecture**: Easy to add new MCP servers and capabilities

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          HTTP API Layer                                  │
│  ┌───────────────────────────────────────────────────────────────────┐ │
│  │            ConversationStreaming.java (Main API Entry)             │ │
│  │                    /host/v1/conversations                          │ │
│  └─────────────────────────────┬─────────────────────────────────────┘ │
└────────────────────────────────┼───────────────────────────────────────┘
                                 │
┌────────────────────────────────┼───────────────────────────────────────┐
│                          Event Bus Layer                                 │
│                         (Vert.x Event Bus)                              │
└────────────────────────────────┼───────────────────────────────────────┘
                                 │
         ┌───────────────────────┴────────────────────────┐
         │                                                │
┌────────┴───────────┐                      ┌────────────┴──────────────┐
│   Host Layer       │                      │    Services Layer         │
├────────────────────┤                      ├───────────────────────────┤
│ OracleDBAnswererHost│                     │ MCPRegistryService        │
│ OracleSQLBuilderHost│                     │ LlmAPIService             │
│ToolFreeDirectLLMHost│                     │ OracleConnectionManager   │
└────────┬───────────┘                      │ InterruptManager          │
         │                                  └───────────────────────────┘
         │                                              
┌────────┴────────────────────────────────────────────────────────────────┐
│                        MCP Layer                                         │
│  ┌─────────────────┐          ┌─────────────────────────────────────┐  │
│  │  MCP Clients    │          │      MCP Servers                    │  │
│  │                 │          ├─────────────────────────────────────┤  │
│  │UniversalMCPClient├──────────┤ • OracleQueryAnalysisServer        │  │
│  │                 │          │ • OracleSQLGenerationServer         │  │
│  └─────────────────┘          │ • OracleSchemaIntelligenceServer    │  │
│                               │ • QueryIntentEvaluationServer       │  │
│                               │ • StrategyGenerationServer          │  │
│                               │ • BusinessMappingServer             │  │
│                               │ • And 5 more...                     │  │
│                               └─────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────┘
                                        │
                                        │
                              ┌─────────┴────────┐
                              │  Oracle Database │
                              └──────────────────┘
```

## Core Components

### Entry Points

| File | Purpose |
|------|---------|
| **Driver.java** | Application bootstrap, initializes Vert.x and deploys all verticles |
| **ConversationStreaming.java** | HTTP API server providing REST endpoints for client interactions |

### Host Layer (Request Orchestrators)

| Host | Purpose |
|------|---------|
| **OracleDBAnswererHost** | Full pipeline orchestration for natural language database Q&A |
| **OracleSQLBuilderHost** | Specialized workflow for SQL query generation |
| **ToolFreeDirectLLMHost** | Direct LLM integration bypassing MCP tools |

### MCP Servers (Specialized Tools)

#### Analysis Servers
- **QueryIntentEvaluationServer**: Determines query intent (data retrieval, SQL generation, exploration)
- **OracleQueryAnalysisServer**: Analyzes query structure, entities, and relationships
- **IntentAnalysisServer**: Deep semantic understanding of user queries

#### Oracle-Specific Servers
- **OracleSchemaIntelligenceServer**: Schema discovery, table matching, relationship inference
- **OracleSQLGenerationServer**: Generates syntactically correct SQL from natural language
- **OracleSQLValidationServer**: Validates SQL syntax and semantic correctness
- **OracleQueryExecutionServer**: Safely executes queries with result formatting

#### Strategy & Orchestration Servers
- **StrategyGenerationServer**: Creates dynamic execution plans based on query complexity
- **StrategyOrchestratorServer**: Monitors and adapts strategy execution in real-time
- **StrategyLearningServer**: Learns from execution history to improve future strategies

#### Utility Servers
- **BusinessMappingServer**: Maps business terminology to database schema elements

### Service Layer

| Service | Purpose |
|---------|---------|
| **MCPRegistryService** | Central registry tracking all MCP clients and available tools |
| **OracleConnectionManager** | Manages database connection pooling and lifecycle |
| **LlmAPIService** | Handles OpenAI API integration for LLM capabilities |
| **StreamingEventPublisher** | Manages SSE event streaming to clients |
| **InterruptManager** | Handles request cancellation and interruption |
| **MCPRouterService** | Routes HTTP requests to appropriate handlers |

### MCP Client
- **UniversalMCPClient**: Generic client implementation for connecting to any MCP server

## Technology Stack

- **Language**: Java 21
- **Framework**: Vert.x 4.5.10 (reactive, event-driven)
- **Protocol**: MCP SDK 0.11.0
- **Database**: Oracle JDBC 21.11.0.0
- **Build**: Gradle 8.8
- **JSON**: Jackson 2.15.3

## Quick Start

### Prerequisites
- Java 21 or higher
- Oracle Database with connection details
- OpenAI API key for LLM functionality

### Environment Variables
```bash
# Required
ORACLE_DB_URL=jdbc:oracle:thin:@//localhost:1521/ORCL
ORACLE_USERNAME=your_username
ORACLE_PASSWORD=your_password
OPENAI_API_KEY=sk-your-api-key

# Optional
LOG_LEVEL=2  # 0=ERROR, 1=INFO, 2=DEBUG, 3=TRACE
```

### Running the Application
```bash
# Build the project
./gradlew build

# Run the application
./gradlew run
```

## API Endpoints

### Main Conversation Endpoint
```
POST /host/v1/conversations
Content-Type: application/json

{
  "messages": [
    {
      "role": "user",
      "content": "Show me total sales by region for Q4 2023"
    }
  ],
  "host": "oracledbanswerer",  // optional, defaults to oracledbanswerer
  "options": {
    "streaming": true  // Enable SSE streaming
  }
}
```

### Health & Status
- `GET /host/v1/health` - System health check
- `GET /host/v1/status` - Comprehensive system status
- `GET /host/v1/mcp/status` - MCP subsystem status
- `GET /host/v1/mcp/tools` - List available MCP tools
- `GET /host/v1/mcp/clients` - List registered MCP clients

## Event Bus Communication

The system uses Vert.x event bus for internal communication:

### Message Patterns
- Host requests: `host.{hostname}.process`
- Streaming events: `streaming.{conversationId}.{eventType}`
- MCP registry: `mcp.registry.{action}`
- System events: `system.{event}`

### Event Types
- `progress`: Query processing progress updates
- `tool_start`/`tool_complete`: MCP tool execution events
- `final`: Final response delivery
- `error`: Error notifications

## Development Guide

### Adding a New MCP Server

1. Extend `MCPServerBase` in the `mcp/servers` package:
```java
public class MyNewServer extends MCPServerBase {
    public MyNewServer() {
        super("MyNewServer", "/mcp/servers/mynew");
    }
    
    @Override
    protected void initializeTools() {
        // Register your tools here
    }
}
```

2. Deploy it in `Driver.java`:
```java
vertx.deployVerticle(new MyNewServer(), deploymentOptions);
```

### Adding a New Host

1. Create a new class extending `AbstractVerticle`
2. Register event bus consumers for your host address
3. Deploy in `Driver.java`

### Logging

The system uses a custom logging framework with levels:
- 0: ERROR - Critical errors only
- 1: INFO - Important events
- 2: DEBUG - Detailed flow information
- 3: TRACE - Verbose debugging

## Architecture Decisions

- **Event-Driven**: Vert.x provides excellent scalability and non-blocking I/O
- **MCP Protocol**: Standardized tool interface allows easy extension
- **Connection Pooling**: Centralized Oracle connections prevent resource exhaustion
- **Streaming**: SSE provides real-time feedback during long-running queries
- **Strategy Pattern**: Dynamic execution plans adapt to query complexity

## Troubleshooting

### Common Issues

1. **"No MCP clients registered"**
   - Ensure all MCP servers are deployed successfully
   - Check logs for initialization errors

2. **Database Connection Failures**
   - Verify Oracle connection string and credentials
   - Check network connectivity to database

3. **LLM Timeout Errors**
   - Increase timeout in `DeliveryOptions`
   - Check OpenAI API key and rate limits

## Contributing

When contributing:
1. Follow existing code patterns
2. Add appropriate logging
3. Update this README for new features
4. Test with various query types

## License

[Add your license information here]
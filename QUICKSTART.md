# Quick Start Guide for New Developers

Welcome to Agents-MCP-Host! This guide will get you up and running in 5 minutes.

## What You're Working With

Agents-MCP-Host is a **complete MCP (Model Context Protocol) implementation** in Java that:
- Runs multiple MCP servers (Calculator, Weather, Database, FileSystem)
- Supports both HTTP and stdio-based servers
- Uses tool name prefixing (`serverName__toolName`) for clarity
- Provides a unified conversation API with automatic tool detection

## Essential Concepts

### 1. Tool Name Prefixing
All tools use the pattern `serverName__toolName`:
- `calculator__add` - Add function from Calculator server
- `weather__forecast` - Forecast function from Weather server
- `database__query` - Query function from Database server

### 2. Transport Mechanisms
- **HTTP/SSE** - For remote MCP servers (ports 8081-8084)
- **Stdio** - For local process-based servers (Python, Node.js, etc.)

### 3. Configuration-Driven
Everything is configured in `src/main/resources/mcp-config.json`

## Quick Setup (2 minutes)

```bash
# 1. Navigate to project
cd /mnt/c/Users/zkysa/OneDrive/Zak/SmartNPCs/MCPThink/Agents-MCP-Host/

# 2. Build the project
./gradlew shadowJar

# 3. Run the server
java -jar build/libs/Agents-MCP-Host-1.0.0-fat.jar

# Server starts with:
# - 4 MCP servers on ports 8081-8084
# - 14+ tools available
# - API on http://localhost:8080
```

## Test It Works (1 minute)

```bash
# Check server health
curl http://localhost:8080/health

# List all available tools (with prefixed names)
curl http://localhost:8080/host/v1/tools

# Check MCP infrastructure status
curl http://localhost:8080/host/v1/mcp/status

# Test a tool via conversation API
curl -X POST http://localhost:8080/host/v1/conversations \
  -H "Content-Type: application/json" \
  -d '{"messages":[{"role":"user","content":"Calculate 25 plus 17"}]}'
```

## Key Files to Know

| File | Purpose | When to Edit |
|------|---------|--------------|
| `mcp-config.json` | All MCP settings | To enable/disable servers, change ports |
| `McpHostManagerVerticle.java` | Orchestrates everything | To add new deployment logic |
| `VertxStdioTransport.java` | Local process communication | To support new stdio protocols |
| `LocalServerClientVerticle.java` | Manages local servers | To handle new local server types |

## Common Tasks

### Enable a Local Python MCP Server

1. Edit `mcp-config.json`:
```json
"localServers": {
  "my-python-server": {
    "enabled": true,
    "command": "python",
    "args": ["my_server.py"],
    "environment": {"PYTHONPATH": "./"}
  }
}
```

2. Restart the server - it will automatically spawn and manage your Python server!

### Add a New Tool to Existing Server

1. Find the server verticle (e.g., `CalculatorServerVerticle.java`)
2. Add to the `tools` array:
```java
.add(new JsonObject()
    .put("name", "newTool")  // Will become calculator__newTool
    .put("description", "My new tool")
    .put("inputSchema", ...))
```

3. Implement the tool execution in `executeTool()` method

### Change Server Ports

Edit `mcp-config.json`:
```json
"httpServers": {
  "calculator": {
    "enabled": true,
    "port": 9081  // Changed from 8081
  }
}
```

Or use environment variable:
```bash
export MCP_CALCULATOR_PORT=9081
java -jar build/libs/Agents-MCP-Host-1.0.0-fat.jar
```

## Architecture at a Glance

```
User Request → API (8080) → ConversationVerticle
                ↓
        Tool needed? → McpHostManager
                ↓
        Routes to correct client (by tool name)
                ↓
        Client forwards to server (8081-8084 or stdio)
                ↓
        Server executes tool
                ↓
        Result flows back to user
```

## Debugging Tips

1. **Check logs** - The console shows all MCP activity
2. **Verify tool names** - Use `curl http://localhost:8080/host/v1/tools`
3. **Test servers directly** - Each MCP server has its own port
4. **Lambda errors** - Make variables `final` before using in lambdas
5. **Config issues** - Verify JSON syntax in `mcp-config.json`

## Next Steps

1. **Read the full docs**:
   - `README.md` - Complete project overview
   - `MCP-IMPLEMENTATION.md` - Deep dive into MCP architecture
   - `DEVELOPMENT.md` - Development guidelines
   - `MCP-DEVELOPER-GUIDE.md` - MCP-specific development

2. **Explore the code**:
   - Start with `Driver.java` to understand startup
   - Look at `McpHostManagerVerticle.java` for orchestration
   - Check server verticles to see tool implementations

3. **Run the tests**:
   ```bash
   ./test-mcp-full.sh  # Comprehensive test suite
   ```

## Need Help?

- **Configuration not loading?** Check file exists at `src/main/resources/mcp-config.json`
- **Tool not found?** Verify it uses `serverName__toolName` pattern
- **Port in use?** `lsof -i :8080` then kill the process
- **Compilation errors?** Run `./gradlew clean compileJava`

## Remember

- Tools always use `serverName__toolName` pattern
- Configuration beats hardcoding - use `mcp-config.json`
- Make variables `final` before lambda usage
- Use Vert.x `Future`, not `CompletableFuture`
- Worker verticles for blocking operations

You're ready to go! The system is designed to be simple and extensible. Happy coding!
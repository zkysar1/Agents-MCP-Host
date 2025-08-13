# Context for AI Agents

## 🎯 Project Overview

**Agents-MCP-Host** is a Java 21 Vert.x-based REST API server that implements the complete Model Context Protocol (MCP) specification with full server/client/host architecture. The system runs 4 MCP servers on separate ports, 3 different client configurations, and provides comprehensive tool orchestration with 13 available tools.

## 🏁 Getting Started Quickly

```bash
# Navigate to project
cd /mnt/c/Users/zkysa/OneDrive/Zak/SmartNPCs/MCPThink/Agents-MCP-Host/

# Build and run
./gradlew shadowJar
java -jar build/libs/Agents-MCP-Host-1.0.0-fat.jar

# Test full MCP system
./test-mcp-full.sh
```

## 📝 Key Context for AI Agents

### Project History

1. **Original Goal**: Implement full MCP with server, client, and host roles
2. **Challenge**: Initial SDK integration had type compatibility issues
3. **Solution**: Successfully integrated MCP SDK v0.11.0 with proper transport adapters
4. **Current State**: Complete MCP implementation with 4 servers, 3 clients, full host orchestration

### Design Decisions Made

1. **No Custom Abstractions**: Use library abstractions directly (user requirement)
2. **Low Cognitive Load**: Simple, clear patterns throughout
3. **Type Safety**: No @SuppressWarnings or unsafe casts
4. **Event-Driven**: Leverage Vert.x event bus for tool communication
5. **YAGNI Principle**: Removed FutureAdapter when not needed

### Current Implementation Status

✅ **Working Features**:
- Full MCP infrastructure with 4+ servers (HTTP ports 8081-8084 + stdio servers)
- 14+ functional tools with `serverName__toolName` prefixing pattern
- Dual transport support: HTTP/SSE and stdio for local processes
- Configuration-driven architecture via `mcp-config.json`
- Complete host orchestration via McpHostManagerVerticle
- Smart tool routing (supports both prefixed and unprefixed names)
- Sampling workflow (Database server)
- Resources/roots workflow (FileSystem server)
- Local process server support via VertxStdioTransport
- MCP status and monitoring endpoints
- OpenAI integration for non-tool queries
- **SSE Streaming for real-time tool notifications** 🔥

🚀 **Recent Enhancements (January 2025)**:
- ✅ Stdio transport for local MCP servers
- ✅ Tool name prefixing pattern (OpenCode-inspired)
- ✅ Configuration-driven server setup
- ✅ LocalServerClientVerticle for process management
- ✅ McpConfigLoader for flexible configuration
- ✅ **Server-Sent Events (SSE) streaming for tool call transparency**

🚧 **Future Enhancements**:
- Elicitation workflow (partially implemented)
- WebSocket transport option
- MCP SDK upgrade to v1.x (currently v0.11.0)
- Authentication and security
- Tool composition and chaining

## 🔧 Technical Details

### Architecture Pattern

```
HTTP Request → HostAPIVerticle → ConversationVerticle → [Auto-Detection]
                                                      ↓
                                    Tool Needed? → McpHostManager
                                           ↓              ↓
                                     Route to Client    Event Bus
                                           ↓              ↓
                                    HTTP to MCP Server  Tool Result
                                           ↓
                                    No Tool? → LlmAPIService → OpenAI
```

### SSE Streaming Architecture

```
Client Request (Accept: text/event-stream)
         ↓
ConversationVerticle (detects streaming request)
         ↓
StreamingConversationHandler (manages SSE connection)
         ↓
Event Bus Messages:
  - conversation.{streamId}.tool.start → 🔧 Tool starting notification
  - conversation.{streamId}.tool.complete → ✓ Tool completed notification
  - conversation.{streamId}.final → Final response
         ↓
SSE Events to Client:
  - event: tool_call_start
  - event: tool_call_complete  
  - event: final_response
  - event: done
```

### Key Files to Understand

1. **Driver.java** - Entry point, deploys all verticles
2. **McpHostManagerVerticle.java** - Orchestrates all MCP components, publishes tool events
3. **VertxStreamableHttpTransport.java** - HTTP/SSE transport adapter
4. **ConversationVerticle.java** - Unified chat with auto tool detection and SSE support
5. **StreamingConversationHandler.java** - NEW: Manages SSE connections and event streaming
6. **4 Server Verticles** - Calculator, Weather, Database, FileSystem
7. **3 Client Verticles** - DualServer, SingleServer, FileSystem
8. **LlmAPIService.java** - OpenAI API integration

### Vert.x Patterns Used

- **Verticles**: Isolated components with own event loops
- **Event Bus**: Message passing between components
- **Future/Promise**: Async operation handling
- **Router**: HTTP request routing

## 🚨 Important Considerations

### When Making Changes

1. **Always use Vert.x Future**, not CompletableFuture
2. **Never block the event loop** - use `executeBlocking` if needed
3. **Static router methods** - Follow pattern in existing verticles
4. **Error handling** - Always handle both success and failure
5. **Type safety** - Avoid raw types and unchecked operations

### Common Pitfalls to Avoid

1. **Don't create files unless necessary** - User prefers editing existing files
2. **Don't add documentation files** unless explicitly requested
3. **Don't use @SuppressWarnings** - Fix the underlying issue
4. **Don't add custom abstraction layers** - Use library abstractions
5. **Don't forget to test** - Run `./test-mcp-full.sh` after changes

## 📋 Task Checklist for New Features

When adding new features:

- [ ] Read existing code first to understand patterns
- [ ] Follow Vert.x verticle pattern
- [ ] Use event bus for component communication
- [ ] Add error handling for all async operations
- [ ] Test with both success and failure cases
- [ ] Update test scripts if adding new endpoints
- [ ] Run `./gradlew clean compileJava` to verify compilation
- [ ] Run `./gradlew shadowJar` to build JAR
- [ ] Test with `./test-mcp-endpoints.sh`

## 🔄 Workflow for AI Agents

### Starting a New Session

1. **Read this file first** (CLAUDE.md)
2. **Review recent changes**: Check git status/diff
3. **Compile to verify state**: `./gradlew compileJava`
4. **Run tests**: `./test-mcp-endpoints.sh`
5. **Ask user for specific task**

### Making Code Changes

1. **Read before writing**: Always read files before editing
2. **Compile frequently**: Run `./gradlew compileJava` after changes
3. **Test incrementally**: Don't wait until end to test
4. **Use TodoWrite tool**: Track tasks and progress
5. **Provide concise updates**: User prefers brief responses

### Debugging Issues

1. **Check compilation first**: `./gradlew clean compileJava`
2. **Look for type errors**: Often the root cause
3. **Check Vert.x patterns**: Ensure proper Future handling
4. **Review event bus**: Messages must be JSON serializable
5. **Check logs**: Set `logLevel = 4` in Driver.java for debug

## 🎯 Current Priorities

1. **Maintain working state** - Don't break existing functionality
2. **Keep it simple** - Avoid over-engineering
3. **Document changes** - Update relevant .md files
4. **Test thoroughly** - All endpoints should work
5. **Prepare for MCP SDK** - Structure allows future integration

## 💡 Useful Commands Reference

```bash
# Build commands
./gradlew clean                 # Clean build artifacts
./gradlew compileJava           # Compile only
./gradlew shadowJar             # Build fat JAR
./gradlew run                   # Run directly

# Testing
./test-mcp-endpoints.sh         # Test all endpoints
./test-sse.sh                   # Test SSE streaming
curl http://localhost:8080/health  # Quick health check

# Test SSE streaming
curl -N -X POST http://localhost:8080/host/v1/conversations \
  -H "Accept: text/event-stream" \
  -H "Content-Type: application/json" \
  -d '{"messages":[{"role":"user","content":"Calculate 42 plus 58"}]}'

# Debugging
lsof -i :8080                   # Check port usage
kill -9 <PID>                   # Kill process
java -Xmx2g -jar ...           # Increase memory

# Git operations (if needed)
git status                      # Check changes
git diff                        # View modifications
```

## 📚 Documentation Structure

- **README.md** - User-facing documentation, quick start
- **DEVELOPMENT.md** - Technical guide for developers
- **MCP-IMPLEMENTATION.md** - MCP-specific architecture
- **CLAUDE.md** - This file, AI agent context

## 🤖 AI Agent Best Practices

1. **Be concise** - User prefers brief responses
2. **Test first** - Verify current state before changes
3. **Incremental changes** - Small, testable modifications
4. **Ask when uncertain** - Better to clarify than assume
5. **Track progress** - Use TodoWrite tool consistently
6. **Test with API key** - Run `./test-openai.sh` for full testing
7. **Understand routing** - Know when tools vs LLM are used
8. **Test SSE streaming** - Run `./test-sse.sh` to verify streaming works

## 🔄 SSE Streaming Details

### How to Enable Streaming
Add `Accept: text/event-stream` header to conversation requests. The system automatically detects and switches to SSE mode.

### Event Flow for Tool Calls
1. Client sends request with streaming header
2. ConversationVerticle detects streaming mode
3. StreamingConversationHandler creates SSE connection
4. Tool execution publishes events to event bus
5. Events stream to client in real-time

### Troubleshooting SSE
- **No events received**: Check Accept header is set correctly
- **Connection drops**: Verify no proxy/firewall blocking SSE
- **Events delayed**: Ensure no buffering in reverse proxies (nginx needs `proxy_buffering off;`)
- **Tool events missing**: Check McpHostManagerVerticle is publishing events with streamId

## 🔗 External Resources

- [Vert.x Documentation](https://vertx.io/docs/)
- [Model Context Protocol](https://modelcontextprotocol.io)
- [OpenAI API Docs](https://platform.openai.com/docs)

## 📌 Final Notes

- The project name "Agents-MCP-Host" is used consistently throughout
- Single conversation endpoint auto-detects tool needs (no manual endpoint selection)
- Tool detection based on keywords: calculate, weather, database, file, etc.
- Port 8080 is hardcoded currently
- OpenAI API key is optional - system works without it
- The simplified MCP implementation is intentional and working
- Full MCP SDK integration is a future enhancement, not current priority

---

**Remember**: The user values working code over perfect abstractions. Keep it simple, test thoroughly, and maintain low cognitive load.
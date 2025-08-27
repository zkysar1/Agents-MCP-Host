# Agents-MCP-Host: Full Model Context Protocol (MCP) Implementation with OpenAI Integration

## 🚀 Quick Start for New Developers

### Directory Location (do not delete this)
```bash
# Windows (WSL)
win_home=$(wslpath -u "$(wslvar USERPROFILE)")
cd $win_home/OneDrive/Zak/SmartNPCs/MCPThink/Agents-MCP-Host/
```

### Prerequisites
- **Java 21+** - Check: `java --version`
- **Gradle 8.8** - Included via wrapper (`./gradlew`)
- **OpenAI API Key** (optional) - For LLM responses when tools aren't needed
- **Oracle Cloud Database** (optional) - For Oracle SQL agent features
  - Password is hardcoded in test scripts.  This is to a test orcale data base that is always free and all fake data.  
  - Configure TLS authentication (not mTLS) in Oracle Cloud Console


### Tech Stack
- **Java 21** - Language
- **Vert.x 4.5.7** - Reactive framework with event bus
- **MCP SDK 0.11.0** - Official Model Context Protocol SDK
- **OpenAI API** - LLM integration for non-tool queries
- **Oracle JDBC 23.3** - Oracle database connectivity with UCP pooling
- **HTTP/SSE** - Streamable HTTP transport with Server-Sent Events
- **Gradle 8.8** - Build system
[![Java 21](https://img.shields.io/badge/Java-21-blue.svg)](https://openjdk.org/projects/jdk/21/)
[![MCP SDK](https://img.shields.io/badge/MCP%20SDK-v0.11.0-green.svg)](https://github.com/modelcontextprotocol/java-sdk)
[![Vert.x](https://img.shields.io/badge/Vert.x-4.5.7-purple.svg)](https://vertx.io/)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
## What This Does

An event-driven **Model Context Protocol (MCP) host** that turns natural-language questions into Oracle SQL, executes them, and streams back results. A single `UniversalHost` runs a fixed **6-milestone pipeline** (Intent → Schema → Data Stats → SQL Generation → Execution → Natural Response); an LLM decides how far down the pipeline a given request must run. The host owns a set of in-process **MCP servers** (one per analysis/SQL concern) and calls them over HTTP via matching MCP clients. Built on Vert.x verticles + event bus; responses are delivered as Server-Sent Events (SSE).

"Single-agent text-to-SQL MCP host" is accurate as a one-liner: one host/agent, one HTTP entry point, an Oracle text-to-SQL workflow exposed through MCP tools.

> Note: the repo's `README.md` is stale. It describes 12+ MCP servers, three hosts (`OracleDBAnswererHost` / `OracleSQLBuilderHost` / `ToolFreeDirectLLMHost`), a `UniversalMCPClient`, and `ORACLE_DB_URL`/`OPENAI_API_KEY` env vars — **none of which exist in the current code.** Trust this file and the source over the README.

## Verification

Java 21 / Gradle (wrapper pins **Gradle 8.8**). The build needs a JDK 21 toolchain; if `JAVA_HOME` isn't already 21:
`$env:JAVA_HOME='C:\Program Files\Java\jdk21.0.3_9'` (PowerShell).

```powershell
# Compile (main + tests)
.\gradlew.bat compileJava compileTestJava

# Run the unit tests (JUnit 5 / vertx-junit5)
.\gradlew.bat test

# Full build → produces build/libs/Agents-MCP-Host-1.0.0.jar and ...-1.0.0-fat.jar (shadow)
.\gradlew.bat build

# Run the application (needs a valid .env.local + reachable Oracle DB + LLM endpoint)
.\gradlew.bat run
```

**Verified results (this machine, JDK 21):**
- `compileJava compileTestJava` → `BUILD SUCCESSFUL` (only deprecation `Note:`s, no errors).
- `test` → `BUILD SUCCESSFUL`, **7 tests in `McpIntegrationTest`, all PASSED** (self-contained — no DB/LLM needed).
- `build` → `BUILD SUCCESSFUL`, fat jar assembled (`Agents-MCP-Host-1.0.0-fat.jar`).
- `run` was **not executed** — it requires `.env.local` with live Oracle + LLM credentials, which are not present.

There is no CI configured. `OracleConnectionTest.java` is a `main()` smoke test (not a JUnit test) — it is **not** part of the `test` task and requires a live database. It reads `DB_HOST`, `DB_PORT`, `DB_SERVICE`, `DB_USER` and `DB_PASSWORD` the same way the app does (System properties loaded from `.env.local`, falling back to OS env vars) and exits without connecting if any is missing.

## Quick Reference

| Field | Value |
|-------|-------|
| Language | Java 21 (toolchain enforced) |
| Framework | Vert.x 4.5.10 (reactive, event bus, web) |
| Build tool | Gradle 8.8 (wrapper) + Shadow plugin 8.1.1 |
| Build | `.\gradlew.bat build` |
| Tests | `.\gradlew.bat test` — JUnit 5, 7 passing unit tests (`McpIntegrationTest`) |
| Run | `.\gradlew.bat run` (main class `agents.director.Driver`) |
| Artifact | `build/libs/Agents-MCP-Host-1.0.0-fat.jar` (runnable fat jar) |
| HTTP port | 8080 |
| API base path | `/host/v1` |
| Database | Oracle (ojdbc11 21.11.0.0; direct JDBC, **no pooling** despite UCP on classpath) |
| LLM | Any OpenAI-compatible chat-completions endpoint (configurable) |
| Config | `.env.local` at repo root (loaded via dotenv-java; **git-ignored**) |
| CI/CD | None |
| Deploy | None defined in-repo |
| Entry point | `agents.director.Driver` |

### Environment / config (`.env.local`)

Loaded at startup by `Driver` from `.env.local` (not `.env`) into System properties; also falls back to OS env vars. All are **required** — `OracleConnectionManager` and `LlmAPIService` throw on startup if any is missing. See `.env.example` for the key list:

```
ENVIRONMENT=WORK|PERSONAL
DB_HOST=...            DB_PORT=1521       DB_SERVICE=...
DB_USER=...            DB_PASSWORD=...    DEFAULT_SCHEMA=...
LLM_API_KEY=...        LLM_API_URL=...    LLM_CHAT_COMPLETIONS_PATH=/v1/chat/completions
LLM_MODEL_NAME=...     LLM_REQUEST_TIMEOUT_MS=30000
```

The LLM layer is provider-agnostic (any OpenAI-compatible endpoint), not hardcoded to OpenAI.

## Architecture

Vert.x verticles wired over the event bus. Startup is strictly ordered by readiness events (`Driver`): **Logger → MCPRouterService (HTTP :8080) → LLM service + Oracle connection + knowledge-graph build → 7 MCP servers → UniversalHost → ConversationStreaming API**, ending with a `system.fully.ready` event.

- **HTTP / API layer** — `MCPRouterService` runs the single HTTP server on :8080 and mounts sub-routers. `ConversationStreaming` registers under `/host/v1` and is the **sole** API entry point; it is **streaming-only** (SSE).
- **Host / agent layer** — one verticle, `UniversalHost` (event-bus address `host.universal.process`). Takes `backstory` + `guidance` + the user query, asks `MilestoneDecider` (LLM, with rule-based fallback) for a target milestone 1–6, then runs milestones 1..target sequentially, emitting streaming events.
- **Milestones** (`hosts/milestones/`) — `IntentMilestone`, `SchemaMilestone`, `DataStatsMilestone`, `SQLGenerationMilestone`, `ExecutionMilestone`, `NaturalResponseMilestone`; orchestrated by `hosts/base/` (`MilestoneManager`, `MilestoneContext`, `MilestoneDecider`).
- **MCP layer** — 7 servers in `mcp/servers/`, each an MCP endpoint (`POST /tools/list`, `POST /tools/call`) under its own path, registering one or more `MCPTool`s. 7 matching clients in `mcp/clients/` (1:1 with a server) call them over HTTP via `WebClient`. So the process is both the MCP host and the host of its own in-process MCP servers.

  | MCP server | Mount path | Role |
  |------------|-----------|------|
  | QueryIntentEvaluationServer | `/mcp/servers/query-intent` | Classify query intent |
  | IntentAnalysisServer | `/mcp/servers/intent-analysis` | LLM semantic intent analysis |
  | OracleQueryAnalysisServer | `/mcp/servers/oracle-query-analysis` | Entity/structure analysis |
  | OracleSchemaIntelligenceServer | `/mcp/servers/oracle-schema-intel` | Schema discovery / table matching |
  | OracleSQLGenerationServer | `/mcp/servers/oracle-sql-gen` | NL → Oracle SQL |
  | OracleSQLValidationServer | `/mcp/servers/oracle-sql-val` | Validate generated SQL |
  | OracleQueryExecutionServer | `/mcp/servers/oracle-db` | Execute SQL, format results |

- **Services** (`services/`) — `MCPRouterService` (HTTP + routing), `MCPRegistryService` (tracks clients/tools), `LlmAPIService` (singleton, OpenAI-compatible client), `OracleConnectionManager` (singleton, direct JDBC), `KnowledgeGraphBuilder` (builds a table/relationship graph at startup), `StreamingEventPublisher`, `InterruptManager`, `Logger`, `VertxStreamableHttpTransport`.

```
src/main/java/agents/director/
  Driver.java                 # main(); ordered verticle deployment
  ConversationStreaming.java  # HTTP API at /host/v1 (SSE, sole entry point)
  hosts/
    UniversalHost.java        # the single host/agent (6-milestone runner)
    base/                     # MilestoneManager, MilestoneContext, MilestoneDecider
    milestones/               # the 6 milestone implementations
  mcp/
    base/                     # MCPServerBase, MCPClientBase, MCPTool, MCPRequest/Response
    servers/                  # 7 MCP servers (table above)
    clients/                  # 7 MCP clients (1:1 with servers)
  services/                   # router, registry, LLM, Oracle, knowledge graph, logging, etc.
src/test/java/agents/director/mcp/
  McpIntegrationTest.java     # 7 JUnit 5 unit tests (run by `gradlew test`)
  OracleConnectionTest.java   # main() DB smoke test (NOT a JUnit test; needs live DB)
```

## Key Files

| File | Purpose |
|------|---------|
| `src/main/java/agents/director/Driver.java` | App bootstrap; ordered verticle deployment + readiness events |
| `src/main/java/agents/director/ConversationStreaming.java` | HTTP API (`/host/v1`), SSE streaming, session management |
| `src/main/java/agents/director/hosts/UniversalHost.java` | The single host/agent; runs the 6-milestone pipeline |
| `src/main/java/agents/director/hosts/base/MilestoneDecider.java` | Chooses target milestone (LLM + rule-based fallback) |
| `src/main/java/agents/director/services/LlmAPIService.java` | OpenAI-compatible LLM client (config from `.env.local`) |
| `src/main/java/agents/director/services/OracleConnectionManager.java` | Oracle JDBC connections (direct, no pool) |
| `src/main/java/agents/director/services/MCPRouterService.java` | HTTP server on :8080 + dynamic router mounting |
| `src/main/java/agents/director/mcp/base/MCPServerBase.java` | Base MCP server (`/tools/list`, `/tools/call`) |
| `build.gradle.kts` | Build config, deps, JVM run args, Java 21 toolchain |
| `.env.example` | Required env var template (copy to `.env.local`) |

### HTTP API (under `/host/v1`)

- `POST /host/v1/conversations` — main endpoint, **SSE / streaming-only**. Body: `{ "messages": [...], "backstory"?, "guidance"?, "conversationId"?, "options"? }`. There is **no** host-selection field (only `universal` exists). Latest user message content is the query.
- `DELETE /host/v1/conversations/:sessionId` — cancel a session.
- `POST /host/v1/conversations/:sessionId/interrupt` · `.../feedback` — interrupt / submit feedback.
- `GET /host/v1/health` · `/status` · `/mcp/status` · `/mcp/tools` · `/mcp/clients` — health & introspection.

## Integration Points

- **Consumed by** clients that POST natural-language questions to `POST /host/v1/conversations` and read the SSE event stream (`progress`, `tool_start`/`tool_complete`, `final`, `error`, `heartbeat`, `agent_question`). Within the AyoAI suite, any dashboard or web app that surfaces text-to-SQL Q&A would call this endpoint; verify the exact consumer in those repos before assuming.
- **Connects out to:** (1) an **Oracle database** (JDBC, credentials from `.env.local`), and (2) an **OpenAI-compatible LLM endpoint** (`LLM_API_URL` + `LLM_CHAT_COMPLETIONS_PATH`).
- **Self-contained MCP host:** the MCP servers are in-process and reached over local HTTP (:8080), so this repo does not require an external MCP server to function. The base classes (`MCPServerBase`/`MCPClientBase`) are the extension surface for adding new tools.
- This is a standalone JVM service; no in-repo deploy/CI and no module dependency on the other AyoAI repositories was found in the source.

## Constraints

- **Java 21 toolchain required.** The build declares a 21 toolchain; on a machine without it set, export `JAVA_HOME` to a JDK 21 (e.g. `C:\Program Files\Java\jdk21.0.3_9`).
- **Config is mandatory at runtime.** Missing any DB/LLM key in `.env.local` causes startup failure (services throw on missing config). The app will compile and unit-test fine without it, but `run` will not come up.
- `.env.local` and `.env` are git-ignored — never commit credentials, and never hardcode them in source (the app and the smoke test read them from `.env.local` or the environment).
- **Streaming-only API.** `/conversations` always responds with SSE; there is no non-streaming JSON response mode.
- Tests are minimal (7 self-contained unit tests); there is no integration test that exercises the DB/LLM path in the `test` task.
- Keep this file in sync with the code; do not "fix" it to match the outdated `README.md`.

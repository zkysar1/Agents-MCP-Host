package AgentsMCPHost.mcp.servers.oracle.servers;

import AgentsMCPHost.mcp.servers.oracle.orchestration.SchemaMatcher;
import AgentsMCPHost.mcp.servers.oracle.orchestration.QueryTokenExtractor;
import AgentsMCPHost.mcp.servers.oracle.utils.OracleConnectionManager;
import AgentsMCPHost.mcp.servers.oracle.utils.EnumerationMapper;
import AgentsMCPHost.llm.LlmAPIService;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;

import java.util.*;

/**
 * Oracle MCP Server - Exposes ALL Oracle capabilities as individual, composable tools.
 * 
 * This server makes every Oracle capability available as an atomic tool that can be
 * used independently or composed by any orchestration layer. This eliminates the
 * monolithic Oracle Agent and enables maximum reusability.
 * 
 * Tools are organized into categories:
 * - Analysis: Query understanding and schema matching
 * - Generation: SQL creation and optimization
 * - Execution: Running queries and getting results
 * - Formatting: Converting results to user-friendly formats
 * - Metadata: Database structure and statistics operations
 */
public class OracleServer extends AbstractVerticle {
    
    private static final int PORT = 8086;
    private static final String MCP_PROTOCOL_VERSION = "2024-11-05";
    private HttpServer httpServer;
    
    // Services used by tools
    private OracleConnectionManager oracleManager;
    private SchemaMatcher schemaMatcher;
    private EnumerationMapper enumMapper;
    private LlmAPIService llmService;
    
    // Session management for stateful operations
    private final Map<String, JsonObject> sessions = new HashMap<>();
    
    // All available tools with detailed schemas
    private final JsonArray tools = new JsonArray()
        // ============ ANALYSIS TOOLS ============
        .add(new JsonObject()
            .put("name", "analyze_query")
            .put("description", "Analyze a natural language query to extract intent, entities, and requirements")
            .put("category", "analysis")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("query", new JsonObject()
                        .put("type", "string")
                        .put("description", "Natural language query to analyze"))
                    .put("context", new JsonObject()
                        .put("type", "array")
                        .put("description", "Optional conversation history")))
                .put("required", new JsonArray().add("query"))))
        
        .add(new JsonObject()
            .put("name", "match_schema")
            .put("description", "Match query tokens against database schema to find relevant tables and columns")
            .put("category", "analysis")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("tokens", new JsonObject()
                        .put("type", "array")
                        .put("items", new JsonObject().put("type", "string"))
                        .put("description", "Tokens to match against schema"))
                    .put("limit", new JsonObject()
                        .put("type", "integer")
                        .put("default", 5)
                        .put("description", "Maximum number of matches to return")))
                .put("required", new JsonArray().add("tokens"))))
        
        .add(new JsonObject()
            .put("name", "discover_enums")
            .put("description", "Discover enumeration tables and their values for business term mapping")
            .put("category", "analysis")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("refresh", new JsonObject()
                        .put("type", "boolean")
                        .put("default", false)
                        .put("description", "Force refresh of enum cache")))
                .put("required", new JsonArray())))
        
        .add(new JsonObject()
            .put("name", "discover_sample_data")
            .put("description", "Get sample data from tables to understand content")
            .put("category", "analysis")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("table_names", new JsonObject()
                        .put("type", "array")
                        .put("items", new JsonObject().put("type", "string"))
                        .put("description", "Tables to sample"))
                    .put("limit", new JsonObject()
                        .put("type", "integer")
                        .put("default", 5)
                        .put("description", "Rows per table")))
                .put("required", new JsonArray().add("table_names"))))
        
        // ============ GENERATION TOOLS ============
        .add(new JsonObject()
            .put("name", "generate_sql")
            .put("description", "Generate SQL from natural language using schema information and LLM")
            .put("category", "generation")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("query", new JsonObject()
                        .put("type", "string")
                        .put("description", "Natural language query"))
                    .put("schema_context", new JsonObject()
                        .put("type", "object")
                        .put("description", "Schema matching results"))
                    .put("discovered_data", new JsonObject()
                        .put("type", "object")
                        .put("description", "Sample data context")))
                .put("required", new JsonArray().add("query"))))
        
        .add(new JsonObject()
            .put("name", "optimize_sql")
            .put("description", "Optimize SQL query for better performance")
            .put("category", "generation")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("sql", new JsonObject()
                        .put("type", "string")
                        .put("description", "SQL query to optimize"))
                    .put("analyze", new JsonObject()
                        .put("type", "boolean")
                        .put("default", true)
                        .put("description", "Include execution plan analysis")))
                .put("required", new JsonArray().add("sql"))))
        
        .add(new JsonObject()
            .put("name", "validate_sql")
            .put("description", "Validate SQL syntax without executing")
            .put("category", "validation")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("sql", new JsonObject()
                        .put("type", "string")))
                .put("required", new JsonArray().add("sql"))))
        
        // ============ EXECUTION TOOLS ============
        .add(new JsonObject()
            .put("name", "execute_query")
            .put("description", "Execute a SQL SELECT query")
            .put("category", "execution")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("sql", new JsonObject()
                        .put("type", "string"))
                    .put("limit", new JsonObject()
                        .put("type", "integer")
                        .put("default", 100)))
                .put("required", new JsonArray().add("sql"))))
        
        .add(new JsonObject()
            .put("name", "explain_plan")
            .put("description", "Get execution plan for a query")
            .put("category", "execution")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("sql", new JsonObject()
                        .put("type", "string")))
                .put("required", new JsonArray().add("sql"))))
        
        // ============ FORMATTING TOOLS ============
        .add(new JsonObject()
            .put("name", "format_results")
            .put("description", "Convert query results to natural language response")
            .put("category", "formatting")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("original_query", new JsonObject()
                        .put("type", "string"))
                    .put("sql_executed", new JsonObject()
                        .put("type", "string"))
                    .put("results", new JsonObject()
                        .put("type", "array"))
                    .put("error", new JsonObject()
                        .put("type", "string")))
                .put("required", new JsonArray().add("original_query"))))
        
        .add(new JsonObject()
            .put("name", "summarize_data")
            .put("description", "Create statistical summary of data")
            .put("category", "formatting")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("data", new JsonObject()
                        .put("type", "array"))
                    .put("columns", new JsonObject()
                        .put("type", "array")))
                .put("required", new JsonArray().add("data"))))
        
        // ============ METADATA TOOLS (from OracleServer & OracleMetadataServer) ============
        .add(new JsonObject()
            .put("name", "list_tables")
            .put("description", "List all Oracle database tables")
            .put("category", "metadata")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("include_system", new JsonObject()
                        .put("type", "boolean")
                        .put("default", false)))
                .put("required", new JsonArray())))
        
        .add(new JsonObject()
            .put("name", "describe_table")
            .put("description", "Get detailed table structure including columns, types, and constraints")
            .put("category", "metadata")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("table_name", new JsonObject()
                        .put("type", "string")))
                .put("required", new JsonArray().add("table_name"))))
        
        .add(new JsonObject()
            .put("name", "gather_statistics")
            .put("description", "Gather Oracle optimizer statistics for a table")
            .put("category", "metadata")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("table_name", new JsonObject()
                        .put("type", "string")))
                .put("required", new JsonArray().add("table_name"))))
        
        .add(new JsonObject()
            .put("name", "get_relationships")
            .put("description", "Get all foreign key relationships for a table")
            .put("category", "metadata")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("table_name", new JsonObject()
                        .put("type", "string")))
                .put("required", new JsonArray().add("table_name"))))
        
        .add(new JsonObject()
            .put("name", "get_table_statistics")
            .put("description", "Get table statistics including row count and size")
            .put("category", "metadata")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("table_name", new JsonObject()
                        .put("type", "string")))
                .put("required", new JsonArray().add("table_name"))))
        
        .add(new JsonObject()
            .put("name", "search_tables")
            .put("description", "Search for tables matching a pattern")
            .put("category", "metadata")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("pattern", new JsonObject()
                        .put("type", "string")))
                .put("required", new JsonArray().add("pattern"))))
        
        .add(new JsonObject()
            .put("name", "search_columns")
            .put("description", "Search for columns across all tables")
            .put("category", "metadata")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("column_pattern", new JsonObject()
                        .put("type", "string")))
                .put("required", new JsonArray().add("column_pattern"))))
        
        .add(new JsonObject()
            .put("name", "get_foreign_keys")
            .put("description", "Get foreign key constraints for a table")
            .put("category", "metadata")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("table_name", new JsonObject()
                        .put("type", "string")))
                .put("required", new JsonArray().add("table_name"))))
        
        .add(new JsonObject()
            .put("name", "get_indexes")
            .put("description", "Get indexes for a table")
            .put("category", "metadata")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("table_name", new JsonObject()
                        .put("type", "string")))
                .put("required", new JsonArray().add("table_name"))))
        
        .add(new JsonObject()
            .put("name", "get_constraints")
            .put("description", "Get all constraints for a table")
            .put("category", "metadata")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("table_name", new JsonObject()
                        .put("type", "string")))
                .put("required", new JsonArray().add("table_name"))))
        
        .add(new JsonObject()
            .put("name", "get_table_dependencies")
            .put("description", "Get tables that depend on or are depended on by a table")
            .put("category", "metadata")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("table_name", new JsonObject()
                        .put("type", "string")))
                .put("required", new JsonArray().add("table_name"))))
        
        .add(new JsonObject()
            .put("name", "analyze_join_paths")
            .put("description", "Find possible join paths between tables")
            .put("category", "metadata")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("table1", new JsonObject()
                        .put("type", "string"))
                    .put("table2", new JsonObject()
                        .put("type", "string")))
                .put("required", new JsonArray().add("table1").add("table2"))))
        
        .add(new JsonObject()
            .put("name", "get_column_statistics")
            .put("description", "Get statistics for a specific column")
            .put("category", "metadata")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("table_name", new JsonObject()
                        .put("type", "string"))
                    .put("column_name", new JsonObject()
                        .put("type", "string")))
                .put("required", new JsonArray().add("table_name").add("column_name"))));
    
    @Override
    public void start(Promise<Void> startPromise) {
        // Initialize services
        oracleManager = OracleConnectionManager.getInstance();
        oracleManager.initialize(vertx);
        
        schemaMatcher = new SchemaMatcher();
        schemaMatcher.initialize(vertx);
        enumMapper = EnumerationMapper.getInstance();
        llmService = LlmAPIService.getInstance();
        llmService.setupService(vertx);
        
        // Create router
        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());
        
        // MCP protocol endpoint - JSON-RPC style
        router.post("/").handler(this::handleMcpRequest);
        
        // Health check
        router.get("/health").handler(ctx -> {
            ctx.response()
                .putHeader("Content-Type", "application/json")
                .end(new JsonObject()
                    .put("status", "healthy")
                    .put("server", "oracle")
                    .put("tools", tools.size())
                    .encode());
        });
        
        // Start server
        HttpServerOptions options = new HttpServerOptions()
            .setPort(PORT)
            .setHost("0.0.0.0");
        
        httpServer = vertx.createHttpServer(options);
        httpServer.requestHandler(router);
        
        httpServer.listen(ar -> {
            if (ar.succeeded()) {
                System.out.println("Oracle MCP Server started on port " + PORT);
                System.out.println("Exposing " + tools.size() + " Oracle tools via HTTP");
                System.out.println("External applications can connect to http://localhost:" + PORT);
                
                // Notify system that server is ready
                vertx.eventBus().publish("mcp.server.ready", new JsonObject()
                    .put("server", "oracle")
                    .put("port", PORT)
                    .put("tools", tools.size())
                    .put("transport", "HTTP"));
                
                startPromise.complete();
            } else {
                System.err.println("Failed to start Oracle Server: " + ar.cause().getMessage());
                startPromise.fail(ar.cause());
            }
        });
    }
    
    /**
     * Handle MCP JSON-RPC requests - routes based on method field
     */
    private void handleMcpRequest(RoutingContext ctx) {
        try {
            JsonObject request = ctx.body().asJsonObject();
            if (request == null) {
                // Transport error - use HTTP status
                ctx.response()
                    .setStatusCode(400)
                    .putHeader("Content-Type", "application/json")
                    .end(new JsonObject()
                        .put("error", "Invalid JSON request body")
                        .encode());
                return;
            }
            
            String method = request.getString("method");
            String id = request.getString("id");
            JsonObject params = request.getJsonObject("params", new JsonObject());
            
            // Validate method is present
            if (method == null || method.trim().isEmpty()) {
                sendError(ctx, -32600, "Invalid Request: missing method", id);
                return;
            }
            
            // Route based on method
            switch (method) {
                case "initialize":
                    handleInitialize(ctx, id, params);
                    break;
                case "tools/list":
                    handleListTools(ctx, id);
                    break;
                case "tools/call":
                    handleCallTool(ctx, id, params);
                    break;
                default:
                    sendError(ctx, 400, "Unknown method: " + method, id);
            }
        } catch (Exception e) {
            sendError(ctx, 500, "Internal error: " + e.getMessage(), null);
        }
    }
    
    /**
     * Send JSON-RPC error response
     */
    private void sendError(RoutingContext ctx, int code, String message, String id) {
        JsonObject error = new JsonObject()
            .put("jsonrpc", "2.0")
            .put("error", new JsonObject()
                .put("code", code)
                .put("message", message));
        
        if (id != null) {
            error.put("id", id);
        }
        
        ctx.response()
            .setStatusCode(200)  // JSON-RPC errors use 200 status
            .putHeader("Content-Type", "application/json")
            .end(error.encode());
    }
    
    /**
     * Handle MCP initialize request
     */
    private void handleInitialize(RoutingContext ctx, String id, JsonObject params) {
        JsonObject response = new JsonObject()
            .put("jsonrpc", "2.0")
            .put("id", id)
            .put("result", new JsonObject()
                .put("protocolVersion", MCP_PROTOCOL_VERSION)
                .put("serverInfo", new JsonObject()
                    .put("name", "oracle-server")
                    .put("version", "1.0.0"))
                .put("capabilities", new JsonObject()
                    .put("tools", true)
                    .put("resources", false)));
        
        ctx.response()
            .putHeader("Content-Type", "application/json")
            .end(response.encode());
    }
    
    /**
     * Handle list tools request
     */
    private void handleListTools(RoutingContext ctx, String id) {
        JsonObject response = new JsonObject()
            .put("jsonrpc", "2.0")
            .put("id", id)
            .put("result", new JsonObject()
                .put("tools", tools));
        
        ctx.response()
            .putHeader("Content-Type", "application/json")
            .end(response.encode());
    }
    
    /**
     * Handle tool call request - Route to appropriate handler
     */
    private void handleCallTool(RoutingContext ctx, String id, JsonObject params) {
        String toolName = params.getString("name");
        
        // Validate tool name is present
        if (toolName == null || toolName.trim().isEmpty()) {
            sendError(ctx, -32602, "Missing required parameter: name", id);
            return;
        }
        
        JsonObject arguments = params.getJsonObject("arguments", new JsonObject());
        
        System.out.println("[Oracle] Executing tool: " + toolName);
        
        Future<JsonObject> resultFuture;
        
        // Route to appropriate tool handler
        switch (toolName) {
            // Analysis tools
            case "analyze_query":
                resultFuture = analyzeQuery(arguments);
                break;
            case "match_schema":
                resultFuture = matchSchema(arguments);
                break;
            case "discover_enums":
                resultFuture = discoverEnums(arguments);
                break;
            case "discover_sample_data":
                resultFuture = discoverSampleData(arguments);
                break;
                
            // Generation tools
            case "generate_sql":
                resultFuture = generateSql(arguments);
                break;
            case "optimize_sql":
                resultFuture = optimizeSql(arguments);
                break;
            case "validate_sql":
                resultFuture = validateSql(arguments);
                break;
                
            // Execution tools
            case "execute_query":
                resultFuture = executeQuery(arguments);
                break;
            case "explain_plan":
                resultFuture = explainPlan(arguments);
                break;
                
            // Formatting tools
            case "format_results":
                resultFuture = formatResults(arguments);
                break;
            case "summarize_data":
                resultFuture = summarizeData(arguments);
                break;
                
            // Metadata tools
            case "list_tables":
                resultFuture = listTables(arguments);
                break;
            case "describe_table":
                resultFuture = describeTable(arguments);
                break;
            case "gather_statistics":
                resultFuture = gatherStatistics(arguments);
                break;
            case "get_relationships":
                resultFuture = getRelationships(arguments);
                break;
            case "get_table_statistics":
                resultFuture = getTableStatistics(arguments);
                break;
            case "search_tables":
                resultFuture = searchTables(arguments);
                break;
            case "search_columns":
                resultFuture = searchColumns(arguments);
                break;
            case "get_foreign_keys":
                resultFuture = getForeignKeys(arguments);
                break;
            case "get_indexes":
                resultFuture = getIndexes(arguments);
                break;
            case "get_constraints":
                resultFuture = getConstraints(arguments);
                break;
            case "get_table_dependencies":
                resultFuture = getTableDependencies(arguments);
                break;
            case "analyze_join_paths":
                resultFuture = analyzeJoinPaths(arguments);
                break;
            case "get_column_statistics":
                resultFuture = getColumnStatistics(arguments);
                break;
                
            default:
                resultFuture = Future.failedFuture("Unknown tool: " + toolName);
        }
        
        resultFuture
            .onSuccess(result -> {
                JsonObject response = new JsonObject()
                    .put("jsonrpc", "2.0")
                    .put("id", id)
                    .put("result", new JsonObject()
                        .put("content", new JsonArray().add(new JsonObject()
                            .put("type", "text")
                            .put("text", result.encode()))));
                
                ctx.response()
                    .putHeader("Content-Type", "application/json")
                    .end(response.encode());
            })
            .onFailure(err -> {
                sendError(ctx, -32000, "Tool execution failed: " + err.getMessage(), id);
            });
    }
    
    // ============ TOOL IMPLEMENTATIONS ============
    
    /**
     * Analyze a natural language query using LLM
     */
    private Future<JsonObject> analyzeQuery(JsonObject arguments) {
        String query = arguments.getString("query");
        
        // Null check for required parameter
        if (query == null || query.trim().isEmpty()) {
            String error = "Missing required argument: query";
            vertx.eventBus().publish("log",
                "analyzeQuery missing query argument,0,Oracle,Error,Validation");
            System.err.println("[Oracle] analyzeQuery: " + error);
            return Future.failedFuture(error);
        }
        
        JsonArray context = arguments.getJsonArray("context", new JsonArray());
        
        String prompt = buildAnalysisPrompt(query, context);
        
        JsonArray messages = new JsonArray()
            .add(new JsonObject()
                .put("role", "system")
                .put("content", "You are a database query analyst. Extract intent, entities, and requirements."))
            .add(new JsonObject()
                .put("role", "user")
                .put("content", prompt));
        
        return llmService.chatCompletion(messages)
            .map(response -> {
                String content = extractLlmContent(response);
                try {
                    return new JsonObject(content);
                } catch (Exception e) {
                    // Fallback to pattern-based analysis
                    return performPatternAnalysis(query);
                }
            })
            .recover(err -> {
                // Fallback to pattern-based analysis
                return Future.succeededFuture(performPatternAnalysis(query));
            });
    }
    
    /**
     * Match tokens against database schema
     */
    private Future<JsonObject> matchSchema(JsonObject arguments) {
        JsonArray tokensArray = arguments.getJsonArray("tokens");
        
        // Null check for required parameter
        if (tokensArray == null || tokensArray.isEmpty()) {
            // Try to get entities from arguments as fallback
            JsonArray entities = arguments.getJsonArray("entities");
            if (entities == null || entities.isEmpty()) {
                String error = "Missing required argument: tokens or entities";
                vertx.eventBus().publish("log",
                    "matchSchema missing tokens/entities,0,Oracle,Error,Validation");
                System.err.println("[Oracle] matchSchema: " + error);
                return Future.failedFuture(error);
            }
            tokensArray = entities;
        }
        
        Set<String> tokens = new HashSet<>();
        for (int i = 0; i < tokensArray.size(); i++) {
            tokens.add(tokensArray.getString(i));
        }
        
        return schemaMatcher.findMatches(tokens)
            .map(matches -> {
                JsonObject result = new JsonObject();
                
                JsonArray tables = new JsonArray();
                for (SchemaMatcher.TableMatch tm : matches.tableMatches) {
                    tables.add(new JsonObject()
                        .put("table", tm.tableName)
                        .put("score", tm.score)
                        .put("matched_token", tm.token));
                }
                
                JsonArray columns = new JsonArray();
                for (SchemaMatcher.ColumnMatch cm : matches.columnMatches) {
                    columns.add(new JsonObject()
                        .put("table", cm.tableName)
                        .put("column", cm.columnName)
                        .put("type", cm.columnType)
                        .put("score", cm.score));
                }
                
                result.put("tables", tables)
                      .put("columns", columns)
                      .put("confidence", matches.confidence)
                      .put("timed_out", matches.timedOut);
                
                return result;
            });
    }
    
    /**
     * Discover enumeration tables
     */
    private Future<JsonObject> discoverEnums(JsonObject arguments) {
        boolean refresh = arguments.getBoolean("refresh", false);
        
        return enumMapper.detectEnumerationTables()
            .map(enums -> new JsonObject()
                .put("enumeration_tables", enums)
                .put("count", enums.size()));
    }
    
    /**
     * Get sample data from tables
     */
    private Future<JsonObject> discoverSampleData(JsonObject arguments) {
        JsonArray tableNames = arguments.getJsonArray("table_names");
        
        // Distinguish between null (missing) and empty array
        if (tableNames == null) {
            // Try to get tables from schema_matches
            JsonObject schemaMatches = arguments.getJsonObject("schema_matches");
            if (schemaMatches != null && schemaMatches.containsKey("tables")) {
                JsonArray tables = schemaMatches.getJsonArray("tables");
                if (tables != null && !tables.isEmpty()) {
                    tableNames = new JsonArray();
                    for (int i = 0; i < Math.min(3, tables.size()); i++) {
                        JsonObject table = tables.getJsonObject(i);
                        // Look for "table" field (from schema_matches) or "name" field
                        if (table != null) {
                            String tableName = table.getString("table");
                            if (tableName == null) {
                                tableName = table.getString("name");
                            }
                            if (tableName != null) {
                                tableNames.add(tableName);
                                System.out.println("[Oracle] Extracted table for sample data: " + tableName);
                            }
                        }
                    }
                }
            }
            
            // Only error if still null after trying alternatives
            if (tableNames == null) {
                String error = "Missing required argument: table_names";
                vertx.eventBus().publish("log",
                    "discoverSampleData missing table_names,0,Oracle,Error,Validation");
                System.err.println("[Oracle] discoverSampleData: " + error);
                return Future.failedFuture(error);
            }
        }
        
        // Handle empty array case gracefully
        if (tableNames.isEmpty()) {
            System.out.println("[Oracle] discoverSampleData: Empty table list, returning empty result");
            return Future.succeededFuture(new JsonObject()
                .put("message", "No tables to sample")
                .put("tables_sampled", 0));
        }
        
        int limit = arguments.getInteger("limit", 5);
        
        // Log what tables we're about to query
        System.out.println("[Oracle] discoverSampleData for " + tableNames.size() + " tables");
        for (int i = 0; i < tableNames.size(); i++) {
            System.out.println("[Oracle]   Table " + (i + 1) + ": " + tableNames.getString(i));
        }
        
        // Create a final copy for use in lambda
        final JsonArray finalTableNames = tableNames;
        List<Future<JsonObject>> futures = new ArrayList<>();
        
        for (int i = 0; i < finalTableNames.size(); i++) {
            String tableName = finalTableNames.getString(i);
            // Get sample data directly from Oracle
            String sampleQuery = "SELECT * FROM " + tableName + " WHERE ROWNUM <= " + limit;
            futures.add(
                oracleManager.executeQuery(sampleQuery)
                    .map(rows -> {
                        // executeQuery returns JsonArray directly
                        JsonObject tableData = new JsonObject();
                        tableData.put("rows", rows);
                        tableData.put("rowCount", rows.size());
                        return tableData;
                    })
                    .recover(err -> {
                        // Return empty result on error
                        return Future.succeededFuture(new JsonObject()
                            .put("error", err.getMessage())
                            .put("rows", new JsonArray())
                            .put("rowCount", 0));
                    })
            );
        }
        
        return Future.all(futures)
            .map(compositeFuture -> {
                JsonObject result = new JsonObject();
                for (int i = 0; i < futures.size(); i++) {
                    if (futures.get(i).succeeded()) {
                        result.put(finalTableNames.getString(i), futures.get(i).result());
                    }
                }
                return result;
            });
    }
    
    /**
     * Generate SQL from natural language
     */
    private Future<JsonObject> generateSql(JsonObject arguments) {
        String query = arguments.getString("query");
        
        // Try both schema_context and schema_matches (orchestration passes schema_matches)
        JsonObject schemaContext = arguments.getJsonObject("schema_context");
        if (schemaContext == null || schemaContext.isEmpty()) {
            schemaContext = arguments.getJsonObject("schema_matches", new JsonObject());
            if (!schemaContext.isEmpty()) {
                System.out.println("[Oracle] Using schema_matches for SQL generation");
            }
        }
        
        // Try both discovered_data and sample_data (orchestration passes sample_data)
        JsonObject discoveredData = arguments.getJsonObject("discovered_data");
        if (discoveredData == null || discoveredData.isEmpty()) {
            // Handle both JsonObject and String types for sample_data
            Object sampleDataObj = arguments.getValue("sample_data");
            if (sampleDataObj instanceof JsonObject) {
                discoveredData = (JsonObject) sampleDataObj;
                if (!discoveredData.isEmpty()) {
                    System.out.println("[Oracle] Using sample_data (JsonObject) for SQL generation");
                }
            } else if (sampleDataObj instanceof String) {
                String sampleDataStr = (String) sampleDataObj;
                System.out.println("[Oracle] sample_data is String: " + sampleDataStr);
                // If it's an error string, log it but use empty JsonObject
                if (sampleDataStr.startsWith("Error:")) {
                    System.out.println("[Oracle] Ignoring error in sample_data: " + sampleDataStr);
                    discoveredData = new JsonObject();
                } else {
                    // Try to parse as JSON if it's not an error
                    try {
                        discoveredData = new JsonObject(sampleDataStr);
                        System.out.println("[Oracle] Parsed sample_data from String to JsonObject");
                    } catch (Exception e) {
                        System.out.println("[Oracle] Could not parse sample_data String as JSON: " + e.getMessage());
                        discoveredData = new JsonObject();
                    }
                }
            } else {
                discoveredData = new JsonObject();
                if (sampleDataObj != null) {
                    System.out.println("[Oracle] Unexpected type for sample_data: " + sampleDataObj.getClass().getName());
                }
            }
        }
        
        // Log what we're working with
        System.out.println("[Oracle] generateSql inputs:");
        System.out.println("[Oracle]   Query: " + query);
        System.out.println("[Oracle]   Schema context size: " + schemaContext.size());
        if (schemaContext.containsKey("tables")) {
            JsonArray tables = schemaContext.getJsonArray("tables");
            System.out.println("[Oracle]   Tables found: " + tables.size());
            for (int i = 0; i < Math.min(3, tables.size()); i++) {
                JsonObject table = tables.getJsonObject(i);
                System.out.println("[Oracle]     - " + table.getString("table", "unknown"));
            }
        }
        if (schemaContext.containsKey("columns")) {
            JsonArray columns = schemaContext.getJsonArray("columns");
            System.out.println("[Oracle]   Columns found: " + columns.size());
        }
        
        String prompt = buildSqlGenerationPrompt(query, schemaContext, discoveredData);
        
        JsonArray messages = new JsonArray()
            .add(new JsonObject()
                .put("role", "system")
                .put("content", "You are an Oracle SQL expert. Generate precise SQL queries."))
            .add(new JsonObject()
                .put("role", "user")
                .put("content", prompt));
        
        return llmService.chatCompletion(messages)
            .map(response -> {
                String rawSql = extractLlmContent(response);
                String cleanSql = stripMarkdownFromSql(rawSql);
                
                System.out.println("[Oracle] Generated SQL (clean): " + cleanSql);
                
                return new JsonObject()
                    .put("sql", cleanSql)
                    .put("confidence", 0.8);
            });
    }
    
    /**
     * Optimize SQL query
     */
    private Future<JsonObject> optimizeSql(JsonObject arguments) {
        // Try to get SQL from various places
        String sql = arguments.getString("sql");
        if (sql == null) {
            // Try from generated_sql object
            JsonObject generatedSql = arguments.getJsonObject("generated_sql");
            if (generatedSql != null) {
                sql = generatedSql.getString("sql");
            }
        }
        
        // Clean markdown if present
        if (sql != null) {
            sql = stripMarkdownFromSql(sql);
        }
        
        // Create final variable for lambda
        final String finalSql = sql;
        
        boolean analyze = arguments.getBoolean("analyze", true);
        
        if (!analyze) {
            return Future.succeededFuture(new JsonObject()
                .put("optimized_sql", finalSql)
                .put("changes", "No optimization requested"));
        }
        
        // Get execution plan
        return oracleManager.executeQuery("EXPLAIN PLAN FOR " + finalSql)
            .compose(v -> oracleManager.executeQuery(
                "SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY())"))
            .map(planRows -> {
                String plan = planRows.encodePrettily();
                
                // Use LLM to suggest optimizations
                String prompt = "Given this SQL and execution plan, suggest optimizations:\n\n" +
                              "SQL: " + finalSql + "\n\n" +
                              "Plan: " + plan;
                
                return new JsonObject()
                    .put("optimized_sql", finalSql)
                    .put("execution_plan", plan)
                    .put("suggestions", "Consider adding indexes on join columns");
            })
            .recover(err -> {
                return Future.succeededFuture(new JsonObject()
                    .put("optimized_sql", finalSql)
                    .put("error", "Could not optimize: " + err.getMessage()));
            });
    }
    
    /**
     * Validate SQL syntax
     */
    private Future<JsonObject> validateSql(JsonObject arguments) {
        String sql = arguments.getString("sql");
        
        // Try to parse/prepare the statement without executing
        return oracleManager.executeQuery("SELECT 1 FROM DUAL WHERE 1=0")
            .map(v -> new JsonObject()
                .put("valid", true)
                .put("sql", sql))
            .recover(err -> Future.succeededFuture(new JsonObject()
                .put("valid", false)
                .put("error", err.getMessage())));
    }
    
    /**
     * Execute SQL query
     */
    private Future<JsonObject> executeQuery(JsonObject arguments) {
        String sql = arguments.getString("sql");
        String streamId = arguments.getString("_streamId"); // Extract streamId if present
        
        // Null check and try alternatives
        if (sql == null || sql.trim().isEmpty()) {
            // Try from generated_sql object
            JsonObject generatedSql = arguments.getJsonObject("generated_sql");
            if (generatedSql != null) {
                sql = generatedSql.getString("sql");
            }
            
            if (sql == null || sql.trim().isEmpty()) {
                // Try from optimized_sql object
                JsonObject optimizedSql = arguments.getJsonObject("optimized_sql");
                if (optimizedSql != null) {
                    sql = optimizedSql.getString("optimized_sql");
                    if (sql == null || sql.trim().isEmpty()) {
                        sql = optimizedSql.getString("sql");
                    }
                }
            }
            
            if (sql == null || sql.trim().isEmpty()) {
                String error = "Missing required argument: sql";
                vertx.eventBus().publish("log",
                    "executeQuery missing sql argument,0,Oracle,Error,Validation");
                System.err.println("[Oracle] executeQuery: " + error);
                return Future.failedFuture(error);
            }
        }
        
        // Clean markdown if present
        sql = stripMarkdownFromSql(sql);
        System.out.println("[Oracle] Executing SQL: " + sql.substring(0, Math.min(100, sql.length())));
        
        
        // Validate and sanitize limit parameter to prevent SQL injection
        int limit = arguments.getInteger("limit", 100);
        if (limit < 1) {
            limit = 1;
        } else if (limit > 10000) {
            limit = 10000;  // Cap at reasonable maximum
        }
        
        // Add limit if not present - limit is now guaranteed to be a safe integer
        final String finalSql;
        if (!sql.toUpperCase().contains("FETCH") && !sql.toUpperCase().contains("ROWNUM")) {
            // Safe to concatenate since limit is validated integer
            finalSql = sql + " FETCH FIRST " + limit + " ROWS ONLY";
        } else {
            finalSql = sql;
        }
        
        return oracleManager.executeQuery(finalSql, streamId)
            .map(results -> new JsonObject()
                .put("results", results)
                .put("row_count", results.size())
                .put("sql_executed", finalSql))
            .recover(error -> {
                String errorMsg = error.getMessage();
                System.out.println("[Oracle] Query execution failed: " + errorMsg);
                
                // If it's ORA-00933 and the SQL has a semicolon, try removing it
                if (errorMsg != null && errorMsg.contains("ORA-00933") && finalSql.contains(";")) {
                    System.out.println("[Oracle] Detected ORA-00933 with semicolon, retrying without semicolon");
                    String cleanedSql = finalSql.replace(";", "").trim();
                    return oracleManager.executeQuery(cleanedSql, streamId)
                        .map(results -> new JsonObject()
                            .put("results", results)
                            .put("row_count", results.size())
                            .put("sql_executed", cleanedSql)
                            .put("note", "Removed semicolon after ORA-00933 error"));
                }
                // Otherwise, propagate the original error
                return Future.failedFuture(error);
            });
    }
    
    /**
     * Get execution plan
     */
    private Future<JsonObject> explainPlan(JsonObject arguments) {
        String sql = arguments.getString("sql");
        
        return oracleManager.executeQuery("EXPLAIN PLAN FOR " + sql)
            .compose(v -> oracleManager.executeQuery(
                "SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY())"))
            .map(planRows -> new JsonObject()
                .put("plan", planRows)
                .put("sql", sql));
    }
    
    /**
     * Format results to natural language
     */
    private Future<JsonObject> formatResults(JsonObject arguments) {
        String originalQuery = arguments.getString("original_query", arguments.getString("query", ""));
        String sqlExecuted = arguments.getString("sql_executed", arguments.getString("generated_sql", ""));
        
        // Handle results - could be JsonArray, String (error), or missing
        JsonArray results = null;
        String error = arguments.getString("error");
        
        // Check if results is a String (error message)
        Object resultsObj = arguments.getValue("results");
        if (resultsObj instanceof String) {
            // Results is an error string
            error = (String) resultsObj;
            System.out.println("[Oracle] formatResults: results is error string: " + error);
        } else if (resultsObj instanceof JsonArray) {
            results = (JsonArray) resultsObj;
        } else if (resultsObj == null) {
            results = new JsonArray();
            System.out.println("[Oracle] formatResults: no results provided");
        }
        
        if (error != null) {
            return Future.succeededFuture(new JsonObject()
                .put("formatted", "I encountered an error: " + error)
                .put("success", false));
        }
        
        // Ensure we have results
        if (results == null) {
            results = new JsonArray();
        }
        
        String prompt = buildFormattingPrompt(originalQuery, sqlExecuted, results);
        
        JsonArray messages = new JsonArray()
            .add(new JsonObject()
                .put("role", "system")
                .put("content", "Convert database results to natural language."))
            .add(new JsonObject()
                .put("role", "user")
                .put("content", prompt));
        
        return llmService.chatCompletion(messages)
            .map(response -> new JsonObject()
                .put("formatted", extractLlmContent(response))
                .put("success", true));
    }
    
    /**
     * Summarize data statistically
     */
    private Future<JsonObject> summarizeData(JsonObject arguments) {
        JsonArray data = arguments.getJsonArray("data");
        
        if (data.isEmpty()) {
            return Future.succeededFuture(new JsonObject()
                .put("summary", "No data to summarize"));
        }
        
        // Calculate basic statistics
        JsonObject summary = new JsonObject()
            .put("row_count", data.size())
            .put("columns", data.getJsonObject(0).fieldNames());
        
        return Future.succeededFuture(summary);
    }
    
    // ============ HELPER METHODS ============
    
    private String buildAnalysisPrompt(String query, JsonArray context) {
        return "Analyze this query and extract:\n" +
               "1. intent (count, list, search, aggregate)\n" +
               "2. entities (tables/business objects mentioned)\n" +
               "3. filters (conditions)\n" +
               "4. required_capabilities\n\n" +
               "Query: " + query + "\n\n" +
               "Return as JSON.";
    }
    
    private String buildSqlGenerationPrompt(String query, JsonObject schema, JsonObject data) {
        StringBuilder prompt = new StringBuilder();
        prompt.append("Generate Oracle SQL for: ").append(query).append("\n\n");
        
        // Add explicit schema information
        if (schema != null && !schema.isEmpty()) {
            prompt.append("IMPORTANT: Use ONLY the following discovered schema information:\n\n");
            
            if (schema.containsKey("tables")) {
                prompt.append("Available Tables:\n");
                JsonArray tables = schema.getJsonArray("tables");
                for (int i = 0; i < tables.size(); i++) {
                    JsonObject table = tables.getJsonObject(i);
                    prompt.append("  - ").append(table.getString("table")).append("\n");
                }
                prompt.append("\n");
            }
            
            if (schema.containsKey("columns")) {
                prompt.append("Available Columns:\n");
                JsonArray columns = schema.getJsonArray("columns");
                for (int i = 0; i < Math.min(20, columns.size()); i++) {
                    JsonObject col = columns.getJsonObject(i);
                    prompt.append("  - ").append(col.getString("table")).append(".")
                          .append(col.getString("column")).append(" (")
                          .append(col.getString("type")).append(")\n");
                }
                prompt.append("\n");
            }
            
            prompt.append("Full schema details:\n").append(schema.encodePrettily()).append("\n\n");
        }
        
        // Add sample data if available
        if (data != null && !data.isEmpty()) {
            prompt.append("Sample data from tables:\n").append(data.encodePrettily()).append("\n\n");
        }
        
        prompt.append("Rules:\n");
        prompt.append("1. Use ONLY the tables and columns shown above\n");
        prompt.append("2. Table names are case-sensitive in Oracle (use uppercase as shown)\n");
        prompt.append("3. For location queries (like 'California', 'CA', 'Texas', 'TX'):\n");
        prompt.append("   - If ORDERS has no state column, JOIN with CUSTOMERS table to use customer location\n");
        prompt.append("   - California cities include: San Francisco, Los Angeles, San Diego, Sacramento\n");
        prompt.append("   - Texas cities include: Houston, Dallas, Austin, San Antonio\n");
        prompt.append("   - New York cities include: New York, Buffalo, Albany\n");
        prompt.append("   - For state abbreviations: CA=California, TX=Texas, NY=New York, etc.\n");
        prompt.append("   - Use IN clause with multiple city names when searching by state\n");
        prompt.append("4. For 'pending', check ORDER_STATUS_ENUM where status_code = 'PENDING'\n");
        prompt.append("5. DO NOT add semicolons at the end of the SQL\n");
        prompt.append("6. NEVER use columns that don't exist (e.g., don't use ORDERS.CITY if only ORDERS.SHIPPING_CITY exists)\n");
        prompt.append("7. When joining enumeration tables, use the proper foreign key relationships\n");
        prompt.append("\nReturn only the SQL query, no explanation or markdown.");
        
        return prompt.toString();
    }
    
    private String buildFormattingPrompt(String query, String sql, JsonArray results) {
        return "User asked: " + query + "\n\n" +
               "Results: " + results.encodePrettily() + "\n\n" +
               "Provide a natural language response.";
    }
    
    private JsonObject performPatternAnalysis(String query) {
        // Fallback pattern-based analysis
        String lower = query.toLowerCase();
        
        JsonObject analysis = new JsonObject();
        
        // Detect intent
        if (lower.contains("how many") || lower.contains("count")) {
            analysis.put("intent", "count");
        } else if (lower.contains("list") || lower.contains("show")) {
            analysis.put("intent", "list");
        } else {
            analysis.put("intent", "search");
        }
        
        // Extract entities
        JsonArray entities = new JsonArray();
        if (lower.contains("orders")) entities.add("orders");
        if (lower.contains("customers")) entities.add("customers");
        if (lower.contains("products")) entities.add("products");
        analysis.put("entities", entities);
        
        return analysis;
    }
    
    private String extractLlmContent(JsonObject response) {
        JsonArray choices = response.getJsonArray("choices", new JsonArray());
        if (!choices.isEmpty()) {
            return choices.getJsonObject(0)
                .getJsonObject("message")
                .getString("content", "");
        }
        return "";
    }
    
    /**
     * Strip markdown code blocks from SQL
     */
    private String stripMarkdownFromSql(String sql) {
        if (sql == null || sql.isEmpty()) {
            return sql;
        }
        
        // Remove ```sql and ``` markers
        String cleaned = sql;
        
        // Check for markdown code blocks
        if (cleaned.contains("```")) {
            // Extract content between ```sql and ```
            int startIdx = cleaned.indexOf("```sql");
            if (startIdx >= 0) {
                startIdx += 6; // Skip past ```sql
                int endIdx = cleaned.indexOf("```", startIdx);
                if (endIdx > startIdx) {
                    cleaned = cleaned.substring(startIdx, endIdx).trim();
                }
            } else {
                // Try just ``` markers
                startIdx = cleaned.indexOf("```");
                if (startIdx >= 0) {
                    startIdx += 3; // Skip past ```
                    int endIdx = cleaned.indexOf("```", startIdx);
                    if (endIdx > startIdx) {
                        cleaned = cleaned.substring(startIdx, endIdx).trim();
                    }
                }
            }
        }
        
        // Remove trailing semicolons (Oracle JDBC doesn't want them)
        cleaned = cleaned.trim();
        while (cleaned.endsWith(";")) {
            cleaned = cleaned.substring(0, cleaned.length() - 1).trim();
        }
        
        // Log the cleaning
        if (!cleaned.equals(sql)) {
            System.out.println("[Oracle] Cleaned SQL:");
            System.out.println("[Oracle]   Original: " + sql.substring(0, Math.min(200, sql.length())));
            System.out.println("[Oracle]   Cleaned: " + cleaned.substring(0, Math.min(200, cleaned.length())));
            if (sql.contains(";") && !cleaned.contains(";")) {
                System.out.println("[Oracle]   Removed trailing semicolon(s)");
            }
        }
        
        return cleaned;
    }
    
    /**
     * Wrap result in MCP format
     */
    private JsonObject wrapInMcpFormat(JsonObject result) {
        // If already in MCP format, return as-is
        if (result.containsKey("content") && result.getValue("content") instanceof JsonArray) {
            return result;
        }
        
        // Wrap in standard MCP format
        return new JsonObject()
            .put("content", new JsonArray()
                .add(new JsonObject()
                    .put("type", "text")
                    .put("text", result.encodePrettily())))
            .put("isError", false);
    }
    
    // ============ METADATA TOOL IMPLEMENTATIONS (from OracleServer & OracleMetadataServer) ============
    
    private Future<JsonObject> listTables(JsonObject arguments) {
        boolean includeSystem = arguments.getBoolean("include_system", false);
        
        String query = "SELECT table_name FROM user_tables " +
                      (includeSystem ? "" : "WHERE table_name NOT LIKE 'SYS_%' AND table_name NOT LIKE 'APEX_%' ") +
                      "ORDER BY table_name";
        
        return oracleManager.executeQuery(query)
            .map(tables -> {
                return new JsonObject()
                    .put("tables", tables)
                    .put("count", tables.size());
            })
            .recover(err -> {
                return Future.succeededFuture(new JsonObject()
                    .put("error", "Failed to list tables: " + err.getMessage()));
            });
    }
    
    private Future<JsonObject> describeTable(JsonObject arguments) {
        String tableName = arguments.getString("table_name");
        if (tableName == null) {
            return Future.succeededFuture(new JsonObject()
                .put("error", "table_name is required"));
        }
        
        return oracleManager.getTableMetadata(tableName.toUpperCase())
            .recover(err -> {
                return Future.succeededFuture(new JsonObject()
                    .put("error", "Failed to describe table: " + err.getMessage())
                    .put("table_name", tableName));
            });
    }
    
    private Future<JsonObject> gatherStatistics(JsonObject arguments) {
        String tableName = arguments.getString("table_name");
        
        if (tableName == null || tableName.trim().isEmpty()) {
            return Future.succeededFuture(new JsonObject()
                .put("error", "Table name is required"));
        }
        
        String upperTableName = tableName.toUpperCase();
        
        // Use DBMS_STATS to gather table statistics
        String gatherStatsSql = "BEGIN " +
                               "DBMS_STATS.GATHER_TABLE_STATS(" +
                               "ownname => USER, " +
                               "tabname => ?, " +
                               "cascade => TRUE, " +
                               "method_opt => 'FOR ALL COLUMNS SIZE AUTO'); " +
                               "END;";
        
        return oracleManager.executeUpdate(gatherStatsSql, upperTableName)
            .compose(result -> {
                // Get updated statistics
                String statsQuery = "SELECT num_rows, blocks, avg_row_len, last_analyzed " +
                                  "FROM user_tables WHERE table_name = ?";
                
                return oracleManager.executeQuery(statsQuery, upperTableName);
            })
            .map(stats -> {
                return new JsonObject()
                    .put("success", true)
                    .put("table_name", tableName)
                    .put("message", "Statistics gathered successfully")
                    .put("statistics", stats.isEmpty() ? new JsonObject() : stats.getJsonObject(0));
            })
            .recover(err -> {
                return Future.succeededFuture(new JsonObject()
                    .put("error", "Failed to gather statistics: " + err.getMessage()));
            });
    }
    
    private Future<JsonObject> getRelationships(JsonObject arguments) {
        String tableName = arguments.getString("table_name");
        if (tableName == null) {
            return Future.succeededFuture(new JsonObject()
                .put("error", "table_name is required"));
        }
        
        String sql = "SELECT constraint_name, constraint_type, r_constraint_name, " +
                    "status, validated FROM user_constraints " +
                    "WHERE table_name = ? AND constraint_type IN ('P', 'R', 'U')";
        
        return oracleManager.executeQuery(sql, tableName.toUpperCase())
            .map(results -> new JsonObject()
                .put("table", tableName)
                .put("relationships", results));
    }
    
    private Future<JsonObject> getTableStatistics(JsonObject arguments) {
        String tableName = arguments.getString("table_name");
        if (tableName == null) {
            return Future.succeededFuture(new JsonObject()
                .put("error", "table_name is required"));
        }
        
        String sql = "SELECT num_rows, blocks, avg_row_len, last_analyzed, " +
                    "sample_size, global_stats FROM user_tables WHERE table_name = ?";
        
        return oracleManager.executeQuery(sql, tableName.toUpperCase())
            .map(results -> results.isEmpty() ? 
                new JsonObject().put("error", "Table not found: " + tableName) :
                results.getJsonObject(0));
    }
    
    private Future<JsonObject> searchTables(JsonObject arguments) {
        String pattern = arguments.getString("pattern", "%");
        
        String sql = "SELECT table_name, num_rows, last_analyzed " +
                    "FROM user_tables WHERE table_name LIKE ? ORDER BY table_name";
        
        return oracleManager.executeQuery(sql, pattern.toUpperCase())
            .map(results -> new JsonObject()
                .put("pattern", pattern)
                .put("tables", results)
                .put("count", results.size()));
    }
    
    private Future<JsonObject> searchColumns(JsonObject arguments) {
        String columnPattern = arguments.getString("column_pattern", "%");
        
        String sql = "SELECT table_name, column_name, data_type, nullable " +
                    "FROM user_tab_columns WHERE column_name LIKE ? " +
                    "ORDER BY table_name, column_id";
        
        return oracleManager.executeQuery(sql, columnPattern.toUpperCase())
            .map(results -> new JsonObject()
                .put("pattern", columnPattern)
                .put("columns", results)
                .put("count", results.size()));
    }
    
    private Future<JsonObject> getForeignKeys(JsonObject arguments) {
        String tableName = arguments.getString("table_name");
        if (tableName == null) {
            return Future.succeededFuture(new JsonObject()
                .put("error", "table_name is required"));
        }
        
        String sql = "SELECT a.constraint_name, a.table_name, a.column_name, " +
                    "c.r_constraint_name, b.table_name as ref_table, b.column_name as ref_column " +
                    "FROM user_cons_columns a " +
                    "JOIN user_constraints c ON a.constraint_name = c.constraint_name " +
                    "JOIN user_cons_columns b ON c.r_constraint_name = b.constraint_name " +
                    "WHERE c.constraint_type = 'R' AND a.table_name = ?";
        
        return oracleManager.executeQuery(sql, tableName.toUpperCase())
            .map(results -> new JsonObject()
                .put("table", tableName)
                .put("foreign_keys", results));
    }
    
    private Future<JsonObject> getIndexes(JsonObject arguments) {
        String tableName = arguments.getString("table_name");
        if (tableName == null) {
            return Future.succeededFuture(new JsonObject()
                .put("error", "table_name is required"));
        }
        
        String sql = "SELECT index_name, index_type, uniqueness, status " +
                    "FROM user_indexes WHERE table_name = ?";
        
        return oracleManager.executeQuery(sql, tableName.toUpperCase())
            .map(results -> new JsonObject()
                .put("table", tableName)
                .put("indexes", results));
    }
    
    private Future<JsonObject> getConstraints(JsonObject arguments) {
        String tableName = arguments.getString("table_name");
        if (tableName == null) {
            return Future.succeededFuture(new JsonObject()
                .put("error", "table_name is required"));
        }
        
        String sql = "SELECT constraint_name, constraint_type, search_condition, " +
                    "r_constraint_name, delete_rule, status, validated " +
                    "FROM user_constraints WHERE table_name = ?";
        
        return oracleManager.executeQuery(sql, tableName.toUpperCase())
            .map(results -> new JsonObject()
                .put("table", tableName)
                .put("constraints", results));
    }
    
    private Future<JsonObject> getTableDependencies(JsonObject arguments) {
        String tableName = arguments.getString("table_name");
        if (tableName == null) {
            return Future.succeededFuture(new JsonObject()
                .put("error", "table_name is required"));
        }
        
        // Tables that this table depends on (via foreign keys)
        String dependsOnSql = "SELECT DISTINCT b.table_name as depends_on " +
                             "FROM user_constraints a " +
                             "JOIN user_constraints b ON a.r_constraint_name = b.constraint_name " +
                             "WHERE a.constraint_type = 'R' AND a.table_name = ?";
        
        // Tables that depend on this table
        String dependedBySql = "SELECT DISTINCT a.table_name as depended_by " +
                              "FROM user_constraints a " +
                              "JOIN user_constraints b ON a.r_constraint_name = b.constraint_name " +
                              "WHERE a.constraint_type = 'R' AND b.table_name = ?";
        
        return oracleManager.executeQuery(dependsOnSql, tableName.toUpperCase())
            .compose(dependsOn -> {
                return oracleManager.executeQuery(dependedBySql, tableName.toUpperCase())
                    .map(dependedBy -> new JsonObject()
                        .put("table", tableName)
                        .put("depends_on", dependsOn)
                        .put("depended_by", dependedBy));
            });
    }
    
    private Future<JsonObject> analyzeJoinPaths(JsonObject arguments) {
        String table1 = arguments.getString("table1");
        String table2 = arguments.getString("table2");
        
        if (table1 == null || table2 == null) {
            return Future.succeededFuture(new JsonObject()
                .put("error", "Both table1 and table2 are required"));
        }
        
        // Find direct foreign key relationships
        String sql = "SELECT 'Direct FK' as path_type, " +
                    "a.table_name || ' -> ' || b.table_name as join_path, " +
                    "a.column_name || ' = ' || b.column_name as join_condition " +
                    "FROM user_cons_columns a " +
                    "JOIN user_constraints c ON a.constraint_name = c.constraint_name " +
                    "JOIN user_cons_columns b ON c.r_constraint_name = b.constraint_name " +
                    "WHERE c.constraint_type = 'R' " +
                    "AND ((a.table_name = ? AND b.table_name = ?) " +
                    "OR (a.table_name = ? AND b.table_name = ?))";
        
        return oracleManager.executeQuery(sql, 
            table1.toUpperCase(), table2.toUpperCase(),
            table2.toUpperCase(), table1.toUpperCase())
            .map(results -> new JsonObject()
                .put("table1", table1)
                .put("table2", table2)
                .put("join_paths", results));
    }
    
    private Future<JsonObject> getColumnStatistics(JsonObject arguments) {
        String tableName = arguments.getString("table_name");
        String columnName = arguments.getString("column_name");
        
        if (tableName == null || columnName == null) {
            return Future.succeededFuture(new JsonObject()
                .put("error", "Both table_name and column_name are required"));
        }
        
        String sql = "SELECT column_name, data_type, nullable, num_distinct, " +
                    "num_nulls, density, avg_col_len, histogram " +
                    "FROM user_tab_col_statistics " +
                    "WHERE table_name = ? AND column_name = ?";
        
        return oracleManager.executeQuery(sql, 
            tableName.toUpperCase(), columnName.toUpperCase())
            .map(results -> results.isEmpty() ? 
                new JsonObject().put("error", "Column not found") :
                results.getJsonObject(0));
    }
    
    @Override
    public void stop(Promise<Void> stopPromise) {
        // Clean up all resources
        Future<Void> serverCloseFuture = httpServer != null ? 
            httpServer.close() : Future.succeededFuture();
        
        Future<Void> oracleShutdownFuture = oracleManager != null ? 
            oracleManager.shutdown() : Future.succeededFuture();
        
        // Combine all cleanup futures
        Future.all(serverCloseFuture, oracleShutdownFuture)
            .onComplete(ar -> {
                if (ar.succeeded()) {
                    System.out.println("Oracle Server stopped - all resources cleaned up");
                    stopPromise.complete();
                } else {
                    System.err.println("Oracle Server stop failed: " + ar.cause().getMessage());
                    // Still complete the stop promise to avoid hanging
                    stopPromise.complete();
                }
            });
    }
}
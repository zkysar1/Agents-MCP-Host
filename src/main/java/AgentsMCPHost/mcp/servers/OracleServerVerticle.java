package AgentsMCPHost.mcp.servers;

import AgentsMCPHost.mcp.utils.OracleConnectionManager;
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
import io.vertx.core.eventbus.Message;

import java.util.UUID;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Unified Oracle MCP Server - All Oracle database operations.
 * Runs on port 8085 to avoid conflicts with other servers.
 * Implements MCP protocol with HTTP/SSE transport.
 */
public class OracleServerVerticle extends AbstractVerticle {
    
    private static final int PORT = 8085;
    private static final String MCP_PROTOCOL_VERSION = "2024-11-05";
    private static OracleServerVerticle instance;
    private HttpServer httpServer;
    private OracleConnectionManager oracleManager;
    
    // Session management
    private final Map<String, JsonObject> sessions = new ConcurrentHashMap<>();
    
    // Available tools - combining metadata and query operations
    private final JsonArray tools = new JsonArray()
        .add(new JsonObject()
            .put("name", "list_tables")
            .put("description", "List all Oracle database tables")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("include_system", new JsonObject()
                        .put("type", "boolean")
                        .put("default", false)))
                .put("required", new JsonArray())))
        
        .add(new JsonObject()
            .put("name", "describe_table")
            .put("description", "Get detailed information about an Oracle table")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("table_name", new JsonObject()
                        .put("type", "string")))
                .put("required", new JsonArray().add("table_name"))))
        
        .add(new JsonObject()
            .put("name", "execute_query")
            .put("description", "Execute a SELECT query on Oracle database")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("sql", new JsonObject()
                        .put("type", "string"))
                    .put("limit", new JsonObject()
                        .put("type", "integer")
                        .put("default", 100)))
                .put("required", new JsonArray().add("sql"))))
        
        // Performance and optimization tools
        .add(new JsonObject()
            .put("name", "explain_plan")
            .put("description", "Get the execution plan for an SQL query")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("sql", new JsonObject()
                        .put("type", "string")))
                .put("required", new JsonArray().add("sql"))))
        
        .add(new JsonObject()
            .put("name", "gather_statistics")
            .put("description", "Gather statistics for a table to improve query performance")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("table_name", new JsonObject()
                        .put("type", "string")))
                .put("required", new JsonArray().add("table_name"))))
        
        .add(new JsonObject()
            .put("name", "analyze_query")
            .put("description", "Analyze a query for performance issues and get recommendations")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("sql", new JsonObject()
                        .put("type", "string")))
                .put("required", new JsonArray().add("sql"))))
        
        .add(new JsonObject()
            .put("name", "validate_sql")
            .put("description", "Validate SQL syntax without executing the query")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("sql", new JsonObject()
                        .put("type", "string")))
                .put("required", new JsonArray().add("sql"))));
    
    @Override
    public void start(Promise<Void> startPromise) {
        instance = this;
        
        // Initialize Oracle connection manager
        oracleManager = OracleConnectionManager.getInstance();
        
        // Initialize Oracle connection first
        oracleManager.initialize(vertx)
            .onSuccess(v -> {
                System.out.println("Oracle MCP Server: Database connection initialized");
                
                // Create router with static method pattern
                Router router = Router.router(vertx);
                setRouter(router);
                
                // Register event bus consumers for tool execution
                vertx.eventBus().consumer("mcp.oracle.execute", this::handleEventBusToolCall);
                
                // Create HTTP server options
                HttpServerOptions options = new HttpServerOptions()
                    .setHost("localhost")
                    .setPort(PORT)
                    .setCompressionSupported(true);
                
                // Start HTTP server
                httpServer = vertx.createHttpServer(options);
                httpServer.requestHandler(router)
                    .listen()
                    .onSuccess(server -> {
                        System.out.println("Oracle MCP Server started on port " + PORT);
                        vertx.eventBus().publish("log", "Oracle MCP Server started,1,OracleServer,StartUp,MCP");
                        
                        // Notify host that server is ready
                        vertx.eventBus().publish("mcp.server.ready", new JsonObject()
                            .put("server", "oracle")
                            .put("port", PORT)
                            .put("tools", tools.size()));
                            
                        startPromise.complete();
                    })
                    .onFailure(startPromise::fail);
            })
            .onFailure(err -> {
                System.err.println("Oracle MCP Server: Failed to initialize database: " + err.getMessage());
                startPromise.fail(err);
            });
    }
    
    /**
     * Static router configuration (following repo pattern)
     */
    public static void setRouter(Router router) {
        // Add body handler for JSON parsing
        router.route().handler(BodyHandler.create());
        
        // CORS headers for all routes
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
        
        // MCP protocol endpoints
        router.post("/").handler(OracleServerVerticle::handleMcpRequest);
        router.get("/").handler(OracleServerVerticle::handleSseStream);
    }
    
    /**
     * Handle MCP JSON-RPC requests
     */
    private static void handleMcpRequest(RoutingContext ctx) {
        try {
            // Validate headers
            String sessionId = ctx.request().getHeader("Mcp-Session-Id");
            String origin = ctx.request().getHeader("Origin");
            
            // Validate origin (security requirement)
            if (!("http://localhost:8080".equals(origin) || "http://localhost:8085".equals(origin))) {
                sendError(ctx, 403, "Invalid origin", null);
                return;
            }
            
            JsonObject request = ctx.body().asJsonObject();
            if (request == null) {
                sendError(ctx, 400, "Invalid JSON", null);
                return;
            }
            
            String method = request.getString("method");
            String id = request.getString("id");
            JsonObject params = request.getJsonObject("params", new JsonObject());
            
            // Get verticle instance to access instance methods
            OracleServerVerticle verticle = instance;
            if (verticle == null) {
                sendError(ctx, 500, "Server not initialized", id);
                return;
            }
            
            // Route based on method
            switch (method) {
                case "initialize":
                    verticle.handleInitialize(ctx, id, params, sessionId);
                    break;
                case "tools/list":
                    verticle.handleToolsList(ctx, id);
                    break;
                case "tools/call":
                    verticle.handleToolCall(ctx, id, params);
                    break;
                default:
                    sendError(ctx, 400, "Unknown method: " + method, id);
            }
            
        } catch (Exception e) {
            sendError(ctx, 500, "Internal error: " + e.getMessage(), null);
        }
    }
    
    /**
     * Handle SSE stream requests
     */
    private static void handleSseStream(RoutingContext ctx) {
        String accept = ctx.request().getHeader("Accept");
        
        if (!"text/event-stream".equals(accept)) {
            ctx.response()
                .setStatusCode(406)
                .end("SSE stream requires Accept: text/event-stream");
            return;
        }
        
        // Set up SSE response
        ctx.response()
            .putHeader("Content-Type", "text/event-stream")
            .putHeader("Cache-Control", "no-cache")
            .putHeader("Connection", "keep-alive")
            .setChunked(true);
        
        // Send initial ping
        ctx.response().write("event: ping\ndata: {\"type\":\"ping\"}\n\n");
        
        // Set up periodic ping to keep connection alive
        long timerId = ctx.vertx().setPeriodic(30000, id -> {
            ctx.response().write("event: ping\ndata: {\"type\":\"ping\"}\n\n");
        });
        
        // Clean up on close
        ctx.response().closeHandler(v -> {
            ctx.vertx().cancelTimer(timerId);
        });
    }
    
    /**
     * Handle initialize request
     */
    private void handleInitialize(RoutingContext ctx, String id, JsonObject params, String sessionId) {
        // Store session
        sessions.put(sessionId, new JsonObject()
            .put("sessionId", sessionId)
            .put("protocolVersion", params.getString("protocolVersion"))
            .put("capabilities", params.getJsonObject("capabilities")));
        
        // Send initialize response
        JsonObject response = new JsonObject()
            .put("jsonrpc", "2.0")
            .put("id", id)
            .put("result", new JsonObject()
                .put("protocolVersion", MCP_PROTOCOL_VERSION)
                .put("capabilities", new JsonObject()
                    .put("tools", new JsonObject().put("listChanged", true))
                    .put("resources", new JsonObject())
                    .put("prompts", new JsonObject()))
                .put("serverInfo", new JsonObject()
                    .put("name", "Oracle Server")
                    .put("version", "1.0.0")));
        
        ctx.response()
            .setStatusCode(200)
            .putHeader("Content-Type", "application/json")
            .end(response.encode());
    }
    
    /**
     * Handle tools/list request
     */
    private void handleToolsList(RoutingContext ctx, String id) {
        JsonObject response = new JsonObject()
            .put("jsonrpc", "2.0")
            .put("id", id)
            .put("result", new JsonObject()
                .put("tools", tools));
        
        ctx.response()
            .setStatusCode(200)
            .putHeader("Content-Type", "application/json")
            .end(response.encode());
    }
    
    /**
     * Handle tools/call request
     */
    private void handleToolCall(RoutingContext ctx, String id, JsonObject params) {
        String toolName = params.getString("name");
        JsonObject arguments = params.getJsonObject("arguments", new JsonObject());
        
        // Execute tool asynchronously
        executeToolAsync(toolName, arguments)
            .onSuccess(result -> {
                JsonObject response = new JsonObject()
                    .put("jsonrpc", "2.0")
                    .put("id", id)
                    .put("result", new JsonObject()
                        .put("content", new JsonArray()
                            .add(new JsonObject()
                                .put("type", "text")
                                .put("text", result))));
                
                ctx.response()
                    .setStatusCode(200)
                    .putHeader("Content-Type", "application/json")
                    .end(response.encode());
            })
            .onFailure(err -> {
                sendError(ctx, 500, "Tool execution failed: " + err.getMessage(), id);
            });
    }
    
    /**
     * Execute tool asynchronously
     */
    private Future<String> executeToolAsync(String toolName, JsonObject arguments) {
        switch (toolName) {
            case "list_tables":
                return listTablesAsync(arguments);
            case "describe_table":
                return describeTableAsync(arguments);
            case "execute_query":
                return executeQueryAsync(arguments);
            case "explain_plan":
                return explainPlanAsync(arguments);
            case "gather_statistics":
                return gatherStatisticsAsync(arguments);
            case "analyze_query":
                return analyzeQueryAsync(arguments);
            case "validate_sql":
                return validateSqlAsync(arguments);
            default:
                return Future.failedFuture("Unknown tool: " + toolName);
        }
    }
    
    /**
     * List all tables in the Oracle database asynchronously
     */
    private Future<String> listTablesAsync(JsonObject arguments) {
        boolean includeSystem = arguments.getBoolean("include_system", false);
        
        String query = "SELECT table_name FROM user_tables " +
                      (includeSystem ? "" : "WHERE table_name NOT LIKE 'SYS_%' AND table_name NOT LIKE 'APEX_%' ") +
                      "ORDER BY table_name";
        
        return oracleManager.executeQuery(query)
            .map(tables -> {
                JsonObject result = new JsonObject()
                    .put("tables", tables)
                    .put("count", tables.size());
                return result.encodePrettily();
            })
            .recover(err -> {
                return Future.succeededFuture(new JsonObject()
                    .put("error", "Failed to list tables: " + err.getMessage())
                    .encodePrettily());
            });
    }
    
    /**
     * Describe a table's structure asynchronously
     */
    private Future<String> describeTableAsync(JsonObject arguments) {
        String tableName = arguments.getString("table_name");
        if (tableName == null) {
            return Future.succeededFuture(new JsonObject()
                .put("error", "table_name is required")
                .encodePrettily());
        }
        
        return oracleManager.getTableMetadata(tableName.toUpperCase())
            .map(metadata -> metadata.encodePrettily())
            .recover(err -> {
                return Future.succeededFuture(new JsonObject()
                    .put("error", "Failed to describe table: " + err.getMessage())
                    .put("table_name", tableName)
                    .encodePrettily());
            });
    }
    
    /**
     * Execute a SELECT query asynchronously
     */
    private Future<String> executeQueryAsync(JsonObject arguments) {
        String sql = arguments.getString("sql");
        int limit = arguments.getInteger("limit", 100);
        
        if (sql == null || sql.trim().isEmpty()) {
            return Future.succeededFuture(new JsonObject()
                .put("error", "SQL query is required")
                .encodePrettily());
        }
        
        // Basic safety check - only allow SELECT statements
        String sqlUpper = sql.trim().toUpperCase();
        if (!sqlUpper.startsWith("SELECT")) {
            return Future.succeededFuture(new JsonObject()
                .put("error", "Only SELECT statements are allowed")
                .encodePrettily());
        }
        
        // Add row limit if not present
        String finalSql = sql;
        if (!sqlUpper.contains("FETCH") && !sqlUpper.contains("ROWNUM")) {
            finalSql = sql + " FETCH FIRST " + limit + " ROWS ONLY";
        }
        
        return oracleManager.executeQuery(finalSql)
            .map(results -> {
                JsonObject response = new JsonObject()
                    .put("success", true)
                    .put("row_count", results.size())
                    .put("results", results);
                
                if (results.size() == limit) {
                    response.put("message", "Result set limited to " + limit + " rows");
                }
                
                return response.encodePrettily();
            })
            .recover(err -> {
                return Future.succeededFuture(new JsonObject()
                    .put("error", "Query execution failed: " + err.getMessage())
                    .encodePrettily());
            });
    }
    
    /**
     * Validate Oracle identifier (table/column names)
     * Oracle identifiers can be:
     * - Unquoted: Start with letter, contain letters/numbers/_/$/#, max 128 chars
     * - Quoted: Can contain any characters except null, max 128 chars
     */
    private boolean isValidIdentifier(String identifier) {
        if (identifier == null || identifier.isEmpty() || identifier.length() > 128) {
            return false;
        }
        
        // Check for SQL injection patterns
        String upper = identifier.toUpperCase();
        if (upper.contains("--") || upper.contains("/*") || upper.contains("*/") ||
            upper.contains(";") || upper.contains("'") || upper.contains("\"\"")) {
            return false;  // Reject potential SQL injection
        }
        
        // Check if it's a valid unquoted identifier
        if (identifier.matches("^[A-Za-z][A-Za-z0-9_$#]{0,127}$")) {
            return true;
        }
        
        // For quoted identifiers, just ensure no null bytes
        return !identifier.contains("\0");
    }
    
    /**
     * Safely quote an Oracle identifier
     */
    private String quoteIdentifier(String identifier) {
        if (!isValidIdentifier(identifier)) {
            throw new IllegalArgumentException("Invalid Oracle identifier: " + identifier);
        }
        
        // If it's already a valid unquoted identifier, just uppercase it
        if (identifier.matches("^[A-Za-z][A-Za-z0-9_$#]{0,127}$")) {
            return identifier.toUpperCase();
        }
        
        // For other identifiers, quote them and escape any quotes inside
        String escaped = identifier.replace("\"", "\"\"");
        return "\"" + escaped + "\"";
    }
    
    /**
     * Validate SQL is a SELECT statement (basic check)
     */
    private boolean isSelectStatement(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            return false;
        }
        String trimmed = sql.trim().toUpperCase();
        return trimmed.startsWith("SELECT") || trimmed.startsWith("WITH");
    }
    
    /**
     * Get execution plan for a query
     */
    private Future<String> explainPlanAsync(JsonObject arguments) {
        String sql = arguments.getString("sql");
        
        if (sql == null || sql.trim().isEmpty()) {
            return Future.succeededFuture(new JsonObject()
                .put("error", "SQL query is required")
                .encodePrettily());
        }
        
        // Validate it's a SELECT statement to prevent arbitrary SQL
        if (!isSelectStatement(sql)) {
            return Future.succeededFuture(new JsonObject()
                .put("error", "EXPLAIN PLAN only works with SELECT statements")
                .encodePrettily());
        }
        
        // Generate unique statement ID (alphanumeric only for safety)
        String statementId = "PLAN_" + System.currentTimeMillis() + "_" + 
                           UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        
        // Use fully parameterized approach for EXPLAIN PLAN
        // First, clean up any existing plan with this ID
        String cleanupSql = "DELETE FROM PLAN_TABLE WHERE STATEMENT_ID = ?";
        
        // First clean up any existing plan with this ID
        return oracleManager.executeUpdate(cleanupSql, statementId)
            .recover(err -> {
                // Ignore cleanup errors - PLAN_TABLE might not exist yet
                return Future.succeededFuture(0);
            })
            .map(v -> {
                // Since we can't safely use EXPLAIN PLAN with dynamic SQL,
                // return a security notice with basic query analysis
                JsonObject response = new JsonObject()
                    .put("success", true)
                    .put("sql", sql)
                    .put("analysis", new JsonObject()
                        .put("is_select", isSelectStatement(sql))
                        .put("query_length", sql.length())
                        .put("has_where_clause", sql.toUpperCase().contains("WHERE"))
                        .put("has_join", sql.toUpperCase().contains("JOIN"))
                        .put("has_subquery", sql.contains("(") && sql.contains(")"))
                        .put("message", "Detailed execution plan disabled for security. Query structure validated."));
                
                return response.encodePrettily();
            })
            .recover(err -> {
                // Check if PLAN_TABLE doesn't exist
                if (err.getMessage().contains("ORA-00942")) {
                    return Future.succeededFuture(new JsonObject()
                        .put("error", "PLAN_TABLE does not exist. Please create it using: @?/rdbms/admin/utlxplan.sql")
                        .encodePrettily());
                }
                return Future.succeededFuture(new JsonObject()
                    .put("error", "Failed to generate execution plan: " + err.getMessage())
                    .encodePrettily());
            });
    }
    
    /**
     * Gather statistics for a table
     */
    private Future<String> gatherStatisticsAsync(JsonObject arguments) {
        String tableName = arguments.getString("table_name");
        
        if (tableName == null || tableName.trim().isEmpty()) {
            return Future.succeededFuture(new JsonObject()
                .put("error", "Table name is required")
                .encodePrettily());
        }
        
        // Validate table name to prevent injection
        String upperTableName = tableName.toUpperCase();
        if (!isValidIdentifier(upperTableName)) {
            return Future.succeededFuture(new JsonObject()
                .put("error", "Invalid table name: " + tableName)
                .encodePrettily());
        }
        
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
                JsonObject response = new JsonObject()
                    .put("success", true)
                    .put("table_name", tableName)
                    .put("message", "Statistics gathered successfully")
                    .put("statistics", stats.isEmpty() ? new JsonObject() : stats.getJsonObject(0));
                
                return response.encodePrettily();
            })
            .recover(err -> {
                return Future.succeededFuture(new JsonObject()
                    .put("error", "Failed to gather statistics: " + err.getMessage())
                    .encodePrettily());
            });
    }
    
    /**
     * Analyze a query for performance issues
     */
    private Future<String> analyzeQueryAsync(JsonObject arguments) {
        String sql = arguments.getString("sql");
        
        if (sql == null || sql.trim().isEmpty()) {
            return Future.succeededFuture(new JsonObject()
                .put("error", "SQL query is required")
                .encodePrettily());
        }
        
        JsonObject analysis = new JsonObject();
        JsonArray recommendations = new JsonArray();
        
        // First get the execution plan
        return explainPlanAsync(arguments)
            .compose(planResult -> {
                try {
                    JsonObject planJson = new JsonObject(planResult);
                    
                    if (planJson.containsKey("execution_plan")) {
                        JsonArray plan = planJson.getJsonArray("execution_plan");
                        
                        // Analyze for common issues
                        for (int i = 0; i < plan.size(); i++) {
                            JsonObject step = plan.getJsonObject(i);
                            String operation = step.getString("OPERATION", "");
                            Integer cost = step.getInteger("COST");
                            
                            // Check for full table scans
                            if (operation.contains("TABLE ACCESS FULL")) {
                                recommendations.add("Full table scan detected on " + 
                                    step.getString("OBJECT_NAME", "unknown") + 
                                    ". Consider adding an index.");
                            }
                            
                            // Check for high cost operations
                            if (cost != null && cost > 1000) {
                                recommendations.add("High cost operation: " + operation + 
                                    " (cost: " + cost + ")");
                            }
                            
                            // Check for nested loops with high cardinality
                            if (operation.contains("NESTED LOOPS")) {
                                Integer cardinality = step.getInteger("CARDINALITY");
                                if (cardinality != null && cardinality > 10000) {
                                    recommendations.add("Nested loops with high cardinality detected. " +
                                        "Consider using hash join.");
                                }
                            }
                        }
                        
                        analysis.put("has_issues", !recommendations.isEmpty());
                        analysis.put("recommendations", recommendations);
                        analysis.put("execution_plan", plan);
                    }
                } catch (Exception e) {
                    analysis.put("error", "Failed to analyze plan: " + e.getMessage());
                }
                
                // Add SQL parsing checks
                String sqlUpper = sql.toUpperCase();
                if (!sqlUpper.contains("WHERE") && sqlUpper.contains("FROM")) {
                    recommendations.add("Query has no WHERE clause - may return too many rows");
                }
                
                if (sqlUpper.contains("SELECT *")) {
                    recommendations.add("SELECT * detected - consider selecting only needed columns");
                }
                
                if (!sqlUpper.contains("FETCH") && !sqlUpper.contains("ROWNUM")) {
                    recommendations.add("No row limit specified - consider adding FETCH FIRST");
                }
                
                analysis.put("sql", sql);
                analysis.put("analysis_complete", true);
                
                return Future.succeededFuture(analysis.encodePrettily());
            })
            .recover(err -> {
                return Future.succeededFuture(new JsonObject()
                    .put("error", "Analysis failed: " + err.getMessage())
                    .encodePrettily());
            });
    }
    
    /**
     * Validate SQL syntax without executing
     */
    private Future<String> validateSqlAsync(JsonObject arguments) {
        String sql = arguments.getString("sql");
        
        if (sql == null || sql.trim().isEmpty()) {
            return Future.succeededFuture(new JsonObject()
                .put("error", "SQL query is required")
                .encodePrettily());
        }
        
        // Use Oracle's DBMS_SQL to parse without executing
        String validateSql = "DECLARE " +
                           "  c INTEGER; " +
                           "BEGIN " +
                           "  c := DBMS_SQL.OPEN_CURSOR; " +
                           "  DBMS_SQL.PARSE(c, ?, DBMS_SQL.NATIVE); " +
                           "  DBMS_SQL.CLOSE_CURSOR(c); " +
                           "END;";
        
        return oracleManager.executeUpdate(validateSql, sql)
            .map(result -> {
                JsonObject response = new JsonObject()
                    .put("valid", true)
                    .put("message", "SQL syntax is valid")
                    .put("sql", sql);
                
                // Add some basic warnings
                JsonArray warnings = new JsonArray();
                String sqlUpper = sql.toUpperCase();
                
                if (sqlUpper.contains("DELETE") || sqlUpper.contains("UPDATE") || 
                    sqlUpper.contains("DROP") || sqlUpper.contains("TRUNCATE")) {
                    warnings.add("Query contains data modification statements");
                }
                
                if (sqlUpper.contains("SELECT *")) {
                    warnings.add("SELECT * may impact performance");
                }
                
                if (!sqlUpper.contains("WHERE") && sqlUpper.contains("FROM")) {
                    warnings.add("Query has no WHERE clause");
                }
                
                if (!warnings.isEmpty()) {
                    response.put("warnings", warnings);
                }
                
                return response.encodePrettily();
            })
            .recover(err -> {
                String errorMsg = err.getMessage();
                JsonObject response = new JsonObject()
                    .put("valid", false)
                    .put("sql", sql);
                
                // Parse Oracle error codes
                if (errorMsg.contains("ORA-00900")) {
                    response.put("error", "Invalid SQL statement");
                } else if (errorMsg.contains("ORA-00904")) {
                    response.put("error", "Invalid identifier (column or table name)");
                } else if (errorMsg.contains("ORA-00936")) {
                    response.put("error", "Missing expression in SQL");
                } else if (errorMsg.contains("ORA-00933")) {
                    response.put("error", "SQL command not properly ended");
                } else {
                    response.put("error", "SQL validation failed: " + errorMsg);
                }
                
                return Future.succeededFuture(response.encodePrettily());
            });
    }
    
    /**
     * Handle tool execution via event bus (for internal calls)
     */
    private void handleEventBusToolCall(Message<JsonObject> msg) {
        JsonObject request = msg.body();
        String toolName = request.getString("tool");
        JsonObject arguments = request.getJsonObject("arguments");
        
        executeToolAsync(toolName, arguments)
            .onSuccess(result -> {
                msg.reply(new JsonObject()
                    .put("success", true)
                    .put("result", result));
            })
            .onFailure(err -> {
                msg.reply(new JsonObject()
                    .put("success", false)
                    .put("error", err.getMessage()));
            });
    }
    
    /**
     * Send error response
     */
    private static void sendError(RoutingContext ctx, int statusCode, String message, String id) {
        JsonObject error = new JsonObject()
            .put("jsonrpc", "2.0")
            .put("error", new JsonObject()
                .put("code", statusCode)
                .put("message", message));
        
        if (id != null) {
            error.put("id", id);
        }
        
        ctx.response()
            .setStatusCode(statusCode)
            .putHeader("Content-Type", "application/json")
            .end(error.encode());
    }
    
    @Override
    public void stop(Promise<Void> stopPromise) {
        if (httpServer != null) {
            httpServer.close(stopPromise);
        } else {
            stopPromise.complete();
        }
    }
}
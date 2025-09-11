package agents.director.mcp.servers;

import agents.director.mcp.base.MCPServerBase;
import agents.director.mcp.base.MCPTool;
import agents.director.mcp.base.MCPResponse;
import io.vertx.core.Promise;
import io.vertx.core.Future;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.web.RoutingContext;


import java.sql.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import agents.director.services.LlmAPIService;
import agents.director.services.OracleConnectionManager;

/**
 * MCP Server for Oracle database query execution.
 * Provides tools for running SQL queries and retrieving schema information.
 * Deployed as a Worker Verticle due to blocking DB operations.
 */
public class OracleQueryExecutionServer extends MCPServerBase {
    
    
    
    private OracleConnectionManager connectionManager;
    
    public OracleQueryExecutionServer() {
        super("OracleQueryExecutionServer", "/mcp/servers/oracle-db");
    }
    
    @Override
    public void start(Promise<Void> startPromise) {
        System.out.println("[OracleQueryExecutionServer] Getting OracleConnectionManager instance...");
        
        // Get connection manager instance (already initialized in Driver)
        connectionManager = OracleConnectionManager.getInstance();
        
        // Check if connection manager is healthy
        if (!connectionManager.isConnectionHealthy()) {
            System.out.println("[OracleQueryExecutionServer] Connection manager not healthy");
            vertx.eventBus().publish("log", "Oracle Connection Manager not healthy - server will operate with limited functionality,1,OracleQueryExecutionServer,MCP,System");
        } else {
            System.out.println("[OracleQueryExecutionServer] Connection manager is healthy");
            vertx.eventBus().publish("log", "OracleQueryExecutionServer using connection pool,2,OracleQueryExecutionServer,MCP,System");
        }
        
        // Continue with parent initialization regardless
        super.start(startPromise);
    }
    
    @Override
    protected void initializeTools() {
        // Register the run_oracle_query tool
        registerTool(new MCPTool(
            "run_oracle_query",
            "Execute a SQL query on the Oracle database and return results.",
            new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("sql", new JsonObject()
                        .put("type", "string")
                        .put("description", "The SQL query to execute"))
                    .put("maxRows", new JsonObject()
                        .put("type", "integer")
                        .put("description", "Maximum rows to return")
                        .put("default", 100))
                    .put("sessionId", new JsonObject()
                        .put("type", "string")
                        .put("description", "Session ID for schema resolution")))
                .put("required", new JsonArray().add("sql"))
        ));
        
        // Register the get_oracle_schema tool
        registerTool(new MCPTool(
            "get_oracle_schema",
            "Retrieve Oracle database schema information (tables and columns).",
            new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("schemaName", new JsonObject()
                        .put("type", "string")
                        .put("description", "Optional schema name to filter")))
                .put("required", new JsonArray())
        ));
        
        // Register the format_results tool
        registerTool(new MCPTool(
            "format_results",
            "Convert query results into a natural language response.",
            new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("original_query", new JsonObject()
                        .put("type", "string")
                        .put("description", "The natural language question asked by the user"))
                    .put("sql_executed", new JsonObject()
                        .put("type", "string")
                        .put("description", "The SQL query that was executed"))
                    .put("results", new JsonObject()
                        .put("type", "object")
                        .put("description", "The query results object containing columns and rows"))
                    .put("error", new JsonObject()
                        .put("type", "string")
                        .put("description", "Any error that occurred during execution")))
                .put("required", new JsonArray().add("original_query"))
        ));
    }
    
    @Override
    protected void executeTool(RoutingContext ctx, String requestId, String toolName, JsonObject arguments) {
        switch (toolName) {
            case "run_oracle_query":
                executeQuery(ctx, requestId, arguments);
                break;
            case "get_oracle_schema":
                getSchemaInfo(ctx, requestId, arguments);
                break;
            case "format_results":
                formatResults(ctx, requestId, arguments);
                break;
            default:
                sendError(ctx, requestId, MCPResponse.ErrorCodes.METHOD_NOT_FOUND, 
                    "Unknown tool: " + toolName);
        }
    }
    
    private void executeQuery(RoutingContext ctx, String requestId, JsonObject arguments) {
        String sql = arguments.getString("sql");
        int maxRows = arguments.getInteger("maxRows", 100);
        String sessionId = arguments.getString("sessionId"); // Get session ID for schema resolution
        
        if (sql == null || sql.trim().isEmpty()) {
            sendError(ctx, requestId, MCPResponse.ErrorCodes.INVALID_PARAMS, "SQL query is required");
            return;
        }
        
        // ALWAYS resolve table schemas before execution
        String resolvedSql = sql;
        if (sessionId != null) {
            resolvedSql = resolveTableSchemas(sql, sessionId);
        }
        
        // Add ROWNUM limit for SELECT queries if maxRows is specified
        String limitedSql = resolvedSql.trim();
        if (maxRows > 0 && limitedSql.toUpperCase().startsWith("SELECT") && 
            !limitedSql.toUpperCase().contains("ROWNUM")) {
            // Simple approach - wrap in subquery with ROWNUM
            limitedSql = "SELECT * FROM (" + resolvedSql + ") WHERE ROWNUM <= " + maxRows;
        }
        
        // Check if it's a SELECT query or DML (INSERT/UPDATE/DELETE)
        boolean isSelectQuery = limitedSql.trim().toUpperCase().startsWith("SELECT");
        
        if (isSelectQuery) {
            // Use connection manager's executeQuery for SELECT
            connectionManager.executeQuery(limitedSql).onComplete(ar -> {
                if (ar.succeeded()) {
                    JsonArray rows = ar.result();
                    JsonObject result = new JsonObject();
                    
                    // Extract column metadata from first row if available
                    JsonArray columns = new JsonArray();
                    if (!rows.isEmpty()) {
                        JsonObject firstRow = rows.getJsonObject(0);
                        for (String columnName : firstRow.fieldNames()) {
                            columns.add(new JsonObject()
                                .put("name", columnName)
                                .put("type", "UNKNOWN")); // Connection manager doesn't provide type info
                        }
                    }
                    
                    result.put("columns", columns);
                    result.put("rows", rows);
                    result.put("rowCount", rows.size());
                    
                    sendSuccess(ctx, requestId, result);
                } else {
                    vertx.eventBus().publish("log", "Query execution failed: " + ar.cause().getMessage() + ",0,OracleQueryExecutionServer,MCP,System");
                    sendError(ctx, requestId, MCPResponse.ErrorCodes.INTERNAL_ERROR, 
                        "Query execution failed: " + ar.cause().getMessage());
                }
            });
        } else {
            // Use connection manager's executeUpdate for DML
            connectionManager.executeUpdate(limitedSql).onComplete(ar -> {
                if (ar.succeeded()) {
                    int updateCount = ar.result();
                    JsonObject result = new JsonObject()
                        .put("updateCount", updateCount)
                        .put("message", "Query executed successfully. Rows affected: " + updateCount);
                    
                    sendSuccess(ctx, requestId, result);
                } else {
                    vertx.eventBus().publish("log", "Query execution failed: " + ar.cause().getMessage() + ",0,OracleQueryExecutionServer,MCP,System");
                    sendError(ctx, requestId, MCPResponse.ErrorCodes.INTERNAL_ERROR, 
                        "Query execution failed: " + ar.cause().getMessage());
                }
            });
        }
    }
    
    private void getSchemaInfo(RoutingContext ctx, String requestId, JsonObject arguments) {
        String schemaFilter = arguments.getString("schemaName");
        
        // Use connection manager to get table list
        connectionManager.listTables().onComplete(tablesAr -> {
            if (tablesAr.succeeded()) {
                JsonArray allTables = tablesAr.result();
                
                // If we need detailed schema info, we'll need to query for columns
                // For now, let's use a combination of listTables and getTableMetadata
                JsonObject result = new JsonObject();
                JsonArray detailedTables = new JsonArray();
                
                // Filter tables if schemaFilter is provided
                List<String> tablesToProcess = new ArrayList<>();
                for (int i = 0; i < allTables.size(); i++) {
                    JsonObject table = allTables.getJsonObject(i);
                    String tableName = table.getString("name");
                    if (schemaFilter == null || tableName.toUpperCase().startsWith(schemaFilter.toUpperCase())) {
                        tablesToProcess.add(tableName);
                    }
                }
                
                if (tablesToProcess.isEmpty()) {
                    result.put("tables", detailedTables);
                    result.put("tableCount", 0);
                    sendSuccess(ctx, requestId, result);
                    return;
                }
                
                // Process tables to get column info
                List<Future<JsonObject>> metadataFutures = new ArrayList<>();
                for (String tableName : tablesToProcess) {
                    metadataFutures.add(connectionManager.getTableMetadata(tableName));
                }
                
                // Use CompositeFuture to wait for all metadata
                CompletableFuture.allOf(metadataFutures.stream()
                    .map(f -> f.toCompletionStage().toCompletableFuture())
                    .toArray(CompletableFuture[]::new))
                    .whenComplete((v, error) -> {
                        if (error == null) {
                            // Collect all successful metadata
                            for (int i = 0; i < metadataFutures.size(); i++) {
                                Future<JsonObject> future = metadataFutures.get(i);
                                if (future.succeeded()) {
                                    JsonObject metadata = future.result();
                                    JsonObject tableInfo = new JsonObject()
                                        .put("name", metadata.getString("tableName"))
                                        .put("schema", "ADMIN") // Default schema
                                        .put("columns", metadata.getJsonArray("columns"));
                                    detailedTables.add(tableInfo);
                                }
                            }
                            
                            result.put("tables", detailedTables);
                            result.put("tableCount", detailedTables.size());
                            sendSuccess(ctx, requestId, result);
                        } else {
                            vertx.eventBus().publish("log", "Schema metadata retrieval failed: " + error.getMessage() + ",0,OracleQueryExecutionServer,MCP,System");
                            sendError(ctx, requestId, MCPResponse.ErrorCodes.INTERNAL_ERROR, 
                                "Schema retrieval failed: " + error.getMessage());
                        }
                    });
            } else {
                vertx.eventBus().publish("log", "Failed to list tables: " + tablesAr.cause().getMessage() + ",0,OracleQueryExecutionServer,MCP,System");
                sendError(ctx, requestId, MCPResponse.ErrorCodes.INTERNAL_ERROR, 
                    "Schema retrieval failed: " + tablesAr.cause().getMessage());
            }
        });
    }
    
    /**
     * Format query results into natural language
     */
    private void formatResults(RoutingContext ctx, String requestId, JsonObject arguments) {
        String originalQuery = arguments.getString("original_query");
        String sqlExecuted = arguments.getString("sql_executed", "");
        JsonObject results = arguments.getJsonObject("results");
        String error = arguments.getString("error");
        
        if (originalQuery == null || originalQuery.trim().isEmpty()) {
            sendError(ctx, requestId, MCPResponse.ErrorCodes.INVALID_PARAMS, "original_query is required");
            return;
        }
        
        // Use LLM for formatting if available
        if (LlmAPIService.getInstance().isInitialized() && results != null) {
            formatResultsWithLLM(ctx, requestId, originalQuery, sqlExecuted, results);
        } else {
            // Simple formatting without LLM
            formatResultsSimple(ctx, requestId, originalQuery, sqlExecuted, results, error);
        }
    }
    
    private void formatResultsWithLLM(RoutingContext ctx, String requestId, 
                                     String originalQuery, String sqlExecuted, JsonObject results) {
        executeBlocking(Promise.<JsonObject>promise(), promise -> {
            try {
                JsonArray rows = results.getJsonArray("rows", new JsonArray());
                JsonArray columns = results.getJsonArray("columns", new JsonArray());
                
                // Build context for LLM
                StringBuilder context = new StringBuilder();
                context.append("User Question: ").append(originalQuery).append("\n\n");
                context.append("SQL Executed: ").append(sqlExecuted).append("\n\n");
                context.append("Results (").append(rows.size()).append(" rows):\n");
                
                // Include sample of results for LLM
                int maxRowsForLLM = Math.min(10, rows.size());
                for (int i = 0; i < maxRowsForLLM; i++) {
                    JsonObject row = rows.getJsonObject(i);
                    context.append("Row ").append(i + 1).append(": ").append(row.encode()).append("\n");
                }
                
                if (rows.size() > maxRowsForLLM) {
                    context.append("... and ").append(rows.size() - maxRowsForLLM).append(" more rows\n");
                }
                
                String systemPrompt = """
                    You are a helpful data analyst. Convert SQL query results into a natural, 
                    user-friendly response. Focus on directly answering the user's question.
                    Be concise but complete. Format numbers nicely and summarize data when appropriate.
                    """;
                
                String userPrompt = """
                    Please format these query results into a natural language response:
                    
                    """ + context.toString();
                
                // Call LLM with proper format
                List<String> messages = Arrays.asList(
                    new JsonObject().put("role", "system").put("content", systemPrompt).encode(),
                    new JsonObject().put("role", "user").put("content", userPrompt).encode()
                );
                
                LlmAPIService.getInstance().chatCompletion(
                    messages,
                    0.3, // Low temperature for factual responses
                    500
                ).whenComplete((llmResult, error) -> {
                    if (error == null) {
                        String formattedResponse = llmResult.getJsonArray("choices")
                            .getJsonObject(0)
                            .getJsonObject("message")
                            .getString("content");
                        
                        JsonObject result = new JsonObject()
                            .put("formatted", formattedResponse)
                            .put("rowCount", rows.size())
                            .put("method", "llm");
                            
                        promise.complete(result);
                    } else {
                        // Fallback to simple formatting
                        promise.complete(createSimpleFormat(originalQuery, rows, columns));
                    }
                });
                
            } catch (Exception e) {
                promise.fail(e);
            }
        }, res -> {
            if (res.succeeded()) {
                sendSuccess(ctx, requestId, res.result());
            } else {
                // Fallback to simple formatting
                formatResultsSimple(ctx, requestId, originalQuery, sqlExecuted, results, null);
            }
        });
    }
    
    private void formatResultsSimple(RoutingContext ctx, String requestId,
                                    String originalQuery, String sqlExecuted,
                                    JsonObject results, String error) {
        JsonObject response = new JsonObject();
        
        if (error != null) {
            response.put("formatted", "I encountered an error: " + error);
            response.put("method", "simple");
        } else if (results != null) {
            JsonArray rows = results.getJsonArray("rows", new JsonArray());
            JsonArray columns = results.getJsonArray("columns", new JsonArray());
            response = createSimpleFormat(originalQuery, rows, columns);
        } else {
            response.put("formatted", "No results were returned from the query.");
            response.put("method", "simple");
        }
        
        sendSuccess(ctx, requestId, response);
    }
    
    private JsonObject createSimpleFormat(String originalQuery, JsonArray rows, JsonArray columns) {
        StringBuilder formatted = new StringBuilder();
        
        if (rows.isEmpty()) {
            formatted.append("No data found for your query.");
        } else if (rows.size() == 1) {
            formatted.append("Found 1 result:\n\n");
            JsonObject row = rows.getJsonObject(0);
            for (String field : row.fieldNames()) {
                formatted.append(field).append(": ").append(row.getValue(field)).append("\n");
            }
        } else {
            formatted.append("Found ").append(rows.size()).append(" results:\n\n");
            
            // For multiple rows, create a simple table format
            if (rows.size() <= 10) {
                // Show all rows
                for (int i = 0; i < rows.size(); i++) {
                    JsonObject row = rows.getJsonObject(i);
                    formatted.append(i + 1).append(". ");
                    int fieldCount = 0;
                    for (String field : row.fieldNames()) {
                        if (fieldCount > 0) formatted.append(", ");
                        formatted.append(field).append(": ").append(row.getValue(field));
                        fieldCount++;
                        if (fieldCount >= 3) { // Limit fields shown per row
                            formatted.append("...");
                            break;
                        }
                    }
                    formatted.append("\n");
                }
            } else {
                // Show summary for large result sets
                formatted.append("(Showing first 5 results)\n\n");
                for (int i = 0; i < 5; i++) {
                    JsonObject row = rows.getJsonObject(i);
                    formatted.append(i + 1).append(". ");
                    int fieldCount = 0;
                    for (String field : row.fieldNames()) {
                        if (fieldCount > 0) formatted.append(", ");
                        formatted.append(field).append(": ").append(row.getValue(field));
                        fieldCount++;
                        if (fieldCount >= 3) break;
                    }
                    formatted.append("\n");
                }
                formatted.append("\n... and ").append(rows.size() - 5).append(" more results.");
            }
        }
        
        return new JsonObject()
            .put("formatted", formatted.toString())
            .put("rowCount", rows.size())
            .put("method", "simple");
    }
    
    /**
     * Resolve table schemas by calling the SessionSchemaResolverServer
     * @param sql Original SQL query
     * @param sessionId Session ID for schema resolution
     * @return SQL with resolved schema prefixes
     */
    private String resolveTableSchemas(String sql, String sessionId) {
        // Pattern to find table references after FROM, JOIN, INTO, UPDATE, DELETE FROM
        // This pattern handles:
        // - Table names with optional aliases
        // - Multiple tables separated by commas
        // - Different SQL keywords
        Pattern tablePattern = Pattern.compile(
            "\\b(FROM|JOIN|INTO|UPDATE|DELETE\\s+FROM)\\s+([A-Za-z_][A-Za-z0-9_]*)(?:\\s+(?:AS\\s+)?[A-Za-z_][A-Za-z0-9_]*)?(?=\\s*(?:,|\\s|JOIN|WHERE|GROUP|ORDER|HAVING|$))",
            Pattern.CASE_INSENSITIVE | Pattern.MULTILINE
        );
        
        Matcher matcher = tablePattern.matcher(sql);
        StringBuffer resolvedSql = new StringBuffer();
        
        while (matcher.find()) {
            String keyword = matcher.group(1);
            String tableName = matcher.group(2);
            
            // Check if table already has schema prefix by looking back
            int tableStartPos = matcher.start(2); // Start position of table name
            boolean hasSchema = false;
            if (tableStartPos > 0) {
                // Look for pattern like "schema." immediately before the table name
                String beforeTable = sql.substring(Math.max(0, tableStartPos - 50), tableStartPos);
                if (beforeTable.matches(".*\\b[A-Za-z_][A-Za-z0-9_]*\\.$")) {
                    hasSchema = true;
                }
            }
            
            if (!hasSchema) {
                // Call schema resolver synchronously (blocking since we're in a Worker thread)
                String resolvedSchema = resolveSchemaForTable(tableName, sessionId);
                
                if (resolvedSchema != null) {
                    // Replace with schema.table, preserving the rest of the match
                    String replacement = matcher.group().replaceFirst(
                        "(" + keyword + "\\s+)(" + tableName + ")",
                        "$1" + resolvedSchema + "." + tableName
                    );
                    matcher.appendReplacement(resolvedSql, replacement);
                    
                    vertx.eventBus().publish("log", 
                        "Resolved table " + tableName + " to " + resolvedSchema + "." + tableName + 
                        ",3,OracleQueryExecutionServer,SchemaResolution,Success");
                } else {
                    // Keep original if no schema found
                    matcher.appendReplacement(resolvedSql, matcher.group());
                }
            } else {
                // Already has schema, keep as is
                matcher.appendReplacement(resolvedSql, matcher.group());
            }
        }
        
        matcher.appendTail(resolvedSql);
        return resolvedSql.toString();
    }
    
    /**
     * Resolve schema for a table using SessionSchemaResolverServer via event bus
     */
    private String resolveSchemaForTable(String tableName, String sessionId) {
        // Since we're in a Worker thread, we can block for the response
        CompletableFuture<String> future = new CompletableFuture<>();
        
        JsonObject request = new JsonObject()
            .put("tableName", tableName)
            .put("sessionId", sessionId != null ? sessionId : "default");
        
        // Request resolution via event bus with timeout
        vertx.eventBus().<JsonObject>request(
            "session.schema.resolver.resolve",
            request,
            new DeliveryOptions().setSendTimeout(2000), // 2 second timeout
            ar -> {
                if (ar.succeeded() && ar.result().body() != null) {
                    String schema = ar.result().body().getString("schema");
                    if (schema != null) {
                        vertx.eventBus().publish("log", 
                            "Resolved schema for " + tableName + ": " + schema + 
                            ",3,OracleQueryExecutionServer,SchemaResolution,Success");
                        future.complete(schema);
                    } else {
                        // Resolver returned null, use DEFAULT_SCHEMA
                        String defaultSchema = OracleConnectionManager.getDefaultSchema();
                        vertx.eventBus().publish("log", 
                            "No schema resolved for " + tableName + ", using default: " + defaultSchema + 
                            ",3,OracleQueryExecutionServer,SchemaResolution,Default");
                        future.complete(defaultSchema);
                    }
                } else {
                    // Event bus error or timeout, fail open with DEFAULT_SCHEMA
                    String defaultSchema = OracleConnectionManager.getDefaultSchema();
                    vertx.eventBus().publish("log", 
                        "Schema resolution failed for " + tableName + ", using default: " + defaultSchema + 
                        (ar.cause() != null ? " (" + ar.cause().getMessage() + ")" : "") +
                        ",2,OracleQueryExecutionServer,SchemaResolution,Fallback");
                    future.complete(defaultSchema);
                }
            }
        );
        
        try {
            // Block and wait for result (acceptable in Worker thread)
            return future.get(3, TimeUnit.SECONDS);
        } catch (Exception e) {
            // Timeout or error, return DEFAULT_SCHEMA
            String defaultSchema = OracleConnectionManager.getDefaultSchema();
            vertx.eventBus().publish("log", 
                "Schema resolution exception for " + tableName + ", using default: " + defaultSchema + 
                ",1,OracleQueryExecutionServer,SchemaResolution,Error");
            return defaultSchema;
        }
    }
    
    @Override
    public void stop(Promise<Void> stopPromise) {
        // Connection manager handles its own lifecycle
        // Just call parent stop
        try { 
            super.stop(stopPromise); 
        } catch (Exception e) { 
            stopPromise.fail(e); 
        }
    }
    
    /**
     * Get deployment options for this server (Worker Verticle)
     */
    public static DeploymentOptions getDeploymentOptions() {
        return new DeploymentOptions()
            .setWorker(true)
            .setWorkerPoolSize(5); // Allow 5 concurrent DB operations
    }
}
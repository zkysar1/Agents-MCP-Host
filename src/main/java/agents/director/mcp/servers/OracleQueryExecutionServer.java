package agents.director.mcp.servers;

import agents.director.mcp.base.MCPServerBase;
import agents.director.mcp.base.MCPTool;
import agents.director.mcp.base.MCPResponse;
import io.vertx.core.Promise;
import io.vertx.core.Future;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.web.RoutingContext;


import java.sql.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

import agents.director.services.LlmAPIService;

/**
 * MCP Server for Oracle database query execution.
 * Provides tools for running SQL queries and retrieving schema information.
 * Deployed as a Worker Verticle due to blocking DB operations.
 */
public class OracleQueryExecutionServer extends MCPServerBase {
    
    
    
    // Oracle connection details (in production, use config file or env vars)
    private static final String ORACLE_URL = System.getenv("ORACLE_URL") != null ? 
        System.getenv("ORACLE_URL") : "jdbc:oracle:thin:@localhost:1521:XE";
    private static final String ORACLE_USER = System.getenv("ORACLE_USER") != null ? 
        System.getenv("ORACLE_USER") : "system";
    private static final String ORACLE_PASSWORD = System.getenv("ORACLE_PASSWORD") != null ? 
        System.getenv("ORACLE_PASSWORD") : "oracle";
    
    private Connection dbConnection;
    
    public OracleQueryExecutionServer() {
        super("OracleQueryExecutionServer", "/mcp/servers/oracle-db");
    }
    
    @Override
    public void start(Promise<Void> startPromise) {
        // Initialize database connection
        initializeDatabase().onComplete(ar -> {
            if (ar.succeeded()) {
                // Continue with parent initialization
                super.start(startPromise);
            } else {
                vertx.eventBus().publish("log", "Failed to initialize database connection" + ",0,OracleQueryExecutionServer,MCP,System");
                startPromise.fail(ar.cause());
            }
        });
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
                        .put("default", 100)))
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
        
        if (sql == null || sql.trim().isEmpty()) {
            sendError(ctx, requestId, MCPResponse.ErrorCodes.INVALID_PARAMS, "SQL query is required");
            return;
        }
        
        // Execute blocking DB operation
        executeBlocking(Promise.<JsonObject>promise(), promise -> {
            try {
                Statement stmt = dbConnection.createStatement();
                stmt.setMaxRows(maxRows);
                
                boolean hasResultSet = stmt.execute(sql);
                JsonObject result = new JsonObject();
                
                if (hasResultSet) {
                    ResultSet rs = stmt.getResultSet();
                    ResultSetMetaData metaData = rs.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    
                    // Get column names
                    JsonArray columns = new JsonArray();
                    for (int i = 1; i <= columnCount; i++) {
                        columns.add(new JsonObject()
                            .put("name", metaData.getColumnName(i))
                            .put("type", metaData.getColumnTypeName(i)));
                    }
                    
                    // Get rows
                    JsonArray rows = new JsonArray();
                    while (rs.next()) {
                        JsonObject row = new JsonObject();
                        for (int i = 1; i <= columnCount; i++) {
                            String columnName = metaData.getColumnName(i);
                            Object value = rs.getObject(i);
                            if (value != null) {
                                row.put(columnName, value.toString());
                            } else {
                                row.putNull(columnName);
                            }
                        }
                        rows.add(row);
                    }
                    
                    result.put("columns", columns);
                    result.put("rows", rows);
                    result.put("rowCount", rows.size());
                    
                    rs.close();
                } else {
                    // Update/Delete/Insert query
                    int updateCount = stmt.getUpdateCount();
                    result.put("updateCount", updateCount);
                    result.put("message", "Query executed successfully. Rows affected: " + updateCount);
                }
                
                stmt.close();
                promise.complete(result);
                
            } catch (SQLException e) {
                promise.fail(e);
            }
        }, res -> {
            if (res.succeeded()) {
                sendSuccess(ctx, requestId, res.result());
            } else {
                vertx.eventBus().publish("log", "Query execution failed" + ",0,OracleQueryExecutionServer,MCP,System");
                sendError(ctx, requestId, MCPResponse.ErrorCodes.INTERNAL_ERROR, 
                    "Query execution failed: " + res.cause().getMessage());
            }
        });
    }
    
    private void getSchemaInfo(RoutingContext ctx, String requestId, JsonObject arguments) {
        String schemaFilter = arguments.getString("schemaName");
        
        executeBlocking(Promise.<JsonObject>promise(), promise -> {
            try {
                JsonObject result = new JsonObject();
                JsonArray tables = new JsonArray();
                
                DatabaseMetaData metaData = dbConnection.getMetaData();
                String currentSchema = dbConnection.getSchema();
                
                // Get tables
                ResultSet tablesRs = metaData.getTables(null, 
                    schemaFilter != null ? schemaFilter : currentSchema, 
                    "%", new String[]{"TABLE"});
                
                while (tablesRs.next()) {
                    String tableName = tablesRs.getString("TABLE_NAME");
                    String tableSchema = tablesRs.getString("TABLE_SCHEM");
                    
                    JsonObject table = new JsonObject()
                        .put("name", tableName)
                        .put("schema", tableSchema);
                    
                    // Get columns for this table
                    JsonArray columns = new JsonArray();
                    ResultSet columnsRs = metaData.getColumns(null, tableSchema, tableName, "%");
                    
                    while (columnsRs.next()) {
                        JsonObject column = new JsonObject()
                            .put("name", columnsRs.getString("COLUMN_NAME"))
                            .put("type", columnsRs.getString("TYPE_NAME"))
                            .put("size", columnsRs.getInt("COLUMN_SIZE"))
                            .put("nullable", columnsRs.getString("IS_NULLABLE"));
                        columns.add(column);
                    }
                    columnsRs.close();
                    
                    table.put("columns", columns);
                    tables.add(table);
                }
                tablesRs.close();
                
                result.put("tables", tables);
                result.put("tableCount", tables.size());
                promise.complete(result);
                
            } catch (SQLException e) {
                promise.fail(e);
            }
        }, res -> {
            if (res.succeeded()) {
                sendSuccess(ctx, requestId, res.result());
            } else {
                vertx.eventBus().publish("log", "Schema retrieval failed" + ",0,OracleQueryExecutionServer,MCP,System");
                sendError(ctx, requestId, MCPResponse.ErrorCodes.INTERNAL_ERROR, 
                    "Schema retrieval failed: " + res.cause().getMessage());
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
    
    private Future<Void> initializeDatabase() {
        Promise<Void> promise = Promise.<Void>promise();
        
        vertx.executeBlocking(blockingPromise -> {
            try {
                // Load Oracle driver
                Class.forName("oracle.jdbc.driver.OracleDriver");
                
                // Create connection
                dbConnection = DriverManager.getConnection(ORACLE_URL, ORACLE_USER, ORACLE_PASSWORD);
                dbConnection.setAutoCommit(false); // Use transactions
                
                vertx.eventBus().publish("log", "Oracle database connection established,2,OracleQueryExecutionServer,MCP,System");
                blockingPromise.complete();
                
            } catch (Exception e) {
                vertx.eventBus().publish("log", "Failed to connect to Oracle database" + ",0,OracleQueryExecutionServer,MCP,System");
                blockingPromise.fail(e);
            }
        }, false, res -> {
            if (res.succeeded()) {
                promise.complete();
            } else {
                promise.fail(res.cause());
            }
        });
        
        return promise.future();
    }
    
    @Override
    public void stop(Promise<Void> stopPromise) {
        if (dbConnection != null) {
            vertx.executeBlocking(promise -> {
                try {
                    dbConnection.close();
                    vertx.eventBus().publish("log", "Database connection closed,2,OracleQueryExecutionServer,MCP,System");
                    promise.complete();
                } catch (SQLException e) {
                    vertx.eventBus().publish("log", "Failed to close database connection" + ",0,OracleQueryExecutionServer,MCP,System");
                    promise.fail(e);
                }
            }, res -> {
                try { super.stop(stopPromise); } catch (Exception e) { stopPromise.fail(e); }
            });
        } else {
            try { super.stop(stopPromise); } catch (Exception e) { stopPromise.fail(e); }
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
package agents.director.hosts.milestones;

import agents.director.hosts.base.MilestoneContext;
import agents.director.hosts.base.MilestoneManager;
import agents.director.mcp.clients.OracleQueryExecutionClient;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import java.util.*;

/**
 * Milestone 5: SQL Execution
 * 
 * Executes the SQL statement and retrieves results from the database.
 * Uses OracleQueryExecutionServer by deploying MCP clients directly.
 * 
 * Output shared with user: Table of results in markdown format
 */
public class ExecutionMilestone extends MilestoneManager {
    
    private static final String EXECUTION_CLIENT = "execution";
    private final Map<String, String> deploymentIds = new HashMap<>();
    
    public ExecutionMilestone(Vertx vertx, String baseUrl) {
        super(vertx, baseUrl, 5, "ExecutionMilestone", 
              "Execute SQL query and retrieve results");
    }
    
    @Override
    public Future<Void> initialize() {
        Promise<Void> promise = Promise.promise();
        
        // Deploy the MCP clients directly
        OracleQueryExecutionClient executionClient = new OracleQueryExecutionClient(baseUrl);
        
        deployClient(EXECUTION_CLIENT, executionClient)
            .onSuccess(v -> {
                log("Execution milestone initialized successfully", 2);
                promise.complete();
            })
            .onFailure(err -> {
                log("Failed to initialize execution milestone: " + err.getMessage(), 0);
                promise.fail(err);
            });
        
        return promise.future();
    }
    
    @Override
    public Future<MilestoneContext> execute(MilestoneContext context) {
        Promise<MilestoneContext> promise = Promise.promise();
        
        String sql = context.getGeneratedSql();
        
        // Check if we have SQL to execute
        if (sql == null || sql.trim().isEmpty()) {
            log("No SQL to execute, skipping execution milestone", 2);
            context.setQueryResults(new JsonArray());
            context.setRowCount(0);
            context.completeMilestone(5);
            promise.complete(context);
            return promise.future();
        }
        
        log("Starting SQL execution: " + sql, 3);
        
        // Publish progress event at start
        if (context.isStreaming() && context.getSessionId() != null) {
            publishProgressEvent(context.getConversationId(),
                "Executing Query",
                "Running SQL query against database...",
                new JsonObject()
                    .put("phase", "sql_execution")
                    .put("query", sql));
        }
        
        // Execute the SQL using clients directly
        long startTime = System.currentTimeMillis();
        
        executeQuery(sql)
            .onSuccess(result -> {
                long executionTime = System.currentTimeMillis() - startTime;
                
                // Extract results
                JsonArray rows = result.getJsonArray("rows");
                JsonArray columns = result.getJsonArray("columns");
                int rowCount = result.getInteger("row_count", rows != null ? rows.size() : 0);
                JsonObject metadata = result.getJsonObject("metadata");
                
                // Update context with execution results
                context.setQueryResults(rows);
                context.setRowCount(rowCount);
                context.setExecutionTime(executionTime);
                context.setExecutionMetadata(new JsonObject()
                    .put("columns", columns)
                    .put("execution_time_ms", executionTime)
                    .put("row_count", rowCount)
                    .put("metadata", metadata)
                    .put("success", true));
                
                // Mark milestone as complete
                context.completeMilestone(5);
                
                // Publish SQL result event
                if (context.isStreaming() && context.getSessionId() != null) {
                    // First publish the SQL result progress event
                    publishProgressEvent(context.getConversationId(),
                        "Query Complete",
                        "Retrieved " + rowCount + " rows in " + executionTime + "ms",
                        new JsonObject()
                            .put("phase", "sql_result")
                            .put("rowCount", rowCount)
                            .put("executionTime", executionTime)
                            .put("preview", getResultPreview(rows)));
                    
                    // Then publish milestone complete
                    publishStreamingEvent(context.getConversationId(), "milestone.execution_complete",
                        getShareableResult(context));
                }
                
                log("SQL execution complete: " + rowCount + " rows in " + executionTime + "ms", 2);
                promise.complete(context);
            })
            .onFailure(err -> {
                log("SQL execution failed: " + err.getMessage(), 0);
                
                // Store error information
                context.setQueryResults(new JsonArray());
                context.setRowCount(0);
                context.setExecutionTime(System.currentTimeMillis() - startTime);
                context.setExecutionMetadata(new JsonObject()
                    .put("success", false)
                    .put("error", err.getMessage())
                    .put("sql", sql));
                
                context.completeMilestone(5);
                
                // Still complete the milestone but with error status
                promise.complete(context);
            });
        
        return promise.future();
    }
    
    @Override
    public JsonObject getShareableResult(MilestoneContext context) {
        JsonObject metadata = context.getExecutionMetadata();
        boolean success = metadata != null && metadata.getBoolean("success", false);
        
        JsonObject result = new JsonObject()
            .put("milestone", 5)
            .put("milestone_name", "SQL Execution")
            .put("success", success)
            .put("row_count", context.getRowCount())
            .put("execution_time_ms", context.getExecutionTime());
        
        if (success) {
            // Format results as markdown table
            String markdownTable = formatResultsAsMarkdown(context);
            result.put("results_markdown", markdownTable);
            result.put("message", "Query executed successfully: " + context.getRowCount() + " row(s) returned");
            
            // Include sample data (first 5 rows)
            if (context.getQueryResults() != null && !context.getQueryResults().isEmpty()) {
                JsonArray sampleData = new JsonArray();
                int sampleSize = Math.min(5, context.getQueryResults().size());
                for (int i = 0; i < sampleSize; i++) {
                    sampleData.add(context.getQueryResults().getValue(i));
                }
                result.put("sample_data", sampleData);
            }
        } else {
            String error = metadata != null ? metadata.getString("error", "Unknown error") : "Execution failed";
            result.put("error", error);
            result.put("message", "Query execution failed: " + error);
        }
        
        return result;
    }
    
    /**
     * Format query results as a markdown table
     */
    private String formatResultsAsMarkdown(MilestoneContext context) {
        JsonArray results = context.getQueryResults();
        JsonObject metadata = context.getExecutionMetadata();
        
        if (results == null || results.isEmpty()) {
            return "*No results returned*";
        }
        
        // Get column names
        JsonArray columns = metadata != null ? metadata.getJsonArray("columns") : null;
        if (columns == null || columns.isEmpty()) {
            // Try to get columns from first row
            if (results.getJsonObject(0) != null) {
                columns = new JsonArray(new ArrayList<>(results.getJsonObject(0).fieldNames()));
            } else {
                return "*Unable to format results*";
            }
        }
        
        StringBuilder markdown = new StringBuilder();
        
        // Build header row
        markdown.append("| ");
        for (int i = 0; i < columns.size(); i++) {
            String colName = columns.getValue(i) instanceof JsonObject ? 
                            columns.getJsonObject(i).getString("name") : 
                            columns.getString(i);
            markdown.append(colName).append(" | ");
        }
        markdown.append("\n");
        
        // Build separator row
        markdown.append("| ");
        for (int i = 0; i < columns.size(); i++) {
            markdown.append("--- | ");
        }
        markdown.append("\n");
        
        // Build data rows (limit to 10 for display)
        int maxRows = Math.min(10, results.size());
        for (int i = 0; i < maxRows; i++) {
            JsonObject row = results.getJsonObject(i);
            if (row != null) {
                markdown.append("| ");
                for (int j = 0; j < columns.size(); j++) {
                    String colName = columns.getValue(j) instanceof JsonObject ? 
                                    columns.getJsonObject(j).getString("name") : 
                                    columns.getString(j);
                    Object value = row.getValue(colName);
                    markdown.append(value != null ? value.toString() : "null").append(" | ");
                }
                markdown.append("\n");
            }
        }
        
        // Add note if there are more rows
        if (results.size() > maxRows) {
            markdown.append("\n*... and ").append(results.size() - maxRows).append(" more rows*\n");
        }
        
        return markdown.toString();
    }
    
    /**
     * Execute a query (replaces manager method)
     */
    private Future<JsonObject> executeQuery(String sql) {
        return callTool(EXECUTION_CLIENT, "run_oracle_query",
            new JsonObject()
                .put("sql", sql)
                .put("max_rows", 1000));
    }
    
    /**
     * Deploy a clients and track it
     */
    private Future<String> deployClient(String clientName, AbstractVerticle client) {
        Promise<String> promise = Promise.promise();
        
        vertx.deployVerticle(client, ar -> {
            if (ar.succeeded()) {
                String deploymentId = ar.result();
                deploymentIds.put(clientName, deploymentId);
                log("Deployed " + clientName + " clients", 3);
                promise.complete(deploymentId);
            } else {
                promise.fail(ar.cause());
            }
        });
        
        return promise.future();
    }
    
    /**
     * Call a tool on a deployed clients
     */
    private Future<JsonObject> callTool(String clientName, String toolName, JsonObject params) {
        Promise<JsonObject> promise = Promise.promise();
        
        // Map clients names to normalized server names
        String serverName;
        switch (clientName) {
            case EXECUTION_CLIENT:
                serverName = "oraclequeryexecution";
                break;
            default:
                serverName = clientName.toLowerCase();
        }
        
        String address = "mcp.clients." + serverName + "." + toolName;
        vertx.eventBus().<JsonObject>request(address, params, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result().body());
            } else {
                promise.fail(ar.cause());
            }
        });
        
        return promise.future();
    }
    
    /**
     * Get preview of query results (first 5 rows)
     */
    private JsonArray getResultPreview(JsonArray results) {
        if (results == null) return new JsonArray();
        
        JsonArray preview = new JsonArray();
        int limit = Math.min(results.size(), 5);
        
        for (int i = 0; i < limit; i++) {
            // Use getValue to avoid ClassCastException if not JsonObject
            Object value = results.getValue(i);
            if (value instanceof JsonObject) {
                preview.add((JsonObject) value);
            } else {
                // If not JsonObject, wrap it
                preview.add(new JsonObject().put("value", value));
            }
        }
        
        return preview;
    }
    
    @Override
    public Future<Void> cleanup() {
        List<Future> undeployFutures = new ArrayList<>();
        
        // Undeploy all clients
        for (Map.Entry<String, String> entry : deploymentIds.entrySet()) {
            Promise<Void> promise = Promise.promise();
            vertx.undeploy(entry.getValue(), ar -> {
                if (ar.succeeded()) {
                    log("Undeployed " + entry.getKey() + " clients", 3);
                    promise.complete();
                } else {
                    promise.fail(ar.cause());
                }
            });
            undeployFutures.add(promise.future());
        }
        
        // Also call parent cleanup
        undeployFutures.add(super.cleanup());
        
        return CompositeFuture.all(undeployFutures).mapEmpty();
    }
}
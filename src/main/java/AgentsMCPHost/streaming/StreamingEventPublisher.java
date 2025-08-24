package AgentsMCPHost.streaming;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;

/**
 * Centralized utility for publishing streaming events with consistent format.
 * Provides detailed streaming for LLM interactions, SQL operations, and tool executions.
 */
public class StreamingEventPublisher {
    private final Vertx vertx;
    private final String streamId;
    private final long startTime;
    
    public StreamingEventPublisher(Vertx vertx, String streamId) {
        this.vertx = vertx;
        this.streamId = streamId;
        this.startTime = System.currentTimeMillis();
    }
    
    /**
     * Publish LLM request event
     */
    public void publishLLMRequest(JsonArray messages) {
        JsonObject event = new JsonObject()
            .put("phase", "llm_request")
            .put("messages", messages)
            .put("messageCount", messages.size())
            .put("elapsed", getElapsed());
        
        publishProgress("Sending request to LLM", "Messages prepared: " + messages.size(), event);
    }
    
    /**
     * Publish LLM response event
     */
    public void publishLLMResponse(String response, JsonObject metadata) {
        JsonObject event = new JsonObject()
            .put("phase", "llm_response")
            .put("response", response)
            .put("responseLength", response.length())
            .put("metadata", metadata)
            .put("elapsed", getElapsed());
        
        publishProgress("Received LLM response", "Response length: " + response.length() + " characters", event);
    }
    
    /**
     * Publish SQL query event
     */
    public void publishSQLQuery(String sql, JsonObject context) {
        JsonObject event = new JsonObject()
            .put("phase", "sql_query")
            .put("query", sql)
            .put("tables", extractTablesFromSQL(sql))
            .put("queryType", detectQueryType(sql))
            .put("context", context)
            .put("elapsed", getElapsed());
        
        publishProgress("Executing SQL query", sql.length() > 100 ? sql.substring(0, 100) + "..." : sql, event);
    }
    
    /**
     * Publish SQL result event
     */
    public void publishSQLResult(JsonArray results, long executionTime) {
        JsonObject event = new JsonObject()
            .put("phase", "sql_result")
            .put("rowCount", results.size())
            .put("executionTime", executionTime)
            .put("preview", getResultPreview(results))
            .put("elapsed", getElapsed());
        
        publishProgress("SQL query completed", "Retrieved " + results.size() + " rows in " + executionTime + "ms", event);
    }
    
    /**
     * Publish metadata exploration event
     */
    public void publishMetadataExploration(String tableName, JsonObject metadata) {
        JsonObject event = new JsonObject()
            .put("phase", "metadata_exploration")
            .put("table", tableName)
            .put("columnCount", metadata.getJsonArray("columns", new JsonArray()).size())
            .put("metadata", metadata)
            .put("elapsed", getElapsed());
        
        publishProgress("Exploring table metadata", "Table: " + tableName, event);
    }
    
    /**
     * Publish schema matching event
     */
    public void publishSchemaMatching(String userQuery, JsonArray matchedTables) {
        JsonObject event = new JsonObject()
            .put("phase", "schema_matching")
            .put("userQuery", userQuery)
            .put("matchedTables", matchedTables)
            .put("matchCount", matchedTables.size())
            .put("elapsed", getElapsed());
        
        publishProgress("Matching query to schema", "Found " + matchedTables.size() + " relevant tables", event);
    }
    
    /**
     * Publish enumeration mapping event
     */
    public void publishEnumerationMapping(String column, JsonObject mappings) {
        JsonObject event = new JsonObject()
            .put("phase", "enum_mapping")
            .put("column", column)
            .put("mappings", mappings)
            .put("mappingCount", mappings.size())
            .put("elapsed", getElapsed());
        
        publishProgress("Mapping enumeration values", "Column: " + column, event);
    }
    
    /**
     * Publish tool selection event
     */
    public void publishToolSelection(String strategy, JsonArray selectedTools) {
        JsonObject event = new JsonObject()
            .put("phase", "tool_selection")
            .put("strategy", strategy)
            .put("selectedTools", selectedTools)
            .put("toolCount", selectedTools.size())
            .put("elapsed", getElapsed());
        
        publishProgress("Tool selection completed", "Strategy: " + strategy + ", Tools: " + selectedTools.size(), event);
    }
    
    /**
     * Publish general progress event
     */
    public void publishProgress(String step, String message, JsonObject details) {
        JsonObject progressEvent = new JsonObject()
            .put("step", step)
            .put("message", message)
            .put("details", details)
            .put("elapsed", getElapsed());
        
        vertx.eventBus().publish("conversation." + streamId + ".progress", progressEvent);
    }
    
    /**
     * Publish event with custom type
     */
    private void publishEvent(String eventType, JsonObject data) {
        vertx.eventBus().publish("conversation." + streamId + "." + eventType, data);
    }
    
    /**
     * Publish interrupt check event
     */
    public void publishInterruptCheck(String operation, boolean interrupted) {
        if (interrupted) {
            JsonObject event = new JsonObject()
                .put("phase", "interrupt_detected")
                .put("operation", operation)
                .put("message", "Operation interrupted by user")
                .put("elapsed", getElapsed());
            
            publishProgress("Interrupt detected", "Pausing " + operation, event);
        }
    }
    
    /**
     * Get elapsed time since start
     */
    private long getElapsed() {
        return System.currentTimeMillis() - startTime;
    }
    
    /**
     * Extract table names from SQL query (simple implementation)
     */
    private JsonArray extractTablesFromSQL(String sql) {
        JsonArray tables = new JsonArray();
        String upperSQL = sql.toUpperCase();
        
        // Simple extraction - find words after FROM, JOIN
        String[] keywords = {"FROM", "JOIN", "INTO", "UPDATE", "TABLE"};
        for (String keyword : keywords) {
            int index = upperSQL.indexOf(keyword);
            if (index != -1) {
                // Extract next word (simplified)
                String afterKeyword = sql.substring(index + keyword.length()).trim();
                String[] parts = afterKeyword.split("\\s+");
                if (parts.length > 0) {
                    String tableName = parts[0].replaceAll("[^a-zA-Z0-9_]", "");
                    if (!tableName.isEmpty() && !tables.contains(tableName)) {
                        tables.add(tableName);
                    }
                }
            }
        }
        
        return tables;
    }
    
    /**
     * Detect SQL query type
     */
    private String detectQueryType(String sql) {
        String upperSQL = sql.trim().toUpperCase();
        if (upperSQL.startsWith("SELECT")) return "SELECT";
        if (upperSQL.startsWith("INSERT")) return "INSERT";
        if (upperSQL.startsWith("UPDATE")) return "UPDATE";
        if (upperSQL.startsWith("DELETE")) return "DELETE";
        if (upperSQL.startsWith("CREATE")) return "CREATE";
        if (upperSQL.startsWith("DROP")) return "DROP";
        if (upperSQL.startsWith("ALTER")) return "ALTER";
        return "OTHER";
    }
    
    /**
     * Get preview of query results (first 5 rows)
     */
    private JsonArray getResultPreview(JsonArray results) {
        JsonArray preview = new JsonArray();
        int limit = Math.min(results.size(), 5);
        
        for (int i = 0; i < limit; i++) {
            preview.add(results.getJsonObject(i));
        }
        
        return preview;
    }
    
    /**
     * Publish execution paused event
     */
    public void publishExecutionPaused(String reason) {
        publishEvent("execution_paused", new JsonObject()
            .put("timestamp", System.currentTimeMillis())
            .put("reason", reason)
            .put("message", "Execution paused. Waiting for user input...")
            .put("canResume", true));
    }
    
    /**
     * Publish agent question event
     */
    public void publishAgentQuestion(String question, JsonArray options) {
        publishEvent("agent_question", new JsonObject()
            .put("timestamp", System.currentTimeMillis())
            .put("question", question)
            .put("options", options != null ? options : new JsonArray())
            .put("requiresResponse", true));
    }
}
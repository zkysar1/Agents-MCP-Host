package agents.director.hosts.base.managers;

import agents.director.mcp.client.OracleQueryExecutionClient;
import agents.director.mcp.client.SessionSchemaResolverClient;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;

import java.util.Arrays;
import java.util.List;

/**
 * Manager for Oracle database execution clients.
 * Coordinates between OracleQueryExecutionClient and SessionSchemaResolverClient
 * to provide enhanced query execution with session context.
 */
public class OracleExecutionManager extends MCPClientManager {
    
    private static final String EXECUTION_CLIENT = "execution";
    private static final String SESSION_CLIENT = "session";
    
    public OracleExecutionManager(Vertx vertx, String baseUrl) {
        super(vertx, baseUrl);
    }
    
    @Override
    public Future<Void> initialize() {
        List<Future> deploymentFutures = Arrays.asList(
            deployClient(EXECUTION_CLIENT, new OracleQueryExecutionClient(baseUrl)),
            deployClient(SESSION_CLIENT, new SessionSchemaResolverClient(baseUrl))
        );
        
        return CompositeFuture.all(deploymentFutures).mapEmpty();
    }
    
    /**
     * Execute a query with session-specific schema resolution
     * @param sql The SQL query to execute
     * @param sessionId The session ID for context
     * @return Future with query results
     */
    public Future<JsonObject> executeWithSessionContext(String sql, String sessionId) {
        // Validate inputs
        if (sql == null || sql.trim().isEmpty()) {
            return Future.failedFuture("SQL query cannot be null or empty");
        }
        if (sessionId == null || sessionId.trim().isEmpty()) {
            return Future.failedFuture("Session ID cannot be null or empty");
        }
        
        // First resolve session-specific schema
        return callClientTool(SESSION_CLIENT, "resolve_table_schema",
            new JsonObject()
                .put("sessionId", sessionId)
                .put("sql", sql))
            .compose(schemaInfo -> {
                // Validate schema resolution result
                if (schemaInfo == null || schemaInfo.isEmpty()) {
                    // Log warning but continue with execution without schema context
                    vertx.eventBus().publish("log", 
                        "Warning: Schema resolution returned empty result for session " + sessionId + 
                        ",1,OracleExecutionManager,Manager,System");
                }
                
                // Execute the query with the resolved schema context (or empty if resolution failed)
                JsonObject executionArgs = new JsonObject()
                    .put("sql", sql)
                    .put("sessionId", sessionId);
                
                if (schemaInfo != null && !schemaInfo.isEmpty()) {
                    executionArgs.put("schemaContext", schemaInfo);
                }
                
                return callClientTool(EXECUTION_CLIENT, "run_oracle_query", executionArgs);
            });
    }
    
    /**
     * Get enriched schema information for a session
     * @param sessionId The session ID
     * @return Future with enriched schema information
     */
    public Future<JsonObject> getEnrichedSchema(String sessionId) {
        // Get base schema
        Future<JsonObject> schemaFuture = callClientTool(EXECUTION_CLIENT, "get_oracle_schema",
            new JsonObject().put("sessionId", sessionId));
        
        // Get session patterns
        Future<JsonObject> patternsFuture = callClientTool(SESSION_CLIENT, "get_session_patterns",
            new JsonObject().put("sessionId", sessionId));
        
        // Get available schemas
        Future<JsonObject> availableSchemasFuture = callClientTool(SESSION_CLIENT, "discover_available_schemas",
            new JsonObject().put("sessionId", sessionId));
        
        // Combine all results
        return CompositeFuture.all(schemaFuture, patternsFuture, availableSchemasFuture)
            .map(composite -> {
                JsonObject schema = composite.resultAt(0);
                JsonObject patterns = composite.resultAt(1);
                JsonObject available = composite.resultAt(2);
                
                return new JsonObject()
                    .put("baseSchema", schema)
                    .put("sessionPatterns", patterns)
                    .put("availableSchemas", available);
            });
    }
    
    /**
     * Format query results using natural language
     * @param results The query results to format
     * @param format The desired output format
     * @return Future with formatted results
     */
    public Future<JsonObject> formatQueryResults(JsonObject results, String format) {
        return callClientTool(EXECUTION_CLIENT, "format_results",
            new JsonObject()
                .put("results", results)
                .put("format", format != null ? format : "narrative"));
    }
    
    /**
     * Discover sample data with column semantics
     * @param tableName The table to analyze
     * @param sessionId The session context
     * @return Future with sample data and semantics
     */
    public Future<JsonObject> discoverTableInsights(String tableName, String sessionId) {
        // Get column semantics
        Future<JsonObject> semanticsFuture = callClientTool(SESSION_CLIENT, "discover_column_semantics",
            new JsonObject()
                .put("tableName", tableName)
                .put("sessionId", sessionId));
        
        // Get sample data
        Future<JsonObject> sampleDataFuture = callClientTool(SESSION_CLIENT, "discover_sample_data",
            new JsonObject()
                .put("tableName", tableName)
                .put("sessionId", sessionId));
        
        return CompositeFuture.all(semanticsFuture, sampleDataFuture)
            .map(composite -> new JsonObject()
                .put("semantics", composite.resultAt(0))
                .put("sampleData", composite.resultAt(1)));
    }
}
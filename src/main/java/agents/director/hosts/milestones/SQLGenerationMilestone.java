package agents.director.hosts.milestones;

import agents.director.hosts.base.MilestoneContext;
import agents.director.hosts.base.MilestoneManager;
import agents.director.mcp.client.OracleSQLGenerationClient;
import agents.director.mcp.client.OracleSQLValidationClient;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import java.util.*;

/**
 * Milestone 4: SQL Generation
 * 
 * Creates SQL statement based on all information gathered from previous milestones.
 * Uses OracleSQLGenerationServer and OracleSQLValidationServer by deploying MCP clients directly.
 * 
 * Output shared with user: The generated SQL statement
 */
public class SQLGenerationMilestone extends MilestoneManager {
    
    private static final String GENERATION_CLIENT = "generation";
    private static final String VALIDATION_CLIENT = "validation";
    private final Map<String, String> deploymentIds = new HashMap<>();
    
    public SQLGenerationMilestone(Vertx vertx, String baseUrl) {
        super(vertx, baseUrl, 4, "SQLGenerationMilestone", 
              "Create SQL statement based on gathered information");
    }
    
    @Override
    public Future<Void> initialize() {
        Promise<Void> promise = Promise.promise();
        
        // Deploy the MCP clients directly
        List<Future> deploymentFutures = new ArrayList<>();
        
        OracleSQLGenerationClient generationClient = new OracleSQLGenerationClient(baseUrl);
        OracleSQLValidationClient validationClient = new OracleSQLValidationClient(baseUrl);
        
        deploymentFutures.add(deployClient(GENERATION_CLIENT, generationClient));
        deploymentFutures.add(deployClient(VALIDATION_CLIENT, validationClient));
        
        CompositeFuture.all(deploymentFutures)
            .onSuccess(v -> {
                log("SQL generation milestone initialized successfully", 2);
                promise.complete();
            })
            .onFailure(err -> {
                log("Failed to initialize SQL generation milestone: " + err.getMessage(), 0);
                promise.fail(err);
            });
        
        return promise.future();
    }
    
    @Override
    public Future<MilestoneContext> execute(MilestoneContext context) {
        Promise<MilestoneContext> promise = Promise.promise();
        
        log("Starting SQL generation for intent: " + context.getIntent(), 3);
        
        // Build SQL generation context from previous milestones
        JsonObject sqlContext = buildSQLContext(context);
        
        // Execute full SQL pipeline (generation + validation) using clients directly
        fullSQLPipeline(context.getQuery(), sqlContext)
            .onSuccess(result -> {
                // Extract the generated SQL
                String generatedSql = result.getString("sql", "");
                String explanation = result.getString("explanation", "");
                JsonObject validation = result.getJsonObject("validation");
                JsonObject optimization = result.getJsonObject("optimization");
                
                // Update context with SQL information
                context.setGeneratedSql(generatedSql);
                context.setSqlExplanation(explanation);
                context.setSqlMetadata(new JsonObject()
                    .put("validation", validation)
                    .put("optimization", optimization)
                    .put("is_valid", validation.getBoolean("is_valid", true))
                    .put("is_optimized", optimization.getBoolean("optimized", false))
                    .put("confidence", result.getDouble("confidence", 0.8)));
                
                // Mark milestone as complete
                context.completeMilestone(4);
                
                // Publish streaming event if applicable
                if (context.isStreaming() && context.getSessionId() != null) {
                    publishStreamingEvent(context.getConversationId(), "milestone.sql_complete",
                        getShareableResult(context));
                }
                
                log("SQL generation complete: " + generatedSql, 2);
                promise.complete(context);
            })
            .onFailure(err -> {
                log("SQL generation failed: " + err.getMessage(), 0);
                
                // Fallback: Generate simple SQL based on intent
                String fallbackSql = generateFallbackSQL(context);
                context.setGeneratedSql(fallbackSql);
                context.setSqlExplanation("Basic SQL generated based on your query");
                context.setSqlMetadata(new JsonObject()
                    .put("fallback", true)
                    .put("error", err.getMessage()));
                
                context.completeMilestone(4);
                promise.complete(context);
            });
        
        return promise.future();
    }
    
    @Override
    public JsonObject getShareableResult(MilestoneContext context) {
        boolean isValid = false;
        boolean isOptimized = false;
        
        if (context.getSqlMetadata() != null) {
            JsonObject validation = context.getSqlMetadata().getJsonObject("validation");
            if (validation != null) {
                isValid = validation.getBoolean("is_valid", false);
            }
            JsonObject optimization = context.getSqlMetadata().getJsonObject("optimization");
            if (optimization != null) {
                isOptimized = optimization.getBoolean("optimized", false);
            }
        }
        
        String status = "Generated";
        if (isValid) status += " and validated";
        if (isOptimized) status += " (optimized)";
        
        return new JsonObject()
            .put("milestone", 4)
            .put("milestone_name", "SQL Generation")
            .put("sql", context.getGeneratedSql())
            .put("explanation", context.getSqlExplanation())
            .put("status", status)
            .put("is_valid", isValid)
            .put("is_optimized", isOptimized)
            .put("message", status + " SQL statement for your query");
    }
    
    /**
     * Build comprehensive SQL context from all previous milestones
     */
    private JsonObject buildSQLContext(MilestoneContext context) {
        JsonObject sqlContext = new JsonObject()
            .put("intent", context.getIntent())
            .put("intent_type", context.getIntentType())
            .put("tables", new JsonArray(context.getRelevantTables()));
        
        // Add table descriptions
        JsonObject tableInfo = new JsonObject();
        for (String table : context.getRelevantTables()) {
            JsonObject info = new JsonObject();
            
            // Add description if available
            String description = context.getTableDescriptions().get(table);
            if (description != null) {
                info.put("description", description);
            }
            
            // Add columns if available
            List<String> columns = context.getTableColumns().get(table);
            if (columns != null && !columns.isEmpty()) {
                info.put("columns", new JsonArray(columns));
            }
            
            tableInfo.put(table, info);
        }
        sqlContext.put("table_info", tableInfo);
        
        // Add column statistics if available
        if (!context.getColumnStats().isEmpty()) {
            JsonObject columnStats = new JsonObject();
            for (Map.Entry<String, JsonObject> entry : context.getColumnStats().entrySet()) {
                columnStats.put(entry.getKey(), entry.getValue());
            }
            sqlContext.put("column_stats", columnStats);
        }
        
        // Add any schema details
        if (context.getSchemaDetails() != null) {
            sqlContext.put("schema_details", context.getSchemaDetails());
        }
        
        // Add backstory and guidance for context
        sqlContext.put("backstory", context.getBackstory());
        sqlContext.put("guidance", context.getGuidance());
        
        return sqlContext;
    }
    
    /**
     * Generate fallback SQL when the pipeline fails
     */
    private String generateFallbackSQL(MilestoneContext context) {
        if (context.getRelevantTables().isEmpty()) {
            return "-- Unable to generate SQL: No tables identified\n" +
                   "-- Please specify the table names in your query";
        }
        
        String primaryTable = context.getRelevantTables().get(0);
        String intentType = context.getIntentType();
        
        // Generate basic SQL based on intent type
        switch (intentType != null ? intentType.toLowerCase() : "query") {
            case "count":
            case "aggregation":
                return String.format("SELECT COUNT(*) AS total_count\nFROM %s", primaryTable);
                
            case "sum":
                return String.format("SELECT SUM(amount) AS total_amount\nFROM %s", primaryTable);
                
            case "average":
                return String.format("SELECT AVG(value) AS average_value\nFROM %s", primaryTable);
                
            case "list":
            case "show":
                List<String> columns = context.getTableColumns().get(primaryTable);
                if (columns != null && !columns.isEmpty()) {
                    String columnList = String.join(", ", columns.subList(0, Math.min(5, columns.size())));
                    return String.format("SELECT %s\nFROM %s\nWHERE ROWNUM <= 100", columnList, primaryTable);
                } else {
                    return String.format("SELECT *\nFROM %s\nWHERE ROWNUM <= 100", primaryTable);
                }
                
            default:
                return String.format("SELECT *\nFROM %s\nWHERE ROWNUM <= 10", primaryTable);
        }
    }
    
    /**
     * Execute full SQL pipeline (replaces manager method)
     */
    private Future<JsonObject> fullSQLPipeline(String query, JsonObject sqlContext) {
        // Build proper analysis object from context
        JsonObject analysis = new JsonObject()
            .put("intent", sqlContext.getString("intent", "retrieve data"))
            .put("queryType", determineQueryType(sqlContext))
            .put("entities", extractEntities(sqlContext))
            .put("aggregations", extractAggregations(query, sqlContext))
            .put("timeframe", extractTimeframe(query));
        
        // Build schemaMatches from context
        JsonObject schemaMatches = buildSchemaMatches(sqlContext);
        
        // Generate SQL with proper parameters
        Future<JsonObject> generationFuture = callTool(GENERATION_CLIENT, "generate_oracle_sql",
            new JsonObject()
                .put("analysis", analysis)
                .put("schemaMatches", schemaMatches)
                .put("includeEnums", true));
        
        // Validate the generated SQL
        Future<JsonObject> validationFuture = generationFuture
            .compose(generated -> {
                String sql = generated.getString("sql", "");
                return callTool(VALIDATION_CLIENT, "validate_oracle_sql",
                    new JsonObject()
                        .put("sql", sql)
                        .put("context", sqlContext))
                    .map(validation -> {
                        // Combine generation and validation results
                        generated.put("validation", validation);
                        return generated;
                    });
            });
        
        // Optimize if needed
        return validationFuture.compose(result -> {
            JsonObject validation = result.getJsonObject("validation");
            boolean isValid = validation != null ? validation.getBoolean("is_valid", true) : true;
            
            if (isValid) {
                // Try to optimize
                String sql = result.getString("sql", "");
                return callTool(GENERATION_CLIENT, "optimize_oracle_sql",
                    new JsonObject()
                        .put("sql", sql)
                        .put("context", sqlContext))
                    .map(optimization -> {
                        result.put("optimization", optimization);
                        // Use optimized SQL if available
                        String optimizedSql = optimization.getString("optimized_sql");
                        if (optimizedSql != null && !optimizedSql.isEmpty()) {
                            result.put("sql", optimizedSql);
                            result.put("is_optimized", true);
                        }
                        return result;
                    })
                    .otherwise(t -> {
                        // If optimization fails, just return the original result
                        result.put("optimization", new JsonObject().put("error", t.getMessage()));
                        return result;
                    });
            } else {
                // Don't optimize invalid SQL
                return Future.succeededFuture(result);
            }
        });
    }
    
    /**
     * Deploy a client and track it
     */
    private Future<String> deployClient(String clientName, AbstractVerticle client) {
        Promise<String> promise = Promise.promise();
        
        vertx.deployVerticle(client, ar -> {
            if (ar.succeeded()) {
                String deploymentId = ar.result();
                deploymentIds.put(clientName, deploymentId);
                log("Deployed " + clientName + " client", 3);
                promise.complete(deploymentId);
            } else {
                promise.fail(ar.cause());
            }
        });
        
        return promise.future();
    }
    
    /**
     * Call a tool on a deployed client
     */
    private Future<JsonObject> callTool(String clientName, String toolName, JsonObject params) {
        Promise<JsonObject> promise = Promise.promise();
        
        // Map client names to normalized server names
        String serverName;
        switch (clientName) {
            case GENERATION_CLIENT:
                serverName = "oraclesqlgeneration";
                break;
            case VALIDATION_CLIENT:
                serverName = "oraclesqlvalidation";
                break;
            default:
                serverName = clientName.toLowerCase();
        }
        
        String address = "mcp.client." + serverName + "." + toolName;
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
     * Determine query type from context
     */
    private String determineQueryType(JsonObject context) {
        String intentType = context.getString("intent_type", "").toLowerCase();
        
        if (intentType.contains("count") || intentType.contains("aggregation")) {
            return "aggregation";
        } else if (intentType.contains("list") || intentType.contains("show")) {
            return "retrieval";
        } else if (intentType.contains("sum") || intentType.contains("average")) {
            return "aggregation";
        }
        
        return "retrieval";  // Default
    }
    
    /**
     * Extract entities from context
     */
    private JsonArray extractEntities(JsonObject context) {
        JsonArray entities = new JsonArray();
        
        // Add tables as entities
        JsonArray tables = context.getJsonArray("tables");
        if (tables != null) {
            for (int i = 0; i < tables.size(); i++) {
                entities.add(tables.getString(i));
            }
        }
        
        return entities;
    }
    
    /**
     * Extract aggregations from query and context
     */
    private JsonArray extractAggregations(String query, JsonObject context) {
        JsonArray aggregations = new JsonArray();
        String lower = query.toLowerCase();
        
        if (lower.contains("count") || lower.contains("how many")) {
            aggregations.add("count");
        }
        if (lower.contains("sum") || lower.contains("total")) {
            aggregations.add("sum");
        }
        if (lower.contains("average") || lower.contains("avg")) {
            aggregations.add("average");
        }
        if (lower.contains("max") || lower.contains("maximum")) {
            aggregations.add("max");
        }
        if (lower.contains("min") || lower.contains("minimum")) {
            aggregations.add("min");
        }
        
        return aggregations;
    }
    
    /**
     * Extract timeframe from query
     */
    private String extractTimeframe(String query) {
        String lower = query.toLowerCase();
        
        if (lower.contains("today")) return "today";
        if (lower.contains("yesterday")) return "yesterday";
        if (lower.contains("this week")) return "this_week";
        if (lower.contains("last week")) return "last_week";
        if (lower.contains("this month")) return "this_month";
        if (lower.contains("last month")) return "last_month";
        if (lower.contains("this year")) return "this_year";
        if (lower.contains("last year")) return "last_year";
        
        return null;  // No specific timeframe
    }
    
    /**
     * Build schemaMatches object from context
     */
    private JsonObject buildSchemaMatches(JsonObject context) {
        JsonArray matches = new JsonArray();
        
        // Get tables from context
        JsonArray tables = context.getJsonArray("tables");
        JsonObject tableInfo = context.getJsonObject("table_info");
        
        if (tables != null) {
            for (int i = 0; i < tables.size(); i++) {
            String tableName = tables.getString(i);
            JsonObject match = new JsonObject();
            
            // Build table object
            JsonObject table = new JsonObject()
                .put("tableName", tableName);
            
            // Add columns if available
            JsonObject info = tableInfo != null ? tableInfo.getJsonObject(tableName) : null;
            if (info != null && info.containsKey("columns")) {
                table.put("columns", info.getJsonArray("columns"));
                match.put("relevantColumns", info.getJsonArray("columns"));
            } else {
                // Default columns
                JsonArray defaultColumns = new JsonArray()
                    .add("ID")
                    .add("NAME")
                    .add("VALUE")
                    .add("DATE_CREATED");
                table.put("columns", defaultColumns);
                match.put("relevantColumns", defaultColumns);
            }
            
            match.put("table", table);
            match.put("confidence", 0.8);  // Default confidence
            matches.add(match);
            }
        }
        
        return new JsonObject().put("matches", matches);
    }
    
    @Override
    public Future<Void> cleanup() {
        List<Future> undeployFutures = new ArrayList<>();
        
        // Undeploy all clients
        for (Map.Entry<String, String> entry : deploymentIds.entrySet()) {
            Promise<Void> promise = Promise.promise();
            vertx.undeploy(entry.getValue(), ar -> {
                if (ar.succeeded()) {
                    log("Undeployed " + entry.getKey() + " client", 3);
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
package agents.director.hosts.milestones;

import agents.director.hosts.base.MilestoneContext;
import agents.director.hosts.base.MilestoneManager;
import agents.director.mcp.clients.OracleSQLGenerationClient;
import agents.director.mcp.clients.OracleSQLValidationClient;
import agents.director.services.LlmAPIService;
import agents.director.services.OracleConnectionManager;
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
    private LlmAPIService llmService;
    private OracleConnectionManager connectionManager;
    
    public SQLGenerationMilestone(Vertx vertx, String baseUrl) {
        super(vertx, baseUrl, 4, "SQLGenerationMilestone", 
              "Create SQL statement based on gathered information");
    }
    
    @Override
    public Future<Void> initialize() {
        Promise<Void> promise = Promise.promise();
        
        // Initialize services
        llmService = LlmAPIService.getInstance();
        connectionManager = OracleConnectionManager.getInstance();
        
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
        
        // Publish progress event at start
        if (context.isStreaming() && context.getSessionId() != null) {
            publishProgressEvent(context.getConversationId(),
                "Step 4: SQL Generation",
                "Creating SQL query...",
                new JsonObject()
                    .put("phase", "sql_generation")
                    .put("intent", context.getIntent()));
        }
        
        // Build SQL generation context from previous milestones (now async)
        buildSQLContextAsync(context)
            .compose(sqlContext -> {
                // Execute full SQL pipeline (generation + validation) using clients directly
                return fullSQLPipeline(context.getQuery(), sqlContext);
            })
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
                
                // Publish SQL query event
                if (context.isStreaming() && context.getSessionId() != null) {
                    publishProgressEvent(context.getConversationId(),
                        "SQL Query Ready",
                        "Generated and validated SQL statement",
                        new JsonObject()
                            .put("phase", "sql_query")
                            .put("query", generatedSql)
                            .put("queryType", detectQueryType(generatedSql)));
                }
                
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
            .recover(err -> {
                // Log the failure explicitly at WARNING level
                log("SQL generation failed, using degraded mode: " + err.getMessage(), 1);
                
                // Mark context as degraded
                context.setMilestoneDegraded(4, "SQL generation failed: " + err.getMessage());
                
                // Publish degradation event
                publishDegradationEvent(context, "sql_generation", err.getMessage());
                
                // Fallback: Generate simple SQL based on intent
                String fallbackSql = generateFallbackSQL(context);
                
                // Log what we're generating as fallback
                log("Using fallback SQL generation based on intent type: " + context.getIntentType(), 1);
                
                context.setGeneratedSql(fallbackSql);
                context.setSqlExplanation("⚠️ Basic SQL generated - advanced features unavailable");
                context.setSqlMetadata(new JsonObject()
                    .put("degraded", true)
                    .put("degradation_reason", err.getMessage())
                    .put("fallback_method", "intent_based_template")
                    .put("confidence", 0.3)); // Low confidence for template SQL
                
                // Mark milestone as complete (but degraded)
                context.completeMilestone(4);
                
                // Publish streaming event about degradation
                if (context.isStreaming() && context.getSessionId() != null) {
                    JsonObject degradedResult = getShareableResult(context);
                    degradedResult.put("degraded", true)
                        .put("message", "⚠️ SQL generation degraded: Using basic template");
                    publishStreamingEvent(context.getConversationId(), "milestone.sql_complete", degradedResult);
                    
                    // Also warn about the fallback SQL
                    publishStreamingEvent(context.getConversationId(), "degradation_warning", new JsonObject()
                        .put("milestone", 4)
                        .put("operation", "sql_generation")
                        .put("fallback_sql", fallbackSql)
                        .put("message", "Could not generate optimized SQL. Using basic template.")
                        .put("severity", "WARNING"));
                }
                
                // Return degraded SQL result
                return Future.succeededFuture(new JsonObject()
                    .put("sql", fallbackSql)
                    .put("explanation", "⚠️ Basic SQL generated - advanced features unavailable")
                    .put("validation", new JsonObject().put("is_valid", false))
                    .put("optimization", new JsonObject().put("optimized", false))
                    .put("degraded", true)
                    .put("confidence", 0.3));
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
     * Build comprehensive SQL context from all previous milestones (async version)
     */
    private Future<JsonObject> buildSQLContextAsync(MilestoneContext context) {
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
            
            // Add columns if available (already a JsonArray of column objects)
            JsonArray columns = context.getTableColumns().get(table);
            if (columns != null) {
                info.put("columns", columns);  // Include even if empty
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
            JsonObject schemaDetails = context.getSchemaDetails();
            sqlContext.put("schema_details", schemaDetails);
            
            // Extract and enhance businessTerms for WHERE clause generation
            JsonObject businessTerms = schemaDetails.getJsonObject("businessTerms");
            if (businessTerms != null) {
                sqlContext.put("business_term_mappings", businessTerms);
                
                // Use LLM-enhanced WHERE clause hints with data sampling
                return extractWhereClauseHintsWithLLM(businessTerms, context)
                    .map(whereClauseHints -> {
                        if (!whereClauseHints.isEmpty()) {
                            sqlContext.put("where_clause_hints", whereClauseHints);
                            log("Generated " + whereClauseHints.size() + " LLM-enhanced WHERE clause hints", 2);
                        }
                        // Add backstory and guidance for context
                        sqlContext.put("backstory", context.getBackstory());
                        sqlContext.put("guidance", context.getGuidance());
                        return sqlContext;
                    });
            }
        }
        
        // Add backstory and guidance for context
        sqlContext.put("backstory", context.getBackstory());
        sqlContext.put("guidance", context.getGuidance());
        
        return Future.succeededFuture(sqlContext);
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
                JsonArray columns = context.getTableColumns().get(primaryTable);
                if (columns != null && !columns.isEmpty()) {
                    // Extract up to 5 column names for the query
                    List<String> columnNames = new ArrayList<>();
                    int limit = Math.min(5, columns.size());
                    for (int i = 0; i < limit; i++) {
                        JsonObject col = columns.getJsonObject(i);
                        String colName = col.getString("columnName");
                        columnNames.add(colName);  // Add even if null
                    }
                    if (!columnNames.isEmpty()) {
                        String columnList = String.join(", ", columnNames);
                        return String.format("SELECT %s\nFROM %s\nWHERE ROWNUM <= 100", columnList, primaryTable);
                    }
                }
                return String.format("SELECT *\nFROM %s\nWHERE ROWNUM <= 100", primaryTable);
                
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
            case GENERATION_CLIENT:
                serverName = "oraclesqlgeneration";
                break;
            case VALIDATION_CLIENT:
                serverName = "oraclesqlvalidation";
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
            
            // Add columns if available (now full column objects with metadata)
            JsonObject info = tableInfo != null ? tableInfo.getJsonObject(tableName) : null;
            if (info != null && info.containsKey("columns")) {
                JsonArray columns = info.getJsonArray("columns");
                table.put("columns", columns);  // Full column objects
                match.put("relevantColumns", columns);
            } else {
                // Fallback: try to get from schema details if available
                JsonObject schemaDetails = context.getJsonObject("schema_details");
                if (schemaDetails != null) {
                    JsonArray schemaTables = schemaDetails.getJsonArray("tables");
                    if (schemaTables != null) {
                        for (int j = 0; j < schemaTables.size(); j++) {
                            JsonObject schemaTable = schemaTables.getJsonObject(j);
                            if (tableName.equals(schemaTable.getString("name"))) {
                                JsonArray columns = schemaTable.getJsonArray("columns");
                                if (columns != null) {
                                    table.put("columns", columns);
                                    match.put("relevantColumns", columns);
                                }
                                break;
                            }
                        }
                    }
                }
                // If still no columns, provide empty arrays
                if (!table.containsKey("columns")) {
                    table.put("columns", new JsonArray());
                    match.put("relevantColumns", new JsonArray());
                }
            }
            
            match.put("table", table);
            match.put("confidence", 0.8);  // Default confidence
            matches.add(match);
            }
        }
        
        return new JsonObject().put("matches", matches);
    }
    
    /**
     * Detect SQL query type from the statement
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
    
    /**
     * Extract basic WHERE clause hints from business terms (synchronous version)
     * This will be enhanced with LLM and data sampling in the full async version
     */
    private JsonArray extractBasicWhereClauseHints(JsonObject businessTerms) {
        JsonArray hints = new JsonArray();
        JsonObject mappings = businessTerms.getJsonObject("mappings");
        
        if (mappings == null) {
            return hints;
        }
        
        JsonArray termMappings = mappings.getJsonArray("mappings");
        if (termMappings == null) {
            return hints;
        }
        
        for (int i = 0; i < termMappings.size(); i++) {
            JsonObject termMapping = termMappings.getJsonObject(i);
            String term = termMapping.getString("term");
            JsonArray columnMappings = termMapping.getJsonArray("mappings");
            
            if (columnMappings != null) {
                for (int j = 0; j < columnMappings.size(); j++) {
                    JsonObject colMapping = columnMappings.getJsonObject(j);
                    double confidence = colMapping.getDouble("confidence", 0.0);
                    
                    if (confidence > 0.7) {
                        String table = colMapping.getString("table");
                        String column = colMapping.getString("column");
                        
                        // Create basic hint for now
                        hints.add(new JsonObject()
                            .put("term", term)
                            .put("table", table)
                            .put("column", column)
                            .put("operator", "LIKE")
                            .put("value", "%" + term.toUpperCase() + "%")
                            .put("condition", String.format("UPPER(%s.%s) LIKE '%%%s%%'", 
                                table, column, term.toUpperCase()))
                            .put("confidence", confidence)
                            .put("type", "filter"));
                    }
                }
            }
        }
        
        return hints;
    }
    
    /**
     * Extract LLM-enhanced WHERE clause hints with data sampling (async version)
     */
    private Future<JsonArray> extractWhereClauseHintsWithLLM(JsonObject businessTerms, MilestoneContext context) {
        Promise<JsonArray> promise = Promise.promise();
        JsonArray allHints = new JsonArray();
        JsonObject mappings = businessTerms.getJsonObject("mappings");
        
        if (mappings == null) {
            return Future.succeededFuture(new JsonArray());
        }
        
        JsonArray termMappings = mappings.getJsonArray("mappings");
        if (termMappings == null || termMappings.isEmpty()) {
            return Future.succeededFuture(new JsonArray());
        }
        
        // Collect all high-confidence column mappings
        List<Future> hintFutures = new ArrayList<>();
        
        for (int i = 0; i < termMappings.size(); i++) {
            JsonObject termMapping = termMappings.getJsonObject(i);
            String term = termMapping.getString("term");
            JsonArray columnMappings = termMapping.getJsonArray("mappings");
            
            if (columnMappings != null) {
                for (int j = 0; j < columnMappings.size(); j++) {
                    JsonObject colMapping = columnMappings.getJsonObject(j);
                    double confidence = colMapping.getDouble("confidence", 0.0);
                    
                    if (confidence > 0.6) {  // Lower threshold to catch more possibilities
                        String table = colMapping.getString("table");
                        String column = colMapping.getString("column");
                        
                        // Sample actual data from this column
                        Future<JsonObject> sampleDataFuture = sampleColumnValues(table, column);
                        
                        Future<JsonObject> hintFuture = sampleDataFuture.compose(sampleData -> {
                            // Use LLM to create intelligent WHERE clause hints
                            return createLLMEnhancedHint(term, table, column, confidence, sampleData);
                        });
                        
                        hintFutures.add(hintFuture);
                    }
                }
            }
        }
        
        // Combine all hints
        CompositeFuture.join(hintFutures)
            .onSuccess(result -> {
                for (int i = 0; i < result.size(); i++) {
                    JsonObject hint = result.resultAt(i);
                    if (hint != null && !hint.isEmpty()) {
                        allHints.add(hint);
                    }
                }
                promise.complete(allHints);
            })
            .onFailure(err -> {
                log("Failed to generate WHERE clause hints: " + err.getMessage(), 1);
                promise.complete(new JsonArray()); // Return empty on failure
            });
        
        return promise.future();
    }
    
    /**
     * Sample actual values from a column to understand the data
     */
    private Future<JsonObject> sampleColumnValues(String table, String column) {
        Promise<JsonObject> promise = Promise.promise();
        
        // Validate identifiers to prevent SQL injection
        if (!isValidOracleIdentifier(table) || !isValidOracleIdentifier(column)) {
            return Future.failedFuture("Invalid table or column name: " + table + "." + column);
        }
        
        // Proper Oracle syntax with CTE for sampling then grouping
        String samplingSQL = String.format("""
            WITH sampled_data AS (
                SELECT %s as value
                FROM %s
                WHERE %s IS NOT NULL
                AND ROWNUM <= 1000
            )
            SELECT value, COUNT(*) as frequency
            FROM sampled_data
            GROUP BY value
            ORDER BY frequency DESC
            FETCH FIRST 20 ROWS ONLY
            """, column, table, column);
        
        connectionManager.executeQuery(samplingSQL)
            .onSuccess(results -> {
                JsonArray topValues = new JsonArray();
                JsonArray allValues = new JsonArray();
                
                for (int i = 0; i < Math.min(results.size(), 10); i++) {
                    JsonObject row = results.getJsonObject(i);
                    String value = row.getString("value");
                    Integer frequency = row.getInteger("frequency");
                    topValues.add(new JsonObject()
                        .put("value", value)
                        .put("frequency", frequency));
                    allValues.add(value);
                }
                
                promise.complete(new JsonObject()
                    .put("topValues", topValues)
                    .put("sampleValues", allValues)
                    .put("totalSampled", results.size()));
            })
            .onFailure(err -> {
                log("Failed to sample column " + table + "." + column + ": " + err.getMessage(), 2);
                promise.fail(err); // Fail properly instead of completing with empty
            });
        
        return promise.future();
    }
    
    /**
     * Validate Oracle identifier to prevent SQL injection
     */
    private boolean isValidOracleIdentifier(String identifier) {
        if (identifier == null || identifier.isEmpty() || identifier.length() > 30) {
            return false;
        }
        // Oracle identifiers: letters, numbers, underscore, $, #
        // Must start with letter
        return identifier.matches("^[A-Za-z][A-Za-z0-9_$#]*$");
    }
    
    /**
     * Create LLM-enhanced hint for WHERE clause generation
     */
    private Future<JsonObject> createLLMEnhancedHint(String searchTerm, String table, 
                                                      String column, double confidence, 
                                                      JsonObject sampleData) {
        Promise<JsonObject> promise = Promise.promise();
        
        // Build LLM prompt with actual data samples
        String systemPrompt = "You are a SQL WHERE clause optimization expert. Given a search term, " +
            "a database column, and sample data from that column, suggest the best WHERE clause " +
            "conditions to find matching records.";
        
        StringBuilder userPrompt = new StringBuilder();
        userPrompt.append("Search term: '").append(searchTerm).append("'\n");
        userPrompt.append("Table.Column: ").append(table).append(".").append(column).append("\n");
        userPrompt.append("Mapping confidence: ").append(confidence).append("\n\n");
        
        JsonArray topValues = sampleData.getJsonArray("topValues");
        if (topValues != null && !topValues.isEmpty()) {
            userPrompt.append("Top values in this column (value: frequency):\n");
            for (int i = 0; i < Math.min(5, topValues.size()); i++) {
                JsonObject val = topValues.getJsonObject(i);
                userPrompt.append("  - '").append(val.getString("value"))
                         .append("': ").append(val.getInteger("frequency")).append(" times\n");
            }
        }
        
        userPrompt.append("\nBased on the search term and actual data, suggest:\n");
        userPrompt.append("1. The best SQL WHERE condition(s) for this column\n");
        userPrompt.append("2. Whether to use LIKE, =, IN, or other operators\n");
        userPrompt.append("3. Any data transformations needed (UPPER, TRIM, etc.)\n");
        userPrompt.append("4. Alternative values that might match the intent\n\n");
        userPrompt.append("For example, if searching for 'California' in a CITY column that contains ");
        userPrompt.append("'Los Angeles', 'San Francisco', suggest using city names instead.\n\n");
        userPrompt.append("Output JSON:\n");
        userPrompt.append("{\n");
        userPrompt.append("  \"operator\": \"LIKE|=|IN|BETWEEN|etc\",\n");
        userPrompt.append("  \"values\": [\"value1\", \"value2\"],\n");
        userPrompt.append("  \"transform\": \"UPPER|LOWER|TRIM|none\",\n");
        userPrompt.append("  \"condition\": \"complete WHERE clause fragment\",\n");
        userPrompt.append("  \"confidence\": 0.0-1.0,\n");
        userPrompt.append("  \"reasoning\": \"why this approach\"\n");
        userPrompt.append("}");
        
        // Check if LLM service is available
        if (llmService == null || !llmService.isInitialized()) {
            return Future.failedFuture("LLM service not available for hint generation");
        }
        
        // Call LLM
        llmService.chatCompletion(
            Arrays.asList(
                new JsonObject().put("role", "system").put("content", systemPrompt).encode(),
                new JsonObject().put("role", "user").put("content", userPrompt.toString()).encode()
            ), 
            0.3,  // Low temperature for consistency
            500
        ).whenComplete((result, error) -> {
            // Get current Vert.x context for thread safety
            io.vertx.core.Context vertxContext = Vertx.currentContext();
            
            Runnable handler = () -> {
                if (error != null) {
                    log("LLM hint generation failed: " + error.getMessage(), 2);
                    promise.fail(error);
                    return;
                }
                
                try {
                    // Parse LLM response
                    String llmResponse = result.getJsonArray("choices")
                        .getJsonObject(0)
                        .getJsonObject("message")
                        .getString("content");
                    
                    // Try to extract JSON from the response
                    int jsonStart = llmResponse.indexOf("{");
                    int jsonEnd = llmResponse.lastIndexOf("}") + 1;
                    if (jsonStart >= 0 && jsonEnd > jsonStart) {
                        llmResponse = llmResponse.substring(jsonStart, jsonEnd);
                    }
                    
                    JsonObject hint = new JsonObject(llmResponse);
                    hint.put("term", searchTerm);
                    hint.put("table", table);
                    hint.put("column", column);
                    hint.put("original_confidence", confidence);
                    hint.put("method", "llm_enhanced");
                    
                    promise.complete(hint);
                } catch (Exception e) {
                    log("Failed to parse LLM hint response: " + e.getMessage(), 2);
                    promise.fail(e);
                }
            };
            
            // Always use context if available, fail if not
            if (vertxContext != null) {
                vertxContext.runOnContext(v -> handler.run());
            } else {
                // This shouldn't happen in a proper Vert.x environment
                log("Warning: No Vert.x context available for LLM callback", 1);
                handler.run();
            }
        });
        
        return promise.future();
    }
}
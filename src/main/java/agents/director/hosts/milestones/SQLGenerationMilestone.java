package agents.director.hosts.milestones;

import agents.director.hosts.base.MilestoneContext;
import agents.director.hosts.base.MilestoneManager;
import agents.director.mcp.clients.OracleSQLGenerationClient;
import agents.director.mcp.clients.OracleSQLValidationClient;
import agents.director.mcp.clients.OracleSchemaIntelligenceClient;
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
    private static final String SCHEMA_CLIENT = "schema";
    private final Map<String, String> deploymentIds = new HashMap<>();
    private LlmAPIService llmService;
    private OracleConnectionManager connectionManager;
    private OracleSchemaIntelligenceClient schemaClient;
    
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
        OracleSchemaIntelligenceClient schemaClient = new OracleSchemaIntelligenceClient(baseUrl);
        this.schemaClient = schemaClient;
        deploymentFutures.add(deployClient(GENERATION_CLIENT, generationClient));
        deploymentFutures.add(deployClient(VALIDATION_CLIENT, validationClient));
        deploymentFutures.add(deployClient(SCHEMA_CLIENT, schemaClient));
        
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
            .onFailure(err -> {
                // Log the failure and let it propagate
                log("SQL generation failed: " + err.getMessage(), 0);
                
                // Mark context as degraded
                context.setMilestoneDegraded(4, "SQL generation failed: " + err.getMessage());
                
                // Publish degradation event
                publishDegradationEvent(context, "sql_generation", err.getMessage());
                
                // Let the failure propagate instead of using fallback
                promise.fail(err);
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

            // Extract and add relationship data for JOIN generation
            JsonObject relationships = schemaDetails.getJsonObject("relationships");
            if (relationships != null && !relationships.isEmpty()) {
                sqlContext.put("table_relationships", relationships);
                log("Added table relationships for JOIN generation", 3);
            }

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
        JsonObject toolParams = new JsonObject()
            .put("analysis", analysis)
            .put("schemaMatches", schemaMatches)
            .put("includeEnums", true);
        
        // Add WHERE clause hints if available
        JsonArray whereClauseHints = sqlContext.getJsonArray("where_clause_hints");
        if (whereClauseHints != null && !whereClauseHints.isEmpty()) {
            toolParams.put("whereClauseHints", whereClauseHints);
        }
        
        Future<JsonObject> generationFuture = callTool(GENERATION_CLIENT, "generate_oracle_sql", toolParams);
        
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
     * Filter WHERE clause hints based on query relevance using LLM
     */
    private Future<JsonArray> filterHintsForRelevance(JsonArray allHints, MilestoneContext context) {
        Promise<JsonArray> promise = Promise.promise();

        // If no hints, return empty array
        if (allHints == null || allHints.isEmpty()) {
            return Future.succeededFuture(new JsonArray());
        }

        // Use LLM to determine which hints are actually relevant to the query
        String systemPrompt = """
            You are a SQL query analyzer. Given a user's query and potential WHERE clause hints,
            determine which hints are ACTUALLY relevant to answering the query.

            Rules:
            - Only include hints that directly relate to filtering criteria mentioned in the query
            - For "How many X" queries without additional criteria, return NO hints (empty array)
            - For queries with explicit filters (e.g., "pending orders", "in California"), include relevant hints
            - Be conservative - when in doubt, exclude the hint
            - The query intent matters more than keyword matching

            Return a JSON array containing only the relevant hints, or an empty array [] if none apply.
            Each hint in your response must be an exact copy from the input hints.
            """;

        // Build query context
        String queryContext = String.format("""
            User Query: "%s"
            Query Intent: %s
            Intent Type: %s
            """,
            context.getQuery(),
            context.getIntent() != null ? context.getIntent() : "unknown",
            context.getIntentType() != null ? context.getIntentType() : "unknown"
        );

        String userPrompt = String.format("""
            %s

            Potential WHERE clause hints to evaluate:
            %s

            Which of these hints should actually be used in the SQL WHERE clause?

            Examples of when to include/exclude hints:
            - "How many orders?" → Return empty array [] (counting all orders, no filtering needed)
            - "How many pending orders?" → Include hints related to status='pending'
            - "Show orders in California" → Include hints related to location/state='California'
            - "List all customers" → Return empty array [] (no filtering needed)

            IMPORTANT: Only return hints that are explicitly relevant to the user's query intent.
            Return the filtered hints as a JSON array. If no hints are relevant, return an empty array [].
            """,
            queryContext,
            allHints.encodePrettily()
        );

        try {
            llmService.chatCompletion(
                Arrays.asList(
                    new JsonObject().put("role", "system").put("content", systemPrompt).encode(),
                    new JsonObject().put("role", "user").put("content", userPrompt).encode()
                ),
                0.3,  // Low temperature for consistency
                2000  // Max tokens
            ).whenComplete((response, error) -> {
                if (error != null) {
                    log("LLM call failed for hint filtering: " + error.getMessage(), 0);
                    // Fail open - no hints rather than wrong hints
                    promise.fail(error);
                } else {
                    try {
                        String content = response.getString("content", "[]");

                        // Extract JSON array from response (handle markdown code blocks)
                        if (content.contains("```json")) {
                            content = content.substring(content.indexOf("```json") + 7);
                            content = content.substring(0, content.indexOf("```"));
                        } else if (content.contains("```")) {
                            content = content.substring(content.indexOf("```") + 3);
                            content = content.substring(0, content.indexOf("```"));
                        }

                        // Parse the filtered hints
                        JsonArray filteredHints = new JsonArray(content.trim());

                        // Validate that returned hints exist in original set (security check)
                        JsonArray validatedHints = new JsonArray();
                        for (int i = 0; i < filteredHints.size(); i++) {
                            JsonObject hint = filteredHints.getJsonObject(i);
                            if (hintExistsInOriginal(hint, allHints)) {
                                validatedHints.add(hint);
                            } else {
                                log("LLM returned invalid hint not in original set, skipping: " + hint.encode(), 1);
                            }
                        }

                        promise.complete(validatedHints);
                    } catch (Exception e) {
                        log("Failed to parse LLM response for hint filtering: " + e.getMessage(), 0);
                        // Fail open - no hints rather than wrong hints
                        promise.fail(new RuntimeException("Failed to parse filtered hints", e));
                    }
                }
            });
        } catch (Exception e) {
            log("Failed to call LLM for hint filtering: " + e.getMessage(), 0);
            promise.fail(e);
        }

        return promise.future();
    }

    /**
     * Check if a hint exists in the original hint array (for security validation)
     */
    private boolean hintExistsInOriginal(JsonObject hint, JsonArray originalHints) {
        String hintTable = hint.getString("table");
        String hintColumn = hint.getString("column");
        String hintValue = hint.getString("value");

        for (int i = 0; i < originalHints.size(); i++) {
            JsonObject original = originalHints.getJsonObject(i);
            if (hintTable != null && hintTable.equals(original.getString("table")) &&
                hintColumn != null && hintColumn.equals(original.getString("column")) &&
                hintValue != null && hintValue.equals(original.getString("value"))) {
                return true;
            }
        }
        return false;
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
        @SuppressWarnings("rawtypes")  // Vert.x CompositeFuture.all() requires raw Future type
        List<Future> hintFutures = new ArrayList<>();
        List<JsonObject> mappingsList = new ArrayList<>();  // Track original mappings for each future
        
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
                        String enumValue = colMapping.getString("enumValue"); // Check for pre-resolved enum value
                        String matchType = colMapping.getString("matchType"); // Check if this is an enum match
                        
                        // Skip nonsensical hints where term matches table name for ID columns
                        // E.g., don't search for "orders" in ORDERS.ORDER_ID or ORDERS.STATUS_ID
                        if (isNonsensicalHint(term, table, column)) {
                            log("Skipping nonsensical hint: searching for '" + term + "' in " + 
                                table + "." + column, 3);
                            continue;
                        }
                        
                        // Also skip if term is just the table name and column is an enum-like column
                        boolean isEnumColumn = column.toUpperCase().endsWith("_ID") || 
                                             column.toUpperCase().endsWith("_CODE") ||
                                             column.toUpperCase().contains("STATUS");
                        boolean isEntityTerm = term.equalsIgnoreCase(table) || 
                                             term.equalsIgnoreCase(table + "s") ||
                                             (table + "s").equalsIgnoreCase(term);
                        
                        // CRITICAL FIX: Don't skip if this is an enum value match
                        if ("enum_value".equals(matchType)) {
                            // This is a matched enum value, not an entity term
                            isEntityTerm = false;
                        }
                        
                        if (isEnumColumn && isEntityTerm) {
                            log("Skipping entity term '" + term + "' for enum column " + 
                                table + "." + column, 3);
                            continue;
                        }
                        
                        // If we already have an enum value from BusinessMappingServer, use it directly
                        if (enumValue != null && !enumValue.isEmpty()) {
                            log("Using pre-resolved enum value for " + term + ": " + column + " = " + enumValue, 2);
                            JsonObject hint = new JsonObject()
                                .put("table", table)
                                .put("column", column)
                                .put("operator", "=")
                                .put("value", enumValue)
                                .put("condition", table + "." + column + " = " + enumValue)
                                .put("confidence", confidence)
                                .put("source", "enum_cache")
                                .put("type", "enum_filter")
                                .put("reason", "Term '" + term + "' matched enum description, using cached ID");
                            allHints.add(hint);
                            continue; // Skip sampling since we have the value
                        }
                        
                        // Create mapping object for tracking
                        JsonObject mapping = new JsonObject()
                            .put("term", term)
                            .put("table", table)
                            .put("column", column)
                            .put("confidence", confidence);
                        
                        // Sample actual data from this column
                        Future<JsonObject> sampleDataFuture = sampleColumnValues(table, column);
                        
                        Future<JsonObject> hintFuture = sampleDataFuture.compose(sampleData -> {
                            // Use LLM to create intelligent WHERE clause hints
                            return createLLMEnhancedHint(term, table, column, confidence, sampleData);
                        });
                        
                        hintFutures.add(hintFuture);
                        mappingsList.add(mapping);  // Keep mapping aligned with future
                    }
                }
            }
        }
        
        // Combine all hints - use .all() to get partial results even if some fail
        CompositeFuture.all(hintFutures)
            .onComplete(ar -> {
                // Track failed mappings for enum resolution
                List<JsonObject> failedMappings = new ArrayList<>();
                
                // Process all results, even if some failed
                for (int i = 0; i < hintFutures.size(); i++) {
                    @SuppressWarnings("unchecked")  // We know these are Future<JsonObject>
                    Future<JsonObject> hintFuture = (Future<JsonObject>) hintFutures.get(i);
                    JsonObject originalMapping = mappingsList.get(i);
                    
                    if (hintFuture.succeeded()) {
                        JsonObject hint = hintFuture.result();
                        if (hint != null && !hint.isEmpty()) {
                            allHints.add(hint);
                        }
                    } else {
                        Throwable cause = hintFuture.cause();
                        String errorMsg = cause != null ? cause.getMessage() : "Unknown error";
                        log("Hint generation failed for mapping: " + originalMapping.encode() + " - " + errorMsg, 2);
                        
                        // Track failed mappings for potential enum resolution
                        if (errorMsg.contains("invalid identifier")) {
                            failedMappings.add(originalMapping);
                        }
                    }
                }
                
                // If we have failed mappings, try enum resolution
                if (!failedMappings.isEmpty()) {
                    log("Attempting enum resolution for " + failedMappings.size() + " failed mappings", 2);
                    attemptEnumResolution(failedMappings)
                        .onComplete(enumAr -> {
                            if (enumAr.succeeded()) {
                                JsonArray enumHints = enumAr.result();
                                for (int i = 0; i < enumHints.size(); i++) {
                                    allHints.add(enumHints.getJsonObject(i));
                                }
                                log("Added " + enumHints.size() + " enum-resolved hints", 2);
                            }
                            
                            // Complete with whatever hints we have after filtering
                            if (allHints.isEmpty()) {
                                log("No initial WHERE clause hints generated", 2);
                                promise.complete(allHints);
                            } else {
                                log("Generated " + allHints.size() + " total WHERE clause hints, filtering for relevance...", 2);
                                // Filter hints based on query relevance using LLM
                                filterHintsForRelevance(allHints, context)
                                    .onSuccess(filteredHints -> {
                                        if (filteredHints.isEmpty()) {
                                            log("No WHERE clause hints relevant to query - will use simple query", 2);
                                        } else {
                                            log("Filtered to " + filteredHints.size() + " relevant WHERE clause hints", 2);
                                        }
                                        promise.complete(filteredHints);
                                    })
                                    .onFailure(err -> {
                                        log("Failed to filter WHERE hints with LLM: " + err.getMessage(), 0);
                                        // Fail open - no hints rather than wrong hints
                                        promise.fail(new RuntimeException("WHERE hint filtering failed", err));
                                    });
                            }
                        });
                } else {
                    // No failed mappings, complete normally
                    if (allHints.isEmpty()) {
                        log("No initial WHERE clause hints generated", 2);
                        promise.complete(allHints);
                    } else {
                        log("Generated " + allHints.size() + " initial WHERE clause hints, filtering for relevance...", 2);
                        // Filter hints based on query relevance using LLM
                        filterHintsForRelevance(allHints, context)
                            .onSuccess(filteredHints -> {
                                if (filteredHints.isEmpty()) {
                                    log("No WHERE clause hints relevant to query - will use simple query", 2);
                                } else {
                                    log("Filtered to " + filteredHints.size() + " relevant WHERE clause hints", 2);
                                }
                                promise.complete(filteredHints);
                            })
                            .onFailure(err -> {
                                log("Failed to filter WHERE hints with LLM: " + err.getMessage(), 0);
                                // Fail open - no hints rather than wrong hints
                                promise.fail(new RuntimeException("WHERE hint filtering failed", err));
                            });
                    }
                }
            });

        return promise.future();
    }
    
    /**
     * Attempt to resolve enum values when direct column sampling fails
     */
    private Future<JsonArray> attemptEnumResolution(List<JsonObject> failedMappings) {
        Promise<JsonArray> promise = Promise.promise();
        JsonArray resolvedHints = new JsonArray();
        
        if (failedMappings.isEmpty()) {
            return Future.succeededFuture(resolvedHints);
        }
        
        // CRITICAL FIX: First check if these are likely enum columns that don't exist
        // For each failed mapping, try to find the correct enum column
        List<Future> resolutionFutures = new ArrayList<>();
        
        for (JsonObject mapping : failedMappings) {
            String term = mapping.getString("term");
            String table = mapping.getString("table");
            String column = mapping.getString("column");
            
            // If column doesn't exist but looks like it should be an enum, try alternatives
            if (column.toUpperCase().equals("STATUS") || column.toUpperCase().equals("TYPE") || 
                column.toUpperCase().equals("STATE") || column.toUpperCase().equals("CATEGORY")) {
                
                // Try common enum column patterns
                String[] possibleColumns = {
                    column + "_ID",        // STATUS -> STATUS_ID
                    column + "_CODE",      // STATUS -> STATUS_CODE
                    column.replace("_NAME", "_ID"),  // STATUS_NAME -> STATUS_ID
                    column.replace("_DESC", "_ID")   // STATUS_DESC -> STATUS_ID
                };
                
                // Create a future that tries each alternative
                Promise<JsonObject> columnPromise = Promise.promise();
                tryAlternativeEnumColumns(term, table, possibleColumns, 0, columnPromise);
                resolutionFutures.add(columnPromise.future());
            } else {
                // Try standard enum resolution
                resolutionFutures.add(translateEnumValue(term, table, column));
            }
        }
        
        // Combine all resolution attempts
        CompositeFuture.all(resolutionFutures)
            .onComplete(ar -> {
                for (int i = 0; i < resolutionFutures.size(); i++) {
                    @SuppressWarnings("unchecked")
                    Future<JsonObject> enumFuture = (Future<JsonObject>) resolutionFutures.get(i);
                    if (enumFuture.succeeded()) {
                        JsonObject enumResult = enumFuture.result();
                        if (enumResult != null && !enumResult.isEmpty()) {
                            resolvedHints.add(enumResult);
                        }
                    }
                }
                
                // If still no hints, try intelligent column discovery
                if (resolvedHints.isEmpty()) {
                    discoverAlternativeColumns(failedMappings)
                        .onComplete(discoverAr -> {
                            if (discoverAr.succeeded()) {
                                resolvedHints.addAll(discoverAr.result());
                            }
                            promise.complete(resolvedHints);
                        });
                } else {
                    promise.complete(resolvedHints);
                }
            });
        
        return promise.future();
    }
    
    /**
     * Try alternative enum column names recursively
     */
    private void tryAlternativeEnumColumns(String term, String table, String[] columns, 
                                          int index, Promise<JsonObject> promise) {
        if (index >= columns.length) {
            // No more alternatives, return empty
            promise.complete(new JsonObject());
            return;
        }
        
        String column = columns[index];
        
        // Try to get enum value for this column using schema client
        schemaClient.callTool("translate_enum", new JsonObject()
            .put("table", table)
            .put("column", column)
            .put("values", new JsonArray().add(term))
            .put("direction", "description_to_code"))
            .onSuccess(result -> {
                JsonArray translations = result.getJsonArray("translations", new JsonArray());
                if (!translations.isEmpty()) {
                    JsonObject translation = translations.getJsonObject(0);
                    if (translation.getBoolean("found", false)) {
                        // Found a match!
                        JsonObject hint = new JsonObject()
                            .put("table", table)
                            .put("column", column)
                            .put("operator", "=")
                            .put("value", translation.getString("translated"))
                            .put("condition", table + "." + column + " = " + translation.getString("translated"))
                            .put("confidence", 0.95)
                            .put("source", "enum_resolution")
                            .put("type", "enum_filter")
                            .put("reason", "Found '" + term + "' in enum column " + column);
                        promise.complete(hint);
                        return;
                    }
                }
                // Try next alternative
                tryAlternativeEnumColumns(term, table, columns, index + 1, promise);
            })
            .onFailure(err -> {
                // Try next alternative
                tryAlternativeEnumColumns(term, table, columns, index + 1, promise);
            });
    }
    
    /**
     * Translate an enum value for a discovered column
     */
    private Future<JsonObject> translateEnumForColumn(String term, String table, String column) {
        Promise<JsonObject> promise = Promise.promise();
        
        // First, try to find an enum table for this column
        String enumTable = findEnumTableForColumn(table, column);
        
        if (enumTable != null) {
            // Query the enum table directly
            String query = String.format(
                "SELECT %s as code FROM %s WHERE UPPER(STATUS_CODE) = '%s' OR UPPER(DESCRIPTION) = '%s'",
column.replace("_ID", "_CODE"),  // Map _ID to _CODE for enum lookup
                enumTable,
                term.toUpperCase(),
                term.toUpperCase()
            );
            
            connectionManager.executeQuery(query)
                .onSuccess(results -> {
                    if (!results.isEmpty()) {
                        String code = results.getJsonObject(0).getValue("CODE").toString();
                        JsonObject hint = new JsonObject()
                            .put("term", term)
                            .put("table", table)
                            .put("column", column)
                            .put("operator", "=")
                            .put("values", new JsonArray().add(code))
                            .put("condition", "OR")
                            .put("enumResolved", true)
                            .put("confidence", 0.9);
                        promise.complete(hint);
                    } else {
                        // Try BusinessMapping service as fallback
                        translateEnumValue(term, table, column)
                            .onSuccess(promise::complete)
                            .onFailure(err -> promise.complete(new JsonObject()));
                    }
                })
                .onFailure(err -> {
                    // Try BusinessMapping service as fallback
                    translateEnumValue(term, table, column)
                        .onSuccess(promise::complete)
                        .onFailure(e -> promise.complete(new JsonObject()));
                });
        } else {
            // No enum table found, try BusinessMapping service
            translateEnumValue(term, table, column)
                .onSuccess(promise::complete)
                .onFailure(err -> promise.complete(new JsonObject()));
        }
        
        return promise.future();
    }
    
    /**
     * Find enum table for a column dynamically
     */
    private String findEnumTableForColumn(String table, String column) {
        // Common patterns for enum table names
        if (column.toUpperCase().endsWith("_ID")) {
            String baseName = column.substring(0, column.length() - 3).toUpperCase();  // Remove _ID
            String tableUpper = table.toUpperCase();
            
            // Try to find enum table dynamically
            // Get DEFAULT_SCHEMA from environment
            String currentSchema = System.getProperty("DEFAULT_SCHEMA");
            if (currentSchema == null) currentSchema = System.getenv("DEFAULT_SCHEMA");

            String checkQuery = String.format(
                "SELECT table_name FROM all_tables WHERE owner = '%s' AND " +
                "(table_name LIKE '%%%s_ENUM' OR " +
                " table_name LIKE '%%%s_LOOKUP' OR " +
                " table_name LIKE '%%%s_REF') " +
                "AND ROWNUM = 1",
                currentSchema.toUpperCase(), baseName, baseName, baseName
            );

            try {
                JsonArray results = connectionManager.executeQuery(checkQuery)
                    .toCompletionStage()
                    .toCompletableFuture()
                    .get(1000, java.util.concurrent.TimeUnit.MILLISECONDS);
                
                if (!results.isEmpty()) {
                    return results.getJsonObject(0).getString("TABLE_NAME");
                }
            } catch (Exception e) {
                // Ignore and use pattern-based fallback
            }
            
            // Return most likely pattern
            return tableUpper.substring(0, Math.min(tableUpper.length(), 5)) + "_" + baseName + "_ENUM";
        }
        
        return null;
    }
    
    /**
     * Translate an enum value using the BusinessMapping service
     */
    private Future<JsonObject> translateEnumValue(String term, String table, String column) {
        Promise<JsonObject> promise = Promise.promise();
        
        // First get enum metadata to know the actual column names
        JsonObject metadataRequest = new JsonObject()
            .put("table", table)
            .put("column", column);
        
        schemaClient.callTool("get_enum_metadata", metadataRequest)
            .compose(metadataResponse -> {
                JsonArray enumTables = metadataResponse.getJsonArray("enumTables", new JsonArray());
                
                // Extract enum table info if available
                JsonObject enumTableInfo = null;
                for (int i = 0; i < enumTables.size(); i++) {
                    JsonObject tableInfo = enumTables.getJsonObject(i);
                    // Look for matching enum table (e.g., ORDER_STATUS_ENUM for STATUS_ID column)
                    if (tableInfo.getString("table", "").contains(column.replace("_ID", "").replace("_CODE", ""))) {
                        enumTableInfo = tableInfo;
                        break;
                    }
                }
                
                // Now translate the enum value
                JsonObject translateRequest = new JsonObject()
                    .put("table", table)
                    .put("column", column)
                    .put("values", new JsonArray().add(term))
                    .put("direction", "description_to_code");
                
                final JsonObject finalEnumTableInfo = enumTableInfo;
                return schemaClient.callTool("translate_enum", translateRequest)
                    .map(translateResponse -> {
                        JsonArray translations = translateResponse.getJsonArray("translations", new JsonArray());
                        if (!translations.isEmpty()) {
                            JsonObject translation = translations.getJsonObject(0);
                            String code = translation.getString("translated");
                            
                            // Create a hint with the enum code and metadata
                            JsonObject hint = new JsonObject()
                                .put("term", term)
                                .put("table", table)
                                .put("column", findEnumColumn(table, column))  // Try to find the actual enum column
                                .put("value", code)
                                .put("type", "enum_code")
                                .put("confidence", 0.9);
                            
                            // Add enum table metadata if found
                            if (finalEnumTableInfo != null) {
                                hint.put("enum_table", finalEnumTableInfo.getString("table"))
                                    .put("enum_id_column", finalEnumTableInfo.getString("idColumn"))
                                    .put("enum_desc_column", finalEnumTableInfo.getString("descriptionColumn"));
                            }
                            
                            return hint;
                        }
                        return new JsonObject();  // Empty result
                    });
            })
            .onSuccess(hint -> promise.complete(hint))
            .onFailure(err -> {
                log("WARNING: Failed to translate enum value '" + term + "': " + err.getMessage(), 1);
                // Return empty result but log the warning - don't hide the issue
                promise.complete(new JsonObject());
            });
        
        return promise.future();
    }
    
    /**
     * Find the actual enum column name (e.g., STATUS_ID instead of STATUS)
     */
    private String findEnumColumn(String table, String originalColumn) {
        // Common patterns for enum columns
        String[] patterns = {
            originalColumn + "_ID",
            originalColumn + "_CODE",
            originalColumn.replace("_NAME", "_ID"),
            originalColumn.replace("_DESC", "_ID"),
            originalColumn.replace("_DESCRIPTION", "_ID")
        };
        
        // For now, try the most common pattern
        // In a full implementation, we'd query the database schema
        return originalColumn.endsWith("_ID") ? originalColumn : originalColumn + "_ID";
    }
    
    /**
     * Discover alternative columns when the mapped column doesn't exist
     */
    private Future<JsonArray> discoverAlternativeColumns(List<JsonObject> failedMappings) {
        Promise<JsonArray> promise = Promise.promise();
        JsonArray hints = new JsonArray();
        
        // For each failed mapping, try to find alternative columns
        List<Future> discoveryFutures = new ArrayList<>();
        
        for (JsonObject mapping : failedMappings) {
            String term = mapping.getString("term");
            String table = mapping.getString("table");
            String column = mapping.getString("column");
            
            // Try common column variations
            discoveryFutures.add(tryAlternativeColumn(term, table, column));
        }
        
        CompositeFuture.all(discoveryFutures)
            .onComplete(ar -> {
                for (int i = 0; i < discoveryFutures.size(); i++) {
                    @SuppressWarnings("unchecked")
                    Future<JsonObject> future = (Future<JsonObject>) discoveryFutures.get(i);
                    if (future.succeeded() && future.result() != null && !future.result().isEmpty()) {
                        hints.add(future.result());
                    }
                }
                promise.complete(hints);
            });
        
        return promise.future();
    }
    
    /**
     * Try alternative column names for a failed mapping
     */
    private Future<JsonObject> tryAlternativeColumn(String term, String table, String originalColumn) {
        Promise<JsonObject> promise = Promise.promise();
        
        // Generate alternative column names using patterns only (database-agnostic)
        List<String> alternatives = new ArrayList<>();
        String upperCol = originalColumn.toUpperCase();
        
        // Add suffix variations
        if (!upperCol.endsWith("_ID")) {
            alternatives.add(originalColumn + "_ID");
        }
        if (!upperCol.endsWith("_CODE")) {
            alternatives.add(originalColumn + "_CODE");
        }
        
        // Replace common patterns
        if (upperCol.contains("STATUS") && !upperCol.contains("STATUS_ID")) {
            alternatives.add(originalColumn.replace("STATUS", "STATUS_ID"));
        }
        if (upperCol.contains("TYPE") && !upperCol.contains("TYPE_ID")) {
            alternatives.add(originalColumn.replace("TYPE", "TYPE_ID"));
        }
        if (upperCol.contains("STATE") && !upperCol.contains("STATE_ID")) {
            alternatives.add(originalColumn.replace("STATE", "STATE_ID"));
        }
        
        // Try without suffix if it has one
        if (upperCol.endsWith("_NAME") || upperCol.endsWith("_DESC") || upperCol.endsWith("_DESCRIPTION")) {
            String base = originalColumn.substring(0, originalColumn.lastIndexOf("_"));
            alternatives.add(base + "_ID");
            alternatives.add(base + "_CODE");
        }
        
        // Try each alternative
        tryNextAlternative(term, table, alternatives, 0, promise);
        
        return promise.future();
    }
    
    /**
     * Recursively try alternative column names
     */
    private void tryNextAlternative(String term, String table, List<String> alternatives, 
                                   int index, Promise<JsonObject> promise) {
        if (index >= alternatives.size()) {
            // No more alternatives to try
            log("WARNING: Could not find alternative column for '" + term + "' in table " + table, 1);
            promise.complete(new JsonObject());
            return;
        }
        
        String column = alternatives.get(index);
        
        // Try to sample this column
        sampleColumnValues(table, column)
            .onSuccess(result -> {
                // Success! But we need to be smart about which values to use
                
                // Check if this looks like an enum ID column (ends with _ID)
                boolean isEnumIdColumn = column.toUpperCase().endsWith("_ID") || 
                                        column.toUpperCase().endsWith("_CODE");
                
                if (isEnumIdColumn) {
                    // Check if term is likely an entity name (table name) rather than a value
                    boolean isEntityTerm = term.equalsIgnoreCase(table) || 
                                         term.equalsIgnoreCase(table + "s") ||
                                         (table + "s").equalsIgnoreCase(term);
                    
                    if (isEntityTerm) {
                        // Don't try to use entity names as enum values
                        log("Skipping enum translation for entity term '" + term + "' in " + table + "." + column, 3);
                        // Try the next alternative instead of failing the entire promise
                        tryNextAlternative(term, table, alternatives, index + 1, promise);
                        return;
                    }
                    
                    // Try to translate the term through enum system
                    translateEnumForColumn(term, table, column)
                        .onSuccess(enumHint -> {
                            if (enumHint != null && !enumHint.isEmpty()) {
                                promise.complete(enumHint);
                            } else {
                                // No fallback - fail if enum translation doesn't work
                                log("Enum translation failed for '" + term + "' in " + table + "." + column, 2);
                                promise.fail("Could not translate enum value");
                            }
                        })
                        .onFailure(err -> {
                            // Try next alternative
                            tryNextAlternative(term, table, alternatives, index + 1, promise);
                        });
                } else {
                    // For non-enum columns, use the term as the value to search for
                    JsonObject hint = new JsonObject()
                        .put("term", term)
                        .put("table", table)
                        .put("column", column)
                        .put("operator", "LIKE")
                        .put("values", new JsonArray().add("%" + term.toUpperCase() + "%"))
                        .put("condition", "OR")
                        .put("discoveredColumn", true)
                        .put("confidence", 0.6);
                    
                    promise.complete(hint);
                }
            })
            .onFailure(err -> {
                // Try next alternative
                tryNextAlternative(term, table, alternatives, index + 1, promise);
            });
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
        
        // Proper Oracle syntax - use subquery for ROWNUM to work correctly
        String samplingSQL = String.format("""
            WITH sampled_data AS (
                SELECT value FROM (
                    SELECT %s as value
                    FROM %s
                    WHERE %s IS NOT NULL
                )
                WHERE ROWNUM <= 1000
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
                    String value = row.getString("VALUE");  // Oracle returns uppercase column aliases
                    Integer frequency = row.getInteger("FREQUENCY");  // Oracle returns uppercase
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
     * Check if a WHERE clause hint would be nonsensical
     * E.g., searching for "orders" in ORDERS.ORDER_ID makes no sense
     */
    private boolean isNonsensicalHint(String term, String table, String column) {
        // Normalize for comparison
        String termUpper = term.toUpperCase();
        String tableUpper = table.toUpperCase();
        String columnUpper = column.toUpperCase();
        
        // Check if term matches table name (or singular/plural variations)
        boolean termMatchesTable = termUpper.equals(tableUpper) ||
                                   termUpper.equals(tableUpper + "S") ||
                                   (termUpper + "S").equals(tableUpper) ||
                                   termUpper.equals(tableUpper.replaceAll("S$", "")) ||
                                   (termUpper + "ES").equals(tableUpper);
        
        // If term matches table name and column is an ID column, it's nonsensical
        if (termMatchesTable) {
            // Check if column is likely an ID column
            if (columnUpper.endsWith("_ID") || 
                columnUpper.equals("ID") ||
                columnUpper.contains(tableUpper.replaceAll("S$", "") + "_ID") ||
                columnUpper.equals(tableUpper.replaceAll("S$", "") + "ID")) {
                return true; // Nonsensical to search for table name in its ID column
            }
            
            // Also nonsensical to search for table name in DATE columns
            if (columnUpper.endsWith("_DATE") || columnUpper.endsWith("_TIME")) {
                return true;
            }
        }
        
        return false;
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
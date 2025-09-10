package agents.director.hosts;

import agents.director.services.LlmAPIService;
import agents.director.services.InterruptManager;
import agents.director.hosts.base.managers.*;
import io.vertx.core.*;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Oracle SQL Builder Host - Generates SQL without execution.
 * This host processes natural language queries and produces validated SQL
 * that users can review, modify, or execute themselves.
 * 
 * Key responsibilities:
 * 1. Analyze user intent for SQL generation
 * 2. Map business terms to schema
 * 3. Generate optimized SQL
 * 4. Validate SQL syntax and schema
 * 5. Return SQL with explanations
 */
public class OracleSQLBuilderHost extends AbstractVerticle {
    
    
    
    // MCP Managers for orchestrating SQL generation
    private SQLPipelineManager sqlPipelineManager;
    private SchemaIntelligenceManager schemaIntelligenceManager;
    private IntentAnalysisManager intentAnalysisManager;
    private StrategyOrchestrationManager strategyOrchestrationManager;
    
    // Service references
    private EventBus eventBus;
    private LlmAPIService llmService;
    
    // Dynamic strategy references (no static config needed)
    
    // SQL generation context
    private final Map<String, SQLBuildContext> buildContexts = new ConcurrentHashMap<>();
    
    // Performance metrics
    private final Map<String, Long> performanceMetrics = new ConcurrentHashMap<>();
    
    // Inner class for SQL build context
    private static class SQLBuildContext {
        String requestId;
        String originalQuery;
        String conversationId;
        String sessionId;
        boolean streaming;
        Map<String, JsonObject> stepResults = new HashMap<>();
        List<String> explanations = new ArrayList<>();
        long startTime;
        
        SQLBuildContext(String requestId, String query) {
            this.requestId = requestId;
            this.originalQuery = query;
            this.startTime = System.currentTimeMillis();
        }
        
        void addExplanation(String explanation) {
            explanations.add(explanation);
        }
        
        void storeStepResult(String stepName, JsonObject result) {
            stepResults.put(stepName, result);
        }
        
        JsonObject getStepResult(String stepName) {
            return stepResults.get(stepName);
        }
    }
    
    @Override
    public void start(Promise<Void> startPromise) {
        eventBus = vertx.eventBus();
        llmService = LlmAPIService.getInstance();
        
        // Load configuration and initialize managers
        loadOrchestrationConfig()
            .compose(v -> initializeManagers())
            .compose(v -> registerEventBusConsumers())
            .onComplete(ar -> {
                if (ar.succeeded()) {
                    vertx.eventBus().publish("log", "OracleSQLBuilderHost started successfully with 4 managers,2,OracleSQLBuilderHost,Host,System");
                    startPromise.complete();
                } else {
                    vertx.eventBus().publish("log", "Failed to start OracleSQLBuilderHost" + ",0,OracleSQLBuilderHost,Host,System");
                    startPromise.fail(ar.cause());
                }
            });
    }
    
    private Future<Void> loadOrchestrationConfig() {
        Promise<Void> promise = Promise.<Void>promise();
        
        // No static configuration needed - using dynamic strategies
        vertx.eventBus().publish("log", "OracleSQLBuilderHost configured for DYNAMIC strategy generation,2,OracleSQLBuilderHost,Host,System");
        promise.complete();
        
        return promise.future();
    }
    
    private Future<Void> initializeManagers() {
        Promise<Void> promise = Promise.<Void>promise();
        String baseUrl = "http://localhost:8080";
        
        // Initialize managers needed for SQL generation (no execution manager)
        sqlPipelineManager = new SQLPipelineManager(vertx, baseUrl);
        schemaIntelligenceManager = new SchemaIntelligenceManager(vertx, baseUrl);
        intentAnalysisManager = new IntentAnalysisManager(vertx, baseUrl);
        strategyOrchestrationManager = new StrategyOrchestrationManager(vertx, baseUrl);
        
        // Initialize all managers in parallel
        List<Future> initFutures = Arrays.asList(
            sqlPipelineManager.initialize(),
            schemaIntelligenceManager.initialize(),
            intentAnalysisManager.initialize(),
            strategyOrchestrationManager.initialize()
        );
        
        CompositeFuture.all(initFutures).onComplete(ar -> {
            if (ar.succeeded()) {
                vertx.eventBus().publish("log", "All SQL builder managers initialized,2,OracleSQLBuilderHost,Host,System");
                promise.complete();
            } else {
                vertx.eventBus().publish("log", "Failed to initialize managers: " + ar.cause().getMessage() + ",0,OracleSQLBuilderHost,Host,System");
                promise.fail(ar.cause());
            }
        });
        
        return promise.future();
    }
    
    private Future<Void> registerEventBusConsumers() {
        Promise<Void> promise = Promise.<Void>promise();
        
        // Main processing endpoint
        eventBus.<JsonObject>consumer("host.oraclesqlbuilder.process", this::processQuery);
        
        // Status endpoint
        eventBus.<JsonObject>consumer("host.oraclesqlbuilder.status", message -> {
            message.reply(new JsonObject()
                .put("status", "ready")
                .put("managers", 4)
                .put("activeBuildContexts", buildContexts.size()));
        });
        
        promise.complete();
        return promise.future();
    }
    
    /**
     * Main query processing - generates SQL without execution
     */
    private void processQuery(Message<JsonObject> message) {
        JsonObject request = message.body();
        String query = request.getString("query");
        String requestId = request.getString("requestId", UUID.randomUUID().toString());
        String conversationId = request.getString("conversationId", requestId); // Use for streaming events
        String sessionId = request.getString("sessionId"); // For streaming
        boolean streaming = request.getBoolean("streaming", false);
        JsonObject options = request.getJsonObject("options", new JsonObject());
        
        vertx.eventBus().publish("log", "Building SQL for request " + requestId + ": " + query + "" + ",2,OracleSQLBuilderHost,Host,System");
        
        // Publish start event if streaming
        if (sessionId != null && streaming) {
            publishStreamingEvent(conversationId, "progress", new JsonObject()
                .put("step", "host_started")
                .put("message", "Starting SQL generation")
                .put("details", new JsonObject()
                    .put("host", "OracleSQLBuilderHost")
                    .put("query", query)));
        }
        
        // Check for interrupts if streaming
        if (sessionId != null && streaming) {
            InterruptManager im = new InterruptManager(vertx);
            if (im.isInterrupted(sessionId)) {
                publishStreamingEvent(conversationId, "interrupt", new JsonObject()
                    .put("reason", "User interrupted")
                    .put("message", "SQL generation interrupted by user"));
                message.reply(new JsonObject()
                    .put("interrupted", true)
                    .put("message", "SQL generation interrupted by user"));
                return;
            }
        }
        
        // Create build context
        SQLBuildContext context = new SQLBuildContext(requestId, query);
        context.conversationId = conversationId;
        context.sessionId = sessionId;
        context.streaming = streaming;
        buildContexts.put(requestId, context);
        
        // Publish tool start event if streaming
        if (sessionId != null && streaming) {
            publishStreamingEvent(conversationId, "tool.start", new JsonObject()
                .put("tool", "sql_generation_pipeline")
                .put("description", "Multi-stage SQL generation and validation")
                .put("parameters", new JsonObject()
                    .put("includeValidation", options.getBoolean("validate", true))
                    .put("includeOptimization", options.getBoolean("optimize", true))));
        }
        
        // Execute SQL generation pipeline
        executeSQLGenerationPipeline(context, options)
            .onComplete(ar -> {
                if (ar.succeeded()) {
                    JsonObject response = ar.result();
                    response.put("requestId", requestId);
                    response.put("conversationId", conversationId);
                    response.put("duration", System.currentTimeMillis() - context.startTime);
                    
                    // Publish complete event if streaming
                    if (sessionId != null && streaming) {
                        publishStreamingEvent(conversationId, "tool.complete", new JsonObject()
                            .put("tool", "sql_generation_pipeline")
                            .put("success", true)
                            .put("resultSummary", "Generated " + 
                                (response.getString("sql", "").split("\n").length) + 
                                " lines of SQL"));
                        
                        // Publish final event
                        publishStreamingEvent(conversationId, "final", new JsonObject()
                            .put("content", formatSQLResponse(response))
                            .put("sql", response.getString("sql", ""))
                            .put("validated", response.getBoolean("validated", false))
                            .put("conversationId", conversationId));
                    }
                    
                    message.reply(response);
                } else {
                    vertx.eventBus().publish("log", "SQL generation failed for request " + requestId + ": " + ar.cause().getMessage() + ",0,OracleSQLBuilderHost,Host,System");
                    
                    // Publish error event if streaming
                    if (sessionId != null && streaming) {
                        publishStreamingEvent(conversationId, "error", new JsonObject()
                            .put("error", ar.cause().getMessage())
                            .put("severity", "ERROR")
                            .put("tool", "sql_generation_pipeline"));
                    }
                    
                    message.fail(500, ar.cause().getMessage());
                }
                
                // Clean up context
                buildContexts.remove(requestId);
                cleanupOldContexts();
            });
    }
    
    /**
     * Execute the SQL generation pipeline with dynamic strategy
     */
    private Future<JsonObject> executeSQLGenerationPipeline(SQLBuildContext context, JsonObject options) {
        Promise<JsonObject> promise = Promise.promise();
        
        // First generate a dynamic strategy for SQL generation
        generateSQLBuilderStrategy(context.originalQuery)
            .compose(strategy -> {
                JsonArray steps = strategy.getJsonArray("steps", new JsonArray());
                String generationMethod = strategy.getString("generation_method", "unknown");
                
                // Track pipeline progress
                if (generationMethod.startsWith("fallback")) {
                    context.addExplanation("Using fallback SQL generation strategy");
                    
                    // Publish fallback notification if streaming
                    if (context.sessionId != null && context.streaming) {
                        publishStreamingEvent(context.conversationId, "progress", new JsonObject()
                            .put("step", "strategy_fallback")
                            .put("message", "Using fallback SQL generation strategy")
                            .put("details", new JsonObject()
                                .put("method", generationMethod)
                                .put("reason", strategy.getString("fallback_reason", "Strategy generation failed"))));
                    }
                } else {
                    context.addExplanation("Using dynamically generated SQL generation strategy");
                    
                    // Publish dynamic generation notification if streaming
                    if (context.sessionId != null && context.streaming) {
                        publishStreamingEvent(context.conversationId, "progress", new JsonObject()
                            .put("step", "strategy_generated")
                            .put("message", "Dynamically generated SQL-specific strategy")
                            .put("details", new JsonObject()
                                .put("method", generationMethod)
                                .put("steps", steps.size())));
                    }
                }
                
                context.addExplanation("Starting SQL generation for: " + context.originalQuery);
                
                // Execute pipeline steps
                return executePipelineSteps(context, steps, 0, options)
                    .compose(result -> formatSQLResult(context, result, options));
            })
            .onComplete(ar -> {
                if (ar.succeeded()) {
                    // Record execution for learning
                    recordSQLGenerationExecution(context, true);
                    promise.complete(ar.result());
                } else {
                    recordSQLGenerationExecution(context, false);
                    promise.fail(ar.cause());
                }
            });
        
        return promise.future();
    }
    
    /**
     * Generate a dynamic strategy specifically for SQL building
     */
    private Future<JsonObject> generateSQLBuilderStrategy(String query) {
        Promise<JsonObject> promise = Promise.<JsonObject>promise();
        
        if (strategyOrchestrationManager == null || !strategyOrchestrationManager.isReady()) {
            // Use fallback strategy
            promise.complete(getFallbackSQLStrategy());
            return promise.future();
        }
        
        // Use strategy orchestration manager to generate SQL-specific strategy
        strategyOrchestrationManager.generateDynamicStrategy(
            query,
            new JsonArray(), // No conversation history for SQL-only
            "intermediate",  // expertise level
            new JsonObject()
                .put("mode", "sql_only")
                .put("max_steps", 8)
                .put("required_validations", new JsonArray()
                    .add("syntax")
                    .add("schema"))
        )
            .onComplete(ar -> {
                if (ar.succeeded()) {
                    promise.complete(ar.result());
                } else {
                    vertx.eventBus().publish("log", "Failed to generate dynamic SQL strategy: " + ar.cause() + ",1,OracleSQLBuilderHost,Host,System");
                    promise.complete(getFallbackSQLStrategy());
                }
            });
        
        return promise.future();
    }
    
    /**
     * Get a fallback SQL generation strategy
     */
    private JsonObject getFallbackSQLStrategy() {
        return new JsonObject()
            .put("name", "Fallback SQL Generation")
            .put("generation_method", "fallback_sql_builder")
            .put("method", "fallback")
            .put("fallback_reason", "Strategy generation service unavailable")
            .put("steps", new JsonArray()
                .add(new JsonObject()
                    .put("tool", "analyze_query")
                    .put("server", "OracleQueryAnalysis")
                    .put("description", "Analyze the natural language query"))
                .add(new JsonObject()
                    .put("tool", "match_oracle_schema")
                    .put("server", "OracleSchemaIntelligence")
                    .put("description", "Find relevant schema elements"))
                .add(new JsonObject()
                    .put("tool", "generate_oracle_sql")
                    .put("server", "OracleSQLGeneration")
                    .put("description", "Generate the SQL query"))
                .add(new JsonObject()
                    .put("tool", "validate_oracle_sql")
                    .put("server", "OracleSQLValidation")
                    .put("description", "Validate the generated SQL")));
    }
    
    /**
     * Record execution for learning
     */
    private void recordSQLGenerationExecution(SQLBuildContext context, boolean success) {
        if (strategyOrchestrationManager != null && strategyOrchestrationManager.isReady()) {
            JsonObject strategy = new JsonObject()
                .put("name", "SQL Generation Pipeline")
                .put("type", "sql_only");
            
            JsonObject executionResults = new JsonObject()
                .put("success", success)
                .put("total_duration", System.currentTimeMillis() - context.startTime)
                .put("steps_completed", context.stepResults.size());
            
            JsonObject performanceMetrics = new JsonObject()
                .put("query_complexity", 0.5f); // Could be enhanced
            
            strategyOrchestrationManager.recordExecution(strategy, executionResults, performanceMetrics)
                .onComplete(ar -> {
                    if (ar.failed()) {
                        vertx.eventBus().publish("log", "Failed to record SQL generation: " + ar.cause() + ",1,OracleSQLBuilderHost,Host,System");
                    }
                });
        }
    }
    
    /**
     * Execute pipeline steps sequentially
     */
    private Future<JsonObject> executePipelineSteps(SQLBuildContext context, 
                                                   JsonArray steps, 
                                                   int currentStepIndex,
                                                   JsonObject options) {
        Promise<JsonObject> promise = Promise.promise();
        
        if (currentStepIndex >= steps.size()) {
            promise.complete(new JsonObject());
            return promise.future();
        }
        
        JsonObject step = steps.getJsonObject(currentStepIndex);
        String tool = step.getString("tool");
        String description = step.getString("description");
        boolean optional = step.getBoolean("optional", false);
        
        vertx.eventBus().publish("log", "Executing step " + tool + ": " + description + "" + ",3,OracleSQLBuilderHost,Host,System");
        context.addExplanation("Step " + (currentStepIndex + 1) + ": " + description);
        
        executeStep(context, step, options)
            .onComplete(ar -> {
                if (ar.succeeded()) {
                    context.storeStepResult(tool, ar.result());
                    
                    // Publish progress event for step completion if streaming
                    if (context.sessionId != null && context.streaming) {
                        publishStreamingEvent(context.conversationId, "progress", new JsonObject()
                            .put("step", "sql_pipeline_step_complete")
                            .put("message", "Completed: " + description)
                            .put("details", new JsonObject()
                                .put("tool", tool)
                                .put("success", true)));
                    }
                    
                    // Check for interrupts before continuing
                    if (context.sessionId != null && context.streaming) {
                        InterruptManager im = new InterruptManager(vertx);
                        if (im.isInterrupted(context.sessionId)) {
                            publishStreamingEvent(context.conversationId, "interrupt", new JsonObject()
                                .put("reason", "User interrupted")
                                .put("message", "SQL generation interrupted at step " + (currentStepIndex + 1)));
                            promise.fail("User interrupted SQL generation");
                            return;
                        }
                    }
                    
                    // Continue to next step
                    executePipelineSteps(context, steps, currentStepIndex + 1, options)
                        .onComplete(promise);
                        
                } else if (optional) {
                    vertx.eventBus().publish("log", "Optional step " + tool + " failed: " + ar.cause() + "" + ",1,OracleSQLBuilderHost,Host,System");
                    context.addExplanation("Optional step skipped: " + description);
                    
                    // Publish warning event if streaming
                    if (context.sessionId != null && context.streaming) {
                        publishStreamingEvent(context.conversationId, "progress", new JsonObject()
                            .put("step", "sql_pipeline_step_skipped")
                            .put("message", "Skipped optional step: " + description)
                            .put("details", new JsonObject()
                                .put("tool", tool)
                                .put("reason", ar.cause().getMessage())
                                .put("severity", "WARNING")));
                    }
                    
                    // Continue anyway
                    executePipelineSteps(context, steps, currentStepIndex + 1, options)
                        .onComplete(promise);
                        
                } else {
                    // Required step failed
                    context.addExplanation("ERROR: " + ar.cause().getMessage());
                    promise.fail(ar.cause());
                }
            });
        
        return promise.future();
    }
    
    /**
     * Execute a single step in the pipeline
     */
    private Future<JsonObject> executeStep(SQLBuildContext context, 
                                         JsonObject step, 
                                         JsonObject options) {
        Promise<JsonObject> promise = Promise.promise();
        
        String tool = step.getString("tool");
        String server = step.getString("server");
        
        // Build arguments based on tool and context
        JsonObject arguments = buildToolArguments(context, tool, options);
        
        long startTime = System.currentTimeMillis();
        
        // Route tool calls through appropriate managers
        Future<JsonObject> toolFuture = routeToolCallToManager(tool, server, arguments, context);
        
        toolFuture
            .onComplete(ar -> {
                long duration = System.currentTimeMillis() - startTime;
                performanceMetrics.put(tool, duration);
                
                if (ar.succeeded()) {
                    vertx.eventBus().publish("log", "Step " + tool + " completed in " + duration + "ms" + ",3,OracleSQLBuilderHost,Host,System");
                    
                    // Add step-specific explanations
                    addStepExplanation(context, tool, ar.result());
                    
                    promise.complete(ar.result());
                } else {
                    vertx.eventBus().publish("log", "Step " + tool + " failed: " + ar.cause().getMessage() + ",0,OracleSQLBuilderHost,Host,System");
                    promise.fail("Step " + tool + " failed: " + ar.cause().getMessage());
                }
            });
        
        return promise.future();
    }
    
    /**
     * Build arguments for each tool
     */
    private JsonObject buildToolArguments(SQLBuildContext context, String tool, JsonObject options) {
        JsonObject args = new JsonObject();
        
        switch (tool) {
            case "analyze_query":
                args.put("query", context.originalQuery);
                args.put("context", new JsonArray()); // No conversation history for SQL builder
                break;
                
            case "match_oracle_schema":
                JsonObject analysis = context.getStepResult("analyze_query");
                if (analysis != null) {
                    args.put("analysis", analysis);
                    args.put("maxSuggestions", options.getInteger("maxTableSuggestions", 5));
                    args.put("confidenceThreshold", options.getDouble("schemaConfidenceThreshold", 0.6));
                }
                break;
                
            case "map_business_terms":
                JsonObject queryAnalysis = context.getStepResult("analyze_query");
                JsonObject schemaMatches = context.getStepResult("match_oracle_schema");
                
                if (queryAnalysis != null) {
                    JsonArray entities = queryAnalysis.getJsonArray("entities", new JsonArray());
                    args.put("terms", entities);
                    
                    if (schemaMatches != null) {
                        args.put("context", new JsonObject()
                            .put("queryAnalysis", queryAnalysis)
                            .put("schemaMatches", schemaMatches));
                    }
                }
                break;
                
            case "generate_oracle_sql":
                args.put("analysis", context.getStepResult("analyze_query"));
                args.put("schemaMatches", context.getStepResult("match_oracle_schema"));
                args.put("includeEnums", options.getBoolean("includeEnums", true));
                args.put("maxComplexity", options.getString("maxComplexity", "complex"));
                break;
                
            case "validate_oracle_sql":
                JsonObject generated = context.getStepResult("generate_oracle_sql");
                if (generated != null) {
                    args.put("sql", generated.getString("sql"));
                    args.put("checkPermissions", options.getBoolean("checkPermissions", false));
                    args.put("suggestFixes", true);
                }
                break;
        }
        
        return args;
    }
    
    /**
     * Add step-specific explanations
     */
    private void addStepExplanation(SQLBuildContext context, String tool, JsonObject result) {
        switch (tool) {
            case "analyze_query":
                String queryType = result.getString("queryType", "unknown");
                String intent = result.getString("intent", "");
                context.addExplanation("Query type identified: " + queryType + 
                    (intent.isEmpty() ? "" : " (" + intent + ")"));
                break;
                
            case "match_oracle_schema":
                JsonArray matches = result.getJsonArray("matches", new JsonArray());
                if (!matches.isEmpty()) {
                    List<String> tables = new ArrayList<>();
                    for (int i = 0; i < matches.size(); i++) {
                        JsonObject match = matches.getJsonObject(i);
                        JsonObject table = match.getJsonObject("table", new JsonObject());
                        tables.add(table.getString("tableName", "unknown"));
                    }
                    context.addExplanation("Relevant tables found: " + String.join(", ", tables));
                }
                break;
                
            case "generate_oracle_sql":
                JsonObject metadata = result.getJsonObject("metadata", new JsonObject());
                boolean hasJoins = metadata.getBoolean("hasJoins", false);
                boolean hasAggregation = metadata.getBoolean("hasAggregation", false);
                
                List<String> features = new ArrayList<>();
                if (hasJoins) features.add("table joins");
                if (hasAggregation) features.add("aggregations");
                
                if (!features.isEmpty()) {
                    context.addExplanation("SQL includes: " + String.join(", ", features));
                }
                break;
                
            case "validate_oracle_sql":
                boolean valid = result.getBoolean("valid", false);
                if (valid) {
                    context.addExplanation("SQL validation passed ✓");
                } else {
                    JsonArray errors = result.getJsonArray("errors", new JsonArray());
                    context.addExplanation("Validation issues found: " + errors.size());
                }
                break;
        }
    }
    
    /**
     * Format the final SQL result with all context
     */
    private Future<JsonObject> formatSQLResult(SQLBuildContext context, 
                                             JsonObject pipelineResult,
                                             JsonObject options) {
        Promise<JsonObject> promise = Promise.promise();
        
        JsonObject response = new JsonObject();
        
        // Get the generated SQL
        JsonObject sqlGenResult = context.getStepResult("generate_oracle_sql");
        JsonObject validationResult = context.getStepResult("validate_oracle_sql");
        
        if (sqlGenResult == null || !sqlGenResult.containsKey("sql")) {
            promise.fail("No SQL was generated");
            return promise.future();
        }
        
        String sql = sqlGenResult.getString("sql");
        
        // Check if validation suggested fixes
        if (validationResult != null && !validationResult.getBoolean("valid", true)) {
            if (validationResult.containsKey("suggestedSQL")) {
                sql = validationResult.getString("suggestedSQL");
                context.addExplanation("SQL was modified to fix validation errors");
            }
        }
        
        // Build the response
        response.put("sql", sql);
        response.put("originalQuery", context.originalQuery);
        response.put("explanations", new JsonArray(context.explanations));
        
        // Add validation status
        if (validationResult != null) {
            response.put("validated", validationResult.getBoolean("valid", false));
            
            if (validationResult.containsKey("errors")) {
                response.put("validationErrors", validationResult.getJsonArray("errors"));
            }
            
            if (validationResult.containsKey("validationSteps")) {
                response.put("validationDetails", validationResult.getJsonArray("validationSteps"));
            }
        }
        
        // Add metadata
        JsonObject metadata = new JsonObject();
        
        // Query analysis metadata
        JsonObject analysis = context.getStepResult("analyze_query");
        if (analysis != null) {
            metadata.put("queryType", analysis.getString("queryType"));
            metadata.put("complexity", analysis.getString("complexity"));
            metadata.put("entities", analysis.getJsonArray("entities", new JsonArray()));
        }
        
        // Schema matching metadata
        JsonObject schemaMatches = context.getStepResult("match_oracle_schema");
        if (schemaMatches != null) {
            JsonArray matches = schemaMatches.getJsonArray("matches", new JsonArray());
            JsonArray tables = new JsonArray();
            
            for (int i = 0; i < matches.size(); i++) {
                JsonObject match = matches.getJsonObject(i);
                JsonObject table = match.getJsonObject("table", new JsonObject());
                tables.add(new JsonObject()
                    .put("name", table.getString("tableName"))
                    .put("confidence", match.getDouble("confidence"))
                    .put("columns", match.getJsonArray("relevantColumns", new JsonArray())));
            }
            
            metadata.put("tablesUsed", tables);
        }
        
        // SQL generation metadata
        if (sqlGenResult.containsKey("metadata")) {
            metadata.mergeIn(sqlGenResult.getJsonObject("metadata"));
        }
        
        response.put("metadata", metadata);
        
        // Add formatted SQL if requested
        if (options.getBoolean("formatSQL", true)) {
            response.put("formattedSQL", formatSQL(sql));
        }
        
        // Add explanation summary if requested
        if (options.getBoolean("includeSummary", true) && llmService.isInitialized()) {
            generateSQLSummary(context, sql, metadata)
                .onComplete(ar -> {
                    if (ar.succeeded()) {
                        response.put("summary", ar.result());
                    }
                    promise.complete(response);
                });
        } else {
            promise.complete(response);
        }
        
        return promise.future();
    }
    
    /**
     * Format SQL for better readability
     */
    private String formatSQL(String sql) {
        // Simple formatting - in production, use a proper SQL formatter
        return sql
            .replace("SELECT", "\nSELECT")
            .replace("FROM", "\nFROM")
            .replace("WHERE", "\nWHERE")
            .replace("GROUP BY", "\nGROUP BY")
            .replace("HAVING", "\nHAVING")
            .replace("ORDER BY", "\nORDER BY")
            .replace("JOIN", "\n  JOIN")
            .replace("LEFT JOIN", "\n  LEFT JOIN")
            .replace("RIGHT JOIN", "\n  RIGHT JOIN")
            .replace("INNER JOIN", "\n  INNER JOIN")
            .replace(",", ",\n  ")
            .trim();
    }
    
    /**
     * Generate a natural language summary of the SQL
     */
    private Future<String> generateSQLSummary(SQLBuildContext context, String sql, JsonObject metadata) {
        Promise<String> promise = Promise.<String>promise();
        
        String systemPrompt = """
            You are a SQL expert. Provide a brief, user-friendly explanation of what this SQL query does.
            Focus on:
            1. What data it retrieves
            2. Any filters or conditions
            3. How results are organized
            4. Any calculations or aggregations
            
            Keep it concise (2-3 sentences max).
            """;
        
        JsonObject promptData = new JsonObject()
            .put("originalQuestion", context.originalQuery)
            .put("sql", sql)
            .put("metadata", metadata);
        
        List<JsonObject> messages = Arrays.asList(
            new JsonObject().put("role", "system").put("content", systemPrompt),
            new JsonObject().put("role", "user").put("content", 
                "Original question: " + context.originalQuery + "\n\n" +
                "Generated SQL:\n" + sql + "\n\n" +
                "Please explain what this SQL does.")
        );
        
        llmService.chatCompletion(
            messages.stream().map(JsonObject::encode).collect(Collectors.toList()),
            0.3, // Low temperature for factual summary
            200
        ).whenComplete((result, error) -> {
            if (error == null) {
                String summary = result.getJsonArray("choices")
                    .getJsonObject(0)
                    .getJsonObject("message")
                    .getString("content");
                promise.complete(summary);
            } else {
                // Fallback summary
                promise.complete("This query retrieves data based on your request.");
            }
        });
        
        return promise.future();
    }
    
    /**
     * Clean up old build contexts
     */
    private void cleanupOldContexts() {
        long cutoffTime = System.currentTimeMillis() - (10 * 60 * 1000); // 10 minutes
        
        buildContexts.entrySet().removeIf(entry -> {
            SQLBuildContext context = entry.getValue();
            return context.startTime < cutoffTime;
        });
    }
    
    @Override
    public void stop(Promise<Void> stopPromise) {
        // Shutdown all managers
        List<Future> shutdownFutures = new ArrayList<>();
        
        if (sqlPipelineManager != null) shutdownFutures.add(sqlPipelineManager.shutdown());
        if (schemaIntelligenceManager != null) shutdownFutures.add(schemaIntelligenceManager.shutdown());
        if (intentAnalysisManager != null) shutdownFutures.add(intentAnalysisManager.shutdown());
        if (strategyOrchestrationManager != null) shutdownFutures.add(strategyOrchestrationManager.shutdown());
        
        CompositeFuture.all(shutdownFutures).onComplete(ar -> {
            // Clean up resources
            buildContexts.clear();
            performanceMetrics.clear();
            
            vertx.eventBus().publish("log", "OracleSQLBuilderHost stopped,2,OracleSQLBuilderHost,Host,System");
            stopPromise.complete();
        });
    }
    
    /**
     * Route tool calls to the appropriate manager
     */
    private Future<JsonObject> routeToolCallToManager(String tool, String server, JsonObject arguments, SQLBuildContext context) {
        // Route based on server/tool type - expect proper server names from strategy
        switch (server) {
            case "OracleQueryAnalysis":
                return sqlPipelineManager.callClientTool("analysis", tool, arguments);
                
            case "OracleSQLGeneration":
                return sqlPipelineManager.callClientTool("generation", tool, arguments);
                
            case "OracleSQLValidation":
                return sqlPipelineManager.callClientTool("validation", tool, arguments);
                
            case "OracleSchemaIntelligence":
                return schemaIntelligenceManager.callClientTool("schema", tool, arguments);
                
            case "BusinessMapping":
                return schemaIntelligenceManager.callClientTool("business", tool, arguments);
                
            case "IntentAnalysis":
                return intentAnalysisManager.callClientTool("analysis", tool, arguments);
                
            case "StrategyGeneration":
                return strategyOrchestrationManager.callClientTool("generation", tool, arguments);
                
            case "StrategyLearning":
                return strategyOrchestrationManager.callClientTool("learning", tool, arguments);
                
            default:
                vertx.eventBus().publish("log", "WARNING: Unknown server '" + server + "' for tool '" + tool + "',1,OracleSQLBuilderHost,Host,System");
                return Future.failedFuture("Unknown server: " + server);
        }
    }
    
    /**
     * Publish streaming event to the correct event bus address
     */
    private void publishStreamingEvent(String conversationId, String eventType, JsonObject data) {
        // Use "streaming." prefix to match ConversationStreaming expectations
        String address = "streaming." + conversationId + "." + eventType;
        data.put("timestamp", System.currentTimeMillis());
        data.put("host", "OracleSQLBuilderHost");
        eventBus.publish(address, data);
    }
    
    /**
     * Format SQL response for final event
     */
    private String formatSQLResponse(JsonObject response) {
        StringBuilder sb = new StringBuilder();
        sb.append("Generated SQL Query:\n\n");
        sb.append(response.getString("sql", "No SQL generated"));
        
        if (response.getBoolean("validated", false)) {
            sb.append("\n\n✓ SQL syntax validated");
        }
        
        if (response.containsKey("explanation")) {
            sb.append("\n\nExplanation: ").append(response.getString("explanation"));
        }
        
        return sb.toString();
    }
}
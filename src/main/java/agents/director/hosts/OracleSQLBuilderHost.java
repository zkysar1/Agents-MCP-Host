package agents.director.hosts;

import agents.director.mcp.client.UniversalMCPClient;
import agents.director.services.LlmAPIService;
import agents.director.services.InterruptManager;
import io.vertx.core.*;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;


import java.io.InputStream;
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
    
    
    
    // MCP Clients needed for SQL generation
    private final Map<String, UniversalMCPClient> mcpClients = new ConcurrentHashMap<>();
    
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
        
        // Load configuration and initialize clients
        loadOrchestrationConfig()
            .compose(v -> initializeMCPClients())
            .compose(v -> registerEventBusConsumers())
            .onComplete(ar -> {
                if (ar.succeeded()) {
                    vertx.eventBus().publish("log", "OracleSQLBuilderHost started successfully with " + mcpClients.size() + " MCP clients,2,OracleSQLBuilderHost,Host,System");
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
    
    private Future<Void> initializeMCPClients() {
        Promise<Void> promise = Promise.<Void>promise();
        List<Future> clientFutures = new ArrayList<>();
        
        String baseUrl = "http://localhost:8080";
        
        // We need these clients for SQL generation
        
        // Query Analysis Client
        UniversalMCPClient analysisClient = new UniversalMCPClient(
            "OracleQueryAnalysis",
            baseUrl + "/mcp/servers/oracle-query-analysis"
        );
        clientFutures.add(deployClient(analysisClient, "oracle-query-analysis"));
        
        // Schema Intelligence Client
        UniversalMCPClient schemaClient = new UniversalMCPClient(
            "OracleSchemaIntelligence",
            baseUrl + "/mcp/servers/oracle-schema-intel"
        );
        clientFutures.add(deployClient(schemaClient, "oracle-schema-intel"));
        
        // Business Mapping Client
        UniversalMCPClient businessClient = new UniversalMCPClient(
            "BusinessMapping",
            baseUrl + "/mcp/servers/business-map"
        );
        clientFutures.add(deployClient(businessClient, "business-map"));
        
        // SQL Generation Client
        UniversalMCPClient sqlGenClient = new UniversalMCPClient(
            "OracleSQLGeneration",
            baseUrl + "/mcp/servers/oracle-sql-gen"
        );
        clientFutures.add(deployClient(sqlGenClient, "oracle-sql-gen"));
        
        // SQL Validation Client
        UniversalMCPClient sqlValClient = new UniversalMCPClient(
            "OracleSQLValidation",
            baseUrl + "/mcp/servers/oracle-sql-val"
        );
        clientFutures.add(deployClient(sqlValClient, "oracle-sql-val"));
        
        // Dynamic Strategy Generation Clients
        // Strategy Generation Client
        UniversalMCPClient strategyGenClient = new UniversalMCPClient(
            "StrategyGeneration",
            baseUrl + "/mcp/servers/strategy-gen"
        );
        clientFutures.add(deployClient(strategyGenClient, "strategy-generation"));
        
        // Intent Analysis Client
        UniversalMCPClient intentAnalysisClient = new UniversalMCPClient(
            "IntentAnalysis",
            baseUrl + "/mcp/servers/intent-analysis"
        );
        clientFutures.add(deployClient(intentAnalysisClient, "intent-analysis"));
        
        // Strategy Learning Client (for recording SQL-only executions)
        UniversalMCPClient learningClient = new UniversalMCPClient(
            "StrategyLearning",
            baseUrl + "/mcp/servers/strategy-learning"
        );
        clientFutures.add(deployClient(learningClient, "strategy-learning"));
        
        // Wait for all clients
        CompositeFuture.all(clientFutures).onComplete(ar -> {
            if (ar.succeeded()) {
                vertx.eventBus().publish("log", "All SQL builder clients initialized,2,OracleSQLBuilderHost,Host,System");
                promise.complete();
            } else {
                promise.fail(ar.cause());
            }
        });
        
        return promise.future();
    }
    
    private Future<String> deployClient(UniversalMCPClient client, String serverKey) {
        Promise<String> promise = Promise.<String>promise();
        
        vertx.deployVerticle(client, ar -> {
            if (ar.succeeded()) {
                mcpClients.put(serverKey, client);
                vertx.eventBus().publish("log", "Deployed client for " + serverKey + "" + ",3,OracleSQLBuilderHost,Host,System");
                promise.complete(ar.result());
            } else {
                vertx.eventBus().publish("log", "Failed to deploy client for " + serverKey + "" + ",0,OracleSQLBuilderHost,Host,System");
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
                .put("clients", mcpClients.size())
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
        
        UniversalMCPClient intentClient = mcpClients.get("intent-analysis");
        UniversalMCPClient strategyGenClient = mcpClients.get("strategy-generation");
        
        if (intentClient == null || strategyGenClient == null) {
            // Use fallback strategy
            promise.complete(getFallbackSQLStrategy());
            return promise.future();
        }
        
        // Analyze intent for SQL generation
        JsonObject intentArgs = new JsonObject()
            .put("query", query)
            .put("conversation_history", new JsonArray());
        
        // First, fetch available tools from MCP Registry
        Promise<JsonArray> toolsPromise = Promise.promise();
        eventBus.request("mcp.tools.list", new JsonObject(), ar -> {
            if (ar.succeeded()) {
                JsonObject toolsResponse = (JsonObject) ar.result().body();
                JsonArray tools = toolsResponse.getJsonArray("tools", new JsonArray());
                
                // Transform tools into the format needed by strategy generation
                JsonArray availableTools = new JsonArray();
                for (int i = 0; i < tools.size(); i++) {
                    JsonObject tool = tools.getJsonObject(i);
                    String toolName = tool.getString("name");
                    
                    // Get the server name from the first client detail
                    JsonArray clientDetails = tool.getJsonArray("clientDetails", new JsonArray());
                    if (clientDetails.size() > 0) {
                        String serverName = clientDetails.getJsonObject(0).getString("serverName", "unknown");
                        availableTools.add(new JsonObject()
                            .put("name", toolName)
                            .put("server", serverName));
                    }
                }
                toolsPromise.complete(availableTools);
            } else {
                vertx.eventBus().publish("log", "Failed to fetch available tools: " + ar.cause() + ",1,OracleSQLBuilderHost,Host,System");
                // Continue with empty tools list
                toolsPromise.complete(new JsonArray());
            }
        });
        
        toolsPromise.future()
            .compose(availableTools -> 
                intentClient.callTool("intent_analysis__extract_intent", intentArgs)
                    .map(intentResult -> {
                        // Force intent to SQL generation mode
                        intentResult.put("primary_intent", "get_sql_only");
                        return new JsonObject()
                            .put("intent", intentResult)
                            .put("availableTools", availableTools);
                    })
            )
            .compose(data -> {
                // Analyze complexity
                JsonObject complexityArgs = new JsonObject()
                    .put("query", query)
                    .put("context", new JsonObject());
                
                return strategyGenClient.callTool("strategy_generation__analyze_complexity", complexityArgs)
                    .map(complexity -> data.put("complexity", complexity));
            })
            .compose(analysis -> {
                // Generate SQL-specific strategy with available tools
                JsonObject strategyArgs = new JsonObject()
                    .put("query", query)
                    .put("intent", analysis.getJsonObject("intent"))
                    .put("complexity_analysis", analysis.getJsonObject("complexity"))
                    .put("available_tools", analysis.getJsonArray("availableTools"))
                    .put("constraints", new JsonObject()
                        .put("max_steps", 8)  // Fewer steps for SQL-only
                        .put("required_validations", new JsonArray()
                            .add("syntax")
                            .add("schema")));
                
                return strategyGenClient.callTool("strategy_generation__create_strategy", strategyArgs);
            })
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
                    .put("server", "oracle-query-analysis")
                    .put("description", "Analyze the natural language query"))
                .add(new JsonObject()
                    .put("tool", "match_oracle_schema")
                    .put("server", "oracle-schema-intel")
                    .put("description", "Find relevant schema elements"))
                .add(new JsonObject()
                    .put("tool", "generate_oracle_sql")
                    .put("server", "oracle-sql-gen")
                    .put("description", "Generate the SQL query"))
                .add(new JsonObject()
                    .put("tool", "validate_oracle_sql")
                    .put("server", "oracle-sql-val")
                    .put("description", "Validate the generated SQL")));
    }
    
    /**
     * Record execution for learning
     */
    private void recordSQLGenerationExecution(SQLBuildContext context, boolean success) {
        UniversalMCPClient learningClient = mcpClients.get("strategy-learning");
        if (learningClient != null && learningClient.isReady()) {
            JsonObject recordArgs = new JsonObject()
                .put("strategy", new JsonObject()
                    .put("name", "SQL Generation Pipeline")
                    .put("type", "sql_only"))
                .put("execution_results", new JsonObject()
                    .put("success", success)
                    .put("total_duration", System.currentTimeMillis() - context.startTime)
                    .put("steps_completed", context.stepResults.size()))
                .put("performance_metrics", new JsonObject()
                    .put("query_complexity", 0.5f)); // Could be enhanced
            
            learningClient.callTool("strategy_learning__record_execution", recordArgs)
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
        
        UniversalMCPClient client = mcpClients.get(server);
        if (client == null || !client.isReady()) {
            promise.fail("Client not ready for: " + server);
            return promise.future();
        }
        
        // Build arguments based on tool and context
        JsonObject arguments = buildToolArguments(context, tool, options);
        
        long startTime = System.currentTimeMillis();
        
        client.callTool(tool, arguments)
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
        buildContexts.clear();
        performanceMetrics.clear();
        
        vertx.eventBus().publish("log", "OracleSQLBuilderHost stopped,2,OracleSQLBuilderHost,Host,System");
        stopPromise.complete();
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
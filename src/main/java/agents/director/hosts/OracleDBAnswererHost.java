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
 * Oracle Database Answerer Host - The full intelligence orchestrator.
 * This host implements the complete pipeline from user question to final answer,
 * orchestrating all MCP servers to provide intelligent database Q&A.
 * 
 * Key responsibilities:
 * 1. Evaluate query intent and select appropriate strategy
 * 2. Execute the full orchestration pipeline
 * 3. Handle errors and fallbacks intelligently
 * 4. Stream responses back to the user
 * 5. Manage conversation context
 */
public class OracleDBAnswererHost extends AbstractVerticle {
    
    // Debug flag for verbose logging
    private static final boolean DEBUG_RESPONSES = System.getProperty("oracledb.debug.responses", "false").equalsIgnoreCase("true");
    
    // MCP Managers for orchestrating clients
    private OracleExecutionManager executionManager;
    private SQLPipelineManager sqlPipelineManager;
    private SchemaIntelligenceManager schemaIntelligenceManager;
    private IntentAnalysisManager intentAnalysisManager;
    private StrategyOrchestrationManager strategyOrchestrationManager;
    
    // Service references
    private LlmAPIService llmService;
    private EventBus eventBus;
    
    // Dynamic strategy configuration
    private JsonObject fallbackStrategies; // Keep minimal fallback for safety
    
    // Conversation context management
    private final Map<String, ConversationContext> conversations = new ConcurrentHashMap<>();
    
    // Performance tracking
    private final Map<String, Long> performanceMetrics = new ConcurrentHashMap<>();
    
    // Inner class for conversation context
    private static class ConversationContext {
        String conversationId;
        String sessionId;
        boolean streaming;
        JsonArray history = new JsonArray();
        Map<String, JsonObject> stepResults = new HashMap<>();
        String currentStrategy;
        int currentStep = 0;
        int stepsCompleted = 0; // Track actually completed steps
        long startTime;
        
        ConversationContext(String conversationId) {
            this.conversationId = conversationId;
            this.startTime = System.currentTimeMillis();
        }
        
        void addMessage(String role, String content) {
            history.add(new JsonObject()
                .put("role", role)
                .put("content", content)
                .put("timestamp", System.currentTimeMillis()));
        }
        
        void storeStepResult(String stepName, JsonObject result) {
            stepResults.put(stepName, result);
        }
        
        JsonObject getStepResult(String stepName) {
            return stepResults.get(stepName);
        }
        
        JsonArray getRecentHistory(int maxMessages) {
            int start = Math.max(0, history.size() - maxMessages);
            JsonArray recent = new JsonArray();
            for (int i = start; i < history.size(); i++) {
                recent.add(history.getValue(i));
            }
            return recent;
        }
    }
    
    @Override
    public void start(Promise<Void> startPromise) {
        eventBus = vertx.eventBus();
        llmService = LlmAPIService.getInstance();
        
        // Load orchestration configuration
        loadOrchestrationConfig()
            .compose(v -> initializeManagers())
            .compose(v -> registerEventBusConsumers())
            .onComplete(ar -> {
                if (ar.succeeded()) {
                    vertx.eventBus().publish("log", "OracleDBAnswererHost started successfully with 5 managers,2,OracleDBAnswererHost,Host,System");
                    startPromise.complete();
                } else {
                    vertx.eventBus().publish("log", "Failed to start OracleDBAnswererHost" + ",0,OracleDBAnswererHost,Host,System");
                    startPromise.fail(ar.cause());
                }
            });
    }
    
    private Future<Void> loadOrchestrationConfig() {
        Promise<Void> promise = Promise.<Void>promise();
        
        // Initialize with empty fallback strategies
        fallbackStrategies = new JsonObject();
        
        vertx.eventBus().publish("log", "OracleDBAnswererHost configured for DYNAMIC strategy generation,2,OracleDBAnswererHost,Host,System");
        
        promise.complete();
        return promise.future();
    }
    
    private Future<Void> initializeManagers() {
        Promise<Void> promise = Promise.<Void>promise();
        String baseUrl = "http://localhost:8080";
        
        // Initialize all managers
        executionManager = new OracleExecutionManager(vertx, baseUrl);
        sqlPipelineManager = new SQLPipelineManager(vertx, baseUrl);
        schemaIntelligenceManager = new SchemaIntelligenceManager(vertx, baseUrl);
        intentAnalysisManager = new IntentAnalysisManager(vertx, baseUrl);
        strategyOrchestrationManager = new StrategyOrchestrationManager(vertx, baseUrl);
        
        // Initialize all managers in parallel
        List<Future> initFutures = Arrays.asList(
            executionManager.initialize(),
            sqlPipelineManager.initialize(),
            schemaIntelligenceManager.initialize(),
            intentAnalysisManager.initialize(),
            strategyOrchestrationManager.initialize()
        );
        
        CompositeFuture.all(initFutures).onComplete(ar -> {
            if (ar.succeeded()) {
                vertx.eventBus().publish("log", "All managers initialized successfully,2,OracleDBAnswererHost,Host,System");
                promise.complete();
            } else {
                vertx.eventBus().publish("log", "Failed to initialize managers: " + ar.cause().getMessage() + ",0,OracleDBAnswererHost,Host,System");
                promise.fail(ar.cause());
            }
        });
        
        return promise.future();
    }
    
    private Future<Void> registerEventBusConsumers() {
        Promise<Void> promise = Promise.<Void>promise();
        
        // Main processing endpoint
        eventBus.<JsonObject>consumer("host.oracledbanswerer.process", this::processQuery);
        
        // Status endpoint
        eventBus.<JsonObject>consumer("host.oracledbanswerer.status", message -> {
            message.reply(new JsonObject()
                .put("status", "ready")
                .put("managers", 5)
                .put("activeConversations", conversations.size())
                .put("supportedStrategies", getSupportedStrategies()));
        });
        
        promise.complete();
        return promise.future();
    }
    
    /**
     * Main query processing method - orchestrates the full pipeline
     */
    private void processQuery(Message<JsonObject> message) {
        JsonObject request = message.body();
        String query = request.getString("query");
        String conversationId = request.getString("conversationId", UUID.randomUUID().toString());
        String sessionId = request.getString("sessionId"); // For streaming
        JsonArray history = request.getJsonArray("history", new JsonArray());
        boolean streaming = request.getBoolean("streaming", true);
        
        vertx.eventBus().publish("log", "Processing query for conversation " + conversationId + ": " + query + "" + ",2,OracleDBAnswererHost,Host,System");
        
        // Publish start event if streaming
        if (sessionId != null && streaming) {
            publishStreamingEvent(conversationId, "progress", new JsonObject()
                .put("step", "host_started")
                .put("message", "Starting Oracle database query processing")
                .put("details", new JsonObject()
                    .put("host", "OracleDBAnswererHost")
                    .put("query", query)));
        }
        
        // Check for interrupts if streaming
        if (sessionId != null && streaming) {
            InterruptManager im = new InterruptManager(vertx);
            if (im.isInterrupted(sessionId)) {
                publishStreamingEvent(conversationId, "interrupt", new JsonObject()
                    .put("reason", "User interrupted")
                    .put("message", "Query processing interrupted by user"));
                message.reply(new JsonObject()
                    .put("interrupted", true)
                    .put("message", "Query processing interrupted by user"));
                return;
            }
        }
        
        // Get or create conversation context
        ConversationContext context = conversations.computeIfAbsent(
            conversationId, 
            k -> new ConversationContext(conversationId)
        );
        
        // Set streaming info
        context.sessionId = sessionId;
        context.streaming = streaming;
        
        // Add history if provided
        if (!history.isEmpty() && context.history.isEmpty()) {
            context.history = history.copy();
        }
        
        // Add user query to context
        context.addMessage("user", query);
        
        // Publish tool start event if streaming
        if (sessionId != null && streaming) {
            publishStreamingEvent(conversationId, "tool.start", new JsonObject()
                .put("tool", "oracle_database_pipeline")
                .put("description", "Full Oracle database Q&A orchestration")
                .put("parameters", new JsonObject()
                    .put("conversationId", conversationId)
                    .put("historySize", history.size())));
        }
        
        // Start the orchestration pipeline
        orchestratePipeline(context, query, streaming)
            .onComplete(ar -> {
                if (ar.succeeded()) {
                    JsonObject response = ar.result();
                    response.put("conversationId", conversationId);
                    response.put("duration", System.currentTimeMillis() - context.startTime);
                    
                    // Log the final response
                    String answer = response.getString("answer", "");
                    String answerPreview = answer.length() > 500 ? answer.substring(0, 497) + "..." : answer;
                    vertx.eventBus().publish("log", "Sending final response for conversation " + conversationId + 
                        " with answer length: " + answer.length() + 
                        " confidence: " + response.getDouble("confidence", 0.0) + ",2,OracleDBAnswererHost,Host,System");
                    
                    // Log the actual answer content
                    vertx.eventBus().publish("log", "Answer content: " + answerPreview.replace("\n", " ") + ",3,OracleDBAnswererHost,Host,System");
                    
                    // Log additional response details
                    if (response.containsKey("data")) {
                        JsonObject data = response.getJsonObject("data");
                        if (data != null && data.containsKey("rows")) {
                            int rowCount = data.getJsonArray("rows").size();
                            vertx.eventBus().publish("log", "Response includes " + rowCount + " data rows,3,OracleDBAnswererHost,Host,System");
                        }
                    }
                    if (response.containsKey("executedSQL")) {
                        String sqlPreview = response.getString("executedSQL", "");
                        if (sqlPreview.length() > 100) {
                            sqlPreview = sqlPreview.substring(0, 97) + "...";
                        }
                        vertx.eventBus().publish("log", "Executed SQL: " + sqlPreview.replace("\n", " ") + ",3,OracleDBAnswererHost,Host,System");
                    }
                    
                    // Debug mode - log full response
                    if (DEBUG_RESPONSES) {
                        vertx.eventBus().publish("log", "DEBUG - Full response JSON: " + response.encode() + ",3,OracleDBAnswererHost,Host,Debug");
                    }
                    
                    // Publish complete event if streaming
                    if (sessionId != null && streaming) {
                        publishStreamingEvent(conversationId, "tool.complete", new JsonObject()
                            .put("tool", "oracle_database_pipeline")
                            .put("success", true)
                            .put("resultSummary", "Completed query with confidence " + 
                                response.getDouble("confidence", 0.0)));
                        
                        // Publish final event with full response
                        JsonObject finalEvent = new JsonObject()
                            .put("content", response.getString("answer", ""))
                            .put("conversationId", conversationId)
                            .put("confidence", response.getDouble("confidence", 0.0))
                            .put("hasData", response.containsKey("data"));
                        
                        // Include the actual data if present
                        if (response.containsKey("data")) {
                            finalEvent.put("data", response.getJsonObject("data"));
                        }
                        
                        // Include executed SQL if present
                        if (response.containsKey("executedSQL")) {
                            finalEvent.put("executedSQL", response.getString("executedSQL"));
                        }
                        
                        // Include any other response fields that might be useful
                        if (response.containsKey("duration")) {
                            finalEvent.put("duration", response.getLong("duration"));
                        }
                        
                        publishStreamingEvent(conversationId, "final", finalEvent);
                    }
                    
                    message.reply(response);
                    
                    // Add assistant response to context
                    context.addMessage("assistant", response.getString("answer", ""));
                } else {
                    vertx.eventBus().publish("log", "Pipeline failed for conversation " + conversationId + "" + ",0,OracleDBAnswererHost,Host,System");
                    
                    // Publish error event if streaming
                    if (sessionId != null && streaming) {
                        publishStreamingEvent(conversationId, "error", new JsonObject()
                            .put("error", ar.cause().getMessage())
                            .put("severity", "ERROR")
                            .put("tool", "oracle_database_pipeline"));
                    }
                    
                    message.fail(500, ar.cause().getMessage());
                }
                
                // Clean up old conversations
                cleanupOldConversations();
            });
    }
    
    /**
     * Orchestrate the full pipeline based on query intent and selected strategy
     */
    private Future<JsonObject> orchestratePipeline(ConversationContext context, String query, boolean streaming) {
        Promise<JsonObject> promise = Promise.<JsonObject>promise();
        
        // Step 1: Evaluate intent and select strategy
        evaluateAndSelectStrategy(context, query)
            .compose(strategy -> {
                context.currentStrategy = strategy.getString("selectedStrategy");
                vertx.eventBus().publish("log", "Selected strategy: " + context.currentStrategy + " for conversation " + context.conversationId + ",2,OracleDBAnswererHost,Host,System");
                
                // Log strategy details
                JsonObject strategyObj = strategy.getJsonObject("strategy");
                if (strategyObj != null) {
                    JsonArray steps = strategyObj.getJsonArray("steps", new JsonArray());
                    vertx.eventBus().publish("log", "Strategy contains " + steps.size() + " steps to execute,3,OracleDBAnswererHost,Host,System");
                }
                
                // Publish strategy selection event if streaming
                if (context.sessionId != null && context.streaming) {
                    String method = strategy.getString("method", "unknown");
                    String message = method.equals("dynamic_generation") ? 
                        "Dynamically generated orchestration strategy" :
                        "Using fallback orchestration strategy";
                    
                    publishStreamingEvent(context.conversationId, "progress", new JsonObject()
                        .put("step", "strategy_selected")
                        .put("message", message)
                        .put("details", new JsonObject()
                            .put("strategy", context.currentStrategy)
                            .put("confidence", strategy.getDouble("confidence", 0.0))
                            .put("method", method)
                            .put("isDynamic", method.equals("dynamic_generation"))));
                }
                
                // Execute the selected strategy
                vertx.eventBus().publish("log", "Calling executeStrategy for conversation " + context.conversationId + ",3,OracleDBAnswererHost,Host,System");
                return executeStrategy(context, strategy, streaming);
            })
            .onComplete(ar -> {
                if (ar.succeeded()) {
                    vertx.eventBus().publish("log", "Pipeline completed successfully for conversation " + context.conversationId + ",2,OracleDBAnswererHost,Host,System");
                    promise.complete(ar.result());
                } else {
                    vertx.eventBus().publish("log", "ERROR: Pipeline failed for conversation " + context.conversationId + ": " + ar.cause().getMessage() + ",0,OracleDBAnswererHost,Host,System");
                    promise.fail(ar.cause());
                }
            });
        
        return promise.future();
    }
    
    /**
     * Evaluate query intent and select or generate optimal strategy
     */
    private Future<JsonObject> evaluateAndSelectStrategy(ConversationContext context, String query) {
        Promise<JsonObject> promise = Promise.<JsonObject>promise();
        
        // Always use dynamic strategy generation
        generateDynamicStrategy(context, query)
            .onComplete(ar -> {
                if (ar.succeeded()) {
                    promise.complete(ar.result());
                } else {
                    vertx.eventBus().publish("log", "Dynamic strategy generation failed, requesting fallback generation" + ",1,OracleDBAnswererHost,Host,System");
                    
                    // Add streaming notification about fallback
                    if (context.sessionId != null && context.streaming) {
                        publishStreamingEvent(context.conversationId, "progress", new JsonObject()
                            .put("step", "strategy_fallback")
                            .put("message", "Using fallback strategy due to generation failure")
                            .put("details", new JsonObject()
                                .put("reason", ar.cause().getMessage())
                                .put("method", "fallback")));
                    }
                    
                    // Instead of returning a non-existent strategy, force generate with fallback
                    generateDynamicStrategyWithFallback(context, query)
                        .onComplete(promise);
                }
            });
        
        return promise.future();
    }
    
    /**
     * Generate a dynamic strategy using the new MCP servers
     */
    private Future<JsonObject> generateDynamicStrategy(ConversationContext context, String query) {
        Promise<JsonObject> promise = Promise.<JsonObject>promise();
        
        // Use strategy orchestration manager for dynamic strategy generation
        if (strategyOrchestrationManager == null || !strategyOrchestrationManager.isReady()) {
            promise.fail("Strategy orchestration manager not available");
            return promise.future();
        }
        
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
                vertx.eventBus().publish("log", "Failed to fetch available tools: " + ar.cause() + ",1,OracleDBAnswererHost,Host,System");
                // Continue with empty tools list
                toolsPromise.complete(new JsonArray());
            }
        });
        
        // Use the strategy orchestration manager to generate a dynamic strategy
        strategyOrchestrationManager.generateDynamicStrategy(
            query,
            context.getRecentHistory(5),
            "intermediate" // expertise level, could be dynamic
        )
            .onComplete(ar -> {
                if (ar.succeeded()) {
                    JsonObject toolResponse = ar.result();
                    context.storeStepResult("generated_strategy", toolResponse);
                    
                    // Extract the actual strategy from the result field
                    JsonObject generatedStrategy = toolResponse.getJsonObject("result");
                    if (generatedStrategy == null) {
                        vertx.eventBus().publish("log", "ERROR: No strategy found in tool response! Response: " + 
                            toolResponse.encodePrettily() + ",0,OracleDBAnswererHost,Host,System");
                        promise.fail("No strategy found in tool response");
                        return;
                    }
                    
                    // Validate the generated strategy has steps
                    JsonArray steps = generatedStrategy.getJsonArray("steps", new JsonArray());
                    if (steps.isEmpty()) {
                        vertx.eventBus().publish("log", "ERROR: Generated strategy has no steps! Strategy: " + 
                            generatedStrategy.encodePrettily() + ",0,OracleDBAnswererHost,Host,System");
                        promise.fail("Generated strategy has no steps");
                        return;
                    }
                    
                    vertx.eventBus().publish("log", "Generated strategy '" + generatedStrategy.getString("name", "unknown") + 
                        "' with " + steps.size() + " steps,2,OracleDBAnswererHost,Host,System");
                    
                    // Format result for compatibility
                    JsonObject result = new JsonObject()
                        .put("selectedStrategy", "dynamic_generated")
                        .put("strategy", generatedStrategy) // The actual strategy object
                        .put("confidence", 0.9)
                        .put("method", "dynamic_generation")
                        .put("reasoning", "Dynamically generated based on query analysis")
                        .put("generation_method", generatedStrategy.getString("generation_method", "dynamic_llm"));
                    
                    // Add streaming notification if this was a fallback
                    if (generatedStrategy.getString("generation_method", "").startsWith("fallback")) {
                        if (context.sessionId != null && context.streaming) {
                            publishStreamingEvent(context.conversationId, "progress", new JsonObject()
                                .put("step", "strategy_generation_fallback")
                                .put("message", "Using fallback strategy due to: " + 
                                    generatedStrategy.getString("fallback_reason", "LLM unavailable"))
                                .put("details", new JsonObject()
                                    .put("method", generatedStrategy.getString("generation_method"))
                                    .put("strategy_name", generatedStrategy.getString("name"))));
                        }
                    }
                    
                    promise.complete(result);
                } else {
                    promise.fail(ar.cause());
                }
            });
        
        return promise.future();
    }
    
    /**
     * Generate a strategy using the fallback mechanism when dynamic generation fails
     */
    private Future<JsonObject> generateDynamicStrategyWithFallback(ConversationContext context, String query) {
        Promise<JsonObject> promise = Promise.<JsonObject>promise();
        
        // Create a simple fallback strategy directly
        JsonObject fallbackStrategy = new JsonObject()
            .put("name", "Fallback Complex Pipeline")
            .put("description", "Comprehensive query processing with validation")
            .put("method", "fallback")
            .put("steps", new JsonArray()
                .add(createStep(1, "evaluate_query_intent", "QueryIntentEvaluation", "Deep intent analysis"))
                .add(createStep(2, "analyze_query", "OracleQueryAnalysis", "Analyze query structure"))
                .add(createStep(3, "match_oracle_schema", "OracleSchemaIntelligence", "Find all related tables"))
                .add(createStep(4, "infer_table_relationships", "OracleSchemaIntelligence", "Discover relationships", true))
                .add(createStep(5, "map_business_terms", "BusinessMapping", "Map business terminology", true))
                .add(createStep(6, "generate_oracle_sql", "OracleSQLGeneration", "Generate complex SQL"))
                .add(createStep(7, "optimize_oracle_sql", "OracleSQLGeneration", "Optimize for performance", true))
                .add(createStep(8, "validate_oracle_sql", "OracleSQLValidation", "Validate SQL"))
                .add(createStep(9, "run_oracle_query", "OracleQueryExecution", "Execute with monitoring"))
                .add(createStep(10, "format_results", "OracleQueryExecution", "Format response", true)))
            .put("decision_points", new JsonArray())
            .put("adaptation_rules", new JsonObject()
                .put("allow_runtime_modification", true)
                .put("max_retries_per_step", 2))
            .put("generation_method", "fallback_hardcoded");
        
        // Format result for compatibility
        JsonObject result = new JsonObject()
            .put("selectedStrategy", "dynamic_generated")
            .put("strategy", fallbackStrategy)
            .put("confidence", 0.7)
            .put("method", "fallback")
            .put("reasoning", "Using hardcoded fallback strategy due to generation failure")
            .put("generation_method", "fallback_hardcoded");
        
        vertx.eventBus().publish("log", "Generated hardcoded fallback strategy with " + 
            fallbackStrategy.getJsonArray("steps").size() + " steps,1,OracleDBAnswererHost,Host,System");
        
        promise.complete(result);
        return promise.future();
    }
    
    private static JsonObject createStep(int stepNum, String tool, String server, String description) {
        return createStep(stepNum, tool, server, description, false);
    }
    
    private static JsonObject createStep(int stepNum, String tool, String server, String description, boolean optional) {
        return new JsonObject()
            .put("step", stepNum)
            .put("tool", tool)
            .put("server", server)
            .put("description", description)
            .put("optional", optional);
    }
    
    /**
     * Execute the selected orchestration strategy step by step
     */
    private Future<JsonObject> executeStrategy(ConversationContext context, 
                                             JsonObject strategySelection, 
                                             boolean streaming) {
        Promise<JsonObject> promise = Promise.<JsonObject>promise();
        
        vertx.eventBus().publish("log", "executeStrategy called for conversation " + context.conversationId + 
            " with strategy selection: " + strategySelection.encodePrettily() + ",3,OracleDBAnswererHost,Host,System");
        
        String strategyName = strategySelection.getString("selectedStrategy");
        JsonObject strategy;
        JsonArray steps;
        
        if ("dynamic_generated".equals(strategyName)) {
            // Use the dynamically generated strategy
            strategy = strategySelection.getJsonObject("strategy");
            if (strategy == null) {
                promise.fail("Dynamic strategy not found in selection");
                return promise.future();
            }
            steps = strategy.getJsonArray("steps", new JsonArray());
            
            // Always use adaptive execution for dynamic strategies
            return executeAdaptiveStrategy(context, strategy, streaming);
        } else {
            // Use static strategy from configuration
            if (fallbackStrategies != null) {
                strategy = fallbackStrategies.getJsonObject(strategyName);
            } else {
                strategy = null;
            }
            
            if (strategy == null) {
                promise.fail("Strategy not found: " + strategyName);
                return promise.future();
            }
            steps = strategy.getJsonArray("steps", new JsonArray());
        }
        
        // Log execution start for static strategies
        vertx.eventBus().publish("log", "Starting strategy execution: " + strategyName + 
            " with " + steps.size() + " steps (static strategy) for conversation " + context.conversationId + ",2,OracleDBAnswererHost,Host,System");
        
        // Reset step counters
        context.currentStep = 0;
        context.stepsCompleted = 0;
        
        // Execute steps sequentially (traditional approach)
        executeSteps(context, steps, 0, new JsonObject())
            .compose(finalResult -> {
                // Log completion
                vertx.eventBus().publish("log", "Strategy execution complete: " + strategyName + 
                    " - " + context.stepsCompleted + " steps executed (static strategy) for conversation " + 
                    context.conversationId + ",2,OracleDBAnswererHost,Host,System");
                
                // Compose the final answer
                return composeFinalAnswer(context, finalResult, streaming);
            })
            .onComplete(promise);
        
        return promise.future();
    }
    
    /**
     * Execute strategy with adaptive capabilities
     */
    private Future<JsonObject> executeAdaptiveStrategy(ConversationContext context,
                                                     JsonObject strategy,
                                                     boolean streaming) {
        Promise<JsonObject> promise = Promise.<JsonObject>promise();
        
        JsonArray steps = strategy.getJsonArray("steps", new JsonArray());
        String strategyName = strategy.getString("name", "dynamic_strategy");
        
        // Log strategy execution start
        vertx.eventBus().publish("log", "Starting strategy execution: " + strategyName + 
            " with " + steps.size() + " steps for conversation " + context.conversationId + ",2,OracleDBAnswererHost,Host,System");
        
        // Publish streaming event for strategy execution start
        if (context.sessionId != null && context.streaming) {
            publishStreamingEvent(context.conversationId, "progress", new JsonObject()
                .put("step", "strategy_execution_start")
                .put("message", "Starting strategy execution: " + strategyName)
                .put("details", new JsonObject()
                    .put("strategyName", strategyName)
                    .put("totalSteps", steps.size())
                    .put("method", "adaptive")));
        }
        
        // Reset step counters
        context.currentStep = 0;
        context.stepsCompleted = 0;
        
        // Execute with adaptation
        executeStepsWithAdaptation(context, strategy, steps, 0, new JsonObject())
            .compose(finalResult -> {
                // Log strategy execution completion
                vertx.eventBus().publish("log", "Strategy execution complete: " + strategyName + 
                    " - " + context.stepsCompleted + " steps executed for conversation " + 
                    context.conversationId + ",2,OracleDBAnswererHost,Host,System");
                
                // Publish streaming event for strategy execution completion
                if (context.sessionId != null && context.streaming) {
                    publishStreamingEvent(context.conversationId, "progress", new JsonObject()
                        .put("step", "strategy_execution_complete")
                        .put("message", "Strategy execution complete: " + context.stepsCompleted + " steps executed")
                        .put("details", new JsonObject()
                            .put("strategyName", strategyName)
                            .put("stepsCompleted", context.stepsCompleted)
                            .put("totalSteps", steps.size())
                            .put("duration", System.currentTimeMillis() - context.startTime)));
                }
                
                // Record execution for learning using strategy orchestration manager
                if (strategyOrchestrationManager != null && strategyOrchestrationManager.isReady()) {
                    vertx.eventBus().publish("log", "Recording execution for strategy: " + strategyName + 
                        " with " + context.stepsCompleted + " completed steps,3,OracleDBAnswererHost,Host,System");
                    
                    JsonObject executionResults = new JsonObject()
                        .put("success", true)
                        .put("total_duration", System.currentTimeMillis() - context.startTime)
                        .put("steps_completed", context.stepsCompleted);
                    
                    JsonObject performanceMetrics = new JsonObject()
                        .put("query_complexity", context.getStepResult("complexity_analysis")
                            .getJsonObject("factors", new JsonObject()).getFloat("complexity_score", 0.5f));
                    
                    strategyOrchestrationManager.recordExecution(strategy, executionResults, performanceMetrics)
                        .onComplete(ar -> {
                            if (ar.failed()) {
                                vertx.eventBus().publish("log", "Failed to record execution: " + ar.cause() + ",1,OracleDBAnswererHost,Host,System");
                            } else {
                                vertx.eventBus().publish("log", "Successfully recorded execution for learning,3,OracleDBAnswererHost,Host,System");
                            }
                        });
                }
                
                return composeFinalAnswer(context, finalResult, streaming);
            })
            .onComplete(promise);
        
        return promise.future();
    }
    
    /**
     * Execute steps with adaptation capability
     */
    private Future<JsonObject> executeStepsWithAdaptation(ConversationContext context,
                                                        JsonObject strategy,
                                                        JsonArray steps,
                                                        int currentIndex,
                                                        JsonObject accumulated) {
        if (currentIndex >= steps.size()) {
            return Future.succeededFuture(accumulated);
        }
        
        JsonObject currentStep = steps.getJsonObject(currentIndex);
        String toolName = currentStep.getString("tool", "unknown");
        
        // Update current step
        context.currentStep = currentIndex;
        
        // Log step execution start
        vertx.eventBus().publish("log", "Executing step " + (currentIndex + 1) + "/" + steps.size() + 
            ": " + toolName + " for conversation " + context.conversationId + ",3,OracleDBAnswererHost,Host,System");
        
        // Execute current step
        return executeStep(context, currentStep, accumulated)
            .compose(result -> {
                // Increment completed steps counter
                context.stepsCompleted++;
                context.storeStepResult(toolName, result);
                
                accumulated.mergeIn(new JsonObject()
                    .put(currentStep.getString("tool") + "_result", result));
                
                vertx.eventBus().publish("log", "Step " + (currentIndex + 1) + " completed: " + toolName + 
                    " - Total completed: " + context.stepsCompleted + ",3,OracleDBAnswererHost,Host,System");
                
                // Evaluate progress using strategy orchestration manager
                if (strategyOrchestrationManager != null && strategyOrchestrationManager.isReady() && 
                    currentIndex % 3 == 2) { // Check every 3 steps
                    
                    return strategyOrchestrationManager.evaluateAndAdaptStrategy(
                        strategy,
                        getCompletedSteps(steps, currentIndex + 1),
                        accumulated,
                        System.currentTimeMillis() - context.startTime
                    ).compose(adaptationResult -> {
                        JsonArray possiblyAdaptedSteps = adaptationResult.getJsonArray("steps", steps);
                        return executeStepsWithAdaptation(context, strategy, possiblyAdaptedSteps, 
                            currentIndex + 1, accumulated);
                    });
                } else {
                    // Continue without evaluation
                    return executeStepsWithAdaptation(context, strategy, steps, 
                        currentIndex + 1, accumulated);
                }
            })
            .recover(error -> {
                // Handle step execution failure
                vertx.eventBus().publish("log", "ERROR: Step " + (currentIndex + 1) + " failed: " + toolName + 
                    " - Error: " + error.getMessage() + ",0,OracleDBAnswererHost,Host,System");
                
                // Check if step is optional
                boolean optional = currentStep.getBoolean("optional", false);
                if (optional) {
                    vertx.eventBus().publish("log", "Skipping optional step " + toolName + " and continuing,1,OracleDBAnswererHost,Host,System");
                    // Continue with next step
                    return executeStepsWithAdaptation(context, strategy, steps, currentIndex + 1, accumulated);
                } else {
                    // Required step failed, propagate error
                    return Future.failedFuture("Required step failed: " + toolName + " - " + error.getMessage());
                }
            });
    }
    
    private JsonArray getCompletedSteps(JsonArray allSteps, int completedCount) {
        JsonArray completed = new JsonArray();
        for (int i = 0; i < completedCount && i < allSteps.size(); i++) {
            completed.add(allSteps.getJsonObject(i));
        }
        return completed;
    }
    
    /**
     * Execute strategy steps recursively
     */
    private Future<JsonObject> executeSteps(ConversationContext context, 
                                          JsonArray steps, 
                                          int currentStepIndex,
                                          JsonObject accumulatedResult) {
        Promise<JsonObject> promise = Promise.<JsonObject>promise();
        
        // Base case - all steps completed
        if (currentStepIndex >= steps.size()) {
            promise.complete(accumulatedResult);
            return promise.future();
        }
        
        JsonObject step = steps.getJsonObject(currentStepIndex);
        String tool = step.getString("tool");
        String server = step.getString("server");
        String description = step.getString("description");
        boolean optional = step.getBoolean("optional", false);
        
        // Update current step
        context.currentStep = currentStepIndex;
        
        vertx.eventBus().publish("log", "Executing step " + (currentStepIndex + 1) + "/" + steps.size() + 
            ": " + tool + " - " + description + ",3,OracleDBAnswererHost,Host,System");
        
        // Publish progress event if streaming
        if (context.sessionId != null && context.streaming) {
            publishStreamingEvent(context.conversationId, "progress", new JsonObject()
                .put("step", "orchestration_step")
                .put("message", "Executing: " + description)
                .put("details", new JsonObject()
                    .put("tool", tool)
                    .put("server", server)
                    .put("stepNumber", currentStepIndex + 1)
                    .put("totalSteps", steps.size())
                    .put("strategy", context.currentStrategy)));
        }
        
        // Execute the current step
        executeStep(context, step, accumulatedResult)
            .onComplete(ar -> {
                if (ar.succeeded()) {
                    JsonObject stepResult = ar.result();
                    
                    // Store step result
                    context.storeStepResult(tool, stepResult);
                    
                    // Increment completed steps counter
                    context.stepsCompleted++;
                    
                    // Accumulate results
                    accumulatedResult.mergeIn(new JsonObject()
                        .put(tool + "_result", stepResult));
                    
                    // Publish step completion if streaming
                    if (context.sessionId != null && context.streaming) {
                        publishStreamingEvent(context.conversationId, "progress", new JsonObject()
                            .put("step", "orchestration_step_complete")
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
                                .put("message", "Pipeline interrupted at step " + (currentStepIndex + 1)));
                            promise.fail("User interrupted orchestration");
                            return;
                        }
                    }
                    
                    // Continue to next step
                    executeSteps(context, steps, currentStepIndex + 1, accumulatedResult)
                        .onComplete(promise);
                        
                } else if (optional) {
                    // Optional step failed, continue anyway
                    vertx.eventBus().publish("log", "Optional step " + tool + " failed: " + ar.cause() + "" + ",1,OracleDBAnswererHost,Host,System");
                    
                    // Publish warning if streaming
                    if (context.sessionId != null && context.streaming) {
                        publishStreamingEvent(context.conversationId, "progress", new JsonObject()
                            .put("step", "orchestration_step_skipped")
                            .put("message", "Skipped optional step: " + description)
                            .put("details", new JsonObject()
                                .put("tool", tool)
                                .put("reason", ar.cause().getMessage())
                                .put("severity", "WARNING")));
                    }
                    
                    executeSteps(context, steps, currentStepIndex + 1, accumulatedResult)
                        .onComplete(promise);
                        
                } else {
                    // Required step failed, propagate error
                    if (context.sessionId != null && context.streaming) {
                        publishStreamingEvent(context.conversationId, "error", new JsonObject()
                            .put("error", "Pipeline step failed: " + tool)
                            .put("severity", "ERROR")
                            .put("details", new JsonObject()
                                .put("tool", tool)
                                .put("step", currentStepIndex + 1)
                                .put("reason", ar.cause().getMessage())));
                    }
                    promise.fail(ar.cause());
                }
            });
        
        return promise.future();
    }
    
    /**
     * Execute a single orchestration step
     */
    private Future<JsonObject> executeStep(ConversationContext context, 
                                         JsonObject step,
                                         JsonObject previousResults) {
        Promise<JsonObject> promise = Promise.<JsonObject>promise();
        
        String tool = step.getString("tool");
        String server = step.getString("server");
        
        // Build arguments based on the tool and previous results
        JsonObject arguments = buildToolArguments(context, tool, previousResults);
        
        vertx.eventBus().publish("log", "Calling MCP tool '" + tool + "' via managers with args: " + 
            arguments.encodePrettily() + ",3,OracleDBAnswererHost,Host,System");
        
        // Track performance
        long startTime = System.currentTimeMillis();
        
        // Route tool calls through appropriate managers
        Future<JsonObject> toolFuture = routeToolCallToManager(tool, server, arguments, context);
        
        toolFuture.onComplete(ar -> {
            long duration = System.currentTimeMillis() - startTime;
            performanceMetrics.put(tool, duration);
            
            if (ar.succeeded()) {
                vertx.eventBus().publish("log", "MCP tool '" + tool + "' completed successfully in " + duration + "ms,3,OracleDBAnswererHost,Host,System");
                promise.complete(ar.result());
            } else {
                vertx.eventBus().publish("log", "ERROR: MCP tool '" + tool + "' failed: " + ar.cause().getMessage() + ",0,OracleDBAnswererHost,Host,System");
                promise.fail("Step " + tool + " failed: " + ar.cause().getMessage());
            }
        });
        
        return promise.future();
    }
    
    /**
     * Route tool calls to the appropriate manager
     */
    private Future<JsonObject> routeToolCallToManager(String tool, String server, JsonObject arguments, ConversationContext context) {
        // Route based on server/tool type
        switch (server) {
            case "OracleQueryExecution":
            case "SessionSchemaResolver":
                if (context.sessionId != null) {
                    String sql = arguments.getString("sql");
                    if (sql != null) {
                        return executionManager.executeWithSessionContext(sql, context.sessionId);
                    }
                }
                return executionManager.callClientTool("execution", tool, arguments);
                
            case "OracleQueryAnalysis":
            case "OracleSQLGeneration":
            case "OracleSQLValidation":
                // Map server names to client names properly
                String sqlClientName;
                if (server.equals("OracleQueryAnalysis")) {
                    sqlClientName = "analysis";
                } else if (server.equals("OracleSQLGeneration")) {
                    sqlClientName = "generation";
                } else {
                    sqlClientName = "validation";
                }
                return sqlPipelineManager.callClientTool(sqlClientName, tool, arguments);
                
            case "OracleSchemaIntelligence":
            case "BusinessMapping":
                return schemaIntelligenceManager.callClientTool(
                    server.equals("BusinessMapping") ? "business" : "schema",
                    tool, arguments);
                
            case "QueryIntentEvaluation":
            case "IntentAnalysis":
                return intentAnalysisManager.callClientTool(
                    server.equals("QueryIntentEvaluation") ? "evaluation" : "analysis",
                    tool, arguments);
                
            case "StrategyGeneration":
            case "StrategyOrchestrator":
            case "StrategyLearning":
                String clientName = server.replace("Strategy", "").toLowerCase();
                return strategyOrchestrationManager.callClientTool(clientName, tool, arguments);
                
            default:
                vertx.eventBus().publish("log", "WARNING: Unknown server '" + server + "' for tool '" + tool + "',1,OracleDBAnswererHost,Host,System");
                return Future.failedFuture("Unknown server: " + server);
        }
    }
    
    /**
     * Build arguments for each tool based on context and previous results
     */
    private JsonObject buildToolArguments(ConversationContext context, 
                                        String tool, 
                                        JsonObject previousResults) {
        JsonObject args = new JsonObject();
        
        vertx.eventBus().publish("log", "Building tool arguments for: " + tool + ",3,OracleDBAnswererHost,Host,System");
        
        switch (tool) {
            case "evaluate_query_intent":
                // Extract the query from the most recent user message
                args.put("query", context.history.getJsonObject(context.history.size() - 1).getString("content"));
                // Include recent conversation history for context
                args.put("conversationHistory", context.getRecentHistory(5));
                break;
                
            case "analyze_query":
                args.put("query", context.history.getJsonObject(context.history.size() - 1).getString("content"));
                args.put("context", context.getRecentHistory(3));
                break;
                
            case "match_oracle_schema":
                JsonObject analysis = context.getStepResult("analyze_query");
                if (analysis == null) {
                    analysis = previousResults.getJsonObject("analyze_query_result", new JsonObject());
                }
                args.put("analysis", analysis);
                args.put("maxSuggestions", 5);
                args.put("confidenceThreshold", 0.6);
                break;
                
            case "map_business_terms":
                // Extract terms from analysis
                JsonObject queryAnalysis = context.getStepResult("analyze_query");
                if (queryAnalysis != null) {
                    JsonArray entities = queryAnalysis.getJsonArray("entities", new JsonArray());
                    args.put("terms", entities);
                    
                    // Add full schema to context
                    JsonObject fullSchema = context.getStepResult("get_oracle_schema");
                    JsonObject schemaContext = context.getStepResult("match_oracle_schema");
                    
                    args.put("context", new JsonObject()
                        .put("schemaMatches", schemaContext)
                        .put("queryAnalysis", queryAnalysis)
                        .put("fullSchema", fullSchema));  // NEW: Add full schema
                }
                break;
                
            case "generate_oracle_sql":
                args.put("analysis", context.getStepResult("analyze_query"));
                args.put("schemaMatches", context.getStepResult("match_oracle_schema"));
                args.put("fullSchema", context.getStepResult("get_oracle_schema")); // NEW: Add full schema
                args.put("includeEnums", true);
                args.put("maxComplexity", determineComplexity(context));
                break;
                
            case "optimize_oracle_sql":
                JsonObject sqlGenResult = context.getStepResult("generate_oracle_sql");
                if (sqlGenResult != null) {
                    args.put("sql", sqlGenResult.getString("sql"));
                    args.put("applyHints", true);
                }
                break;
                
            case "validate_oracle_sql":
                JsonObject sqlResult = context.getStepResult("optimize_oracle_sql");
                if (sqlResult == null) {
                    sqlResult = context.getStepResult("generate_oracle_sql");
                }
                if (sqlResult != null) {
                    String sql = sqlResult.getString("optimizedSQL", sqlResult.getString("sql"));
                    args.put("sql", sql);
                    args.put("checkPermissions", true);
                    args.put("suggestFixes", true);
                    args.put("fullSchema", context.getStepResult("get_oracle_schema")); // NEW: Add full schema
                }
                break;
                
            case "run_oracle_query":
                JsonObject validatedSQL = context.getStepResult("validate_oracle_sql");
                String finalSQL = null;
                
                if (validatedSQL != null && validatedSQL.getBoolean("valid", false)) {
                    finalSQL = validatedSQL.getString("sql");
                } else if (validatedSQL != null && validatedSQL.containsKey("suggestedSQL")) {
                    finalSQL = validatedSQL.getString("suggestedSQL");
                } else {
                    JsonObject generated = context.getStepResult("generate_oracle_sql");
                    if (generated != null) {
                        finalSQL = generated.getString("sql");
                    }
                }
                
                if (finalSQL != null) {
                    args.put("sql", finalSQL);
                    args.put("maxRows", 1000);
                    // Add sessionId for schema resolution
                    if (context.sessionId != null) {
                        args.put("sessionId", context.sessionId);
                    }
                }
                break;
                
            case "translate_enum":
                // Handle enum translation for results
                JsonObject queryResults = context.getStepResult("run_oracle_query");
                if (queryResults != null) {
                    // This would need to identify enum columns in results
                    // For now, skip if no specific enum columns identified
                }
                break;
                
            case "get_oracle_schema":
                // For schema exploration
                args.put("schemaName", null); // Get all schemas
                break;
                
            case "discover_column_semantics":
                // Would need table and column names from previous steps
                break;
                
            case "infer_table_relationships":
                // Would need table names from schema matches
                JsonObject schemaMatches = context.getStepResult("match_oracle_schema");
                if (schemaMatches != null) {
                    JsonArray matches = schemaMatches.getJsonArray("matches", new JsonArray());
                    JsonArray tables = new JsonArray();
                    for (int i = 0; i < matches.size(); i++) {
                        JsonObject match = matches.getJsonObject(i);
                        JsonObject table = match.getJsonObject("table", new JsonObject());
                        tables.add(table.getString("tableName"));
                    }
                    args.put("tables", tables);
                    args.put("includeIndirect", false);
                }
                break;
                
            case "format_results":
                // Format query results into natural language
                // Get the original user query from conversation history
                String originalQuery = context.history.getJsonObject(context.history.size() - 1).getString("content");
                args.put("original_query", originalQuery);
                
                // Get the SQL that was executed
                String executedSQL = getExecutedSQL(context);
                if (!executedSQL.isEmpty()) {
                    args.put("sql_executed", executedSQL);
                }
                
                // Get query results if available
                JsonObject queryResultsForFormat = context.getStepResult("run_oracle_query");
                if (queryResultsForFormat != null) {
                    args.put("results", queryResultsForFormat);
                }
                
                // Add any error information if execution failed
                // This would be handled by the step execution error handling
                break;
                
            default:
                // For unknown tools, pass minimal arguments
                vertx.eventBus().publish("log", "WARNING: Unknown tool '" + tool + "' - using minimal arguments,1,OracleDBAnswererHost,Host,System");
                // Add query if available
                if (!context.history.isEmpty()) {
                    args.put("query", context.history.getJsonObject(context.history.size() - 1).getString("content"));
                }
                // Add any previous results that might be relevant
                if (!previousResults.isEmpty()) {
                    args.put("context", previousResults);
                }
                break;
        }
        
        return args;
    }
    
    /**
     * Compose the final answer from all step results
     */
    private Future<JsonObject> composeFinalAnswer(ConversationContext context, 
                                                 JsonObject allResults,
                                                 boolean streaming) {
        Promise<JsonObject> promise = Promise.<JsonObject>promise();
        
        vertx.eventBus().publish("log", "Composing final answer for conversation " + context.conversationId + 
            " with strategy: " + context.currentStrategy + ",3,OracleDBAnswererHost,Host,System");
        
        // Log available step results for debugging
        if (!context.stepResults.isEmpty()) {
            vertx.eventBus().publish("log", "Available step results: " + 
                context.stepResults.keySet().toString() + ",3,OracleDBAnswererHost,Host,System");
        } else {
            vertx.eventBus().publish("log", "WARNING: No step results available for answer composition,1,OracleDBAnswererHost,Host,System");
        }
        
        // Log allResults parameter
        vertx.eventBus().publish("log", "AllResults keys: " + allResults.fieldNames().toString() + ",3,OracleDBAnswererHost,Host,System");
        
        JsonObject response = new JsonObject();
        
        // Check if we have formatted results first (highest priority)
        JsonObject formattedResults = context.getStepResult("format_results");
        if (formattedResults != null) {
            vertx.eventBus().publish("log", "Found formatted results, using pre-formatted answer,3,OracleDBAnswererHost,Host,System");
            // Extract the formatted answer
            String formattedAnswer = formattedResults.getString("formatted");
            if (formattedAnswer != null && !formattedAnswer.isEmpty()) {
                response.put("answer", formattedAnswer);
                response.put("confidence", calculateConfidence(context));
                response.put("executedSQL", getExecutedSQL(context));
                
                // Include raw data if available
                JsonObject queryResults = context.getStepResult("run_oracle_query");
                if (queryResults != null && queryResults.containsKey("rows")) {
                    response.put("data", queryResults);
                }
                
                promise.complete(response);
                return promise.future();
            }
        }
        
        // Check if we have query results (second priority)
        JsonObject initialQueryResults = context.getStepResult("run_oracle_query");
        
        // Check if result is nested in a "result" field (common MCP pattern)
        final JsonObject queryResults;
        if (initialQueryResults != null && initialQueryResults.containsKey("result") && !initialQueryResults.containsKey("rows")) {
            JsonObject nestedResult = initialQueryResults.getJsonObject("result");
            if (nestedResult != null) {
                vertx.eventBus().publish("log", "Extracting nested query result from MCP response,3,OracleDBAnswererHost,Host,System");
                queryResults = nestedResult;
            } else {
                queryResults = initialQueryResults;
            }
        } else {
            queryResults = initialQueryResults;
        }
        
        if (queryResults != null && queryResults.containsKey("rows")) {
            JsonArray rows = queryResults.getJsonArray("rows");
            vertx.eventBus().publish("log", "Found query results with " + rows.size() + " rows, composing data answer,3,OracleDBAnswererHost,Host,System");
            // We have data results
            JsonArray columns = queryResults.getJsonArray("columns", new JsonArray());
            
            // Use LLM to compose a natural language answer
            if (llmService.isInitialized()) {
                composeAnswerWithLLM(context, queryResults)
                    .onComplete(ar -> {
                        if (ar.succeeded()) {
                            String llmAnswer = ar.result();
                            vertx.eventBus().publish("log", "LLM composed answer: " + 
                                (llmAnswer.length() > 200 ? llmAnswer.substring(0, 197) + "..." : llmAnswer).replace("\n", " ") + 
                                ",3,OracleDBAnswererHost,Host,System");
                            response.put("answer", llmAnswer);
                            response.put("data", queryResults);
                            response.put("executedSQL", getExecutedSQL(context));
                            response.put("confidence", calculateConfidence(context));
                            promise.complete(response);
                        } else {
                            // Fallback to structured response
                            response.put("answer", formatStructuredAnswer(queryResults));
                            response.put("data", queryResults);
                            response.put("executedSQL", getExecutedSQL(context));
                            response.put("confidence", 0.7);
                            promise.complete(response);
                        }
                    });
                return promise.future();
            } else {
                // No LLM available, return structured response
                String structuredAnswer = formatStructuredAnswer(queryResults);
                vertx.eventBus().publish("log", "No LLM available, using structured answer: " + 
                    (structuredAnswer.length() > 200 ? structuredAnswer.substring(0, 197) + "..." : structuredAnswer).replace("\n", " ") + 
                    ",3,OracleDBAnswererHost,Host,System");
                response.put("answer", structuredAnswer);
                response.put("data", queryResults);
                response.put("executedSQL", getExecutedSQL(context));
                response.put("confidence", 0.7);
                promise.complete(response);
                return promise.future();
            }
        } 
        
        // Handle dynamic generated strategies
        if ("dynamic_generated".equals(context.currentStrategy)) {
            vertx.eventBus().publish("log", "Handling dynamic generated strategy results,3,OracleDBAnswererHost,Host,System");
            
            // Check what results we have from the dynamic pipeline
            JsonObject sqlGenResult = context.getStepResult("generate_oracle_sql");
            JsonObject sqlValResult = context.getStepResult("validate_oracle_sql");
            JsonObject schemaResult = context.getStepResult("match_oracle_schema");
            
            // Log what we found for debugging
            if (sqlGenResult != null) {
                vertx.eventBus().publish("log", "SQL generation result structure: " + 
                    sqlGenResult.fieldNames().toString() + ",3,OracleDBAnswererHost,Host,System");
                // Check if result is nested
                if (sqlGenResult.containsKey("result") && sqlGenResult.getJsonObject("result") != null) {
                    sqlGenResult = sqlGenResult.getJsonObject("result");
                    vertx.eventBus().publish("log", "Extracted nested SQL result: " + 
                        sqlGenResult.fieldNames().toString() + ",3,OracleDBAnswererHost,Host,System");
                }
            }
            
            // If we generated SQL but couldn't execute it
            if (sqlGenResult != null && sqlGenResult.containsKey("sql")) {
                vertx.eventBus().publish("log", "Found SQL generation result without execution,3,OracleDBAnswererHost,Host,System");
                String sql = sqlGenResult.getString("sql");
                String answer = "I generated the following SQL query for your request:\n\n```sql\n" + sql + "\n```";
                
                if (sqlValResult != null) {
                    boolean isValid = sqlValResult.getBoolean("valid", false);
                    if (!isValid && sqlValResult.containsKey("errors")) {
                        answer += "\n\nNote: The query validation found some issues: " + 
                                sqlValResult.getJsonArray("errors").encode();
                    } else if (isValid) {
                        answer += "\n\nThe query has been validated and is ready to execute.";
                    }
                }
                
                vertx.eventBus().publish("log", "SQL generation without execution answer: " + 
                    answer.replace("\n", " ").substring(0, Math.min(answer.length(), 200)) + 
                    ",3,OracleDBAnswererHost,Host,System");
                response.put("answer", answer);
                response.put("sql", sql);
                response.put("validated", sqlValResult != null);
                response.put("executedSQL", sql);
                response.put("confidence", calculateConfidence(context));
                promise.complete(response);
                return promise.future();
            }
            
            // Check for nested schema result
            if (schemaResult != null) {
                if (schemaResult.containsKey("result") && schemaResult.getJsonObject("result") != null) {
                    schemaResult = schemaResult.getJsonObject("result");
                    vertx.eventBus().publish("log", "Extracted nested schema result: " + 
                        schemaResult.fieldNames().toString() + ",3,OracleDBAnswererHost,Host,System");
                }
            }
            
            // If we only have schema matching results
            if (schemaResult != null && schemaResult.containsKey("matches")) {
                vertx.eventBus().publish("log", "Found schema matching results only,3,OracleDBAnswererHost,Host,System");
                JsonArray matches = schemaResult.getJsonArray("matches", new JsonArray());
                String answer = "I found the following relevant database objects for your query:\n\n";
                
                for (int i = 0; i < Math.min(matches.size(), 5); i++) {
                    JsonObject match = matches.getJsonObject(i);
                    JsonObject table = match.getJsonObject("table", new JsonObject());
                    answer += "- Table: " + table.getString("tableName", "Unknown") + "\n";
                }
                
                answer += "\nTo proceed, I would need to generate and execute a SQL query against these tables.";
                
                response.put("answer", answer);
                response.put("schemaMatches", schemaResult);
                response.put("confidence", 0.6);
                promise.complete(response);
                return promise.future();
            }
            
            // If we have other step results, compose a summary
            if (!context.stepResults.isEmpty()) {
                vertx.eventBus().publish("log", "Composing answer from available step results: " + 
                    context.stepResults.keySet().toString() + ",3,OracleDBAnswererHost,Host,System");
                String answer = composeAnswerFromStepResults(context);
                vertx.eventBus().publish("log", "Composed fallback answer: " + 
                    (answer.length() > 200 ? answer.substring(0, 197) + "..." : answer).replace("\n", " ") + 
                    ",3,OracleDBAnswererHost,Host,System");
                response.put("answer", answer);
                // Convert Map<String,JsonObject> to JsonObject
                JsonObject stepResultsJson = new JsonObject();
                context.stepResults.forEach(stepResultsJson::put);
                response.put("stepResults", stepResultsJson);
                response.put("confidence", calculateConfidence(context));
                promise.complete(response);
                return promise.future();
            }
        } 
        
        // Handle specific strategy types
        else if (context.currentStrategy.contains("sql_only")) {
            // SQL generation only - return the SQL
            JsonObject sqlResult = context.getStepResult("generate_oracle_sql");
            if (sqlResult != null) {
                response.put("answer", "Here is the generated SQL query:");
                response.put("sql", sqlResult.getString("sql"));
                response.put("validated", context.getStepResult("validate_oracle_sql") != null);
                response.put("confidence", calculateConfidence(context));
                promise.complete(response);
            } else {
                promise.fail("No SQL generated");
            }
        } else if (context.currentStrategy.contains("schema_exploration")) {
            // Schema exploration - return schema info
            JsonObject schemaInfo = context.getStepResult("get_oracle_schema");
            if (schemaInfo != null) {
                response.put("answer", formatSchemaAnswer(schemaInfo));
                response.put("schema", schemaInfo);
                response.put("confidence", 0.9);
                promise.complete(response);
            } else {
                promise.fail("No schema information retrieved");
            }
        } else {
            // Unknown strategy - try to compose from available results
            vertx.eventBus().publish("log", "WARNING: Unknown strategy '" + context.currentStrategy + 
                "', attempting to compose answer from available results,1,OracleDBAnswererHost,Host,System");
            
            if (!context.stepResults.isEmpty()) {
                String answer = composeAnswerFromStepResults(context);
                vertx.eventBus().publish("log", "Unknown strategy fallback answer: " + 
                    (answer.length() > 200 ? answer.substring(0, 197) + "..." : answer).replace("\n", " ") + 
                    ",3,OracleDBAnswererHost,Host,System");
                response.put("answer", answer);
                // Convert Map<String,JsonObject> to JsonObject
                JsonObject stepResultsJson = new JsonObject();
                context.stepResults.forEach(stepResultsJson::put);
                response.put("stepResults", stepResultsJson);
                response.put("confidence", calculateConfidence(context) * 0.8); // Lower confidence for unknown strategy
                promise.complete(response);
            } else {
                response.put("answer", "I was unable to process your query successfully.");
                response.put("error", "No valid results from pipeline");
                response.put("confidence", 0.1);
                promise.complete(response);
            }
        }
        
        return promise.future();
    }
    
    /**
     * Use LLM to compose a natural language answer from query results
     */
    private Future<String> composeAnswerWithLLM(ConversationContext context, JsonObject queryResults) {
        Promise<String> promise = Promise.<String>promise();
        
        String originalQuery = context.history.getJsonObject(context.history.size() - 1).getString("content");
        
        String systemPrompt = """
            You are a helpful database assistant. Convert the query results into a natural, 
            conversational answer that directly addresses the user's question.
            
            Guidelines:
            1. Be concise and direct
            2. Highlight key findings
            3. Use natural language, not technical database terms
            4. If the result set is large, summarize key points
            5. Format numbers and dates for readability
            6. If no results, explain clearly
            """;
        
        JsonObject promptData = new JsonObject()
            .put("userQuestion", originalQuery)
            .put("queryResults", queryResults)
            .put("rowCount", queryResults.getJsonArray("rows").size())
            .put("executedSQL", getExecutedSQL(context));
        
        List<JsonObject> messages = Arrays.asList(
            new JsonObject().put("role", "system").put("content", systemPrompt),
            new JsonObject().put("role", "user").put("content", 
                "User asked: " + originalQuery + "\n\n" +
                "Query results:\n" + promptData.encodePrettily() + "\n\n" +
                "Please provide a natural language answer.")
        );
        
        vertx.eventBus().publish("log", "Calling LLM to compose answer from " + 
            queryResults.getJsonArray("rows").size() + " rows,3,OracleDBAnswererHost,Host,System");
        
        llmService.chatCompletion(
            messages.stream().map(JsonObject::encode).collect(Collectors.toList()),
            0.7, // Some creativity for natural answers
            500
        ).whenComplete((result, error) -> {
            if (error == null) {
                vertx.eventBus().publish("log", "LLM response received: " + result.encode().substring(0, Math.min(200, result.encode().length())) + 
                    "...,3,OracleDBAnswererHost,Host,System");
                
                JsonArray choices = result.getJsonArray("choices");
                if (choices != null && !choices.isEmpty()) {
                    JsonObject firstChoice = choices.getJsonObject(0);
                    JsonObject message = firstChoice.getJsonObject("message");
                    if (message != null) {
                        String answer = message.getString("content", "No content in LLM response");
                        vertx.eventBus().publish("log", "Extracted answer from LLM: " + 
                            answer.substring(0, Math.min(100, answer.length())) + "...,3,OracleDBAnswererHost,Host,System");
                        promise.complete(answer);
                    } else {
                        vertx.eventBus().publish("log", "ERROR: No message in LLM response,0,OracleDBAnswererHost,Host,System");
                        promise.fail("No message in LLM response");
                    }
                } else {
                    vertx.eventBus().publish("log", "ERROR: No choices in LLM response,0,OracleDBAnswererHost,Host,System");
                    promise.fail("No choices in LLM response");
                }
            } else {
                vertx.eventBus().publish("log", "ERROR: LLM call failed: " + error.getMessage() + ",0,OracleDBAnswererHost,Host,System");
                promise.fail(error);
            }
        });
        
        return promise.future();
    }
    
    /**
     * Format a structured answer when LLM is not available
     */
    private String formatStructuredAnswer(JsonObject queryResults) {
        JsonArray rows = queryResults.getJsonArray("rows", new JsonArray());
        int rowCount = rows.size();
        
        if (rowCount == 0) {
            return "No results found for your query.";
        } else if (rowCount == 1) {
            return "Found 1 result:\n" + formatSingleRow(rows.getJsonObject(0));
        } else {
            return String.format("Found %d results. Here are the first few:\n%s", 
                rowCount, formatMultipleRows(rows, 5));
        }
    }
    
    private String formatSingleRow(JsonObject row) {
        StringBuilder sb = new StringBuilder();
        for (String key : row.fieldNames()) {
            sb.append(key).append(": ").append(row.getValue(key)).append("\n");
        }
        return sb.toString();
    }
    
    private String formatMultipleRows(JsonArray rows, int limit) {
        StringBuilder sb = new StringBuilder();
        int count = Math.min(rows.size(), limit);
        
        for (int i = 0; i < count; i++) {
            sb.append("\n").append(i + 1).append(". ");
            JsonObject row = rows.getJsonObject(i);
            List<String> values = new ArrayList<>();
            for (String key : row.fieldNames()) {
                values.add(key + ": " + row.getValue(key));
            }
            sb.append(String.join(", ", values));
        }
        
        if (rows.size() > limit) {
            sb.append("\n... and ").append(rows.size() - limit).append(" more results");
        }
        
        return sb.toString();
    }
    
    private String formatSchemaAnswer(JsonObject schemaInfo) {
        JsonArray tables = schemaInfo.getJsonArray("tables", new JsonArray());
        return String.format("Found %d tables in the schema. Use these table names in your queries.", 
            tables.size());
    }
    
    /**
     * Compose an answer from available step results when no specific result type is found
     */
    private String composeAnswerFromStepResults(ConversationContext context) {
        StringBuilder answer = new StringBuilder();
        answer.append("Based on the analysis of your query, here's what I found:\n\n");
        
        // Check intent evaluation
        JsonObject intentEval = context.getStepResult("evaluate_query_intent");
        if (intentEval != null) {
            // Check for nested result
            if (intentEval.containsKey("result") && intentEval.getJsonObject("result") != null) {
                intentEval = intentEval.getJsonObject("result");
            }
            String intent = intentEval.getString("intent", "");
            if (!intent.isEmpty()) {
                answer.append("Query Intent: ").append(intent).append("\n");
            }
        }
        
        // Check query analysis
        JsonObject queryAnalysis = context.getStepResult("analyze_query");
        if (queryAnalysis != null) {
            // Check for nested result
            if (queryAnalysis.containsKey("result") && queryAnalysis.getJsonObject("result") != null) {
                queryAnalysis = queryAnalysis.getJsonObject("result");
            }
            JsonArray entities = queryAnalysis.getJsonArray("entities", new JsonArray());
            if (!entities.isEmpty()) {
                answer.append("\nIdentified entities:\n");
                for (int i = 0; i < entities.size(); i++) {
                    JsonObject entity = entities.getJsonObject(i);
                    answer.append("- ").append(entity.getString("text", ""))
                          .append(" (").append(entity.getString("type", "")).append(")\n");
                }
            }
        }
        
        // Check schema matches
        JsonObject schemaMatches = context.getStepResult("match_oracle_schema");
        if (schemaMatches != null) {
            // Check for nested result
            if (schemaMatches.containsKey("result") && schemaMatches.getJsonObject("result") != null) {
                schemaMatches = schemaMatches.getJsonObject("result");
            }
            JsonArray matches = schemaMatches.getJsonArray("matches", new JsonArray());
            if (!matches.isEmpty()) {
                answer.append("\nRelevant database objects:\n");
                for (int i = 0; i < Math.min(matches.size(), 3); i++) {
                    JsonObject match = matches.getJsonObject(i);
                    JsonObject table = match.getJsonObject("table", new JsonObject());
                    answer.append("- ").append(table.getString("tableName", "Unknown table")).append("\n");
                }
            }
        }
        
        // Check SQL generation
        JsonObject sqlGen = context.getStepResult("generate_oracle_sql");
        if (sqlGen != null) {
            // Check for nested result
            if (sqlGen.containsKey("result") && sqlGen.getJsonObject("result") != null) {
                sqlGen = sqlGen.getJsonObject("result");
            }
            if (sqlGen.containsKey("sql")) {
                answer.append("\nGenerated SQL:\n```sql\n")
                      .append(sqlGen.getString("sql"))
                      .append("\n```\n");
            }
        }
        
        // Check validation results
        JsonObject validation = context.getStepResult("validate_oracle_sql");
        if (validation != null) {
            // Check for nested result
            if (validation.containsKey("result") && validation.getJsonObject("result") != null) {
                validation = validation.getJsonObject("result");
            }
            boolean isValid = validation.getBoolean("valid", false);
            if (!isValid) {
                answer.append("\nValidation issues found. ");
                if (validation.containsKey("errors")) {
                    answer.append("Errors: ").append(validation.getJsonArray("errors").encode());
                }
            }
        }
        
        // If we completed some steps but couldn't get a full result
        if (context.stepsCompleted > 0) {
            answer.append("\n\nI completed ")
                  .append(context.stepsCompleted)
                  .append(" steps of the analysis but was unable to provide a complete result. ");
            
            if (context.getStepResult("run_oracle_query") == null && sqlGen != null) {
                answer.append("The SQL query was generated but could not be executed.");
            }
        }
        
        return answer.toString();
    }
    
    /**
     * Calculate confidence based on pipeline execution
     */
    private double calculateConfidence(ConversationContext context) {
        double confidence = 1.0;
        
        // Reduce confidence if optional steps failed
        if (context.getStepResult("map_business_terms") == null) {
            confidence *= 0.95;
        }
        if (context.getStepResult("optimize_oracle_sql") == null) {
            confidence *= 0.95;
        }
        
        // Reduce confidence if validation had issues
        JsonObject validation = context.getStepResult("validate_oracle_sql");
        if (validation != null && !validation.getBoolean("valid", true)) {
            confidence *= 0.8;
        }
        
        // Check strategy confidence
        JsonObject strategySelection = context.getStepResult("strategy_selection");
        if (strategySelection != null) {
            confidence *= strategySelection.getDouble("confidence", 1.0);
        }
        
        return Math.max(0.1, confidence);
    }
    
    /**
     * Get the SQL that was executed
     */
    private String getExecutedSQL(ConversationContext context) {
        // Try validated SQL first
        JsonObject validated = context.getStepResult("validate_oracle_sql");
        if (validated != null) {
            return validated.getString("sql", "");
        }
        
        // Then optimized
        JsonObject optimized = context.getStepResult("optimize_oracle_sql");
        if (optimized != null) {
            return optimized.getString("optimizedSQL", optimized.getString("sql", ""));
        }
        
        // Then generated
        JsonObject generated = context.getStepResult("generate_oracle_sql");
        if (generated != null) {
            return generated.getString("sql", "");
        }
        
        return "";
    }
    
    /**
     * Determine SQL complexity based on intent analysis
     */
    private String determineComplexity(ConversationContext context) {
        // First check the tool strategy decision which includes complexity
        JsonObject toolDecision = context.getStepResult("tool_strategy_decision");
        if (toolDecision != null) {
            JsonObject intentAnalysis = toolDecision.getJsonObject("intentAnalysis");
            if (intentAnalysis != null && intentAnalysis.containsKey("complexity")) {
                return intentAnalysis.getString("complexity", "medium");
            }
        }
        
        // Then check direct intent evaluation
        JsonObject intent = context.getStepResult("intent_evaluation");
        if (intent != null && intent.containsKey("complexity")) {
            return intent.getString("complexity", "medium");
        }
        
        // Check query analysis result
        JsonObject queryAnalysis = context.getStepResult("analyze_query");
        if (queryAnalysis != null) {
            // Infer complexity from query analysis
            JsonArray entities = queryAnalysis.getJsonArray("entities", new JsonArray());
            JsonArray aggregations = queryAnalysis.getJsonArray("aggregations", new JsonArray());
            
            if (entities.size() > 2 || aggregations.size() > 0) {
                return "complex";
            } else if (entities.size() > 1) {
                return "medium";
            }
        }
        
        // Default to medium complexity
        return "medium";
    }
    
    /**
     * Get supported strategies for this host
     */
    private JsonArray getSupportedStrategies() {
        JsonArray supported = new JsonArray();
        supported.add("oracle_full_pipeline");
        supported.add("oracle_schema_exploration");
        supported.add("oracle_discovery_first");
        supported.add("oracle_performance_focused");
        supported.add("oracle_simple_query");
        supported.add("oracle_adaptive_pipeline");
        supported.add("oracle_truly_intelligent");
        supported.add("oracle_validated_pipeline");
        return supported;
    }
    
    /**
     * Clean up old conversations to prevent memory leaks
     */
    private void cleanupOldConversations() {
        long cutoffTime = System.currentTimeMillis() - (30 * 60 * 1000); // 30 minutes
        
        conversations.entrySet().removeIf(entry -> {
            ConversationContext context = entry.getValue();
            return context.startTime < cutoffTime;
        });
    }
    
    @Override
    public void stop(Promise<Void> stopPromise) {
        // Shutdown all managers
        List<Future> shutdownFutures = new ArrayList<>();
        
        if (executionManager != null) shutdownFutures.add(executionManager.shutdown());
        if (sqlPipelineManager != null) shutdownFutures.add(sqlPipelineManager.shutdown());
        if (schemaIntelligenceManager != null) shutdownFutures.add(schemaIntelligenceManager.shutdown());
        if (intentAnalysisManager != null) shutdownFutures.add(intentAnalysisManager.shutdown());
        if (strategyOrchestrationManager != null) shutdownFutures.add(strategyOrchestrationManager.shutdown());
        
        CompositeFuture.all(shutdownFutures).onComplete(ar -> {
            // Clean up resources
            conversations.clear();
            performanceMetrics.clear();
            
            vertx.eventBus().publish("log", "OracleDBAnswererHost stopped,2,OracleDBAnswererHost,Host,System");
            stopPromise.complete();
        });
    }
    
    /**
     * Publish streaming event to the correct event bus address
     */
    private void publishStreamingEvent(String conversationId, String eventType, JsonObject data) {
        // Use "streaming." prefix to match ConversationStreaming expectations
        String address = "streaming." + conversationId + "." + eventType;
        data.put("timestamp", System.currentTimeMillis());
        data.put("host", "OracleDBAnswererHost");
        eventBus.publish(address, data);
    }
}
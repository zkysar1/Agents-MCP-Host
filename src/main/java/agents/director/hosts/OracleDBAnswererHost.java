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
    
    
    
    // MCP Clients for all servers
    private final Map<String, UniversalMCPClient> mcpClients = new ConcurrentHashMap<>();
    
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
            .compose(v -> initializeMCPClients())
            .compose(v -> registerEventBusConsumers())
            .onComplete(ar -> {
                if (ar.succeeded()) {
                    vertx.eventBus().publish("log", "OracleDBAnswererHost started successfully with " + mcpClients.size() + " MCP clients,2,OracleDBAnswererHost,Host,System");
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
    
    private Future<Void> initializeMCPClients() {
        Promise<Void> promise = Promise.<Void>promise();
        List<Future> clientFutures = new ArrayList<>();
        
        // Create clients for all MCP servers
        String baseUrl = "http://localhost:8080";
        
        // Query Intent Evaluation Client
        UniversalMCPClient intentClient = new UniversalMCPClient(
            "QueryIntentEvaluation", 
            baseUrl + "/mcp/servers/query-intent"
        );
        clientFutures.add(deployClient(intentClient, "query-intent"));
        
        // Oracle Query Analysis Client
        UniversalMCPClient analysisClient = new UniversalMCPClient(
            "OracleQueryAnalysis",
            baseUrl + "/mcp/servers/oracle-query-analysis"
        );
        clientFutures.add(deployClient(analysisClient, "oracle-query-analysis"));
        
        // Oracle Schema Intelligence Client
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
        
        // Oracle SQL Generation Client
        UniversalMCPClient sqlGenClient = new UniversalMCPClient(
            "OracleSQLGeneration",
            baseUrl + "/mcp/servers/oracle-sql-gen"
        );
        clientFutures.add(deployClient(sqlGenClient, "oracle-sql-gen"));
        
        // Oracle SQL Validation Client
        UniversalMCPClient sqlValClient = new UniversalMCPClient(
            "OracleSQLValidation",
            baseUrl + "/mcp/servers/oracle-sql-val"
        );
        clientFutures.add(deployClient(sqlValClient, "oracle-sql-val"));
        
        // Oracle Query Execution Client
        UniversalMCPClient execClient = new UniversalMCPClient(
            "OracleQueryExecution",
            baseUrl + "/mcp/servers/oracle-db"
        );
        clientFutures.add(deployClient(execClient, "oracle-db"));
        
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
        
        // Strategy Orchestrator Client
        UniversalMCPClient orchestratorClient = new UniversalMCPClient(
            "StrategyOrchestrator",
            baseUrl + "/mcp/servers/strategy-orchestrator"
        );
        clientFutures.add(deployClient(orchestratorClient, "strategy-orchestrator"));
        
        // Strategy Learning Client
        UniversalMCPClient learningClient = new UniversalMCPClient(
            "StrategyLearning",
            baseUrl + "/mcp/servers/strategy-learning"
        );
        clientFutures.add(deployClient(learningClient, "strategy-learning"));
        
        // Wait for all clients to be ready
        CompositeFuture.all(clientFutures).onComplete(ar -> {
            if (ar.succeeded()) {
                vertx.eventBus().publish("log", "All MCP clients initialized successfully,2,OracleDBAnswererHost,Host,System");
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
                vertx.eventBus().publish("log", "Deployed client for " + serverKey + "" + ",3,OracleDBAnswererHost,Host,System");
                promise.complete(ar.result());
            } else {
                vertx.eventBus().publish("log", "Failed to deploy client for " + serverKey + "" + ",0,OracleDBAnswererHost,Host,System");
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
                .put("clients", mcpClients.size())
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
                    
                    // Publish complete event if streaming
                    if (sessionId != null && streaming) {
                        publishStreamingEvent(conversationId, "tool.complete", new JsonObject()
                            .put("tool", "oracle_database_pipeline")
                            .put("success", true)
                            .put("resultSummary", "Completed query with confidence " + 
                                response.getDouble("confidence", 0.0)));
                        
                        // Publish final event
                        publishStreamingEvent(conversationId, "final", new JsonObject()
                            .put("content", response.getString("answer", ""))
                            .put("conversationId", conversationId)
                            .put("confidence", response.getDouble("confidence", 0.0))
                            .put("hasData", response.containsKey("data")));
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
                return executeStrategy(context, strategy, streaming);
            })
            .onComplete(promise);
        
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
                    vertx.eventBus().publish("log", "Dynamic strategy generation failed, using fallback" + ",1,OracleDBAnswererHost,Host,System");
                    JsonObject fallback = getFallbackStrategySelection();
                    
                    // Add streaming notification about fallback
                    if (context.sessionId != null && context.streaming) {
                        publishStreamingEvent(context.conversationId, "progress", new JsonObject()
                            .put("step", "strategy_fallback")
                            .put("message", "Using fallback strategy due to generation failure")
                            .put("details", new JsonObject()
                                .put("reason", ar.cause().getMessage())
                                .put("method", "fallback")));
                    }
                    
                    promise.complete(fallback);
                }
            });
        
        return promise.future();
    }
    
    /**
     * Generate a dynamic strategy using the new MCP servers
     */
    private Future<JsonObject> generateDynamicStrategy(ConversationContext context, String query) {
        Promise<JsonObject> promise = Promise.<JsonObject>promise();
        
        // Step 1: Analyze intent
        UniversalMCPClient intentClient = mcpClients.get("intent-analysis");
        UniversalMCPClient strategyGenClient = mcpClients.get("strategy-generation");
        
        if (intentClient == null || strategyGenClient == null) {
            promise.fail("Required strategy generation clients not available");
            return promise.future();
        }
        
        // Analyze intent first
        JsonObject intentArgs = new JsonObject()
            .put("query", query)
            .put("conversation_history", context.getRecentHistory(5))
            .put("user_profile", new JsonObject()
                .put("expertise_level", "intermediate")); // Could be dynamic
        
        intentClient.callTool("intent_analysis__extract_intent", intentArgs)
            .compose(intentResult -> {
                context.storeStepResult("intent_analysis", intentResult);
                
                // Analyze complexity
                JsonObject complexityArgs = new JsonObject()
                    .put("query", query)
                    .put("context", new JsonObject()
                        .put("previous_queries", context.getRecentHistory(3)));
                
                return strategyGenClient.callTool("strategy_generation__analyze_complexity", complexityArgs)
                    .map(complexity -> {
                        context.storeStepResult("complexity_analysis", complexity);
                        return new JsonObject()
                            .put("intent", intentResult)
                            .put("complexity", complexity);
                    });
            })
            .compose(analysis -> {
                // Generate strategy
                JsonObject strategyArgs = new JsonObject()
                    .put("query", query)
                    .put("intent", analysis.getJsonObject("intent"))
                    .put("complexity_analysis", analysis.getJsonObject("complexity"))
                    .put("constraints", new JsonObject()
                        .put("max_steps", 12)
                        .put("timeout_seconds", 60));
                
                return strategyGenClient.callTool("strategy_generation__create_strategy", strategyArgs);
            })
            .onComplete(ar -> {
                if (ar.succeeded()) {
                    JsonObject generatedStrategy = ar.result();
                    context.storeStepResult("generated_strategy", generatedStrategy);
                    
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
    
    private JsonObject getFallbackStrategySelection() {
        return new JsonObject()
            .put("selectedStrategy", "oracle_full_pipeline")
            .put("selectedHost", "oracledbanswerer")
            .put("confidence", 0.5)
            .put("method", "fallback")
            .put("reasoning", "Using default pipeline");
    }
    
    /**
     * Execute the selected orchestration strategy step by step
     */
    private Future<JsonObject> executeStrategy(ConversationContext context, 
                                             JsonObject strategySelection, 
                                             boolean streaming) {
        Promise<JsonObject> promise = Promise.<JsonObject>promise();
        
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
        
        // Execute steps sequentially (traditional approach)
        executeSteps(context, steps, 0, new JsonObject())
            .compose(finalResult -> {
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
        
        UniversalMCPClient orchestratorClient = mcpClients.get("strategy-orchestrator");
        UniversalMCPClient learningClient = mcpClients.get("strategy-learning");
        
        JsonArray steps = strategy.getJsonArray("steps", new JsonArray());
        
        // Execute with adaptation
        executeStepsWithAdaptation(context, strategy, steps, 0, new JsonObject())
            .compose(finalResult -> {
                // Record execution for learning
                if (learningClient != null && learningClient.isReady()) {
                    JsonObject recordArgs = new JsonObject()
                        .put("strategy", strategy)
                        .put("execution_results", new JsonObject()
                            .put("success", true)
                            .put("total_duration", System.currentTimeMillis() - context.startTime)
                            .put("steps_completed", steps.size()))
                        .put("performance_metrics", new JsonObject()
                            .put("query_complexity", context.getStepResult("complexity_analysis")
                                .getJsonObject("factors", new JsonObject()).getFloat("complexity_score", 0.5f)));
                    
                    learningClient.callTool("strategy_learning__record_execution", recordArgs)
                        .onComplete(ar -> {
                            if (ar.failed()) {
                                vertx.eventBus().publish("log", "Failed to record execution: " + ar.cause() + ",1,OracleDBAnswererHost,Host,System");
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
        
        UniversalMCPClient orchestratorClient = mcpClients.get("strategy-orchestrator");
        JsonObject currentStep = steps.getJsonObject(currentIndex);
        
        // Execute current step
        return executeStep(context, currentStep, accumulated)
            .compose(result -> {
                accumulated.mergeIn(new JsonObject()
                    .put(currentStep.getString("tool") + "_result", result));
                
                // Evaluate progress if orchestrator available
                if (orchestratorClient != null && orchestratorClient.isReady() && 
                    currentIndex % 3 == 2) { // Check every 3 steps
                    
                    JsonObject evalArgs = new JsonObject()
                        .put("strategy", strategy)
                        .put("completed_steps", getCompletedSteps(steps, currentIndex + 1))
                        .put("current_results", accumulated)
                        .put("time_elapsed", System.currentTimeMillis() - context.startTime);
                    
                    return orchestratorClient.callTool("strategy_orchestrator__evaluate_progress", evalArgs)
                        .compose(evaluation -> {
                            if (!evaluation.getBoolean("on_track", true)) {
                                // Adapt strategy if needed
                                JsonObject adaptArgs = new JsonObject()
                                    .put("current_strategy", strategy)
                                    .put("evaluation", evaluation);
                                
                                return orchestratorClient.callTool("strategy_orchestrator__adapt_strategy", adaptArgs)
                                    .map(adapted -> {
                                        JsonArray newSteps = adapted.getJsonObject("adapted_strategy")
                                            .getJsonArray("steps", steps);
                                        return newSteps;
                                    });
                            }
                            return Future.succeededFuture(steps);
                        })
                        .compose(possiblyAdaptedSteps -> {
                            return executeStepsWithAdaptation(context, strategy, possiblyAdaptedSteps, 
                                currentIndex + 1, accumulated);
                        });
                } else {
                    // Continue without evaluation
                    return executeStepsWithAdaptation(context, strategy, steps, 
                        currentIndex + 1, accumulated);
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
        
        vertx.eventBus().publish("log", "Executing step " + currentStepIndex + 1 + ": " + tool + " - " + description + "" + ",3,OracleDBAnswererHost,Host,System");
        
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
        
        UniversalMCPClient client = mcpClients.get(server);
        if (client == null || !client.isReady()) {
            promise.fail("Client not ready for server: " + server);
            return promise.future();
        }
        
        // Build arguments based on the tool and previous results
        JsonObject arguments = buildToolArguments(context, tool, previousResults);
        
        // Track performance
        long startTime = System.currentTimeMillis();
        
        client.callTool(tool, arguments)
            .onComplete(ar -> {
                long duration = System.currentTimeMillis() - startTime;
                performanceMetrics.put(tool, duration);
                
                if (ar.succeeded()) {
                    vertx.eventBus().publish("log", "Step " + tool + " completed in " + duration + "ms" + ",3,OracleDBAnswererHost,Host,System");
                    promise.complete(ar.result());
                } else {
                    promise.fail("Step " + tool + " failed: " + ar.cause().getMessage());
                }
            });
        
        return promise.future();
    }
    
    /**
     * Build arguments for each tool based on context and previous results
     */
    private JsonObject buildToolArguments(ConversationContext context, 
                                        String tool, 
                                        JsonObject previousResults) {
        JsonObject args = new JsonObject();
        
        switch (tool) {
            case "evaluate_query_intent":
                // Already handled in strategy selection
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
                    
                    // Add context from schema matching
                    JsonObject schemaContext = context.getStepResult("match_oracle_schema");
                    if (schemaContext != null) {
                        args.put("context", new JsonObject()
                            .put("schemaMatches", schemaContext)
                            .put("queryAnalysis", queryAnalysis));
                    }
                }
                break;
                
            case "generate_oracle_sql":
                args.put("analysis", context.getStepResult("analyze_query"));
                args.put("schemaMatches", context.getStepResult("match_oracle_schema"));
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
        
        JsonObject response = new JsonObject();
        
        // Check if we have query results
        JsonObject queryResults = context.getStepResult("run_oracle_query");
        
        if (queryResults != null && queryResults.containsKey("rows")) {
            // We have data results
            JsonArray rows = queryResults.getJsonArray("rows");
            JsonArray columns = queryResults.getJsonArray("columns", new JsonArray());
            
            // Use LLM to compose a natural language answer
            if (llmService.isInitialized()) {
                composeAnswerWithLLM(context, queryResults)
                    .onComplete(ar -> {
                        if (ar.succeeded()) {
                            response.put("answer", ar.result());
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
            } else {
                // No LLM available, return structured response
                response.put("answer", formatStructuredAnswer(queryResults));
                response.put("data", queryResults);
                response.put("executedSQL", getExecutedSQL(context));
                response.put("confidence", 0.7);
                promise.complete(response);
            }
        } else if (context.currentStrategy.contains("sql_only")) {
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
            // Unknown or error case
            response.put("answer", "I was unable to process your query successfully.");
            response.put("error", "No valid results from pipeline");
            response.put("confidence", 0.1);
            promise.complete(response);
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
        
        llmService.chatCompletion(
            messages.stream().map(JsonObject::encode).collect(Collectors.toList()),
            0.7, // Some creativity for natural answers
            500
        ).whenComplete((result, error) -> {
            if (error == null) {
                String answer = result.getJsonArray("choices")
                    .getJsonObject(0)
                    .getJsonObject("message")
                    .getString("content");
                promise.complete(answer);
            } else {
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
        // Clean up resources
        conversations.clear();
        performanceMetrics.clear();
        
        vertx.eventBus().publish("log", "OracleDBAnswererHost stopped,2,OracleDBAnswererHost,Host,System");
        stopPromise.complete();
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
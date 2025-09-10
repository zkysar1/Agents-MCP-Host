package agents.director.hosts;

import agents.director.Driver;
import agents.director.config.SimpleAgentConfig;
import agents.director.hosts.base.intelligence.IntentEngine;
import agents.director.hosts.base.intelligence.StrategyPicker;
import agents.director.hosts.base.managers.*;
import agents.director.services.LlmAPIService;
import agents.director.services.InterruptManager;
import io.vertx.core.*;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Universal Host - Simplified host that interprets backstory and guidance to determine actions.
 * 
 * This simplified host takes just two strings from the frontend:
 * - backstory: context about what kind of agent this should be
 * - guidance: specific guidance about what to do
 * 
 * It uses LLM to interpret these strings and determine:
 * - What managers to initialize
 * - What execution strategy to use
 * - Whether database execution is needed
 * 
 * Simple Architecture:
 * 1. Accept backstory + guidance from frontend
 * 2. Use LLM to interpret requirements 
 * 3. Initialize needed managers dynamically
 * 4. Execute simple strategy
 * 5. Return results
 */
public class UniversalHost extends AbstractVerticle {
    
    // Simple utility components
    private IntentEngine intentEngine;
    private StrategyPicker strategyPicker;
    
    // Service references
    private LlmAPIService llmService;
    private EventBus eventBus;
    
    // Dynamic manager registry
    private final Map<String, MCPClientManager> managers = new ConcurrentHashMap<>();
    private volatile boolean managersReady = false;
    
    // Conversation management
    private final Map<String, ConversationContext> conversations = new ConcurrentHashMap<>();
    
    // Configuration
    private static final long CONVERSATION_TIMEOUT = 30 * 60 * 1000; // 30 minutes
    
    /**
     * Inner class for conversation context management
     */
    private static class ConversationContext {
        String conversationId;
        String sessionId;
        boolean streaming;
        JsonArray history = new JsonArray();
        Map<String, JsonObject> stepResults = new HashMap<>();
        Map<String, Exception> stepErrors = new HashMap<>();
        int currentStep = 0;
        int stepsCompleted = 0;
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
        
        JsonObject getStepResultSafe(String stepName) {
            return stepResults.getOrDefault(stepName, new JsonObject());
        }
        
        void incrementCompletedSteps() {
            stepsCompleted++;
        }
        
        int getCompletedSteps() {
            return stepsCompleted;
        }
        
        void recordStepError(String stepName, Exception error) {
            stepErrors.put(stepName, error);
        }
        
        Exception getStepError(String stepName) {
            return stepErrors.get(stepName);
        }
        
        boolean isExpired() {
            return System.currentTimeMillis() - startTime > CONVERSATION_TIMEOUT;
        }
    }
    
    @Override
    public void start(Promise<Void> startPromise) {
        eventBus = vertx.eventBus();
        llmService = LlmAPIService.getInstance();
        
        // Initialize simple utility components
        intentEngine = new IntentEngine(llmService);
        strategyPicker = new StrategyPicker();
        
        // Initialize all managers once at startup for fixed pipeline
        initializeAllManagers()
            .compose(v -> registerEventBusConsumers())
            .compose(this::startConversationCleanup)
            .onComplete(ar -> {
                if (ar.succeeded()) {
                    managersReady = true;
                    vertx.eventBus().publish("log", 
                        "UniversalHost started successfully with FIXED PIPELINE v2.0 architecture (10 levels)" +
                        ",2,UniversalHost,Host,System");
                    startPromise.complete();
                } else {
                    vertx.eventBus().publish("log", 
                        "Failed to start UniversalHost: " + ar.cause().getMessage() +
                        ",0,UniversalHost,Host,System");
                    startPromise.fail(ar.cause());
                }
            });
    }
    
    /**
     * Register event bus consumers for this host
     */
    private Future<Void> registerEventBusConsumers() {
        Promise<Void> promise = Promise.promise();
        
        // Main processing endpoint - accepts backstory and guidance
        eventBus.<JsonObject>consumer("host.universal.process", this::processQuery);
        
        // Status endpoint
        eventBus.<JsonObject>consumer("host.universal.status", message -> {
            message.reply(new JsonObject()
                .put("status", "ready")
                .put("managersLoaded", managers.size())
                .put("activeConversations", conversations.size())
                .put("architecture", "fixed_pipeline_v2")
                .put("pipeline_levels", 10)
                .put("pipeline_description", strategyPicker.getPipelineDescription())
                .put("version", "2.0"));
        });
        
        // Clear conversation endpoint
        eventBus.<JsonObject>consumer("host.universal.clear", message -> {
            String conversationId = message.body().getString("conversationId");
            if (conversationId != null) {
                conversations.remove(conversationId);
                message.reply(new JsonObject().put("cleared", true));
            } else {
                message.fail(400, "conversationId required");
            }
        });
        
        promise.complete();
        return promise.future();
    }
    
    /**
     * Start conversation cleanup timer
     */
    private Future<Void> startConversationCleanup(Void v) {
        // Run cleanup every 5 minutes
        vertx.setPeriodic(5 * 60 * 1000, id -> {
            int removed = 0;
            
            Iterator<Map.Entry<String, ConversationContext>> iterator = conversations.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, ConversationContext> entry = iterator.next();
                if (entry.getValue().isExpired()) {
                    iterator.remove();
                    removed++;
                }
            }
            
            if (removed > 0) {
                vertx.eventBus().publish("log", 
                    "Cleaned up " + removed + " expired conversations" +
                    ",3,UniversalHost,Host,System");
            }
        });
        
        return Future.succeededFuture();
    }
    
    /**
     * Main query processing method - simplified pipeline with backstory/guidance
     */
    private void processQuery(Message<JsonObject> message) {
        JsonObject request = message.body();
        String query = request.getString("query");
        String backstory = request.getJsonObject("options", new JsonObject()).getString("backstory", "General assistant");
        String guidance = request.getJsonObject("options", new JsonObject()).getString("guidance", "Help the user with their request");
        String conversationId = request.getString("conversationId", UUID.randomUUID().toString());
        String sessionId = request.getString("sessionId");
        boolean streaming = request.getBoolean("streaming", true);
        
        vertx.eventBus().publish("log", 
            "Processing query with backstory and guidance for conversation " + conversationId +
            ",2,UniversalHost,Host,System");
        
        // Publish start event if streaming
        if (sessionId != null && streaming) {
            publishStreamingEvent(conversationId, "progress", new JsonObject()
                .put("step", "host_started")
                .put("message", "Starting UniversalHost processing")
                .put("details", new JsonObject()
                    .put("host", "UniversalHost")
                    .put("hasBackstory", backstory != null && !backstory.trim().isEmpty())
                    .put("hasGuidance", guidance != null && !guidance.trim().isEmpty())
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
        
        context.sessionId = sessionId;
        context.streaming = streaming;
        context.addMessage("user", query);
        
        // Execute FIXED PIPELINE: Analyze Depth → Initialize All → Execute to Depth
        executeFixedPipeline(context, backstory, guidance, query, streaming)
            .onComplete(ar -> {
                if (ar.succeeded()) {
                    JsonObject response = ar.result();
                    response.put("conversationId", conversationId);
                    response.put("duration", System.currentTimeMillis() - context.startTime);
                    
                    // Publish final event if streaming
                    if (sessionId != null && streaming) {
                        publishStreamingEvent(conversationId, "final", new JsonObject()
                            .put("content", response.getString("answer", ""))
                            .put("conversationId", conversationId)
                            .put("confidence", response.getDouble("confidence", 0.0))
                            .put("hasData", response.containsKey("data")));
                    }
                    
                    vertx.eventBus().publish("log", 
                        "Completed processing for conversation " + conversationId +
                        ",2,UniversalHost,Host,System");
                    
                    message.reply(response);
                    context.addMessage("assistant", response.getString("answer", ""));
                } else {
                    String errorMsg = ar.cause() != null ? ar.cause().getMessage() : "Unknown error";
                    
                    // Publish error event if streaming
                    if (sessionId != null && streaming) {
                        publishStreamingEvent(conversationId, "error", new JsonObject()
                            .put("error", errorMsg)
                            .put("severity", "ERROR")
                            .put("host", "UniversalHost"));
                        
                        // Also publish final event to close the stream
                        publishStreamingEvent(conversationId, "final", new JsonObject()
                            .put("content", "")
                            .put("error", errorMsg)
                            .put("conversationId", conversationId));
                    }
                    
                    vertx.eventBus().publish("log", 
                        "Failed processing for conversation " + conversationId + ": " + errorMsg +
                        ",0,UniversalHost,Host,System");
                    
                    message.fail(500, errorMsg);
                }
                
                cleanupOldConversations();
            });
    }
    
    /**
     * Execute the fixed pipeline architecture: Analyze Depth → Initialize All → Execute to Depth
     */
    private Future<JsonObject> executeFixedPipeline(ConversationContext context, String backstory, 
                                                  String guidance, String query, boolean streaming) {
        Promise<JsonObject> promise = Promise.promise();
        
        vertx.eventBus().publish("log", 
            "Starting FIXED PIPELINE architecture for conversation " + context.conversationId +
            ",2,UniversalHost,Host,System");
        
        // Step 1: Analyze execution depth using IntentEngine
        intentEngine.analyzeExecutionDepth(backstory, guidance, query)
            .compose(depthAnalysis -> {
                // Publish depth analysis event if streaming
                if (streaming && context.sessionId != null) {
                    publishStreamingEvent(context.conversationId, "pipeline.depth_determined", new JsonObject()
                        .put("execution_depth", depthAnalysis.getInteger("execution_depth"))
                        .put("query_type", depthAnalysis.getString("query_type"))
                        .put("complexity", depthAnalysis.getJsonObject("complexity"))
                        .put("reasoning", depthAnalysis.getString("reasoning")));
                }
                
                // Step 2: Managers already initialized at startup, just return depth analysis
                return Future.succeededFuture(depthAnalysis);
            })
            .compose(depthAnalysis -> {
                // Step 3: Create fixed pipeline strategy using StrategyPicker
                int executionDepth = depthAnalysis.getInteger("execution_depth");
                JsonObject queryContext = new JsonObject()
                    .put("query", query)
                    .put("backstory", backstory)
                    .put("guidance", guidance)
                    .put("depth_analysis", depthAnalysis);
                
                JsonObject strategy = strategyPicker.createFixedPipelineStrategy(executionDepth, queryContext);
                
                // Publish strategy creation event if streaming
                if (streaming && context.sessionId != null) {
                    publishStreamingEvent(context.conversationId, "pipeline.strategy_created", new JsonObject()
                        .put("strategy_type", "fixed_pipeline")
                        .put("execution_depth", executionDepth)
                        .put("total_steps", strategy.getJsonObject("strategy").getJsonArray("steps").size())
                        .put("architecture_version", "2.0_fixed_pipeline"));
                }
                
                // Step 4: Execute the fixed pipeline to the determined depth
                return executeFixedPipelineStrategy(context, strategy, depthAnalysis, streaming);
            })
            .onComplete(promise);
        
        return promise.future();
    }
    
    /**
     * Initialize ALL managers for fixed pipeline architecture.
     * Unlike the old dynamic approach, we now initialize all managers
     * since the pipeline is fixed and predictable.
     */
    private Future<Void> initializeAllManagers() {
        Promise<Void> promise = Promise.promise();
        String baseUrl = Driver.BASE_URL;
        
        // Fixed set of ALL managers required for the 10-level pipeline
        String[] allManagers = {
            "IntentAnalysisManager",
            "SchemaIntelligenceManager", 
            "SQLPipelineManager",
            "OracleExecutionManager",
            "StrategyOrchestrationManager"
        };
        
        List<Future> initFutures = new ArrayList<>();
        
        vertx.eventBus().publish("log", 
            "Initializing ALL managers for fixed pipeline architecture" +
            ",2,UniversalHost,Host,System");
        
        for (String managerName : allManagers) {
            // Only create manager if not already created
            if (!managers.containsKey(managerName)) {
                MCPClientManager manager = createManager(managerName, baseUrl);
                
                if (manager != null) {
                    managers.put(managerName, manager);
                    initFutures.add(manager.initialize());
                    
                    vertx.eventBus().publish("log", 
                        "Created manager: " + managerName +
                        ",3,UniversalHost,Host,System");
                } else {
                    vertx.eventBus().publish("log", 
                        "Failed to create manager: " + managerName +
                        ",0,UniversalHost,Host,System");
                }
            }
        }
        
        if (initFutures.isEmpty()) {
            vertx.eventBus().publish("log", 
                "All required managers already initialized" +
                ",2,UniversalHost,Host,System");
            promise.complete();
        } else {
            CompositeFuture.all(initFutures).onComplete(ar -> {
                if (ar.succeeded()) {
                    vertx.eventBus().publish("log", 
                        "All " + initFutures.size() + " new managers initialized successfully" +
                        ",2,UniversalHost,Host,System");
                    promise.complete();
                } else {
                    vertx.eventBus().publish("log", 
                        "Failed to initialize managers: " + ar.cause().getMessage() +
                        ",0,UniversalHost,Host,System");
                    promise.fail(ar.cause());
                }
            });
        }
        
        return promise.future();
    }
    
    /**
     * Initialize managers dynamically based on LLM interpretation (DEPRECATED - kept for compatibility)
     */
    private Future<Void> initializeManagers(JsonArray requiredManagers) {
        Promise<Void> promise = Promise.promise();
        String baseUrl = Driver.BASE_URL;
        
        List<Future> initFutures = new ArrayList<>();
        
        vertx.eventBus().publish("log", 
            "Initializing " + requiredManagers.size() + " managers based on LLM interpretation" +
            ",2,UniversalHost,Host,System");
        
        for (int i = 0; i < requiredManagers.size(); i++) {
            String managerName = requiredManagers.getString(i);
            
            // Only create manager if not already created
            if (!managers.containsKey(managerName)) {
                MCPClientManager manager = createManager(managerName, baseUrl);
                
                if (manager != null) {
                    managers.put(managerName, manager);
                    initFutures.add(manager.initialize());
                    
                    vertx.eventBus().publish("log", 
                        "Created manager: " + managerName +
                        ",3,UniversalHost,Host,System");
                } else {
                    vertx.eventBus().publish("log", 
                        "Unknown manager type: " + managerName +
                        ",1,UniversalHost,Host,System");
                }
            }
        }
        
        if (initFutures.isEmpty()) {
            vertx.eventBus().publish("log", 
                "All required managers already initialized" +
                ",2,UniversalHost,Host,System");
            promise.complete();
        } else {
            CompositeFuture.all(initFutures).onComplete(ar -> {
                if (ar.succeeded()) {
                    vertx.eventBus().publish("log", 
                        "All " + initFutures.size() + " new managers initialized successfully" +
                        ",2,UniversalHost,Host,System");
                    promise.complete();
                } else {
                    vertx.eventBus().publish("log", 
                        "Failed to initialize managers: " + ar.cause().getMessage() +
                        ",0,UniversalHost,Host,System");
                    promise.fail(ar.cause());
                }
            });
        }
        
        return promise.future();
    }
    
    /**
     * Create manager instance based on type
     */
    private MCPClientManager createManager(String managerName, String baseUrl) {
        switch (managerName) {
            case "OracleExecutionManager":
                return new OracleExecutionManager(vertx, baseUrl);
            case "SQLPipelineManager":
                return new SQLPipelineManager(vertx, baseUrl);
            case "SchemaIntelligenceManager":
                return new SchemaIntelligenceManager(vertx, baseUrl);
            case "IntentAnalysisManager":
                return new IntentAnalysisManager(vertx, baseUrl);
            case "StrategyOrchestrationManager":
                return new StrategyOrchestrationManager(vertx, baseUrl);
            default:
                return null;
        }
    }
    
    /**
     * Execute the fixed pipeline strategy up to the determined depth.
     * This method implements streaming events at each pipeline level completion.
     */
    private Future<JsonObject> executeFixedPipelineStrategy(ConversationContext context, JsonObject strategyWrapper, 
                                                           JsonObject depthAnalysis, boolean streaming) {
        JsonObject strategy = strategyWrapper.getJsonObject("strategy", new JsonObject());
        JsonArray steps = strategy.getJsonArray("steps", new JsonArray());
        int executionDepth = strategy.getInteger("execution_depth", 1);
        
        if (steps.isEmpty()) {
            // No steps - use direct LLM execution
            return handleDirectLLMExecution(context);
        }
        
        // Publish pipeline start event
        if (streaming && context.sessionId != null) {
            publishStreamingEvent(context.conversationId, "pipeline.execution_start", new JsonObject()
                .put("total_levels", executionDepth)
                .put("total_steps", steps.size())
                .put("architecture", "fixed_pipeline_v2"));
        }
        
        // Execute fixed pipeline levels sequentially with streaming
        return executeFixedPipelineLevels(context, steps, 0, new JsonObject(), executionDepth, streaming);
    }
    
    /**
     * Execute fixed pipeline levels sequentially with streaming events
     */
    private Future<JsonObject> executeFixedPipelineLevels(ConversationContext context, JsonArray steps, 
                                                         int currentLevelIndex, JsonObject accumulatedResult,
                                                         int maxDepth, boolean streaming) {
        if (currentLevelIndex >= steps.size() || currentLevelIndex >= maxDepth) {
            // Pipeline execution complete
            if (streaming && context.sessionId != null) {
                publishStreamingEvent(context.conversationId, "pipeline.execution_complete", new JsonObject()
                    .put("levels_completed", currentLevelIndex)
                    .put("max_depth", maxDepth)
                    .put("total_duration", System.currentTimeMillis() - context.startTime));
            }
            return Future.succeededFuture(composeFixedPipelineResponse(context, accumulatedResult));
        }
        
        // Check for interrupts before each level
        if (context.sessionId != null && streaming) {
            InterruptManager im = new InterruptManager(vertx);
            if (im.isInterrupted(context.sessionId)) {
                publishStreamingEvent(context.conversationId, "interrupt", new JsonObject()
                    .put("reason", "User interrupted")
                    .put("message", "Fixed pipeline interrupted at level " + (currentLevelIndex + 1)));
                return Future.failedFuture("User interrupted execution");
            }
        }
        
        JsonObject step = steps.getJsonObject(currentLevelIndex);
        int pipelineLevel = step.getInteger("level", currentLevelIndex + 1);
        String tool = step.getString("tool");
        String description = step.getString("description");
        boolean optional = step.getBoolean("optional", false);
        
        vertx.eventBus().publish("log", 
            "Executing FIXED PIPELINE Level " + pipelineLevel + "/" + maxDepth + ": " + tool +
            ",3,UniversalHost,Host,System");
        
        // Publish level start event if streaming
        if (context.sessionId != null && streaming) {
            publishStreamingEvent(context.conversationId, "pipeline.level_start", new JsonObject()
                .put("level", pipelineLevel)
                .put("max_levels", maxDepth)
                .put("tool", tool)
                .put("description", description)
                .put("optional", optional)
                .put("streaming_event", step.getString("streaming_event")));
        }
        
        // Execute current pipeline level
        long levelStartTime = System.currentTimeMillis();
        return executeStep(context, step, accumulatedResult)
            .compose(stepResult -> {
                long levelDuration = System.currentTimeMillis() - levelStartTime;
                
                // Store step result
                context.storeStepResult(tool, stepResult);
                context.incrementCompletedSteps();
                
                // Accumulate results
                accumulatedResult.mergeIn(new JsonObject()
                    .put(tool + "_result", stepResult));
                
                // Publish level completion event if streaming (THIS IS THE KEY FEATURE!)
                if (context.sessionId != null && streaming) {
                    publishStreamingEvent(context.conversationId, step.getString("streaming_event", "pipeline_level_complete"), new JsonObject()
                        .put("level", pipelineLevel)
                        .put("tool", tool)
                        .put("success", true)
                        .put("duration_ms", levelDuration)
                        .put("result_summary", summarizeStepResult(stepResult))
                        .put("levels_remaining", maxDepth - pipelineLevel));
                }
                
                // Continue to next level
                return executeFixedPipelineLevels(context, steps, currentLevelIndex + 1, accumulatedResult, maxDepth, streaming);
            })
            .recover(error -> {
                String errorMsg = error != null ? error.getMessage() : "Unknown error";
                
                vertx.eventBus().publish("log", 
                    "FIXED PIPELINE Level " + pipelineLevel + " (" + tool + ") failed: " + errorMsg +
                    "," + (optional ? "1" : "0") + ",UniversalHost,Host,System");
                
                if (optional) {
                    // Optional level failed, continue anyway
                    if (context.sessionId != null && streaming) {
                        publishStreamingEvent(context.conversationId, "pipeline.level_skipped", new JsonObject()
                            .put("level", pipelineLevel)
                            .put("tool", tool)
                            .put("reason", errorMsg)
                            .put("severity", "WARNING")
                            .put("optional", true));
                    }
                    
                    // Continue to next level
                    return executeFixedPipelineLevels(context, steps, currentLevelIndex + 1, accumulatedResult, maxDepth, streaming);
                } else {
                    // Required level failed
                    if (context.sessionId != null && streaming) {
                        publishStreamingEvent(context.conversationId, "pipeline.level_failed", new JsonObject()
                            .put("level", pipelineLevel)
                            .put("tool", tool)
                            .put("error", errorMsg)
                            .put("severity", "ERROR")
                            .put("required", true));
                    }
                    
                    return Future.failedFuture(error);
                }
            });
    }
    
    /**
     * Execute a strategy by running its steps (DEPRECATED - kept for compatibility)
     */
    private Future<JsonObject> executeStrategy(ConversationContext context, JsonObject strategyWrapper, boolean streaming) {
        JsonObject strategy = strategyWrapper.getJsonObject("strategy", new JsonObject());
        JsonArray steps = strategy.getJsonArray("steps", new JsonArray());
        
        if (steps.isEmpty()) {
            // No steps - use direct LLM execution
            return handleDirectLLMExecution(context);
        }
        
        // Execute strategy steps
        return executeStepsSequentially(context, steps, 0, new JsonObject());
    }
    
    /**
     * Handle direct LLM execution for simple queries
     */
    private Future<JsonObject> handleDirectLLMExecution(ConversationContext context) {
        Promise<JsonObject> promise = Promise.promise();
        
        if (!llmService.isInitialized()) {
            promise.complete(new JsonObject()
                .put("answer", "I apologize, but the language model service is currently unavailable. " +
                    "Please try again later or contact support if this issue persists.")
                .put("error", "LLM service unavailable")
                .put("confidence", 0.0));
            return promise.future();
        }
        
        // Publish tool start event if streaming
        if (context.sessionId != null && context.streaming) {
            publishStreamingEvent(context.conversationId, "tool.start", new JsonObject()
                .put("tool", "direct_llm_chat")
                .put("description", "Direct LLM conversation"));
        }
        
        // Build conversation messages for LLM
        List<String> messages = new ArrayList<>();
        
        // Add system prompt
        String systemPrompt = "You are a helpful assistant.";
        messages.add(new JsonObject()
            .put("role", "system")
            .put("content", systemPrompt)
            .encode());
        
        // Add conversation history
        for (int i = 0; i < context.history.size(); i++) {
            messages.add(context.history.getJsonObject(i).encode());
        }
        
        llmService.chatCompletion(messages, 0.7, 2000)
            .whenComplete((result, error) -> {
                // Ensure we run on the correct Vert.x context
                vertx.getOrCreateContext().runOnContext(v -> {
                    try {
                        if (error != null) {
                            vertx.eventBus().publish("log", 
                                "LLM service error: " + error.getMessage() +
                                ",0,UniversalHost,LLM,Error");
                            promise.fail(error);
                            return;
                        }
                        
                        if (result == null) {
                            promise.fail(new RuntimeException("LLM service returned null result"));
                            return;
                        }
                        
                        JsonArray choices = result.getJsonArray("choices");
                        if (choices == null || choices.isEmpty()) {
                            promise.fail(new RuntimeException("LLM service returned no choices"));
                            return;
                        }
                        
                        JsonObject firstChoice = choices.getJsonObject(0);
                        if (firstChoice == null) {
                            promise.fail(new RuntimeException("LLM service returned invalid choice structure"));
                            return;
                        }
                        
                        JsonObject messageObj = firstChoice.getJsonObject("message");
                        if (messageObj == null) {
                            promise.fail(new RuntimeException("LLM service returned invalid message structure"));
                            return;
                        }
                        
                        String answer = messageObj.getString("content");
                        if (answer == null || answer.trim().isEmpty()) {
                            promise.fail(new RuntimeException("LLM service returned empty answer"));
                            return;
                        }
                        
                        // Publish tool complete event if streaming
                        if (context.sessionId != null && context.streaming) {
                            publishStreamingEvent(context.conversationId, "tool.complete", new JsonObject()
                                .put("tool", "direct_llm_chat")
                                .put("success", true)
                                .put("resultSummary", "Generated response of " + answer.length() + " characters"));
                        }
                        
                        promise.complete(new JsonObject()
                            .put("answer", answer)
                            .put("method", "direct_llm")
                            .put("model", result.getString("model", "unknown"))
                            .put("confidence", 0.8));
                    } catch (Exception e) {
                        vertx.eventBus().publish("log", 
                            "Error processing LLM response: " + e.getMessage() +
                            ",0,UniversalHost,LLM,Error");
                        promise.fail(e);
                    }
                });
            });
        
        return promise.future();
    }
    
    /**
     * Execute strategy steps sequentially
     */
    private Future<JsonObject> executeStepsSequentially(ConversationContext context, JsonArray steps, 
                                                       int currentStepIndex, JsonObject accumulatedResult) {
        if (currentStepIndex >= steps.size()) {
            return Future.succeededFuture(composeResponse(context, accumulatedResult));
        }
        
        // Check for interrupts before each step
        if (context.sessionId != null && context.streaming) {
            InterruptManager im = new InterruptManager(vertx);
            if (im.isInterrupted(context.sessionId)) {
                publishStreamingEvent(context.conversationId, "interrupt", new JsonObject()
                    .put("reason", "User interrupted")
                    .put("message", "Pipeline interrupted at step " + (currentStepIndex + 1)));
                return Future.failedFuture("User interrupted execution");
            }
        }
        
        JsonObject step = steps.getJsonObject(currentStepIndex);
        String tool = step.getString("tool");
        String server = step.getString("server");
        boolean optional = step.getBoolean("optional", false);
        
        vertx.eventBus().publish("log", 
            "Executing step " + (currentStepIndex + 1) + "/" + steps.size() + ": " + tool +
            ",3,UniversalHost,Host,System");
        
        // Publish progress event if streaming
        if (context.sessionId != null && context.streaming) {
            publishStreamingEvent(context.conversationId, "progress", new JsonObject()
                .put("step", "orchestration_step")
                .put("message", "Executing: " + tool)
                .put("details", new JsonObject()
                    .put("tool", tool)
                    .put("server", server)
                    .put("stepNumber", currentStepIndex + 1)
                    .put("totalSteps", steps.size())));
        }
        
        // Execute current step
        return executeStep(context, step, accumulatedResult)
            .compose(stepResult -> {
                // Store step result
                context.storeStepResult(tool, stepResult);
                context.stepsCompleted++;
                
                // Accumulate results
                accumulatedResult.mergeIn(new JsonObject()
                    .put(tool + "_result", stepResult));
                
                // Publish step completion if streaming
                if (context.sessionId != null && context.streaming) {
                    publishStreamingEvent(context.conversationId, "progress", new JsonObject()
                        .put("step", "orchestration_step_complete")
                        .put("message", "Completed: " + tool)
                        .put("details", new JsonObject()
                            .put("tool", tool)
                            .put("success", true)));
                }
                
                // Continue to next step
                return executeStepsSequentially(context, steps, currentStepIndex + 1, accumulatedResult);
            })
            .recover(error -> {
                String errorMsg = error != null ? error.getMessage() : "Unknown error";
                
                vertx.eventBus().publish("log", 
                    "Step " + tool + " failed: " + errorMsg +
                    "," + (optional ? "1" : "0") + ",UniversalHost,Host,System");
                
                if (optional) {
                    // Optional step failed, continue anyway
                    if (context.sessionId != null && context.streaming) {
                        publishStreamingEvent(context.conversationId, "progress", new JsonObject()
                            .put("step", "orchestration_step_skipped")
                            .put("message", "Skipped optional step: " + tool)
                            .put("details", new JsonObject()
                                .put("tool", tool)
                                .put("reason", errorMsg)
                                .put("severity", "WARNING")));
                    }
                    
                    // Continue to next step
                    return executeStepsSequentially(context, steps, currentStepIndex + 1, accumulatedResult);
                } else {
                    // Required step failed
                    if (context.sessionId != null && context.streaming) {
                        publishStreamingEvent(context.conversationId, "error", new JsonObject()
                            .put("error", "Pipeline step failed: " + tool)
                            .put("severity", "ERROR")
                            .put("details", new JsonObject()
                                .put("tool", tool)
                                .put("step", currentStepIndex + 1)
                                .put("reason", errorMsg)));
                    }
                    
                    return Future.failedFuture(error);
                }
            });
    }
    
    /**
     * Execute a single step by routing to appropriate manager
     */
    private Future<JsonObject> executeStep(ConversationContext context, JsonObject step, JsonObject previousResults) {
        String tool = step.getString("tool");
        String server = step.getString("server");
        
        // Build arguments for the tool
        JsonObject arguments = buildToolArguments(context, tool, previousResults);
        
        // Route to appropriate manager
        MCPClientManager manager = findManagerForServer(server);
        if (manager == null) {
            return Future.failedFuture("No manager available for server: " + server);
        }
        
        // Determine client name for the manager
        String clientName = getClientNameForServer(server);
        
        return manager.callClientTool(clientName, tool, arguments);
    }
    
    /**
     * Find manager that can handle the specified server
     */
    private MCPClientManager findManagerForServer(String server) {
        switch (server) {
            case "OracleQueryExecution":
            case "SessionSchemaResolver":
                return managers.get("OracleExecutionManager");
            case "OracleQueryAnalysis":
            case "OracleSQLGeneration": 
            case "OracleSQLValidation":
                return managers.get("SQLPipelineManager");
            case "OracleSchemaIntelligence":
            case "BusinessMapping":
                return managers.get("SchemaIntelligenceManager");
            case "QueryIntentEvaluation":
            case "IntentAnalysis":
                return managers.get("IntentAnalysisManager");
            case "StrategyGeneration":
            case "StrategyOrchestrator":
            case "StrategyLearning":
                return managers.get("StrategyOrchestrationManager");
            default:
                return null;
        }
    }
    
    /**
     * Get client name for server
     */
    private String getClientNameForServer(String server) {
        switch (server) {
            case "OracleQueryExecution":
            case "SessionSchemaResolver":
                return "execution";
            case "OracleQueryAnalysis":
                return "analysis";
            case "OracleSQLGeneration":
                return "generation";
            case "OracleSQLValidation":
                return "validation";
            case "OracleSchemaIntelligence":
                return "schema";
            case "BusinessMapping":
                return "business";
            case "QueryIntentEvaluation":
                return "evaluation";
            case "IntentAnalysis":
                return "analysis";
            case "StrategyGeneration":
                return "generation";
            case "StrategyOrchestrator":
                return "orchestrator";
            case "StrategyLearning":
                return "learning";
            default:
                return "default";
        }
    }
    
    /**
     * Build arguments for tool execution
     */
    private JsonObject buildToolArguments(ConversationContext context, String tool, JsonObject previousResults) {
        JsonObject args = new JsonObject();
        
        // Get the original query from conversation
        String originalQuery = "";
        if (!context.history.isEmpty()) {
            JsonObject lastMessage = context.history.getJsonObject(context.history.size() - 1);
            if ("user".equals(lastMessage.getString("role"))) {
                originalQuery = lastMessage.getString("content", "");
            }
        }
        
        // Build arguments based on tool type
        switch (tool) {
            case "evaluate_query_intent":
                args.put("query", originalQuery);
                args.put("conversationHistory", context.history);
                break;
            case "analyze_query":
                args.put("query", originalQuery);
                args.put("context", context.history);
                break;
            case "match_oracle_schema":
                JsonObject analysis = context.getStepResultSafe("analyze_query");
                args.put("analysis", analysis);
                args.put("maxSuggestions", 5);
                args.put("confidenceThreshold", 0.6);
                break;
            case "generate_oracle_sql":
                args.put("analysis", context.getStepResultSafe("analyze_query"));
                args.put("schemaMatches", context.getStepResultSafe("match_oracle_schema"));
                args.put("includeEnums", true);
                args.put("maxComplexity", 5);
                break;
            case "validate_oracle_sql":
                JsonObject sqlGenResult = context.getStepResultSafe("generate_oracle_sql");
                if (!sqlGenResult.isEmpty()) {
                    args.put("sql", sqlGenResult.getString("sql"));
                    args.put("checkPermissions", false);
                    args.put("suggestFixes", true);
                }
                break;
            case "run_oracle_query":
                String finalSQL = getFinalSQL(context);
                if (!finalSQL.isEmpty()) {
                    args.put("sql", finalSQL);
                    args.put("maxRows", 1000);
                    if (context.sessionId != null) {
                        args.put("sessionId", context.sessionId);
                    }
                }
                break;
            default:
                args.put("query", originalQuery);
                if (!previousResults.isEmpty()) {
                    args.put("context", previousResults);
                }
                break;
        }
        
        return args;
    }
    
    /**
     * Get final SQL from step results
     */
    private String getFinalSQL(ConversationContext context) {
        // Try validated SQL first
        JsonObject validated = context.getStepResultSafe("validate_oracle_sql");
        if (!validated.isEmpty() && validated.getString("sql") != null) {
            return validated.getString("sql");
        }
        
        // Then generated SQL
        JsonObject generated = context.getStepResultSafe("generate_oracle_sql");
        if (!generated.isEmpty() && generated.getString("sql") != null) {
            return generated.getString("sql");
        }
        
        return "";
    }
    
    /**
     * Compose final response for fixed pipeline execution
     */
    private JsonObject composeFixedPipelineResponse(ConversationContext context, JsonObject executionResult) {
        JsonObject response = new JsonObject();
        
        // Check for query results from level 7 (query execution)
        JsonObject queryResults = context.getStepResultSafe("run_oracle_query");
        if (!queryResults.isEmpty() && queryResults.containsKey("rows")) {
            JsonArray rows = queryResults.getJsonArray("rows");
            if (rows.size() == 0) {
                response.put("answer", "No results found for your query.");
            } else if (rows.size() == 1) {
                response.put("answer", "Found 1 result.");
            } else {
                response.put("answer", "Found " + rows.size() + " results.");
            }
            response.put("data", queryResults);
        } else {
            // Check for formatted results from level 8 (result formatting)
            JsonObject formattedResults = context.getStepResultSafe("format_results");
            if (!formattedResults.isEmpty() && formattedResults.containsKey("formatted_output")) {
                response.put("answer", formattedResults.getString("formatted_output"));
                response.put("formatted_data", formattedResults);
            } else {
                // Check for generated SQL from level 4 (SQL generation)
                JsonObject sqlGenResult = context.getStepResultSafe("generate_oracle_sql");
                if (!sqlGenResult.isEmpty() && sqlGenResult.containsKey("sql")) {
                    response.put("answer", "Generated SQL query:\n\n```sql\n" + 
                        sqlGenResult.getString("sql") + "\n```");
                    response.put("executedSQL", sqlGenResult.getString("sql"));
                } else {
                    // Check for schema analysis from level 2 (schema intelligence)
                    JsonObject schemaResult = context.getStepResultSafe("match_oracle_schema");
                    if (!schemaResult.isEmpty()) {
                        response.put("answer", "Schema analysis completed. Found relevant database elements.");
                        response.put("schema_analysis", schemaResult);
                    } else {
                        // Fallback response based on completed levels
                        if (context.getCompletedSteps() > 0) {
                            response.put("answer", "I processed your request through " + 
                                context.getCompletedSteps() + " pipeline levels, but was unable to provide a complete result.");
                        } else {
                            response.put("answer", "I was unable to process your request. Please try rephrasing your question.");
                        }
                    }
                }
            }
        }
        
        // Add fixed pipeline metadata
        response.put("method", "fixed_pipeline_v2");
        response.put("architecture", "10_level_fixed_pipeline");
        response.put("confidence", calculateFixedPipelineConfidence(context));
        response.put("pipeline_levels_executed", context.getCompletedSteps());
        response.put("pipeline_version", "2.0");
        
        return response;
    }
    
    /**
     * Calculate confidence based on fixed pipeline execution
     */
    private double calculateFixedPipelineConfidence(ConversationContext context) {
        double confidence = 1.0;
        
        // Base confidence on how many levels were successfully completed
        int completedLevels = context.getCompletedSteps();
        if (completedLevels < 3) {
            confidence *= 0.6; // Less than basic analysis
        } else if (completedLevels < 5) {
            confidence *= 0.8; // Basic processing
        } else if (completedLevels < 7) {
            confidence *= 0.9; // Good processing without execution
        }
        // Full confidence for 7+ levels
        
        // Reduce confidence for validation failures
        JsonObject validation = context.getStepResultSafe("validate_oracle_sql");
        if (!validation.isEmpty() && !validation.getBoolean("valid", true)) {
            confidence *= 0.8;
        }
        
        // Reduce confidence for errors in required levels
        if (context.getStepError("intent_analysis__extract_intent") != null) {
            confidence *= 0.7; // Intent analysis failed
        }
        if (context.getStepError("generate_oracle_sql") != null) {
            confidence *= 0.8; // SQL generation failed
        }
        
        return Math.max(0.1, confidence);
    }
    
    /**
     * Summarize step result for streaming events
     */
    private String summarizeStepResult(JsonObject stepResult) {
        if (stepResult == null || stepResult.isEmpty()) {
            return "No result data";
        }
        
        // Check for common result patterns
        if (stepResult.containsKey("sql")) {
            String sql = stepResult.getString("sql");
            return "Generated SQL (" + (sql.length() > 50 ? sql.substring(0, 50) + "..." : sql) + ")";
        }
        
        if (stepResult.containsKey("rows")) {
            JsonArray rows = stepResult.getJsonArray("rows");
            return "Query returned " + rows.size() + " rows";
        }
        
        if (stepResult.containsKey("tables")) {
            JsonArray tables = stepResult.getJsonArray("tables");
            return "Found " + tables.size() + " relevant tables";
        }
        
        if (stepResult.containsKey("valid")) {
            boolean valid = stepResult.getBoolean("valid");
            return valid ? "SQL validation passed" : "SQL validation failed";
        }
        
        if (stepResult.containsKey("confidence")) {
            double confidence = stepResult.getDouble("confidence");
            return String.format("Analysis complete (%.1f%% confidence)", confidence * 100);
        }
        
        // Count keys as a general measure of result richness
        int keyCount = stepResult.size();
        return "Result with " + keyCount + " data elements";
    }
    
    /**
     * Compose final response from execution results (DEPRECATED - kept for compatibility)
     */
    private JsonObject composeResponse(ConversationContext context, JsonObject executionResult) {
        JsonObject response = new JsonObject();
        
        // Check for query results first
        JsonObject queryResults = context.getStepResultSafe("run_oracle_query");
        if (!queryResults.isEmpty() && queryResults.containsKey("rows")) {
            JsonArray rows = queryResults.getJsonArray("rows");
            if (rows.size() == 0) {
                response.put("answer", "No results found for your query.");
            } else if (rows.size() == 1) {
                response.put("answer", "Found 1 result.");
            } else {
                response.put("answer", "Found " + rows.size() + " results.");
            }
            response.put("data", queryResults);
        } else {
            // Check for generated SQL
            JsonObject sqlGenResult = context.getStepResultSafe("generate_oracle_sql");
            if (!sqlGenResult.isEmpty() && sqlGenResult.containsKey("sql")) {
                response.put("answer", "Generated SQL query:\n\n```sql\n" + 
                    sqlGenResult.getString("sql") + "\n```");
                response.put("executedSQL", sqlGenResult.getString("sql"));
            } else {
                // Fallback response
                if (context.stepsCompleted > 0) {
                    response.put("answer", "I processed your request through " + 
                        context.stepsCompleted + " steps, but was unable to provide a complete result.");
                } else {
                    response.put("answer", "I was unable to process your request. Please try rephrasing your question or contact support.");
                }
            }
        }
        
        response.put("method", "step_composition");
        response.put("confidence", 0.8);
        response.put("stepsExecuted", context.stepsCompleted);
        
        return response;
    }
    
    /**
     * Clean up old conversations
     */
    private void cleanupOldConversations() {
        conversations.entrySet().removeIf(entry -> entry.getValue().isExpired());
    }
    
    /**
     * Publish streaming event to the correct event bus address
     */
    private void publishStreamingEvent(String conversationId, String eventType, JsonObject data) {
        // Use "streaming." prefix to match ConversationStreaming expectations
        String address = "streaming." + conversationId + "." + eventType;
        data.put("timestamp", System.currentTimeMillis());
        data.put("host", "UniversalHost");
        eventBus.publish(address, data);
    }
    
    @Override
    public void stop(Promise<Void> stopPromise) {
        // Shutdown all managers with proper error handling
        List<Future> shutdownFutures = new ArrayList<>();
        
        for (MCPClientManager manager : managers.values()) {
            if (manager != null) {
                shutdownFutures.add(manager.shutdown().recover(error -> {
                    // Log shutdown errors but don't fail the overall shutdown
                    vertx.eventBus().publish("log", 
                        "Warning: Manager shutdown error: " + error.getMessage() +
                        ",1,UniversalHost,Host,Shutdown");
                    return null;
                }));
            }
        }
        
        if (shutdownFutures.isEmpty()) {
            // No managers to shut down
            conversations.clear();
            managers.clear();
            vertx.eventBus().publish("log", 
                "UniversalHost (simplified) stopped" +
                ",2,UniversalHost,Host,System");
            stopPromise.complete();
        } else {
            CompositeFuture.all(shutdownFutures).onComplete(ar -> {
                // Clean up resources regardless of shutdown result
                conversations.clear();
                managers.clear();
                
                if (ar.failed()) {
                    vertx.eventBus().publish("log", 
                        "UniversalHost stopped with manager shutdown warnings" +
                        ",1,UniversalHost,Host,System");
                } else {
                    vertx.eventBus().publish("log", 
                        "UniversalHost (simplified) stopped successfully" +
                        ",2,UniversalHost,Host,System");
                }
                
                // Always complete the stop promise
                stopPromise.complete();
            });
        }
    }
}
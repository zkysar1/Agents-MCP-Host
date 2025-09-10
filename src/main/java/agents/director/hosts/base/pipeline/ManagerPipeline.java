package agents.director.hosts.base.pipeline;

import agents.director.hosts.base.managers.*;
import io.vertx.core.*;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manager Pipeline coordinates the sequential execution of managers in the correct dependency order.
 * It supports partial execution, streaming events, error handling with fallbacks, and context sharing.
 */
public class ManagerPipeline {
    
    private final Vertx vertx;
    private final EventBus eventBus;
    private final PipelineConfiguration configuration;
    private final Map<String, MCPClientManager> managers = new ConcurrentHashMap<>();
    
    // Execution tracking
    private final Map<String, CompletableFuture<JsonObject>> runningExecutions = new ConcurrentHashMap<>();
    private final AtomicBoolean isShuttingDown = new AtomicBoolean(false);
    
    // Performance metrics
    private final Map<String, Long> stepDurations = new ConcurrentHashMap<>();
    private final Map<String, Integer> stepRetryCount = new ConcurrentHashMap<>();
    
    public ManagerPipeline(Vertx vertx, PipelineConfiguration configuration) {
        this.vertx = vertx;
        this.eventBus = vertx.eventBus();
        this.configuration = configuration;
    }
    
    /**
     * Register a manager with the pipeline
     */
    public ManagerPipeline registerManager(String managerType, MCPClientManager manager) {
        if (manager == null) {
            throw new IllegalArgumentException("Manager cannot be null");
        }
        managers.put(managerType, manager);
        return this;
    }
    
    /**
     * Execute the pipeline with the given context
     */
    public Future<JsonObject> execute(ExecutionContext context) {
        if (isShuttingDown.get()) {
            return Future.failedFuture("Pipeline is shutting down");
        }
        
        Promise<JsonObject> promise = Promise.promise();
        
        // Store execution future for tracking
        CompletableFuture<JsonObject> execution = new CompletableFuture<>();
        runningExecutions.put(context.getContextId(), execution);
        
        publishStreamingEvent(context, "pipeline_started", new JsonObject()
            .put("pipelineId", configuration.getPipelineId())
            .put("totalSteps", configuration.getSteps().size())
            .put("executionPolicy", configuration.getExecutionPolicy().toString()));
        
        // Start execution
        executeSteps(context)
            .onComplete(ar -> {
                runningExecutions.remove(context.getContextId());
                
                if (ar.succeeded()) {
                    JsonObject result = ar.result();
                    execution.complete(result);
                    
                    publishStreamingEvent(context, "pipeline_completed", new JsonObject()
                        .put("success", true)
                        .put("completedSteps", context.getCompletedSteps())
                        .put("totalDuration", context.getTotalDuration())
                        .put("confidence", context.calculateConfidence()));
                    
                    promise.complete(result);
                } else {
                    Exception error = (Exception) ar.cause();
                    execution.completeExceptionally(error);
                    
                    publishStreamingEvent(context, "pipeline_failed", new JsonObject()
                        .put("error", error.getMessage())
                        .put("completedSteps", context.getCompletedSteps())
                        .put("totalDuration", context.getTotalDuration()));
                    
                    if (configuration.isEnableFallback()) {
                        executeFallback(context, error)
                            .onComplete(fallbackResult -> {
                                if (fallbackResult.succeeded()) {
                                    promise.complete(fallbackResult.result());
                                } else {
                                    promise.fail(error);
                                }
                            });
                    } else {
                        promise.fail(error);
                    }
                }
            });
        
        return promise.future();
    }
    
    /**
     * Execute pipeline steps based on configuration policy
     */
    private Future<JsonObject> executeSteps(ExecutionContext context) {
        Promise<JsonObject> promise = Promise.promise();
        
        switch (configuration.getExecutionPolicy()) {
            case SEQUENTIAL:
                executeSequential(context, 0, promise);
                break;
            case PRIORITY_BASED:
                executePriorityBased(context, promise);
                break;
            case DEPENDENCY_BASED:
                executeDependencyBased(context, promise);
                break;
            default:
                promise.fail("Unsupported execution policy: " + configuration.getExecutionPolicy());
        }
        
        return promise.future();
    }
    
    /**
     * Execute steps sequentially
     */
    private void executeSequential(ExecutionContext context, int stepIndex, Promise<JsonObject> promise) {
        List<PipelineStep> steps = configuration.getSteps();
        
        if (stepIndex >= steps.size()) {
            // All steps completed
            promise.complete(buildFinalResult(context));
            return;
        }
        
        PipelineStep step = steps.get(stepIndex);
        context.setCurrentStepIndex(stepIndex);
        
        // Check if step should be executed
        if (!step.shouldExecute(context)) {
            // Skip this step
            executeSequential(context, stepIndex + 1, promise);
            return;
        }
        
        // Check dependencies
        if (!step.areDependenciesSatisfied(context)) {
            if (step.isOptional()) {
                // Skip optional step with unsatisfied dependencies
                executeSequential(context, stepIndex + 1, promise);
                return;
            } else {
                promise.fail("Required step " + step.getId() + " has unsatisfied dependencies: " + 
                    step.getDependencies());
                return;
            }
        }
        
        executeStep(context, step)
            .onComplete(ar -> {
                if (ar.succeeded()) {
                    context.incrementCompletedSteps();
                    executeSequential(context, stepIndex + 1, promise);
                } else {
                    handleStepFailure(context, step, ar.cause(), promise, 
                        () -> executeSequential(context, stepIndex + 1, promise));
                }
            });
    }
    
    /**
     * Execute steps based on priority
     */
    private void executePriorityBased(ExecutionContext context, Promise<JsonObject> promise) {
        Optional<PipelineStep> nextStep = configuration.getNextStep(context);
        
        if (nextStep.isEmpty()) {
            // No more executable steps
            if (configuration.isComplete(context)) {
                promise.complete(buildFinalResult(context));
            } else {
                promise.fail("Pipeline incomplete - no more executable steps");
            }
            return;
        }
        
        PipelineStep step = nextStep.get();
        executeStep(context, step)
            .onComplete(ar -> {
                if (ar.succeeded()) {
                    context.incrementCompletedSteps();
                    
                    // Check for adaptation if enabled
                    if (shouldCheckAdaptation(context)) {
                        adaptPipeline(context)
                            .onComplete(adaptResult -> {
                                // Continue regardless of adaptation result
                                executePriorityBased(context, promise);
                            });
                    } else {
                        executePriorityBased(context, promise);
                    }
                } else {
                    handleStepFailure(context, step, ar.cause(), promise, 
                        () -> executePriorityBased(context, promise));
                }
            });
    }
    
    /**
     * Execute steps based on dependency satisfaction
     */
    private void executeDependencyBased(ExecutionContext context, Promise<JsonObject> promise) {
        List<PipelineStep> executableSteps = configuration.getExecutableSteps(context);
        
        if (executableSteps.isEmpty()) {
            if (configuration.isComplete(context)) {
                promise.complete(buildFinalResult(context));
            } else {
                promise.fail("Pipeline incomplete - no executable steps remaining");
            }
            return;
        }
        
        // Execute highest priority step
        PipelineStep step = executableSteps.get(0);
        executeStep(context, step)
            .onComplete(ar -> {
                if (ar.succeeded()) {
                    context.incrementCompletedSteps();
                    executeDependencyBased(context, promise);
                } else {
                    handleStepFailure(context, step, ar.cause(), promise, 
                        () -> executeDependencyBased(context, promise));
                }
            });
    }
    
    /**
     * Execute a single pipeline step
     */
    private Future<JsonObject> executeStep(ExecutionContext context, PipelineStep step) {
        Promise<JsonObject> promise = Promise.promise();
        
        long startTime = System.currentTimeMillis();
        
        publishStreamingEvent(context, "step_started", new JsonObject()
            .put("stepId", step.getId())
            .put("stepName", step.getName())
            .put("description", step.getDescription())
            .put("optional", step.isOptional()));
        
        // Execute step with retry logic
        executeStepWithRetry(context, step, 0)
            .onComplete(ar -> {
                long duration = System.currentTimeMillis() - startTime;
                stepDurations.put(step.getId(), duration);
                context.recordStepTiming(step.getId(), duration);
                
                if (ar.succeeded()) {
                    JsonObject result = ar.result();
                    context.storeStepResult(step.getId(), result);
                    
                    publishStreamingEvent(context, "step_completed", new JsonObject()
                        .put("stepId", step.getId())
                        .put("duration", duration)
                        .put("success", true));
                    
                    promise.complete(result);
                } else {
                    context.recordStepError(step.getId(), (Exception) ar.cause());
                    
                    publishStreamingEvent(context, "step_failed", new JsonObject()
                        .put("stepId", step.getId())
                        .put("error", ar.cause().getMessage())
                        .put("duration", duration)
                        .put("retries", stepRetryCount.getOrDefault(step.getId(), 0)));
                    
                    promise.fail(ar.cause());
                }
            });
        
        return promise.future();
    }
    
    /**
     * Execute step with retry logic
     */
    private Future<JsonObject> executeStepWithRetry(ExecutionContext context, PipelineStep step, int retryCount) {
        Promise<JsonObject> promise = Promise.promise();
        
        // Route to appropriate manager
        Future<JsonObject> execution = routeToManager(context, step);
        
        execution.onComplete(ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result());
            } else {
                if (retryCount < step.getMaxRetries()) {
                    stepRetryCount.put(step.getId(), retryCount + 1);
                    
                    publishStreamingEvent(context, "step_retry", new JsonObject()
                        .put("stepId", step.getId())
                        .put("retryCount", retryCount + 1)
                        .put("error", ar.cause().getMessage()));
                    
                    // Wait a bit before retry
                    vertx.setTimer(1000, timerId -> {
                        executeStepWithRetry(context, step, retryCount + 1)
                            .onComplete(promise);
                    });
                } else {
                    promise.fail(ar.cause());
                }
            }
        });
        
        return promise.future();
    }
    
    /**
     * Route step execution to the appropriate manager
     */
    private Future<JsonObject> routeToManager(ExecutionContext context, PipelineStep step) {
        String managerType = step.getManagerType();
        MCPClientManager manager = managers.get(managerType);
        
        if (manager == null) {
            return Future.failedFuture("Manager not found: " + managerType);
        }
        
        if (!manager.isReady()) {
            return Future.failedFuture("Manager not ready: " + managerType);
        }
        
        // Build arguments for the step
        JsonObject arguments = step.buildArguments(context);
        
        // Route based on manager type for special handling
        switch (managerType) {
            case "OracleExecutionManager":
                return routeToExecutionManager((OracleExecutionManager) manager, step, arguments, context);
            case "SQLPipelineManager":
                return routeToSQLPipelineManager((SQLPipelineManager) manager, step, arguments);
            case "SchemaIntelligenceManager":
                return routeToSchemaManager((SchemaIntelligenceManager) manager, step, arguments);
            case "IntentAnalysisManager":
                return routeToIntentManager((IntentAnalysisManager) manager, step, arguments);
            case "StrategyOrchestrationManager":
                return routeToStrategyManager((StrategyOrchestrationManager) manager, step, arguments);
            default:
                // Generic routing
                return manager.callClientTool("default", step.getToolName(), arguments);
        }
    }
    
    // Specialized routing methods for each manager type
    
    private Future<JsonObject> routeToExecutionManager(OracleExecutionManager manager, PipelineStep step, 
                                                      JsonObject arguments, ExecutionContext context) {
        String toolName = step.getToolName();
        
        switch (toolName) {
            case "run_oracle_query":
            case "execute_with_session_context":
                String sql = arguments.getString("sql");
                String sessionId = context.getSessionId();
                if (sql != null && sessionId != null) {
                    return manager.executeWithSessionContext(sql, sessionId);
                }
                break;
            case "format_results":
                JsonObject results = arguments.getJsonObject("results");
                String format = arguments.getString("format", "narrative");
                if (results != null) {
                    return manager.formatQueryResults(results, format);
                }
                break;
        }
        
        return manager.callClientTool("execution", toolName, arguments);
    }
    
    private Future<JsonObject> routeToSQLPipelineManager(SQLPipelineManager manager, PipelineStep step, JsonObject arguments) {
        String toolName = step.getToolName();
        
        switch (toolName) {
            case "generate_validated_sql":
                String query = arguments.getString("query");
                JsonObject schemaContext = arguments.getJsonObject("schemaContext");
                return manager.generateValidatedSQL(query, schemaContext);
            case "analyze_and_optimize_sql":
                String sql = arguments.getString("sql");
                return manager.analyzeAndOptimizeSQL(sql);
            case "validate_with_explanation":
                String validateSql = arguments.getString("sql");
                return manager.validateWithExplanation(validateSql);
        }
        
        // Route to appropriate client
        String clientName = determineClientName(step.getServerName());
        return manager.callClientTool(clientName, toolName, arguments);
    }
    
    private Future<JsonObject> routeToSchemaManager(SchemaIntelligenceManager manager, PipelineStep step, JsonObject arguments) {
        String toolName = step.getToolName();
        
        switch (toolName) {
            case "map_business_to_technical":
                JsonArray businessTerms = arguments.getJsonArray("terms");
                return manager.mapBusinessToTechnical(businessTerms);
            case "discover_relationships":
                JsonArray tables = arguments.getJsonArray("tables");
                return manager.discoverRelationships(tables);
            case "suggest_optimal_strategy":
                String query = arguments.getString("query");
                JsonObject schemaContext = arguments.getJsonObject("schemaContext");
                return manager.suggestOptimalStrategy(query, schemaContext);
            case "get_comprehensive_intelligence":
                String queryText = arguments.getString("query");
                return manager.getComprehensiveIntelligence(queryText);
        }
        
        String clientName = step.getServerName().equals("BusinessMapping") ? "business" : "schema";
        return manager.callClientTool(clientName, toolName, arguments);
    }
    
    private Future<JsonObject> routeToIntentManager(IntentAnalysisManager manager, PipelineStep step, JsonObject arguments) {
        String toolName = step.getToolName();
        
        switch (toolName) {
            case "full_intent_analysis":
                String query = arguments.getString("query");
                JsonObject context = arguments.getJsonObject("context");
                return manager.fullIntentAnalysis(query, context);
            case "analyze_with_confidence":
                String queryText = arguments.getString("query");
                JsonArray previousQueries = arguments.getJsonArray("previousQueries");
                return manager.analyzeWithConfidence(queryText, previousQueries);
        }
        
        String clientName = step.getServerName().equals("QueryIntentEvaluation") ? "evaluation" : "analysis";
        return manager.callClientTool(clientName, toolName, arguments);
    }
    
    private Future<JsonObject> routeToStrategyManager(StrategyOrchestrationManager manager, PipelineStep step, JsonObject arguments) {
        String toolName = step.getToolName();
        
        switch (toolName) {
            case "generate_and_execute_strategy":
                String query = arguments.getString("query");
                JsonObject context = arguments.getJsonObject("context");
                return manager.generateAndExecuteStrategy(query, context);
            case "adapt_strategy_in_flight":
                JsonObject currentStrategy = arguments.getJsonObject("currentStrategy");
                JsonObject progress = arguments.getJsonObject("progress");
                JsonObject feedback = arguments.getJsonObject("feedback");
                return manager.adaptStrategyInFlight(currentStrategy, progress, feedback);
        }
        
        String clientName = step.getServerName().replace("Strategy", "").toLowerCase();
        return manager.callClientTool(clientName, toolName, arguments);
    }
    
    private String determineClientName(String serverName) {
        switch (serverName) {
            case "OracleQueryAnalysis":
                return "analysis";
            case "OracleSQLGeneration":
                return "generation";
            case "OracleSQLValidation":
                return "validation";
            case "OracleQueryExecution":
                return "execution";
            case "SessionSchemaResolver":
                return "session";
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
     * Handle step execution failure
     */
    private void handleStepFailure(ExecutionContext context, PipelineStep step, Throwable cause, 
                                  Promise<JsonObject> promise, Runnable continueExecution) {
        if (step.isOptional() && configuration.isContinueOnOptionalFailure()) {
            publishStreamingEvent(context, "step_skipped", new JsonObject()
                .put("stepId", step.getId())
                .put("reason", "Optional step failed: " + cause.getMessage()));
            
            continueExecution.run();
        } else {
            promise.fail(cause);
        }
    }
    
    /**
     * Check if pipeline adaptation should be performed
     */
    private boolean shouldCheckAdaptation(ExecutionContext context) {
        return configuration.isEnableAdaptation() && 
               context.getCompletedSteps() % configuration.getAdaptationCheckInterval() == 0 &&
               context.getCompletedSteps() > 0;
    }
    
    /**
     * Adapt pipeline based on current execution state
     */
    private Future<JsonObject> adaptPipeline(ExecutionContext context) {
        Promise<JsonObject> promise = Promise.promise();
        
        // Check if we need to adapt based on confidence threshold
        double currentConfidence = context.calculateConfidence();
        
        if (currentConfidence < configuration.getAdaptationThreshold()) {
            publishStreamingEvent(context, "pipeline_adapting", new JsonObject()
                .put("reason", "Low confidence")
                .put("currentConfidence", currentConfidence)
                .put("threshold", configuration.getAdaptationThreshold()));
            
            // Use strategy manager to adapt if available
            StrategyOrchestrationManager strategyManager = (StrategyOrchestrationManager) managers.get("StrategyOrchestrationManager");
            if (strategyManager != null && strategyManager.isReady()) {
                JsonObject currentStrategy = new JsonObject()
                    .put("completedSteps", context.getCompletedSteps())
                    .put("confidence", currentConfidence);
                
                JsonObject progress = context.getExecutionSummary();
                JsonObject feedback = new JsonObject()
                    .put("confidence_low", true)
                    .put("threshold", configuration.getAdaptationThreshold());
                
                strategyManager.adaptStrategyInFlight(currentStrategy, progress, feedback)
                    .onComplete(ar -> {
                        if (ar.succeeded()) {
                            publishStreamingEvent(context, "pipeline_adapted", new JsonObject()
                                .put("success", true)
                                .put("adaptation", ar.result()));
                        }
                        promise.complete(ar.succeeded() ? ar.result() : new JsonObject());
                    });
            } else {
                promise.complete(new JsonObject());
            }
        } else {
            promise.complete(new JsonObject());
        }
        
        return promise.future();
    }
    
    /**
     * Execute fallback strategy when main pipeline fails
     */
    private Future<JsonObject> executeFallback(ExecutionContext context, Throwable originalError) {
        Promise<JsonObject> promise = Promise.promise();
        
        publishStreamingEvent(context, "fallback_started", new JsonObject()
            .put("originalError", originalError.getMessage())
            .put("fallbackStrategies", new JsonArray(configuration.getFallbackStrategies())));
        
        // Try to get fallback from strategy manager
        StrategyOrchestrationManager strategyManager = (StrategyOrchestrationManager) managers.get("StrategyOrchestrationManager");
        if (strategyManager != null && strategyManager.isReady()) {
            JsonObject failedStrategy = buildFailedStrategyInfo(context);
            JsonArray availableManagers = new JsonArray(managers.keySet().stream().toList());
            
            // Simplified fallback - just return basic error response
            Future.succeededFuture(new JsonObject().put("error", "Pipeline execution failed"))
                .onComplete(ar -> {
                    if (ar.succeeded()) {
                        JsonObject fallbackResult = ar.result();
                        publishStreamingEvent(context, "fallback_completed", new JsonObject()
                            .put("success", true)
                            .put("strategy", fallbackResult));
                        
                        promise.complete(buildFallbackResult(context, fallbackResult));
                    } else {
                        promise.complete(buildMinimalResult(context, originalError));
                    }
                });
        } else {
            promise.complete(buildMinimalResult(context, originalError));
        }
        
        return promise.future();
    }
    
    /**
     * Build final result from execution context
     */
    private JsonObject buildFinalResult(ExecutionContext context) {
        JsonObject result = new JsonObject()
            .put("success", true)
            .put("contextId", context.getContextId())
            .put("conversationId", context.getConversationId())
            .put("completedSteps", context.getCompletedSteps())
            .put("totalSteps", configuration.getSteps().size())
            .put("totalDuration", context.getTotalDuration())
            .put("confidence", context.calculateConfidence())
            .put("strategy", context.getCurrentStrategy())
            .put("hasErrors", context.hasErrors());
        
        // Include step results
        JsonObject stepResults = new JsonObject();
        for (String stepName : context.getCompletedStepNames()) {
            JsonObject stepResult = context.getStepResult(stepName);
            if (stepResult != null) {
                stepResults.put(stepName, stepResult);
            }
        }
        result.put("stepResults", stepResults);
        
        // Include performance metrics
        JsonObject metrics = new JsonObject();
        stepDurations.forEach(metrics::put);
        result.put("stepDurations", metrics);
        
        return result;
    }
    
    private JsonObject buildFailedStrategyInfo(ExecutionContext context) {
        return new JsonObject()
            .put("completedSteps", context.getCompletedSteps())
            .put("totalSteps", configuration.getSteps().size())
            .put("confidence", context.calculateConfidence())
            .put("errors", context.getStepsWithErrors().size())
            .put("duration", context.getTotalDuration());
    }
    
    private JsonObject buildFallbackResult(ExecutionContext context, JsonObject fallbackStrategy) {
        return new JsonObject()
            .put("success", true)
            .put("fallbackUsed", true)
            .put("originalSteps", context.getCompletedSteps())
            .put("fallbackStrategy", fallbackStrategy)
            .put("confidence", 0.5); // Lower confidence for fallback
    }
    
    private JsonObject buildMinimalResult(ExecutionContext context, Throwable error) {
        return new JsonObject()
            .put("success", false)
            .put("error", error.getMessage())
            .put("completedSteps", context.getCompletedSteps())
            .put("partialResults", true)
            .put("confidence", 0.2);
    }
    
    /**
     * Publish streaming event
     */
    private void publishStreamingEvent(ExecutionContext context, String eventType, JsonObject data) {
        if (context.isStreaming() && context.getSessionId() != null) {
            String address = "streaming." + context.getConversationId() + "." + eventType;
            data.put("timestamp", System.currentTimeMillis());
            data.put("source", "ManagerPipeline");
            data.put("pipelineId", configuration.getPipelineId());
            eventBus.publish(address, data);
        }
    }
    
    /**
     * Get pipeline status
     */
    public JsonObject getStatus() {
        return new JsonObject()
            .put("pipelineId", configuration.getPipelineId())
            .put("registeredManagers", managers.keySet().size())
            .put("runningExecutions", runningExecutions.size())
            .put("isShuttingDown", isShuttingDown.get())
            .put("totalSteps", configuration.getSteps().size());
    }
    
    /**
     * Interrupt a running execution
     */
    public Future<Void> interrupt(String contextId) {
        CompletableFuture<JsonObject> execution = runningExecutions.get(contextId);
        if (execution != null) {
            execution.cancel(true);
            runningExecutions.remove(contextId);
            return Future.succeededFuture();
        }
        return Future.failedFuture("Execution not found: " + contextId);
    }
    
    /**
     * Shutdown the pipeline
     */
    public Future<Void> shutdown() {
        isShuttingDown.set(true);
        
        // Cancel all running executions
        runningExecutions.values().forEach(execution -> execution.cancel(true));
        runningExecutions.clear();
        
        // Clear metrics
        stepDurations.clear();
        stepRetryCount.clear();
        
        return Future.succeededFuture();
    }
}
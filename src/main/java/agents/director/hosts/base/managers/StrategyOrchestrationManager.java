package agents.director.hosts.base.managers;

import agents.director.mcp.client.StrategyGenerationClient;
import agents.director.mcp.client.StrategyOrchestratorClient;
import agents.director.mcp.client.StrategyLearningClient;
import agents.director.mcp.client.IntentAnalysisClient;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;

import java.util.Arrays;
import java.util.List;

/**
 * Manager for strategy orchestration clients.
 * Coordinates between StrategyGeneration, StrategyOrchestrator, and StrategyLearning clients
 * to provide complete strategy lifecycle management.
 */
public class StrategyOrchestrationManager extends MCPClientManager {
    
    private static final String GENERATION_CLIENT = "generation";
    private static final String ORCHESTRATOR_CLIENT = "orchestrator";
    private static final String LEARNING_CLIENT = "learning";
    private static final String ANALYSIS_CLIENT = "analysis";
    
    public StrategyOrchestrationManager(Vertx vertx, String baseUrl) {
        super(vertx, baseUrl);
    }
    
    @Override
    public Future<Void> initialize() {
        List<Future> deploymentFutures = Arrays.asList(
            deployClient(GENERATION_CLIENT, new StrategyGenerationClient(baseUrl)),
            deployClient(ORCHESTRATOR_CLIENT, new StrategyOrchestratorClient(baseUrl)),
            deployClient(LEARNING_CLIENT, new StrategyLearningClient(baseUrl)),
            deployClient(ANALYSIS_CLIENT, new IntentAnalysisClient(baseUrl))
        );
        
        return CompositeFuture.all(deploymentFutures).mapEmpty();
    }
    
    /**
     * Generate and execute a complete strategy
     * @param query The user's query
     * @param context Execution context
     * @return Future with execution results
     */
    public Future<JsonObject> generateAndExecuteStrategy(String query, JsonObject context) {
        // Step 1: Analyze complexity
        return callClientTool(GENERATION_CLIENT, "strategy_generation__analyze_complexity",
            new JsonObject()
                .put("query", query)
                .put("context", context))
            .compose(complexity -> {
                // Step 2: Create strategy based on complexity
                return callClientTool(GENERATION_CLIENT, "strategy_generation__create_strategy",
                    new JsonObject()
                        .put("query", query)
                        .put("complexity", complexity)
                        .put("context", context))
                    .map(strategy -> {
                        strategy.put("complexity", complexity);
                        return strategy;
                    });
            })
            .compose(strategy -> {
                // Step 3: Execute strategy steps
                return executeStrategySteps(strategy);
            })
            .compose(executionResult -> {
                // Step 4: Record execution for learning
                return callClientTool(LEARNING_CLIENT, "strategy_learning__record_execution",
                    new JsonObject()
                        .put("query", query)
                        .put("strategy", executionResult.getJsonObject("strategy"))
                        .put("result", executionResult)
                        .put("timestamp", System.currentTimeMillis()))
                    .map(v -> executionResult);
            });
    }
    
    /**
     * Execute strategy steps sequentially
     * @param strategy The strategy to execute
     * @return Future with execution results
     */
    private Future<JsonObject> executeStrategySteps(JsonObject strategy) {
        JsonArray steps = strategy.getJsonArray("steps", new JsonArray());
        if (steps.isEmpty()) {
            return Future.succeededFuture(new JsonObject()
                .put("strategy", strategy)
                .put("results", new JsonArray())
                .put("success", true));
        }
        
        Promise<JsonObject> promise = Promise.promise();
        JsonArray results = new JsonArray();
        
        executeStepRecursive(steps, 0, results, strategy, promise);
        
        return promise.future();
    }
    
    /**
     * Recursively execute strategy steps
     */
    private void executeStepRecursive(JsonArray steps, int index, JsonArray results, 
                                     JsonObject strategy, Promise<JsonObject> promise) {
        if (index >= steps.size()) {
            // All steps completed
            promise.complete(new JsonObject()
                .put("strategy", strategy)
                .put("results", results)
                .put("success", true));
            return;
        }
        
        JsonObject step = steps.getJsonObject(index);
        
        // Execute current step
        callClientTool(ORCHESTRATOR_CLIENT, "strategy_orchestrator__execute_step",
            new JsonObject()
                .put("step", step)
                .put("stepIndex", index)
                .put("previousResults", results)
                .put("strategy", strategy))
            .compose(stepResult -> {
                results.add(stepResult);
                
                // Evaluate progress
                return callClientTool(ORCHESTRATOR_CLIENT, "strategy_orchestrator__evaluate_progress",
                    new JsonObject()
                        .put("strategy", strategy)
                        .put("currentStep", index)
                        .put("results", results));
            })
            .onComplete(ar -> {
                if (ar.succeeded()) {
                    JsonObject evaluation = ar.result();
                    
                    if (evaluation.getBoolean("needsAdaptation", false)) {
                        // Adapt strategy if needed
                        adaptAndContinue(strategy, evaluation, steps, index, results, promise);
                    } else {
                        // Continue with next step
                        executeStepRecursive(steps, index + 1, results, strategy, promise);
                    }
                } else {
                    promise.fail(ar.cause());
                }
            });
    }
    
    /**
     * Adapt strategy based on evaluation and continue execution
     */
    private void adaptAndContinue(JsonObject strategy, JsonObject evaluation, 
                                 JsonArray steps, int currentIndex, JsonArray results, 
                                 Promise<JsonObject> promise) {
        callClientTool(ORCHESTRATOR_CLIENT, "strategy_orchestrator__adapt_strategy",
            new JsonObject()
                .put("strategy", strategy)
                .put("evaluation", evaluation)
                .put("currentStep", currentIndex)
                .put("results", results))
            .onComplete(ar -> {
                if (ar.succeeded()) {
                    JsonObject adaptedStrategy = ar.result();
                    JsonArray adaptedSteps = adaptedStrategy.getJsonArray("steps", steps);
                    
                    // Continue with adapted strategy
                    executeStepRecursive(adaptedSteps, currentIndex + 1, results, adaptedStrategy, promise);
                } else {
                    // Log the adaptation failure and fail fast
                    vertx.eventBus().publish("log", 
                        "Failed to adapt strategy: " + ar.cause().getMessage() + 
                        ",0,StrategyOrchestrationManager,Manager,System");
                    promise.fail("Strategy adaptation failed: " + ar.cause().getMessage());
                }
            });
    }
    
    /**
     * Adapt strategy in flight based on feedback
     * @param currentStrategy The current strategy
     * @param progress Current progress
     * @param feedback User or system feedback
     * @return Future with adapted strategy
     */
    public Future<JsonObject> adaptStrategyInFlight(JsonObject currentStrategy, 
                                                   JsonObject progress, 
                                                   JsonObject feedback) {
        // Evaluate current progress
        Future<JsonObject> evaluationFuture = callClientTool(ORCHESTRATOR_CLIENT, 
            "strategy_orchestrator__evaluate_progress",
            new JsonObject()
                .put("strategy", currentStrategy)
                .put("progress", progress)
                .put("feedback", feedback));
        
        // Adapt based on evaluation
        return evaluationFuture.compose(evaluation -> 
            callClientTool(ORCHESTRATOR_CLIENT, "strategy_orchestrator__adapt_strategy",
                new JsonObject()
                    .put("strategy", currentStrategy)
                    .put("evaluation", evaluation)
                    .put("feedback", feedback)));
    }
    
    /**
     * Learn from execution and get improvement suggestions
     * @param strategy The executed strategy
     * @param results The execution results
     * @return Future with improvement suggestions
     */
    public Future<JsonObject> learnFromExecution(JsonObject strategy, JsonObject results) {
        // Record the execution
        Future<JsonObject> recordFuture = callClientTool(LEARNING_CLIENT, 
            "strategy_learning__record_execution",
            new JsonObject()
                .put("strategy", strategy)
                .put("results", results)
                .put("timestamp", System.currentTimeMillis()));
        
        // Analyze patterns
        Future<JsonObject> patternsFuture = callClientTool(LEARNING_CLIENT, 
            "strategy_learning__analyze_patterns",
            new JsonObject()
                .put("strategy", strategy)
                .put("results", results));
        
        // Get improvement suggestions
        Future<JsonObject> improvementsFuture = callClientTool(LEARNING_CLIENT, 
            "strategy_learning__suggest_improvements",
            new JsonObject()
                .put("strategy", strategy)
                .put("results", results));
        
        return CompositeFuture.all(recordFuture, patternsFuture, improvementsFuture)
            .map(composite -> new JsonObject()
                .put("recorded", composite.resultAt(0))
                .put("patterns", composite.resultAt(1))
                .put("improvements", composite.resultAt(2)));
    }
    
    /**
     * Optimize an existing strategy
     * @param strategy The strategy to optimize
     * @param historicalData Historical execution data
     * @return Future with optimized strategy
     */
    public Future<JsonObject> optimizeStrategy(JsonObject strategy, JsonObject historicalData) {
        // Analyze patterns from historical data
        Future<JsonObject> patternsFuture = callClientTool(LEARNING_CLIENT, 
            "strategy_learning__analyze_patterns",
            new JsonObject().put("historicalData", historicalData));
        
        // Optimize based on patterns
        return patternsFuture.compose(patterns -> 
            callClientTool(GENERATION_CLIENT, "strategy_generation__optimize_strategy",
                new JsonObject()
                    .put("strategy", strategy)
                    .put("patterns", patterns)
                    .put("historicalData", historicalData)));
    }
    
    /**
     * Generate a dynamic strategy based on query analysis
     * @param query The user query
     * @param conversationHistory Recent conversation history
     * @param expertiseLevel User expertise level
     * @return Future with generated strategy
     */
    public Future<JsonObject> generateDynamicStrategy(String query, JsonArray conversationHistory, String expertiseLevel) {
        return generateDynamicStrategy(query, conversationHistory, expertiseLevel, new JsonObject());
    }
    
    /**
     * Generate a dynamic strategy with additional options
     * @param query The user query
     * @param conversationHistory Recent conversation history
     * @param expertiseLevel User expertise level
     * @param options Additional generation options
     * @return Future with generated strategy
     */
    public Future<JsonObject> generateDynamicStrategy(String query, JsonArray conversationHistory, String expertiseLevel, JsonObject options) {
        // First analyze intent
        Future<JsonObject> intentFuture = callClientTool(ANALYSIS_CLIENT, "intent_analysis__extract_intent",
            new JsonObject()
                .put("query", query)
                .put("conversation_history", conversationHistory)
                .put("user_profile", new JsonObject().put("expertise_level", expertiseLevel)));
        
        // Then analyze complexity
        Future<JsonObject> complexityFuture = callClientTool(GENERATION_CLIENT, "strategy_generation__analyze_complexity",
            new JsonObject()
                .put("query", query)
                .put("context", new JsonObject().put("previous_queries", conversationHistory)));
        
        // Combine both analyses and generate strategy
        return CompositeFuture.all(intentFuture, complexityFuture)
            .compose(composite -> {
                JsonObject intent = composite.resultAt(0);
                JsonObject complexity = composite.resultAt(1);
                
                // Override intent if mode is specified
                if (options.containsKey("mode") && "sql_only".equals(options.getString("mode"))) {
                    intent.put("primary_intent", "get_sql_only");
                }
                
                JsonObject strategyArgs = new JsonObject()
                    .put("query", query)
                    .put("intent", intent)
                    .put("complexity_analysis", complexity)
                    .put("constraints", new JsonObject()
                        .put("max_steps", options.getInteger("max_steps", 12))
                        .put("timeout_seconds", 60));
                
                // Add any additional constraints from options
                if (options.containsKey("required_validations")) {
                    strategyArgs.getJsonObject("constraints")
                        .put("required_validations", options.getJsonArray("required_validations"));
                }
                
                return callClientTool(GENERATION_CLIENT, "strategy_generation__create_strategy", strategyArgs);
            });
    }
    
    /**
     * Record strategy execution for learning
     * @param strategy The executed strategy
     * @param executionResults Execution results
     * @param performanceMetrics Performance metrics
     * @return Future with recording result
     */
    public Future<JsonObject> recordExecution(JsonObject strategy, JsonObject executionResults, JsonObject performanceMetrics) {
        return callClientTool(LEARNING_CLIENT, "strategy_learning__record_execution",
            new JsonObject()
                .put("strategy", strategy)
                .put("execution_results", executionResults)
                .put("performance_metrics", performanceMetrics));
    }
    
    /**
     * Evaluate and potentially adapt a running strategy
     * @param strategy The current strategy
     * @param completedSteps Steps completed so far
     * @param currentResults Current accumulated results
     * @param timeElapsed Time elapsed in milliseconds
     * @return Future with evaluation and possibly adapted strategy
     */
    public Future<JsonObject> evaluateAndAdaptStrategy(JsonObject strategy, JsonArray completedSteps, 
                                                      JsonObject currentResults, long timeElapsed) {
        // Evaluate progress
        Future<JsonObject> evaluationFuture = callClientTool(ORCHESTRATOR_CLIENT, "strategy_orchestrator__evaluate_progress",
            new JsonObject()
                .put("strategy", strategy)
                .put("completed_steps", completedSteps)
                .put("current_results", currentResults)
                .put("time_elapsed", timeElapsed));
        
        return evaluationFuture.compose(evaluation -> {
            if (!evaluation.getBoolean("on_track", true)) {
                // Adapt strategy if needed
                return callClientTool(ORCHESTRATOR_CLIENT, "strategy_orchestrator__adapt_strategy",
                    new JsonObject()
                        .put("current_strategy", strategy)
                        .put("evaluation", evaluation))
                    .map(adapted -> {
                        JsonObject result = new JsonObject()
                            .put("adapted", true)
                            .put("steps", adapted.getJsonObject("adapted_strategy", strategy)
                                .getJsonArray("steps", strategy.getJsonArray("steps")));
                        return result;
                    });
            } else {
                // No adaptation needed
                return Future.succeededFuture(new JsonObject()
                    .put("adapted", false)
                    .put("steps", strategy.getJsonArray("steps")));
            }
        });
    }
}
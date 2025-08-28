package agents.director.mcp.servers;

import agents.director.mcp.base.MCPServerBase;
import agents.director.mcp.base.MCPTool;
import agents.director.mcp.base.MCPResponse;
import agents.director.services.LlmAPIService;
import io.vertx.core.Promise;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.web.RoutingContext;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import static agents.director.Driver.logLevel;

/**
 * MCP Server for orchestrating strategy execution with real-time adaptation.
 * Monitors execution progress and dynamically modifies strategies as needed.
 */
public class StrategyOrchestratorServer extends MCPServerBase {
    
    private LlmAPIService llmService;
    
    // Track execution contexts for monitoring
    private final Map<String, ExecutionContext> activeExecutions = new ConcurrentHashMap<>();
    
    // Performance thresholds
    private static final long STEP_TIMEOUT_MS = 30000; // 30 seconds per step
    private static final float PROGRESS_WARNING_THRESHOLD = 0.7f; // Warn if < 70% progress expected
    
    public StrategyOrchestratorServer() {
        super("StrategyOrchestratorServer", "/mcp/servers/strategy-orchestrator");
    }
    
    @Override
    public void start(Promise<Void> startPromise) {
        llmService = LlmAPIService.getInstance();
        
        if (!llmService.isInitialized()) {
            if (logLevel >= 1) vertx.eventBus().publish("log", "LLM service not initialized - adaptation capabilities limited,1,StrategyOrchestratorServer,Init,Warning");
        }
        
        // Start cleanup timer for old execution contexts
        vertx.setPeriodic(60000, id -> cleanupOldContexts()); // Every minute
        
        super.start(startPromise);
    }
    
    @Override
    protected void initializeTools() {
        // Register execute_step tool
        registerTool(new MCPTool(
            "strategy_orchestrator__execute_step",
            "Execute a single strategy step and collect detailed results",
            new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("step", new JsonObject()
                        .put("type", "object")
                        .put("description", "The step definition from the strategy")
                        .put("properties", new JsonObject()
                            .put("step", new JsonObject().put("type", "integer"))
                            .put("tool", new JsonObject().put("type", "string"))
                            .put("server", new JsonObject().put("type", "string"))
                            .put("description", new JsonObject().put("type", "string"))
                            .put("optional", new JsonObject().put("type", "boolean"))
                            .put("depends_on", new JsonObject()
                                .put("type", "array")
                                .put("items", new JsonObject().put("type", "integer")))
                            .put("parallel_allowed", new JsonObject().put("type", "boolean"))))
                    .put("context", new JsonObject()
                        .put("type", "object")
                        .put("description", "Execution context including query and intermediate results"))
                    .put("previous_results", new JsonObject()
                        .put("type", "array")
                        .put("description", "Results from previously executed steps")
                        .put("items", new JsonObject().put("type", "object"))))
                .put("required", new JsonArray().add("step").add("context"))
        ));
        
        // Register evaluate_progress tool
        registerTool(new MCPTool(
            "strategy_orchestrator__evaluate_progress",
            "Evaluate strategy execution progress and identify issues",
            new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("strategy", new JsonObject()
                        .put("type", "object")
                        .put("description", "The complete strategy being executed"))
                    .put("completed_steps", new JsonObject()
                        .put("type", "array")
                        .put("description", "Steps that have been completed")
                        .put("items", new JsonObject().put("type", "object")))
                    .put("current_results", new JsonObject()
                        .put("type", "object")
                        .put("description", "Current accumulated results"))
                    .put("time_elapsed", new JsonObject()
                        .put("type", "integer")
                        .put("description", "Milliseconds elapsed since start"))
                    .put("execution_id", new JsonObject()
                        .put("type", "string")
                        .put("description", "Optional execution ID for tracking")))
                .put("required", new JsonArray().add("strategy").add("completed_steps"))
        ));
        
        // Register adapt_strategy tool
        registerTool(new MCPTool(
            "strategy_orchestrator__adapt_strategy",
            "Dynamically adapt strategy based on evaluation results",
            new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("current_strategy", new JsonObject()
                        .put("type", "object")
                        .put("description", "The current strategy"))
                    .put("evaluation", new JsonObject()
                        .put("type", "object")
                        .put("description", "Progress evaluation results"))
                    .put("user_feedback", new JsonObject()
                        .put("type", "string")
                        .put("description", "Optional user feedback or preferences"))
                    .put("constraints", new JsonObject()
                        .put("type", "object")
                        .put("description", "Constraints for adaptation")
                        .put("properties", new JsonObject()
                            .put("max_additional_steps", new JsonObject()
                                .put("type", "integer")
                                .put("default", 5))
                            .put("preserve_critical_steps", new JsonObject()
                                .put("type", "boolean")
                                .put("default", true)))))
                .put("required", new JsonArray().add("current_strategy").add("evaluation"))
        ));
    }
    
    @Override
    protected void executeTool(RoutingContext ctx, String requestId, String toolName, JsonObject arguments) {
        switch (toolName) {
            case "strategy_orchestrator__execute_step":
                executeStep(ctx, requestId, arguments);
                break;
            case "strategy_orchestrator__evaluate_progress":
                evaluateProgress(ctx, requestId, arguments);
                break;
            case "strategy_orchestrator__adapt_strategy":
                adaptStrategy(ctx, requestId, arguments);
                break;
            default:
                sendError(ctx, requestId, MCPResponse.ErrorCodes.METHOD_NOT_FOUND, 
                    "Unknown tool: " + toolName);
        }
    }
    
    private void executeStep(RoutingContext ctx, String requestId, JsonObject arguments) {
        JsonObject step = arguments.getJsonObject("step");
        JsonObject context = arguments.getJsonObject("context");
        JsonArray previousResults = arguments.getJsonArray("previous_results", new JsonArray());
        
        String tool = step.getString("tool");
        String server = step.getString("server");
        boolean optional = step.getBoolean("optional", false);
        
        if (logLevel >= 3) vertx.eventBus().publish("log", "Executing step: " + tool + " on server: " + server + ",3,StrategyOrchestratorServer,Execute,Step");
        
        long startTime = System.currentTimeMillis();
        
        // Simulate step execution (in real implementation, would call actual tool)
        JsonObject stepResult = new JsonObject()
            .put("step_number", step.getInteger("step"))
            .put("tool", tool)
            .put("server", server)
            .put("start_time", startTime)
            .put("status", "pending");
        
        // Check dependencies
        JsonArray dependsOn = step.getJsonArray("depends_on");
        if (dependsOn != null && !dependsOn.isEmpty()) {
            if (!checkDependencies(dependsOn, previousResults)) {
                stepResult
                    .put("status", "skipped")
                    .put("reason", "Dependencies not met")
                    .put("execution_time", 0);
                    
                if (!optional) {
                    stepResult.put("error", "Required dependencies not satisfied");
                }
                
                sendSuccess(ctx, requestId, new JsonObject().put("result", stepResult));
                return;
            }
        }
        
        // In real implementation, this would call the actual MCP tool
        // For now, simulate execution with some logic
        simulateStepExecution(step, context, previousResults)
            .onComplete(ar -> {
                long executionTime = System.currentTimeMillis() - startTime;
                
                if (ar.succeeded()) {
                    stepResult
                        .put("status", "completed")
                        .put("execution_time", executionTime)
                        .put("result", ar.result())
                        .put("success", true);
                    
                    // Suggest next step optimization if execution was slow
                    if (executionTime > STEP_TIMEOUT_MS * 0.8) {
                        stepResult.put("next_step_recommendation", "Consider parallelization or caching");
                    }
                } else {
                    stepResult
                        .put("status", optional ? "failed_optional" : "failed")
                        .put("execution_time", executionTime)
                        .put("error", ar.cause().getMessage())
                        .put("success", false);
                    
                    if (!optional) {
                        stepResult.put("next_step_recommendation", "Retry with modified parameters or use fallback");
                    }
                }
                
                sendSuccess(ctx, requestId, new JsonObject().put("result", stepResult));
            });
    }
    
    private void evaluateProgress(RoutingContext ctx, String requestId, JsonObject arguments) {
        JsonObject strategy = arguments.getJsonObject("strategy");
        JsonArray completedSteps = arguments.getJsonArray("completed_steps");
        JsonObject currentResults = arguments.getJsonObject("current_results", new JsonObject());
        Long timeElapsed = arguments.getLong("time_elapsed", 0L);
        String executionId = arguments.getString("execution_id", UUID.randomUUID().toString());
        
        // Create or update execution context
        ExecutionContext execContext = activeExecutions.computeIfAbsent(executionId, 
            k -> new ExecutionContext(executionId, strategy));
        execContext.updateProgress(completedSteps, currentResults, timeElapsed);
        
        if (logLevel >= 3) vertx.eventBus().publish("log", "Evaluating progress for execution: " + executionId + ",3,StrategyOrchestratorServer,Progress,Evaluate");
        
        JsonArray allSteps = strategy.getJsonArray("steps", new JsonArray());
        int totalSteps = allSteps.size();
        int completedCount = completedSteps.size();
        float progressPercentage = totalSteps > 0 ? (float) completedCount / totalSteps : 0;
        
        // Analyze bottlenecks
        JsonArray bottlenecks = new JsonArray();
        JsonArray adaptationSuggestions = new JsonArray();
        boolean onTrack = true;
        
        // Check for slow steps
        for (int i = 0; i < completedSteps.size(); i++) {
            JsonObject completed = completedSteps.getJsonObject(i);
            long stepTime = completed.getLong("execution_time", 0L);
            
            if (stepTime > STEP_TIMEOUT_MS * 0.8) {
                bottlenecks.add("Step " + completed.getInteger("step_number") + 
                    " (" + completed.getString("tool") + ") took " + (stepTime / 1000) + "s");
            }
            
            if (completed.getString("status", "").equals("failed")) {
                onTrack = false;
                adaptationSuggestions.add("Replace or skip failed step: " + completed.getString("tool"));
            }
        }
        
        // Check overall progress rate
        if (timeElapsed > 0 && totalSteps > 0) {
            long estimatedTotalTime = (timeElapsed * totalSteps) / Math.max(completedCount, 1);
            if (estimatedTotalTime > 120000) { // > 2 minutes
                onTrack = false;
                adaptationSuggestions.add("Consider simplifying strategy - estimated " + 
                    (estimatedTotalTime / 1000) + "s total");
            }
        }
        
        // Check if we're behind expected progress
        float expectedProgress = Math.min(1.0f, timeElapsed / 60000f); // Expect completion in 1 minute
        if (progressPercentage < expectedProgress * PROGRESS_WARNING_THRESHOLD) {
            onTrack = false;
            adaptationSuggestions.add("Progress slower than expected - consider parallel execution");
        }
        
        // Look for optimization opportunities
        Map<String, Integer> serverUsage = analyzeServerUsage(completedSteps);
        for (Map.Entry<String, Integer> entry : serverUsage.entrySet()) {
            if (entry.getValue() > 3) {
                adaptationSuggestions.add("Multiple calls to " + entry.getKey() + 
                    " - consider batching");
            }
        }
        
        // Build evaluation result
        JsonObject evaluation = new JsonObject()
            .put("progress_percentage", progressPercentage)
            .put("on_track", onTrack)
            .put("completed_steps", completedCount)
            .put("total_steps", totalSteps)
            .put("time_elapsed", timeElapsed)
            .put("estimated_remaining_time", estimateRemainingTime(execContext))
            .put("bottlenecks", bottlenecks)
            .put("adaptation_suggestions", adaptationSuggestions)
            .put("current_phase", determinePhase(progressPercentage))
            .put("confidence_level", calculateConfidence(completedSteps));
        
        sendSuccess(ctx, requestId, new JsonObject().put("result", evaluation));
    }
    
    private void adaptStrategy(RoutingContext ctx, String requestId, JsonObject arguments) {
        JsonObject currentStrategy = arguments.getJsonObject("current_strategy");
        JsonObject evaluation = arguments.getJsonObject("evaluation");
        String userFeedback = arguments.getString("user_feedback", "");
        JsonObject constraints = arguments.getJsonObject("constraints", new JsonObject());
        
        if (logLevel >= 1) vertx.eventBus().publish("log", "Adapting strategy based on evaluation,1,StrategyOrchestratorServer,Strategy,Adapt");
        
        // If no LLM available, use rule-based adaptation
        if (!llmService.isInitialized()) {
            JsonObject adapted = performRuleBasedAdaptation(currentStrategy, evaluation, constraints);
            sendSuccess(ctx, requestId, new JsonObject().put("result", adapted));
            return;
        }
        
        // Use LLM for intelligent adaptation
        adaptStrategyWithLLM(currentStrategy, evaluation, userFeedback, constraints)
            .onComplete(ar -> {
                if (ar.succeeded()) {
                    sendSuccess(ctx, requestId, new JsonObject().put("result", ar.result()));
                } else {
                    // Fallback to rule-based
                    JsonObject fallback = performRuleBasedAdaptation(currentStrategy, evaluation, constraints);
                    sendSuccess(ctx, requestId, new JsonObject().put("result", fallback));
                }
            });
    }
    
    private Future<JsonObject> adaptStrategyWithLLM(JsonObject currentStrategy, JsonObject evaluation,
                                                   String userFeedback, JsonObject constraints) {
        Promise<JsonObject> promise = Promise.promise();
        
        String prompt = buildAdaptationPrompt(currentStrategy, evaluation, userFeedback, constraints);
        
        // Convert prompt to messages array for chatCompletion
        JsonArray messages = new JsonArray()
            .add(new JsonObject()
                .put("role", "system")
                .put("content", "You are a strategy adaptation assistant. Adapt and improve database query strategies based on feedback."))
            .add(new JsonObject()
                .put("role", "user")
                .put("content", prompt));
        
        llmService.chatCompletion(messages)
            .onComplete(ar -> {
                if (ar.succeeded()) {
                    try {
                        JsonObject response = ar.result();
                        JsonArray choices = response.getJsonArray("choices", new JsonArray());
                        JsonObject firstChoice = choices.size() > 0 ? choices.getJsonObject(0) : new JsonObject();
                        JsonObject message = firstChoice.getJsonObject("message", new JsonObject());
                        String content = message.getString("content", "");
                        JsonObject adapted = parseAdaptedStrategy(content);
                        promise.complete(wrapAdaptationResult(adapted, currentStrategy));
                    } catch (Exception e) {
                        vertx.eventBus().publish("log", "Failed to parse adapted strategy: " + e.getMessage() + ",0,StrategyOrchestratorServer,Adapt,Parse");
                        promise.fail(e);
                    }
                } else {
                    promise.fail(ar.cause());
                }
            });
        
        return promise.future();
    }
    
    private String buildAdaptationPrompt(JsonObject currentStrategy, JsonObject evaluation,
                                       String userFeedback, JsonObject constraints) {
        JsonArray suggestions = evaluation.getJsonArray("adaptation_suggestions", new JsonArray());
        String suggestionsStr = String.join("\n- ", suggestions.getList());
        
        return String.format("""
            Adapt this orchestration strategy based on execution feedback.
            
            Current Strategy:
            %s
            
            Evaluation Results:
            - Progress: %.1f%% complete
            - On Track: %s
            - Bottlenecks: %s
            - Time Elapsed: %dms
            
            Adaptation Suggestions:
            - %s
            
            User Feedback: %s
            
            Constraints:
            - Max additional steps: %d
            - Preserve critical steps: %s
            
            Adapt the strategy to address the issues. You may:
            1. Remove optional steps that are bottlenecks
            2. Add parallel_allowed flags where safe
            3. Insert new steps to handle failures
            4. Reorder steps for better efficiency
            5. Add decision points for dynamic branching
            
            Output ONLY the adapted strategy JSON with these additions:
            - A "changes_made" array listing what was changed
            - A "reason_for_changes" field explaining the adaptation
            
            Adapted strategy JSON:
            """,
            currentStrategy.encode(),
            evaluation.getFloat("progress_percentage", 0f) * 100,
            evaluation.getBoolean("on_track", false),
            evaluation.getJsonArray("bottlenecks", new JsonArray()).encode(),
            evaluation.getLong("time_elapsed", 0L),
            suggestionsStr,
            userFeedback.isEmpty() ? "None" : userFeedback,
            constraints.getInteger("max_additional_steps", 5),
            constraints.getBoolean("preserve_critical_steps", true));
    }
    
    private JsonObject performRuleBasedAdaptation(JsonObject currentStrategy, JsonObject evaluation,
                                                 JsonObject constraints) {
        JsonObject adapted = currentStrategy.copy();
        JsonArray steps = adapted.getJsonArray("steps", new JsonArray());
        JsonArray changesMade = new JsonArray();
        
        // Get suggestions from evaluation
        JsonArray suggestions = evaluation.getJsonArray("adaptation_suggestions", new JsonArray());
        
        // Apply simple rules based on suggestions
        for (int i = 0; i < suggestions.size(); i++) {
            String suggestion = suggestions.getString(i);
            
            if (suggestion.contains("parallel execution")) {
                // Enable parallel execution for independent steps
                enableParallelExecution(steps, changesMade);
            } else if (suggestion.contains("skip failed step")) {
                // Mark failed steps as optional
                markFailedStepsAsOptional(steps, evaluation, changesMade);
            } else if (suggestion.contains("simplifying strategy")) {
                // Remove optional steps
                removeOptionalSteps(steps, changesMade);
            }
        }
        
        adapted.put("changes_made", changesMade);
        adapted.put("reason_for_changes", "Rule-based adaptation based on performance evaluation");
        
        return new JsonObject()
            .put("adapted_strategy", adapted)
            .put("changes_made", changesMade)
            .put("reason_for_changes", "Applied rule-based optimizations");
    }
    
    // Helper methods
    
    private boolean checkDependencies(JsonArray dependsOn, JsonArray previousResults) {
        Set<Integer> completedSteps = new HashSet<>();
        for (int i = 0; i < previousResults.size(); i++) {
            JsonObject result = previousResults.getJsonObject(i);
            if ("completed".equals(result.getString("status"))) {
                completedSteps.add(result.getInteger("step_number"));
            }
        }
        
        for (int i = 0; i < dependsOn.size(); i++) {
            if (!completedSteps.contains(dependsOn.getInteger(i))) {
                return false;
            }
        }
        return true;
    }
    
    private Future<JsonObject> simulateStepExecution(JsonObject step, JsonObject context, 
                                                    JsonArray previousResults) {
        Promise<JsonObject> promise = Promise.promise();
        
        // Simulate async execution with delay
        long delay = 100 + (long)(Math.random() * 500); // 100-600ms
        
        vertx.setTimer(delay, id -> {
            // Simulate different outcomes based on tool
            String tool = step.getString("tool");
            
            if (tool.contains("validate") && Math.random() > 0.9) {
                // 10% chance of validation failure
                promise.fail("Validation error: simulated failure");
            } else if (tool.contains("optimize") && previousResults.size() < 2) {
                // Optimization requires previous results
                promise.fail("Insufficient data for optimization");
            } else {
                // Success with mock result
                JsonObject result = new JsonObject()
                    .put("tool", tool)
                    .put("output", "Simulated output for " + tool)
                    .put("metadata", new JsonObject()
                        .put("rows_processed", (int)(Math.random() * 1000))
                        .put("execution_time_ms", delay));
                promise.complete(result);
            }
        });
        
        return promise.future();
    }
    
    private Map<String, Integer> analyzeServerUsage(JsonArray completedSteps) {
        Map<String, Integer> usage = new HashMap<>();
        for (int i = 0; i < completedSteps.size(); i++) {
            JsonObject step = completedSteps.getJsonObject(i);
            String server = step.getString("server", "unknown");
            usage.merge(server, 1, Integer::sum);
        }
        return usage;
    }
    
    private long estimateRemainingTime(ExecutionContext context) {
        if (context.completedSteps == 0) return 0;
        
        long avgStepTime = context.totalElapsedTime / context.completedSteps;
        int remainingSteps = context.totalSteps - context.completedSteps;
        
        return avgStepTime * remainingSteps;
    }
    
    private String determinePhase(float progress) {
        if (progress < 0.2) return "initialization";
        else if (progress < 0.5) return "analysis";
        else if (progress < 0.8) return "execution";
        else return "finalization";
    }
    
    private float calculateConfidence(JsonArray completedSteps) {
        if (completedSteps.isEmpty()) return 1.0f;
        
        int successful = 0;
        for (int i = 0; i < completedSteps.size(); i++) {
            JsonObject step = completedSteps.getJsonObject(i);
            if ("completed".equals(step.getString("status"))) {
                successful++;
            }
        }
        
        return (float) successful / completedSteps.size();
    }
    
    private void enableParallelExecution(JsonArray steps, JsonArray changesMade) {
        // Find steps that don't depend on each other
        for (int i = 1; i < steps.size() - 1; i++) {
            JsonObject current = steps.getJsonObject(i);
            JsonObject next = steps.getJsonObject(i + 1);
            
            // Check if next step depends on current
            JsonArray nextDeps = next.getJsonArray("depends_on");
            if (nextDeps == null || !nextDeps.contains(current.getInteger("step"))) {
                current.put("parallel_allowed", true);
                changesMade.add("Enabled parallel execution for step " + current.getInteger("step"));
            }
        }
    }
    
    private void markFailedStepsAsOptional(JsonArray steps, JsonObject evaluation, JsonArray changesMade) {
        // This is simplified - in real implementation would check actual failures
        for (int i = 0; i < steps.size(); i++) {
            JsonObject step = steps.getJsonObject(i);
            if (step.getString("tool", "").contains("validate") || 
                step.getString("tool", "").contains("optimize")) {
                if (!step.getBoolean("optional", false)) {
                    step.put("optional", true);
                    changesMade.add("Made step " + step.getInteger("step") + 
                        " (" + step.getString("tool") + ") optional");
                }
            }
        }
    }
    
    private void removeOptionalSteps(JsonArray steps, JsonArray changesMade) {
        JsonArray filtered = new JsonArray();
        for (int i = 0; i < steps.size(); i++) {
            JsonObject step = steps.getJsonObject(i);
            if (!step.getBoolean("optional", false)) {
                filtered.add(step);
            } else {
                changesMade.add("Removed optional step " + step.getInteger("step") + 
                    " (" + step.getString("tool") + ")");
            }
        }
        
        // Renumber steps
        for (int i = 0; i < filtered.size(); i++) {
            filtered.getJsonObject(i).put("step", i + 1);
        }
        
        steps.clear();
        steps.addAll(filtered);
    }
    
    private JsonObject parseAdaptedStrategy(String llmResponse) {
        String response = llmResponse.trim();
        int start = response.indexOf("{");
        int end = response.lastIndexOf("}");
        
        if (start >= 0 && end > start) {
            String jsonStr = response.substring(start, end + 1);
            return new JsonObject(jsonStr);
        }
        
        throw new IllegalArgumentException("No valid JSON found in adaptation response");
    }
    
    private JsonObject wrapAdaptationResult(JsonObject adaptedStrategy, JsonObject original) {
        // Extract changes if provided by LLM
        JsonArray changesMade = adaptedStrategy.getJsonArray("changes_made", new JsonArray());
        String reason = adaptedStrategy.getString("reason_for_changes", "LLM-based intelligent adaptation");
        
        // Remove these fields from the strategy itself
        adaptedStrategy.remove("changes_made");
        adaptedStrategy.remove("reason_for_changes");
        
        return new JsonObject()
            .put("adapted_strategy", adaptedStrategy)
            .put("changes_made", changesMade)
            .put("reason_for_changes", reason)
            .put("original_strategy_name", original.getString("name"));
    }
    
    private void cleanupOldContexts() {
        long cutoffTime = System.currentTimeMillis() - 300000; // 5 minutes
        
        activeExecutions.entrySet().removeIf(entry -> {
            ExecutionContext context = entry.getValue();
            if (context.lastUpdateTime < cutoffTime) {
                if (logLevel >= 3) vertx.eventBus().publish("log", "Removing old execution context: " + entry.getKey() + ",3,StrategyOrchestratorServer,Cleanup,Context");
                return true;
            }
            return false;
        });
    }
    
    // Inner class for tracking execution context
    private static class ExecutionContext {
        final String executionId;
        final JsonObject strategy;
        int completedSteps = 0;
        int totalSteps;
        long totalElapsedTime = 0;
        long lastUpdateTime;
        JsonObject latestResults;
        
        ExecutionContext(String executionId, JsonObject strategy) {
            this.executionId = executionId;
            this.strategy = strategy;
            this.totalSteps = strategy.getJsonArray("steps", new JsonArray()).size();
            this.lastUpdateTime = System.currentTimeMillis();
        }
        
        void updateProgress(JsonArray completed, JsonObject results, long elapsed) {
            this.completedSteps = completed.size();
            this.latestResults = results;
            this.totalElapsedTime = elapsed;
            this.lastUpdateTime = System.currentTimeMillis();
        }
    }
}
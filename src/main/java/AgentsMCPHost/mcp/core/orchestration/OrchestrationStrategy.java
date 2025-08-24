package AgentsMCPHost.mcp.core.orchestration;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.core.eventbus.Message;
import io.vertx.core.file.FileSystem;
import AgentsMCPHost.streaming.StreamingEventPublisher;

import java.util.*;

/**
 * Base class for orchestration strategies - coordinates tool execution patterns.
 * 
 * This is the foundation of the unified architecture where orchestration is just
 * a configuration of tool usage. Each strategy defines a sequence of tool calls
 * with data passing between steps.
 * 
 * Key principles:
 * - Orchestrators have NO business logic - they only coordinate
 * - All capabilities are exposed as tools
 * - Strategies are configurations, not code
 * - Maximum reusability and composability
 */
public abstract class OrchestrationStrategy extends AbstractVerticle {
    
    protected JsonObject strategyConfig;
    protected String strategyName;
    private JsonObject orchestrationStrategies;
    
    @Override
    public void start(Promise<Void> startPromise) {
        // Load orchestration strategies configuration
        loadStrategiesConfig()
            .compose(config -> {
                orchestrationStrategies = config;
                strategyName = getStrategyName();
                strategyConfig = config.getJsonObject("strategies", new JsonObject())
                    .getJsonObject(strategyName);
                
                if (strategyConfig == null) {
                    return Future.failedFuture("Strategy not found: " + strategyName);
                }
                
                System.out.println("[Orchestration] " + strategyName + " strategy initialized");
                System.out.println("[Orchestration] Steps: " + 
                    strategyConfig.getJsonArray("steps", new JsonArray()).size());
                
                // Register event bus handler for this strategy
                vertx.eventBus().consumer("orchestration." + strategyName, this::handleOrchestrationRequest);
                
                // Notify ready
                vertx.eventBus().publish("orchestration.ready", new JsonObject()
                    .put("strategy", strategyName)
                    .put("description", strategyConfig.getString("description")));
                
                return Future.succeededFuture();
            })
            .onSuccess(v -> startPromise.complete())
            .onFailure(startPromise::fail);
    }
    
    /**
     * Get the strategy name - must be implemented by subclasses
     */
    protected abstract String getStrategyName();
    
    /**
     * Load orchestration strategies configuration
     */
    private Future<JsonObject> loadStrategiesConfig() {
        FileSystem fs = vertx.fileSystem();
        String configPath = "src/main/resources/orchestration-strategies.json";
        
        return fs.readFile(configPath)
            .map(buffer -> new JsonObject(buffer))
            .recover(err -> {
                System.err.println("[Orchestration] Could not load strategies config: " + err.getMessage());
                // Return default config
                return Future.succeededFuture(new JsonObject()
                    .put("strategies", new JsonObject())
                    .put("configuration", new JsonObject()));
            });
    }
    
    /**
     * Handle orchestration request
     */
    private void handleOrchestrationRequest(Message<JsonObject> msg) {
        JsonObject request = msg.body();
        String query = request.getString("query");
        String sessionId = request.getString("sessionId", UUID.randomUUID().toString());
        String streamId = request.getString("streamId");
        
        System.out.println("[Orchestration] Starting " + strategyName + " for: " + query);
        
        // Create execution context
        ExecutionContext context = new ExecutionContext();
        context.originalQuery = query;
        context.sessionId = sessionId;
        context.streamId = streamId;
        context.startTime = System.currentTimeMillis();
        context.currentStep = 0;
        context.stepResults = new HashMap<>();
        
        // Get strategy steps
        JsonArray steps = strategyConfig.getJsonArray("steps", new JsonArray());
        long timeoutMs = strategyConfig.getLong("timeout_ms", 30000L);
        
        // Start progress updates if streaming
        final Long[] progressTimerId = {null};
        if (streamId != null && orchestrationStrategies.getJsonObject("configuration", new JsonObject())
                .getBoolean("enable_progress_updates", true)) {
            progressTimerId[0] = startProgressUpdates(context);
        }
        
        // Track if message has been replied to
        final boolean[] replied = {false};
        
        // Execute strategy
        executeStrategy(context, steps, 0)
            .onSuccess(result -> {
                if (!replied[0]) {
                    replied[0] = true;
                    System.out.println("[Orchestration] " + strategyName + " completed successfully");
                    if (progressTimerId[0] != null) {
                        vertx.cancelTimer(progressTimerId[0]);
                    }
                    msg.reply(result);
                }
            })
            .onFailure(err -> {
                if (!replied[0]) {
                    replied[0] = true;
                    System.err.println("[Orchestration] " + strategyName + " failed: " + err.getMessage());
                    if (progressTimerId[0] != null) {
                        vertx.cancelTimer(progressTimerId[0]);
                    }
                    msg.fail(500, err.getMessage());
                }
            });
        
        // Set overall timeout
        vertx.setTimer(timeoutMs, id -> {
            if (!replied[0]) {
                replied[0] = true;
                System.err.println("[Orchestration] " + strategyName + " timed out after " + timeoutMs + "ms");
                msg.fail(504, "Orchestration timed out");
                if (progressTimerId[0] != null) {
                    vertx.cancelTimer(progressTimerId[0]);
                }
            }
        });
    }
    
    /**
     * Execute strategy steps recursively
     */
    private Future<JsonObject> executeStrategy(ExecutionContext context, JsonArray steps, int stepIndex) {
        if (stepIndex >= steps.size()) {
            // All steps complete - return final result
            return Future.succeededFuture(buildFinalResult(context));
        }
        
        JsonObject step = steps.getJsonObject(stepIndex);
        context.currentStep = stepIndex + 1;
        
        // Send progress update
        sendStepProgress(context, step);
        
        // Check if step is optional and should be skipped
        if (step.getBoolean("optional", false) && shouldSkipStep(context, step)) {
            System.out.println("[Orchestration] Skipping optional step " + context.currentStep);
            return executeStrategy(context, steps, stepIndex + 1);
        }
        
        // Build tool arguments from context
        JsonObject toolArguments = buildToolArguments(context, step);
        
        // Call the tool
        String toolName = step.getString("tool");
        System.out.println("[Orchestration] Step " + context.currentStep + ": Calling " + toolName);
        
        return callTool(toolName, toolArguments, context.streamId)
            .compose(result -> {
                // Log raw result for debugging
                System.out.println("[Orchestration] Raw result from " + toolName + ": " + 
                    result.encode().substring(0, Math.min(200, result.encode().length())));
                
                // Store raw result
                String resultKey = step.getString("name", "step_" + context.currentStep);
                context.stepResults.put(resultKey, result);
                
                // Check for error in result
                boolean isCriticalStep = step.getBoolean("critical", true); // Default to critical
                if (result.containsKey("error") || result.getBoolean("isError", false)) {
                    String errorMsg = result.getString("error", "Unknown error");
                    System.err.println("[Orchestration] Step " + context.currentStep + " (" + toolName + ") failed with error: " + errorMsg);
                    
                    if (isCriticalStep) {
                        // Critical step failed - stop orchestration
                        System.err.println("[Orchestration] Critical step failed - stopping orchestration");
                        String failureMessage = "Critical step " + context.currentStep + " (" + toolName + ") failed: " + errorMsg;
                        
                        // Avoid recursive error wrapping - check if error already contains "Step X failed"
                        if (errorMsg.contains("Step") && errorMsg.contains("failed")) {
                            failureMessage = errorMsg; // Use original error message without wrapping
                        }
                        
                        return Future.failedFuture(failureMessage);
                    } else {
                        System.out.println("[Orchestration] Non-critical step failed - continuing");
                    }
                }
                
                // Extract data from MCP format FIRST
                Object extractedData = extractDataFromMcpResponse(result);
                System.out.println("[Orchestration] Extracted data type: " + 
                    (extractedData == null ? "null" : extractedData.getClass().getSimpleName()));
                
                // Store data to pass to next steps
                JsonArray passToNext = step.getJsonArray("pass_to_next", new JsonArray());
                for (int i = 0; i < passToNext.size(); i++) {
                    String key = passToNext.getString(i);
                    Object valueToPass = null;
                    
                    // Extract the specific field from the extracted data
                    if (extractedData instanceof JsonObject) {
                        JsonObject dataObj = (JsonObject) extractedData;
                        valueToPass = dataObj.getValue(key);
                        
                        // If specific key not found but only one key requested, store whole object
                        if (valueToPass == null && passToNext.size() == 1) {
                            valueToPass = dataObj;
                            System.out.println("[Orchestration] Storing entire extracted object for " + key);
                        }
                    } else if (extractedData instanceof String) {
                        // If extracted data is a string (error or plain text)
                        valueToPass = extractedData;
                        System.out.println("[Orchestration] Storing string data for " + key + ": " + valueToPass);
                    }
                    
                    if (valueToPass != null) {
                        context.dataToPass.put(key, valueToPass);
                        System.out.println("[Orchestration] Stored for next step: " + key + " = " + 
                            (valueToPass instanceof JsonObject || valueToPass instanceof JsonArray ? 
                             "JSON data" : valueToPass.toString()));
                        
                        vertx.eventBus().publish("log",
                            "Step " + resultKey + " passing " + key + ",3,OrchestrationStrategy,DataFlow,Tool");
                    } else {
                        System.out.println("[Orchestration] WARNING: Could not extract " + key + " from result");
                        vertx.eventBus().publish("log",
                            "Failed to extract " + key + " from " + resultKey + ",1,OrchestrationStrategy,Warning,DataFlow");
                    }
                }
                
                // Check if this is the final step
                if (step.getBoolean("final", false)) {
                    // Extract from MCP format for final result
                    Object finalExtracted = extractDataFromMcpResponse(result);
                    
                    // For format_results tool, preserve the formatted field
                    if (finalExtracted instanceof JsonObject) {
                        JsonObject extractedObj = (JsonObject) finalExtracted;
                        // Check if this is from format_results tool
                        if (extractedObj.containsKey("formatted")) {
                            // Preserve the entire response from format_results
                            context.finalResult = extractedObj;
                            System.out.println("[Orchestration] Final step - preserved formatted response");
                        } else {
                            context.finalResult = extractedObj;
                        }
                    } else if (finalExtracted instanceof String) {
                        // Wrap string in a result object
                        context.finalResult = new JsonObject()
                            .put("formatted", finalExtracted)
                            .put("success", !finalExtracted.toString().startsWith("Error:"));
                    } else {
                        context.finalResult = result; // Fallback to raw result
                    }
                    System.out.println("[Orchestration] Final result extracted: " + 
                        (context.finalResult != null ? context.finalResult.encode().substring(0, Math.min(200, context.finalResult.encode().length())) : "null"));
                    return Future.succeededFuture(buildFinalResult(context));
                }
                
                // Continue to next step
                return executeStrategy(context, steps, stepIndex + 1);
            })
            .recover(err -> {
                System.err.println("[Orchestration] Step " + context.currentStep + " failed: " + err.getMessage());
                
                // Check if step is critical
                boolean isCriticalStep = step.getBoolean("critical", true);
                
                // Check if we allow partial success and step is not critical
                if (strategyConfig.getBoolean("allow_partial_success", false) && !isCriticalStep) {
                    // Skip failed step and continue
                    System.out.println("[Orchestration] Skipping non-critical failed step and continuing");
                    return executeStrategy(context, steps, stepIndex + 1);
                } else {
                    // Fail the entire orchestration
                    System.err.println("[Orchestration] Critical step or no partial success allowed - stopping orchestration");
                    String errorMessage = err.getMessage();
                    
                    // Avoid recursive error wrapping
                    if (errorMessage != null && errorMessage.contains("Step") && errorMessage.contains("failed")) {
                        return Future.failedFuture(errorMessage); // Don't wrap again
                    } else {
                        return Future.failedFuture("Step " + context.currentStep + " failed: " + errorMessage);
                    }
                }
            });
    }
    
    /**
     * Call a tool via event bus
     */
    private Future<JsonObject> callTool(String toolName, JsonObject arguments, String streamId) {
        Promise<JsonObject> promise = Promise.promise();
        
        // Log tool call attempt
        vertx.eventBus().publish("log",
            "Orchestration routing " + toolName + " via MCP,3,OrchestrationStrategy,Routing,Tool");
        
        System.out.println("[Orchestration] Routing tool: " + toolName + " through MCP host");
        
        // Publish streaming event for tool execution start
        if (streamId != null) {
            StreamingEventPublisher publisher = new StreamingEventPublisher(vertx, streamId);
            publisher.publishProgress("tool_execution", "Executing tool: " + toolName,
                new JsonObject()
                    .put("phase", "tool_execution")
                    .put("tool", toolName)
                    .put("status", "routing")
                    .put("arguments", arguments));
        }
        
        // Build tool request
        JsonObject toolRequest = new JsonObject()
            .put("tool", toolName)
            .put("arguments", arguments);
        
        if (streamId != null) {
            toolRequest.put("streamId", streamId);
        }
        
        // Route through MCP host (single path - no fallback)
        vertx.eventBus().<JsonObject>request("mcp.host.route", toolRequest)
            .onSuccess(reply -> {
                JsonObject result = reply.body();
                
                // Log success
                vertx.eventBus().publish("log",
                    "Tool " + toolName + " routed successfully,2,OrchestrationStrategy,Success,Tool");
                
                // Publish streaming event for tool completion
                if (streamId != null) {
                    StreamingEventPublisher publisher = new StreamingEventPublisher(vertx, streamId);
                    publisher.publishProgress("tool_execution", "Tool completed: " + toolName,
                        new JsonObject()
                            .put("phase", "tool_execution")
                            .put("tool", toolName)
                            .put("status", "completed")
                            .put("hasResult", true));
                }
                
                promise.complete(result);
            })
            .onFailure(err -> {
                // Log routing failure - NO FALLBACK per architecture
                String error = "Tool routing failed for " + toolName + ": " + err.getMessage();
                
                vertx.eventBus().publish("log",
                    error + ",0,OrchestrationStrategy,Error,Tool");
                
                System.err.println("[Orchestration] " + error);
                System.err.println("[Orchestration] No fallback - maintaining single routing path");
                
                // Publish streaming event for tool failure
                if (streamId != null) {
                    StreamingEventPublisher publisher = new StreamingEventPublisher(vertx, streamId);
                    publisher.publishProgress("tool_execution", "Tool failed: " + toolName,
                        new JsonObject()
                            .put("phase", "tool_execution")
                            .put("tool", toolName)
                            .put("status", "failed")
                            .put("error", err.getMessage()));
                }
                
                promise.fail(err);
            });
        
        return promise.future();
    }
    
    
    /**
     * Build tool arguments from context and step configuration
     */
    private JsonObject buildToolArguments(ExecutionContext context, JsonObject step) {
        JsonObject arguments = new JsonObject();
        String stepName = step.getString("name", "Unknown");
        int stepNum = step.getInteger("step", 0);
        
        // Log what we're building
        vertx.eventBus().publish("log",
            "Building args for " + stepName + " step,3,OrchestrationStrategy,Arguments,Tool");
        
        // ALWAYS add original query for the first step
        if (stepNum == 1) {
            arguments.put("query", context.originalQuery);
            System.out.println("[Orchestration] Step 1: Auto-adding original query: " + context.originalQuery);
        }
        
        // Add original query if needed (backward compatibility)
        if (step.getBoolean("use_original_query", false)) {
            arguments.put("query", context.originalQuery);
            System.out.println("[Orchestration] Added original query to arguments");
        }
        
        // Add data from previous steps
        JsonArray useFromPrevious = step.getJsonArray("use_from_previous", new JsonArray());
        for (int i = 0; i < useFromPrevious.size(); i++) {
            String key = useFromPrevious.getString(i);
            
            if ("original_query".equals(key)) {
                // Map original_query to query parameter
                arguments.put("query", context.originalQuery);
                System.out.println("[Orchestration] Mapped original_query -> query: " + context.originalQuery);
            } else if (key.contains(".")) {
                // Handle nested references like "analysis.entities"
                String[] parts = key.split("\\.", 2);
                String topKey = parts[0];
                String nestedKey = parts[1];
                
                Object topValue = context.dataToPass.get(topKey);
                if (topValue instanceof JsonObject) {
                    JsonObject topObj = (JsonObject) topValue;
                    Object nestedValue = topObj.getValue(nestedKey);
                    if (nestedValue != null) {
                        // Special handling for schema_matches.tables -> table_names
                        if ("schema_matches".equals(topKey) && "tables".equals(nestedKey)) {
                            // Extract table names from array of table objects
                            if (nestedValue instanceof JsonArray) {
                                JsonArray tables = (JsonArray) nestedValue;
                                JsonArray tableNames = new JsonArray();
                                for (int j = 0; j < tables.size(); j++) {
                                    JsonObject table = tables.getJsonObject(j);
                                    if (table != null && table.containsKey("table")) {
                                        tableNames.add(table.getString("table"));
                                    }
                                }
                                arguments.put("table_names", tableNames);
                                System.out.println("[Orchestration] Extracted table_names from schema_matches.tables: " + tableNames.encode());
                            }
                        } else {
                            // Map nested key to simpler key for the tool
                            String targetKey = nestedKey.equals("entities") ? "tokens" : nestedKey;
                            arguments.put(targetKey, nestedValue);
                            System.out.println("[Orchestration] Added nested " + key + " as " + targetKey);
                        }
                    } else {
                        System.out.println("[Orchestration] WARNING: Could not find nested value: " + key);
                    }
                } else {
                    System.out.println("[Orchestration] WARNING: Top-level key not a JsonObject: " + topKey);
                }
            } else if (context.dataToPass.containsKey(key)) {
                Object value = context.dataToPass.get(key);
                
                // Validate and potentially convert the value type
                if (value instanceof String && ((String)value).startsWith("Error:")) {
                    // Skip error strings or convert to empty object for tools expecting objects
                    System.out.println("[Orchestration] WARNING: Skipping error value for " + key + ": " + value);
                    // Don't add error strings to arguments - let the tool handle missing data
                } else {
                    // Safe to add the value
                    arguments.put(key, value);
                    System.out.println("[Orchestration] Added from dataToPass: " + key + 
                                     " (type: " + (value != null ? value.getClass().getSimpleName() : "null") + ")");
                }
            } else {
                // Try to extract from step results
                boolean found = false;
                for (Map.Entry<String, JsonObject> entry : context.stepResults.entrySet()) {
                    JsonObject stepResult = entry.getValue();
                    if (stepResult != null && stepResult.containsKey(key)) {
                        Object value = stepResult.getValue(key);
                        arguments.put(key, value);
                        System.out.println("[Orchestration] Added from step " + entry.getKey() + ": " + key);
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    System.out.println("[Orchestration] WARNING: Could not find data for: " + key);
                    vertx.eventBus().publish("log",
                        "Missing data for " + key + " in step " + stepName + ",1,OrchestrationStrategy,Warning,Tool");
                }
            }
        }
        
        // Add any static arguments from step config
        JsonObject staticArgs = step.getJsonObject("arguments", new JsonObject());
        arguments.mergeIn(staticArgs);
        
        // Log the final arguments
        String argsLog = arguments.encodePrettily().replace("\n", " ").substring(0, Math.min(200, arguments.encodePrettily().length()));
        vertx.eventBus().publish("log",
            "Args for " + stepName + ": " + argsLog + ",3,OrchestrationStrategy,Arguments,Tool");
        
        System.out.println("[Orchestration] Final arguments for " + stepName + ": " + arguments.encode());
        
        return arguments;
    }
    
    /**
     * Check if optional step should be skipped
     */
    protected boolean shouldSkipStep(ExecutionContext context, JsonObject step) {
        // Override in subclasses for custom logic
        return false;
    }
    
    /**
     * Extract data from MCP-formatted response
     */
    private Object extractDataFromMcpResponse(JsonObject response) {
        // Log the raw response for debugging
        System.out.println("[Orchestration] Extracting from MCP response type: " + 
            (response.containsKey("content") ? "MCP format" : "Direct format"));
        
        // Check if it's an error response
        if (response.getBoolean("isError", false)) {
            String errorMsg = response.getString("error", "Unknown error");
            System.out.println("[Orchestration] MCP error response: " + errorMsg);
            return "Error: " + errorMsg;
        }
        
        // Check if response is in MCP format with content array
        if (response.containsKey("content") && response.getValue("content") instanceof JsonArray) {
            JsonArray content = response.getJsonArray("content");
            if (!content.isEmpty()) {
                JsonObject firstContent = content.getJsonObject(0);
                if (firstContent != null && firstContent.containsKey("text")) {
                    String text = firstContent.getString("text");
                    
                    // Try to parse as JSON
                    try {
                        JsonObject parsed = new JsonObject(text);
                        System.out.println("[Orchestration] Extracted JSON from MCP response");
                        return parsed;
                    } catch (Exception e) {
                        // Not JSON, check if it looks like a SQL query
                        if (text.trim().toUpperCase().startsWith("SELECT") || 
                            text.trim().toUpperCase().startsWith("WITH")) {
                            // It's SQL, wrap it
                            JsonObject sqlWrapper = new JsonObject();
                            sqlWrapper.put("sql", text.trim());
                            sqlWrapper.put("generated_sql", text.trim());
                            System.out.println("[Orchestration] Extracted SQL from MCP response");
                            return sqlWrapper;
                        }
                        // Return as plain text
                        System.out.println("[Orchestration] Extracted text from MCP response");
                        return text;
                    }
                }
            }
        }
        
        // Not MCP format, return as-is
        System.out.println("[Orchestration] Using direct response (not MCP format)");
        return response;
    }
    
    /**
     * Build final result from context
     */
    private JsonObject buildFinalResult(ExecutionContext context) {
        if (context.finalResult != null) {
            // Add metadata to the final result
            JsonObject result = context.finalResult.copy();
            result.put("strategy", strategyName)
                  .put("execution_time", System.currentTimeMillis() - context.startTime)
                  .put("steps_completed", context.currentStep);
            
            System.out.println("[Orchestration] Returning final result with formatted field: " + 
                result.containsKey("formatted"));
            return result;
        }
        
        // Aggregate results from all steps
        JsonObject result = new JsonObject()
            .put("strategy", strategyName)
            .put("success", true)
            .put("execution_time", System.currentTimeMillis() - context.startTime)
            .put("steps_completed", context.currentStep);
        
        // Add the last meaningful result
        if (!context.stepResults.isEmpty()) {
            // Find the last step with actual results
            for (int i = context.stepResults.size() - 1; i >= 0; i--) {
                JsonObject stepResult = context.stepResults.get("step_" + (i + 1));
                if (stepResult != null && !stepResult.isEmpty()) {
                    result.mergeIn(stepResult);
                    break;
                }
            }
        }
        
        return result;
    }
    
    /**
     * Start progress updates for streaming
     */
    private Long startProgressUpdates(ExecutionContext context) {
        long intervalMs = orchestrationStrategies.getJsonObject("configuration", new JsonObject())
            .getLong("progress_update_interval_ms", 5000L);
        
        return vertx.setPeriodic(intervalMs, id -> {
            if (context.streamId != null) {
                sendProgressUpdate(context);
            }
        });
    }
    
    /**
     * Send progress update via event bus
     */
    private void sendProgressUpdate(ExecutionContext context) {
        long elapsed = System.currentTimeMillis() - context.startTime;
        int totalSteps = strategyConfig.getJsonArray("steps", new JsonArray()).size();
        
        vertx.eventBus().publish("conversation." + context.streamId + ".progress",
            new JsonObject()
                .put("step", context.currentStep + "/" + totalSteps)
                .put("message", "Processing " + strategyName + "...")
                .put("elapsed", elapsed)
                .put("strategy", strategyName));
    }
    
    /**
     * Send step-specific progress
     */
    private void sendStepProgress(ExecutionContext context, JsonObject step) {
        if (context.streamId != null) {
            int totalSteps = strategyConfig.getJsonArray("steps", new JsonArray()).size();
            String stepName = step.getString("name", "Step " + context.currentStep);
            String description = step.getString("description", "Processing...");
            
            // Send detailed progress update
            JsonObject progressUpdate = new JsonObject()
                .put("step", stepName + " (" + context.currentStep + "/" + totalSteps + ")")
                .put("message", "[PROCESSING] " + description)
                .put("current", context.currentStep)
                .put("total", totalSteps)
                .put("elapsed", System.currentTimeMillis() - context.startTime);
            
            vertx.eventBus().publish("conversation." + context.streamId + ".progress", progressUpdate);
            
            System.out.println("[Orchestration] Progress update sent: " + stepName + " to address: conversation." + context.streamId + ".progress");
            System.out.println("[Orchestration]   Step: " + context.currentStep + "/" + totalSteps + ", Elapsed: " + (System.currentTimeMillis() - context.startTime) + "ms");
        }
    }
    
    /**
     * Execution context for strategy
     */
    protected static class ExecutionContext {
        String originalQuery;
        String sessionId;
        String streamId;
        long startTime;
        int currentStep;
        private Map<String, JsonObject> stepResults;
        Map<String, Object> dataToPass;
        JsonObject finalResult;
        
        ExecutionContext() {
            stepResults = new HashMap<>();
            dataToPass = new HashMap<>();
        }
        
        public Map<String, JsonObject> getStepResults() {
            return stepResults;
        }
    }
}
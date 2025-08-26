package AgentsMCPHost.mcp.core.orchestration;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Future;
import io.vertx.core.CompositeFuture;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.core.eventbus.Message;
import io.vertx.core.file.FileSystem;
import AgentsMCPHost.streaming.StreamingEventPublisher;
import AgentsMCPHost.llm.LlmAPIService;

import java.util.UUID;
import java.util.List;
import java.util.ArrayList;

/**
 * Base class for orchestration strategies - acts as the Intent Engine.
 * 
 * This is the intelligent orchestrator that understands intent, manages context,
 * and ensures each tool gets exactly what it needs. Tools communicate through
 * this Intent Engine, not directly with each other.
 * 
 * Key principles:
 * - The orchestrator IS the intelligence (Intent Engine)
 * - Context is a first-class argument to tools that need it
 * - No complex mappings or side channels - just smart orchestration
 * - Tools remain simple functions with clear contracts
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
                
                // Handle both JsonArray steps and "dynamic" steps
                Object stepsValue = strategyConfig.getValue("steps");
                if (stepsValue instanceof JsonArray) {
                    System.out.println("[Orchestration] Steps: " + ((JsonArray) stepsValue).size());
                } else if ("dynamic".equals(stepsValue)) {
                    System.out.println("[Orchestration] Steps: dynamic");
                } else {
                    System.out.println("[Orchestration] Steps: unknown format");
                }
                
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
        
        // Create orchestration context (Intent Engine's brain)
        OrchestrationContext context = new OrchestrationContext(query, sessionId, streamId);
        
        // Get strategy steps
        JsonArray steps;
        Object stepsValue = strategyConfig.getValue("steps");
        if (stepsValue instanceof String && "dynamic".equals(stepsValue)) {
            // Handle dynamic steps for multi_tool_sequential
            // Extract tools from request
            JsonArray tools = request.getJsonArray("tools", new JsonArray());
            if (tools.isEmpty()) {
                msg.fail(400, "No tools provided for dynamic strategy");
                return;
            }
            // Convert tools to steps format
            steps = new JsonArray();
            for (int i = 0; i < tools.size(); i++) {
                JsonObject tool = tools.getJsonObject(i);
                steps.add(new JsonObject()
                    .put("step", i + 1)
                    .put("name", "Execute " + tool.getString("tool"))
                    .put("tool", tool.getString("tool"))
                    .put("arguments", tool.getJsonObject("arguments", new JsonObject())));
            }
        } else {
            steps = strategyConfig.getJsonArray("steps", new JsonArray());
        }
        
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
                    String errorMessage = err.getMessage();
                    System.err.println("[Orchestration] " + strategyName + " failed: " + errorMessage);
                    if (progressTimerId[0] != null) {
                        vertx.cancelTimer(progressTimerId[0]);
                    }
                    // Pass through the error message without adding any prefix
                    msg.fail(500, errorMessage);
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
     * Execute strategy steps recursively with intelligent retry
     */
    private Future<JsonObject> executeStrategy(OrchestrationContext context, JsonArray steps, int stepIndex) {
        if (stepIndex >= steps.size()) {
            // All steps complete - return final result
            return Future.succeededFuture(buildFinalResult(context));
        }
        
        JsonObject step = steps.getJsonObject(stepIndex);
        context.setCurrentStep(stepIndex + 1);
        
        // Send progress update
        sendStepProgress(context, step);
        
        // Check if step is optional and should be skipped
        if (step.getBoolean("optional", false) && shouldSkipStep(context, step)) {
            System.out.println("[Orchestration] Skipping optional step " + context.getCurrentStep());
            return executeStrategy(context, steps, stepIndex + 1);
        }
        
        // Build tool arguments from context
        JsonObject toolArguments = buildToolArguments(context, step);
        
        // Call the tool
        String toolName = step.getString("tool");
        System.out.println("[Orchestration] Step " + context.getCurrentStep() + ": Calling " + toolName);
        
        long toolStartTime = System.currentTimeMillis();
        return callTool(toolName, toolArguments, context)
            .compose(result -> {
                long toolDuration = System.currentTimeMillis() - toolStartTime;
                
                // Log raw result for debugging
                System.out.println("[Orchestration] Raw result from " + toolName + ": " + 
                    result.encode().substring(0, Math.min(200, result.encode().length())));
                
                // Extract actual data from MCP response format
                Object extractedData = extractDataFromMcpResponse(result);
                JsonObject cleanResult;
                
                if (extractedData instanceof JsonObject) {
                    cleanResult = (JsonObject) extractedData;
                } else if (extractedData instanceof String) {
                    // Wrap string results in a proper structure
                    cleanResult = new JsonObject().put("result", extractedData);
                } else if (extractedData instanceof JsonArray) {
                    // Wrap array results properly
                    cleanResult = new JsonObject().put("results", extractedData);
                } else if (extractedData == null) {
                    // If extraction returned null, check if original has error
                    if (result.containsKey("error")) {
                        cleanResult = result; // Keep error info
                    } else {
                        // Empty successful result
                        cleanResult = new JsonObject().put("success", true);
                    }
                } else {
                    // Fallback to original for unexpected types
                    cleanResult = result;
                }
                
                // Log extracted result for debugging
                System.out.println("[Orchestration] Extracted result from " + toolName + ": " + 
                    cleanResult.encode().substring(0, Math.min(200, cleanResult.encode().length())));
                
                // Record tool execution in context with CLEAN data
                context.recordExecution(toolName, toolArguments, cleanResult, toolDuration);
                
                // Check for critical errors FIRST
                if (cleanResult.containsKey("severity") && "CRITICAL".equals(cleanResult.getString("severity"))) {
                    String errorMsg = cleanResult.getString("error", "Critical error occurred");
                    String stepName = step.getString("name", "step_" + context.getCurrentStep());
                    System.err.println("[Orchestration] CRITICAL ERROR in step " + context.getCurrentStep() + 
                        " (" + toolName + "): " + errorMsg);
                    
                    // Publish critical error event
                    vertx.eventBus().publish("critical.error", new JsonObject()
                        .put("eventType", "critical.error")
                        .put("component", "OrchestrationStrategy")
                        .put("orchestration", strategyName)
                        .put("step", stepName)
                        .put("error", errorMsg)
                        .put("severity", "CRITICAL")
                        .put("timestamp", java.time.Instant.now().toString())
                        .put("streamId", context.getStreamId()));
                    
                    // Always stop orchestration for critical errors
                    return Future.failedFuture("CRITICAL: " + errorMsg);
                }
                
                // Check for regular errors
                boolean isCriticalStep = step.getBoolean("critical", true); // Default to critical
                if (cleanResult.containsKey("error") || cleanResult.getBoolean("isError", false)) {
                    String errorMsg = cleanResult.getString("error", "Unknown error");
                    System.err.println("[Orchestration] Step " + context.getCurrentStep() + " (" + toolName + ") failed with error: " + errorMsg);
                    
                    // Check if this is a database connection error (treat as critical)
                    if (errorMsg.contains("Database connection") || 
                        errorMsg.contains("UCP-0") ||
                        errorMsg.contains("connection pool")) {
                        String stepName = step.getString("name", "step_" + context.getCurrentStep());
                        System.err.println("[Orchestration] Database connection error detected - treating as critical");
                        
                        // Publish critical error
                        vertx.eventBus().publish("critical.error", new JsonObject()
                            .put("eventType", "critical.error")
                            .put("component", "OrchestrationStrategy")
                            .put("orchestration", strategyName)
                            .put("step", stepName)
                            .put("error", errorMsg)
                            .put("severity", "CRITICAL")
                            .put("timestamp", java.time.Instant.now().toString())
                            .put("streamId", context.getStreamId()));
                        
                        return Future.failedFuture("CRITICAL: " + errorMsg);
                    }
                    
                    // NEW: Intelligent error handling for semantic understanding
                    // Check if we have error feedback and haven't exceeded retry limit
                    JsonObject errorFeedback = cleanResult.getJsonObject("error_feedback");
                    int stepRetries = context.getStepRetries(stepIndex);
                    
                    if (errorFeedback != null && stepRetries < 3) {
                        // Perform semantic error analysis
                        System.out.println("[Orchestration] Analyzing error semantically for step " + context.getCurrentStep());
                        
                        // Publish user-friendly progress
                        StreamingEventPublisher publisher = new StreamingEventPublisher(vertx, context.getStreamId());
                        publisher.publishProgress("semantic_analysis", 
                            "I encountered an issue. Let me analyze what went wrong and try a different approach...",
                            new JsonObject()
                                .put("phase", "error_analysis")
                                .put("attempt", stepRetries + 1)
                                .put("error_type", errorFeedback.getString("error_type", "unknown")));
                        
                        // Perform intelligent semantic analysis
                        return analyzeErrorSemantically(context, errorMsg, errorFeedback, cleanResult)
                            .compose(analysis -> {
                                String recommendedAction = analysis.getString("recommended_action");
                                
                                switch (recommendedAction) {
                                    case "RETRY_WITH_UNDERSTANDING":
                                        return retryWithDeeperUnderstanding(context, steps, stepIndex, analysis);
                                        
                                    case "EXPLORE_SCHEMA":
                                        return exploreSchemaIntelligently(context, analysis)
                                            .compose(schemaKnowledge -> {
                                                context.updateSchemaKnowledge(schemaKnowledge);
                                                return executeStrategy(context, steps, stepIndex);
                                            });
                                        
                                    case "BACKTRACK":
                                        int targetStep = analysis.getInteger("target_step", 0);
                                        return executeStrategy(context, steps, targetStep);
                                        
                                    default:
                                        // Continue with original error handling
                                        break;
                                }
                                
                                return Future.succeededFuture(cleanResult);
                            });
                    }
                    
                    if (isCriticalStep) {
                        // Critical step failed - stop orchestration
                        System.err.println("[Orchestration] Critical step failed - stopping orchestration");
                        
                        // Return the original error without any wrapping
                        // The error already contains enough context from the tool
                        return Future.failedFuture(errorMsg);
                    } else {
                        System.out.println("[Orchestration] Non-critical step failed - continuing");
                    }
                }
                
                // In the Intent Engine model, the context automatically manages data flow
                // The context.recordExecution() already stored the tool result
                // Tools that need previous results can access them from context
                
                // Check if this is the final step
                if (step.getBoolean("final", false)) {
                    // In Intent Engine model, context already has all results
                    // buildFinalResult will extract what's needed
                    System.out.println("[Intent Engine] Final step completed - building result from context");
                    return Future.succeededFuture(buildFinalResult(context));
                }
                
                // Continue to next step
                return executeStrategy(context, steps, stepIndex + 1);
            })
            .recover(err -> {
                String errorMessage = err.getMessage();
                System.err.println("[Orchestration] Step " + context.getCurrentStep() + " failed: " + errorMessage);
                
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
                    
                    // Don't add any wrapping - just pass through the original error
                    return Future.failedFuture(errorMessage);
                }
            });
    }
    
    /**
     * Check if a tool needs context based on its name
     * This is a simple implementation - could be enhanced to read from tool metadata
     */
    private boolean toolNeedsContext(String toolName) {
        // Tools that need the full orchestration context
        // Generally, "smart" or "intelligent" tools need context
        // Using JsonArray for thread-safety in Vert.x
        JsonArray contextAwareTools = new JsonArray()
            .add("smart_schema_match")
            .add("match_schema")  // Both variants
            .add("generate_sql")
            .add("analyze_query")  // Both variants
            .add("deep_analyze_query")
            .add("format_results")
            .add("optimize_sql")  // Both variants
            .add("optimize_sql_smart")
            .add("discover_column_semantics")
            .add("map_business_terms")
            .add("infer_relationships")
            .add("execute_query")  // Needs generated SQL from context
            .add("validate_schema_sql")  // Needs schema context
            .add("discover_sample_data")  // May need schema matches from context
            .add("parse_oracle_error");  // Error context is important
        
        return contextAwareTools.contains(toolName);
    }
    
    /**
     * Call a tool via event bus - Intent Engine provides context when needed
     */
    private Future<JsonObject> callTool(String toolName, JsonObject arguments, OrchestrationContext context) {
        Promise<JsonObject> promise = Promise.promise();
        String streamId = context.getStreamId();
        
        // Intent Engine logic: Add context if tool needs it
        JsonObject finalArguments = arguments.copy();
        if (toolNeedsContext(toolName)) {
            // Add the full context to arguments for tools that need it
            finalArguments.put("context", context.toJson());
            System.out.println("[Intent Engine] Adding context for " + toolName);
        }
        
        // Log tool call attempt
        vertx.eventBus().publish("log",
            "Intent Engine routing " + toolName + ",3,OrchestrationStrategy,Routing,Tool");
        
        System.out.println("[Intent Engine] Routing tool: " + toolName + " through MCP host");
        
        // Publish streaming event for tool execution start
        if (streamId != null) {
            StreamingEventPublisher publisher = new StreamingEventPublisher(vertx, streamId);
            publisher.publishProgress("tool_execution", "Executing tool: " + toolName,
                new JsonObject()
                    .put("phase", "tool_execution")
                    .put("tool", toolName)
                    .put("status", "routing")
                    .put("has_context", toolNeedsContext(toolName)));
        }
        
        // Build tool request
        JsonObject toolRequest = new JsonObject()
            .put("tool", toolName)
            .put("arguments", finalArguments);
        
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
     * Build tool arguments - Intent Engine provides context when needed
     * 
     * In the Intent Engine model, tools that need context get it automatically.
     * This method now only handles explicit arguments from the orchestration.
     */
    private JsonObject buildToolArguments(OrchestrationContext context, JsonObject step) {
        // In the Intent Engine model, argument building is simple:
        // 1. Take any explicit arguments from the orchestration config
        // 2. The Intent Engine (callTool) will add context if the tool needs it
        
        JsonObject arguments = new JsonObject();
        String stepName = step.getString("name", "Unknown");
        String toolName = step.getString("tool", "");
        
        // Add any explicit arguments from step configuration
        JsonObject staticArgs = step.getJsonObject("arguments", new JsonObject());
        arguments.mergeIn(staticArgs);
        
        // Special handling for tools that need table names
        if (toolName.equals("discover_sample_data") && !arguments.containsKey("table_names")) {
            // Try multiple ways to get table names from context
            JsonArray tableNames = new JsonArray();
            
            // 1. Try schema knowledge from smart_schema_match result
            JsonObject schemaKnowledge = context.getSchemaKnowledge();
            if (schemaKnowledge != null) {
                // Check if it has the standard schema match format
                if (schemaKnowledge.containsKey("tableMatches")) {
                    JsonArray tableMatches = schemaKnowledge.getJsonArray("tableMatches", new JsonArray());
                    for (int i = 0; i < Math.min(3, tableMatches.size()); i++) {
                        JsonObject match = tableMatches.getJsonObject(i);
                        if (match != null) {
                            String tableName = match.getString("tableName");
                            if (tableName != null) {
                                tableNames.add(tableName);
                            }
                        }
                    }
                }
                // Also check if we stored available_tables from exploration
                else if (schemaKnowledge.containsKey("available_tables")) {
                    tableNames = schemaKnowledge.getJsonArray("available_tables");
                }
            }
            
            // If we found tables, use them
            if (tableNames.size() > 0) {
                arguments.put("table_names", tableNames);
            }
            
            // If still no tables, try common tables
            if (!arguments.containsKey("table_names")) {
                arguments.put("table_names", new JsonArray()
                    .add("ORDERS")
                    .add("CUSTOMERS")
                    .add("ORDER_STATUS_ENUM"));
            }
        }
        
        // Log what we're sending
        System.out.println("[Intent Engine] Arguments for " + stepName + " (" + toolName + "): " + arguments.encode());
        
        return arguments;
    }
    
    /**
     * Check if optional step should be skipped
     */
    protected boolean shouldSkipStep(OrchestrationContext context, JsonObject step) {
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
                        // Improved SQL detection - handle comments and various SQL statements
                        String trimmedUpper = text.trim().toUpperCase();
                        // Remove leading comments (limit iterations to prevent infinite loops)
                        int maxCommentIterations = 10;
                        int iterations = 0;
                        while ((trimmedUpper.startsWith("--") || trimmedUpper.startsWith("/*")) && 
                               iterations < maxCommentIterations) {
                            iterations++;
                            if (trimmedUpper.startsWith("--")) {
                                int newlineIdx = trimmedUpper.indexOf('\n');
                                if (newlineIdx > 0) {
                                    trimmedUpper = trimmedUpper.substring(newlineIdx + 1).trim();
                                } else {
                                    break; // Single line comment only
                                }
                            } else if (trimmedUpper.startsWith("/*")) {
                                int endIdx = trimmedUpper.indexOf("*/");
                                if (endIdx > 0) {
                                    trimmedUpper = trimmedUpper.substring(endIdx + 2).trim();
                                } else {
                                    break; // Unclosed comment
                                }
                            }
                        }
                        
                        if (trimmedUpper.startsWith("SELECT") || 
                            trimmedUpper.startsWith("WITH") ||
                            trimmedUpper.startsWith("INSERT") ||
                            trimmedUpper.startsWith("UPDATE") ||
                            trimmedUpper.startsWith("DELETE") ||
                            trimmedUpper.startsWith("MERGE")) {
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
    private JsonObject buildFinalResult(OrchestrationContext context) {
        // Get tool results from context
        JsonObject toolResults = context.getToolResults();
        
        // Build result with metadata
        JsonObject result = new JsonObject()
            .put("strategy", strategyName)
            .put("success", true)
            .put("execution_time", System.currentTimeMillis() - context.getStartTime())
            .put("steps_completed", context.getCurrentStep());
        
        // Find the last meaningful result (typically from format_results)
        JsonArray history = context.getExecutionHistory();
        if (history.size() > 0) {
            // Look for format_results or the last successful tool
            for (int i = history.size() - 1; i >= 0; i--) {
                JsonObject execution = history.getJsonObject(i);
                JsonObject toolResult = execution.getJsonObject("result");
                
                // Check if this is a meaningful result
                if (toolResult != null && !toolResult.containsKey("error") && !toolResult.getBoolean("isError", false)) {
                    // If it's format_results, use its output
                    String toolName = execution.getString("toolName");
                    if ("format_results".equals(toolName) && toolResult.containsKey("formatted")) {
                        result.put("formatted", toolResult.getString("formatted"));
                        result.put("confidence", toolResult.getDouble("confidence", 0.8));
                    }
                    // For other tools, check if they have results
                    else if (toolResult.containsKey("results") || toolResult.containsKey("sql") || 
                             toolResult.containsKey("data") || toolResult.containsKey("tables")) {
                        result.mergeIn(toolResult);
                    }
                    
                    // Stop at the first meaningful result when going backwards
                    if (result.containsKey("formatted") || result.containsKey("results")) {
                        break;
                    }
                }
            }
        }
        
        System.out.println("[Intent Engine] Final result has formatted field: " + result.containsKey("formatted"));
        
        return result;
    }
    
    /**
     * Analyze error semantically using LLM
     */
    private Future<JsonObject> analyzeErrorSemantically(OrchestrationContext context, 
                                                      String errorMsg, 
                                                      JsonObject errorFeedback,
                                                      JsonObject toolResult) {
        // Build comprehensive prompt for semantic analysis
        JsonObject analysisRequest = new JsonObject()
            .put("original_query", context.getOriginalQuery())
            .put("error_message", errorMsg)
            .put("error_feedback", errorFeedback)
            .put("failed_sql", toolResult.getString("sql_executed", ""))
            .put("deep_analysis", context.getDeepAnalysis())
            .put("schema_knowledge", context.getSchemaKnowledge())
            .put("error_history", context.getErrors())
            .put("accumulated_knowledge", context.getSemanticUnderstandings());
        
        String prompt = buildSemanticAnalysisPrompt(analysisRequest);
        
        // Use LLM to analyze the error semantically
        return LlmAPIService.getInstance().chatCompletion(
            new JsonArray().add(new JsonObject()
                .put("role", "system")
                .put("content", "You are helping debug database queries by understanding user intent, not just fixing syntax.")
            ).add(new JsonObject()
                .put("role", "user")
                .put("content", prompt))
        ).map(response -> {
            try {
                JsonArray choices = response.getJsonArray("choices");
                if (choices != null && !choices.isEmpty()) {
                    String content = choices.getJsonObject(0)
                        .getJsonObject("message")
                        .getString("content");
                    
                    // Parse the semantic analysis (strip markdown if present)
                    String jsonContent = content;
                    if (content.contains("```json")) {
                        int start = content.indexOf("```json") + 7;
                        int end = content.lastIndexOf("```");
                        if (end > start) {
                            jsonContent = content.substring(start, end).trim();
                        }
                    } else if (content.contains("```")) {
                        // Handle case where LLM uses just ``` without json
                        int start = content.indexOf("```") + 3;
                        int end = content.lastIndexOf("```");
                        if (end > start) {
                            jsonContent = content.substring(start, end).trim();
                        }
                    }
                    return new JsonObject(jsonContent);
                }
            } catch (Exception e) {
                System.err.println("[Orchestration] Failed to parse semantic analysis: " + e.getMessage());
            }
            
            // Default fallback
            return new JsonObject()
                .put("recommended_action", "CONTINUE")
                .put("reason", "Unable to perform semantic analysis");
        });
    }
    
    /**
     * Build prompt for semantic error analysis
     */
    private String buildSemanticAnalysisPrompt(JsonObject request) {
        return String.format(
            "You are helping debug a database query. Think deeply about user intent, not just syntax.\n\n" +
            "User's Original Request: \"%s\"\n" +
            "What We Tried: %s\n" +
            "Error Received: %s\n" +
            "Error Details: %s\n\n" +
            "Current Understanding:\n" +
            "- User Intent: %s\n" +
            "- Schema Knowledge: %s\n" +
            "- Past Attempts: %s\n\n" +
            "DO NOT suggest simple fixes like dropping columns or string replacements.\n\n" +
            "Instead, think:\n" +
            "1. What is the user REALLY asking for?\n" +
            "2. What concept does \"high priority\" represent in their mind?\n" +
            "3. How might this business concept be modeled in the database?\n" +
            "4. What additional discovery would help us understand?\n\n" +
            "Provide a response that seeks to understand, not just fix:\n" +
            "{\n" +
            "  \"semantic_analysis\": {\n" +
            "    \"user_intent\": \"What they really want\",\n" +
            "    \"missing_concept\": \"What business concept we need to map\",\n" +
            "    \"possible_interpretations\": [\"list of ways this could be modeled\"]\n" +
            "  },\n" +
            "  \"recommended_action\": \"EXPLORE_SCHEMA|RETRY_WITH_UNDERSTANDING|BACKTRACK\",\n" +
            "  \"exploration_strategy\": {\n" +
            "    \"what_to_look_for\": \"Specific patterns or relationships\",\n" +
            "    \"tools_to_use\": [\"list_tables\", \"describe_table\", \"discover_enums\", \"discover_column_semantics\"],\n" +
            "    \"questions_to_answer\": [\"what would resolve the ambiguity\"]\n" +
            "  },\n" +
            "  \"target_step\": 0\n" +
            "}",
            request.getString("original_query"),
            request.getString("failed_sql", "N/A"),
            request.getString("error_message"),
            request.getJsonObject("error_feedback", new JsonObject()).encodePrettily(),
            request.getJsonObject("deep_analysis", new JsonObject()).encodePrettily(),
            request.getJsonObject("schema_knowledge", new JsonObject()).encodePrettily(),
            request.getJsonArray("error_history", new JsonArray()).encodePrettily()
        );
    }
    
    /**
     * Retry with deeper understanding from semantic analysis
     */
    private Future<JsonObject> retryWithDeeperUnderstanding(OrchestrationContext context,
                                                          JsonArray steps,
                                                          int stepIndex,
                                                          JsonObject analysis) {
        // Update context with new understanding
        JsonObject semanticAnalysis = analysis.getJsonObject("semantic_analysis");
        if (semanticAnalysis != null) {
            String missingConcept = semanticAnalysis.getString("missing_concept");
            context.recordSemanticLearning(missingConcept, semanticAnalysis);
            
            // Publish progress update
            StreamingEventPublisher publisher = new StreamingEventPublisher(vertx, context.getStreamId());
            publisher.publishProgress("understanding_gained",
                "I understand better now. " + semanticAnalysis.getString("user_intent") + 
                ". Let me try again with this understanding.",
                new JsonObject()
                    .put("phase", "retry_with_understanding")
                    .put("concept", missingConcept)
                    .put("new_understanding", semanticAnalysis));
        }
        
        // Increment retry count
        context.incrementStepRetry(stepIndex);
        
        // Retry the same step with new understanding
        return executeStrategy(context, steps, stepIndex);
    }
    
    /**
     * Explore schema intelligently based on error analysis
     */
    private Future<JsonObject> exploreSchemaIntelligently(OrchestrationContext context, 
                                                        JsonObject analysis) {
        JsonObject explorationStrategy = analysis.getJsonObject("exploration_strategy");
        String missingConcept = analysis.getJsonObject("semantic_analysis")
            .getString("missing_concept");
        
        StreamingEventPublisher publisher = new StreamingEventPublisher(vertx, context.getStreamId());
        publisher.publishProgress("schema_exploration",
            "I need to understand how '" + missingConcept + 
            "' is represented in this database. Let me explore the schema more deeply.",
            new JsonObject()
                .put("phase", "intelligent_exploration")
                .put("exploring_concept", missingConcept));
        
        // Use multiple tools to understand the concept
        JsonArray toolsToUse = explorationStrategy.getJsonArray("tools_to_use", 
            new JsonArray().add("list_tables").add("discover_enums").add("discover_column_semantics"));
        
        List<Future> futures = new ArrayList<>();
        
        // First, get table list if we need it
        Future<JsonObject> tableListFuture = callTool("list_tables", new JsonObject(), context);
        
        return tableListFuture.compose(tableResult -> {
            // Extract table names for tools that need them
            JsonArray tables = new JsonArray();
            if (tableResult.containsKey("tables")) {
                JsonArray allTables = tableResult.getJsonArray("tables");
                // Focus on relevant tables based on concept or common patterns
                for (int i = 0; i < allTables.size(); i++) {
                    JsonObject table = allTables.getJsonObject(i);
                    String tableName = table.getString("name");
                    if (tableName != null) {
                        // Always include tables that might be relevant
                        String upperName = tableName.toUpperCase();
                        if (upperName.contains("ORDER") || 
                            upperName.contains("CUSTOMER") || 
                            upperName.contains("PRODUCT") ||
                            upperName.contains("ENUM") ||
                            upperName.contains("STATUS") ||
                            upperName.contains("TYPE") ||
                            // Also include if concept is mentioned in table name
                            (missingConcept != null && upperName.contains(missingConcept.toUpperCase()))) {
                            tables.add(tableName);
                        }
                    }
                }
            }
            
            // Store tables in context for other tools
            context.updateSchemaKnowledge(new JsonObject().put("available_tables", tables));
            
            // Now call exploration tools with proper arguments
            for (int i = 0; i < toolsToUse.size(); i++) {
                String tool = toolsToUse.getString(i);
                JsonObject args = new JsonObject();
                
                // Add appropriate arguments based on tool
                if (tool.equals("discover_sample_data") && tables.size() > 0) {
                    args.put("table_names", tables);
                    args.put("limit", 5);
                } else if (tool.equals("discover_column_semantics") && tables.size() > 0) {
                    args.put("table_names", tables);
                } else {
                    args.put("concept", missingConcept);
                }
                
                futures.add(callTool(tool, args, context));
            }
            
            return CompositeFuture.all(futures);
        }).map(compositeFuture -> {
            JsonObject schemaKnowledge = new JsonObject();
            
            // Synthesize all findings
            for (int i = 0; i < compositeFuture.size(); i++) {
                JsonObject result = compositeFuture.resultAt(i);
                schemaKnowledge.mergeIn(result);
            }
            
            // Add synthesis
            schemaKnowledge.put("synthesis", synthesizeFindings(schemaKnowledge, missingConcept));
            
            publisher.publishProgress("exploration_complete",
                "I've discovered how the database models this concept. " +
                schemaKnowledge.getJsonObject("synthesis").getString("summary"),
                new JsonObject()
                    .put("phase", "exploration_complete")
                    .put("findings", schemaKnowledge));
            
            return schemaKnowledge;
        });
    }
    
    /**
     * Synthesize exploration findings
     */
    private JsonObject synthesizeFindings(JsonObject findings, String concept) {
        // For now, return a structured summary
        // In a full implementation, this would use LLM to synthesize
        return new JsonObject()
            .put("concept", concept)
            .put("summary", "Discovered mappings and relationships for " + concept)
            .put("confidence", 0.8);
    }
    
    /**
     * Start progress updates for streaming
     */
    private Long startProgressUpdates(OrchestrationContext context) {
        long intervalMs = orchestrationStrategies.getJsonObject("configuration", new JsonObject())
            .getLong("progress_update_interval_ms", 5000L);
        
        return vertx.setPeriodic(intervalMs, id -> {
            if (context.getStreamId() != null) {
                sendProgressUpdate(context);
            }
        });
    }
    
    /**
     * Send progress update via event bus
     */
    private void sendProgressUpdate(OrchestrationContext context) {
        long elapsed = System.currentTimeMillis() - context.getStartTime();
        int totalSteps = strategyConfig.getJsonArray("steps", new JsonArray()).size();
        
        vertx.eventBus().publish("conversation." + context.getStreamId() + ".progress",
            new JsonObject()
                .put("step", context.getCurrentStep() + "/" + totalSteps)
                .put("message", "Processing " + strategyName + "...")
                .put("elapsed", elapsed)
                .put("strategy", strategyName));
    }
    
    /**
     * Send step-specific progress
     */
    private void sendStepProgress(OrchestrationContext context, JsonObject step) {
        if (context.getStreamId() != null) {
            int totalSteps = strategyConfig.getJsonArray("steps", new JsonArray()).size();
            String stepName = step.getString("name", "Step " + context.getCurrentStep());
            String description = step.getString("description", "Processing...");
            
            // Send detailed progress update
            JsonObject progressUpdate = new JsonObject()
                .put("step", stepName + " (" + context.getCurrentStep() + "/" + totalSteps + ")")
                .put("message", "[PROCESSING] " + description)
                .put("current", context.getCurrentStep())
                .put("total", totalSteps)
                .put("elapsed", System.currentTimeMillis() - context.getStartTime());
            
            vertx.eventBus().publish("conversation." + context.getStreamId() + ".progress", progressUpdate);
            
            System.out.println("[Orchestration] Progress update sent: " + stepName + " to address: conversation." + context.getStreamId() + ".progress");
            System.out.println("[Orchestration]   Step: " + context.getCurrentStep() + "/" + totalSteps + ", Elapsed: " + (System.currentTimeMillis() - context.getStartTime()) + "ms");
        }
    }
    
    // OrchestrationContext replaced by OrchestrationContext for Intent Engine
    // See OrchestrationContext.java for the enhanced context implementation
}
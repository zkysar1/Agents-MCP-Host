package agents.director.hosts.milestones;

import agents.director.hosts.base.MilestoneContext;
import agents.director.hosts.base.MilestoneManager;
import agents.director.mcp.clients.QueryIntentEvaluationClient;
import agents.director.mcp.clients.IntentAnalysisClient;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import java.util.*;

/**
 * Milestone 1: Intent Extraction
 * 
 * Extracts and understands the user's intent from their query.
 * Uses IntentAnalysisServer and QueryIntentEvaluationServer by deploying MCP clients directly.
 * 
 * Output shared with user: Clear statement of what we understand the user wants
 */
public class IntentMilestone extends MilestoneManager {
    
    private static final String EVALUATION_CLIENT = "evaluation";
    private static final String ANALYSIS_CLIENT = "analysis";
    private final Map<String, String> deploymentIds = new HashMap<>();
    
    public IntentMilestone(Vertx vertx, String baseUrl) {
        super(vertx, baseUrl, 1, "IntentMilestone", 
              "Extract user intent and share understanding with user");
    }
    
    @Override
    public Future<Void> initialize() {
        Promise<Void> promise = Promise.promise();
        
        // Deploy the MCP clients directly
        List<Future> deploymentFutures = new ArrayList<>();
        
        QueryIntentEvaluationClient evaluationClient = new QueryIntentEvaluationClient(baseUrl);
        IntentAnalysisClient analysisClient = new IntentAnalysisClient(baseUrl);
        
        deploymentFutures.add(deployClient(EVALUATION_CLIENT, evaluationClient));
        deploymentFutures.add(deployClient(ANALYSIS_CLIENT, analysisClient));
        
        CompositeFuture.all(deploymentFutures)
            .onSuccess(v -> {
                log("Intent milestone initialized successfully", 2);
                promise.complete();
            })
            .onFailure(err -> {
                log("Failed to initialize intent milestone: " + err.getMessage(), 0);
                promise.fail(err);
            });
        
        return promise.future();
    }
    
    @Override
    public Future<MilestoneContext> execute(MilestoneContext context) {
        Promise<MilestoneContext> promise = Promise.promise();
        
        log("Starting intent extraction for query: " + context.getQuery(), 3);
        
        // Publish progress event at start
        if (context.isStreaming() && context.getSessionId() != null) {
            publishProgressEvent(context.getConversationId(), 
                "Step 1: Intent Analysis",
                "Analyzing your question...",
                new JsonObject()
                    .put("phase", "intent_analysis")
                    .put("query", context.getQuery()));
        }
        
        // Build context for intent analysis
        JsonObject analysisContext = new JsonObject()
            .put("backstory", context.getBackstory())
            .put("guidance", context.getGuidance())
            .put("history", new JsonArray());
        
        // Perform full intent analysis using the clients directly
        fullIntentAnalysis(context, context.getQuery(), analysisContext)
            .onSuccess(result -> {
                // Extract key intent information
                JsonObject extractedIntent = result.getJsonObject("extractedIntent");
                JsonObject intentEvaluation = result.getJsonObject("intentEvaluation");
                JsonObject outputFormat = result.getJsonObject("outputFormat");
                
                // Get the primary intent from the result within extractedIntent
                // Note: IntentAnalysisServer returns {result: {primary_intent: "...", ...}}
                JsonObject intentResult = extractedIntent != null ? 
                    extractedIntent.getJsonObject("result", new JsonObject()) : new JsonObject();
                String primaryIntent = intentResult.getString("primary_intent", "");
                // Let it fail if intentEvaluation is null - indicates pipeline issue
                String intentType = intentEvaluation.getString("intent_type", "query");
                
                // Build a clear intent statement for the user
                String intentStatement = buildIntentStatement(primaryIntent, intentType, 
                                                              extractedIntent, intentEvaluation, context.getQuery());
                
                // Update context with intent information
                context.setIntent(intentStatement);
                context.setIntentType(intentType);
                context.setIntentDetails(new JsonObject()
                    .put("primary_intent", primaryIntent)
                    .put("secondary_intents", extractedIntent.getJsonArray("secondary_intents"))
                    .put("intent_evaluation", intentEvaluation)
                    .put("output_format", outputFormat)
                    .put("confidence", extractedIntent.getDouble("confidence", 0.8)));
                
                // Mark milestone as complete
                context.completeMilestone(1);
                
                // Publish streaming event if applicable
                if (context.isStreaming() && context.getSessionId() != null) {
                    JsonObject shareableResult = getShareableResult(context);
                    shareableResult.put("message", "✅ Intent understood: " + intentStatement);
                    publishStreamingEvent(context.getConversationId(), "milestone.intent_complete", shareableResult);
                }
                
                log("Intent extraction complete: " + intentStatement, 2);
                promise.complete(context);
            })
            .onFailure(err -> {
                log("Intent extraction failed: " + err.getMessage(), 0);
                
                // Fallback: Use simple intent extraction
                String fallbackIntent = extractSimpleIntent(context.getQuery());
                context.setIntent(fallbackIntent);
                context.setIntentType("unknown");
                context.setIntentDetails(new JsonObject()
                    .put("primary_intent", fallbackIntent)
                    .put("fallback", true)
                    .put("error", err.getMessage()));
                
                context.completeMilestone(1);
                promise.complete(context);
            });
        
        return promise.future();
    }
    
    /**
     * Perform full intent analysis on a query (replaces manager method)
     */
    private Future<JsonObject> fullIntentAnalysis(MilestoneContext milestoneContext, String query, JsonObject context) {
        // Extract primary and secondary intents first
        Future<JsonObject> intentExtractFuture = callToolWithEvents(milestoneContext, ANALYSIS_CLIENT, 
            "intent_analysis__extract_intent",
            new JsonObject()
                .put("query", query)
                .put("conversation_history", context != null ? 
                    context.getJsonArray("history") : new JsonArray()),
            "Extracting user intent from query");
        
        // Chain the results - use extracted intent for output format determination
        Future<JsonObject> chainedAnalysis = intentExtractFuture.compose(extractedIntent -> {
            // Evaluate query intent in parallel with output format
            Future<JsonObject> evaluationFuture = callToolWithEvents(milestoneContext, EVALUATION_CLIENT, 
                "evaluate_query_intent",
                new JsonObject()
                    .put("query", query)
                    .put("conversationHistory", context != null ? 
                        context.getJsonArray("history") : new JsonArray()),
                "Evaluating query intent");
            
            // Determine output format using the extracted intent
            Future<JsonObject> outputFormatFuture = callToolWithEvents(milestoneContext, ANALYSIS_CLIENT, 
                "intent_analysis__determine_output_format",
                new JsonObject()
                    .put("intent", extractedIntent.getJsonObject("result"))
                    .put("query", query)
                    .put("user_preferences", context != null ? 
                        context.getJsonObject("userProfile") : new JsonObject()),
                "Determining output format");
            
            // Suggest interaction style
            Future<JsonObject> interactionStyleFuture = callToolWithEvents(milestoneContext, ANALYSIS_CLIENT,
                "intent_analysis__suggest_interaction_style",
                new JsonObject()
                    .put("intent", extractedIntent.getJsonObject("result"))
                    .put("user_expertise", context != null && context.getJsonObject("userProfile") != null ?
                        context.getJsonObject("userProfile").getString("expertise_level", "intermediate") : "intermediate")
                    .put("query_complexity", 0.5f),
                "Suggesting interaction style");
            
            return CompositeFuture.all(
                Future.succeededFuture(extractedIntent),
                evaluationFuture, 
                outputFormatFuture,
                interactionStyleFuture
            ).map(composite -> new JsonObject()
                .put("extractedIntent", composite.resultAt(0))
                .put("intentEvaluation", composite.resultAt(1))
                .put("outputFormat", composite.resultAt(2))
                .put("interactionStyle", composite.resultAt(3))
                .put("query", query));
        });
        
        return chainedAnalysis;
    }
    
    /**
     * Deploy a clients and track it
     */
    private Future<String> deployClient(String clientName, AbstractVerticle client) {
        Promise<String> promise = Promise.promise();
        
        vertx.deployVerticle(client, ar -> {
            if (ar.succeeded()) {
                String deploymentId = ar.result();
                deploymentIds.put(clientName, deploymentId);
                log("Deployed " + clientName + " clients", 3);
                promise.complete(deploymentId);
            } else {
                promise.fail(ar.cause());
            }
        });
        
        return promise.future();
    }
    
    /**
     * Call a tool on a deployed clients
     */
    private Future<JsonObject> callTool(String clientName, String toolName, JsonObject params) {
        Promise<JsonObject> promise = Promise.promise();
        
        // Map clients names to normalized server names
        String serverName;
        switch (clientName) {
            case EVALUATION_CLIENT:
                serverName = "queryintentevaluation";
                break;
            case ANALYSIS_CLIENT:
                serverName = "intentanalysis";
                break;
            default:
                serverName = clientName.toLowerCase();
        }
        
        String address = "mcp.clients." + serverName + "." + toolName;
        vertx.eventBus().<JsonObject>request(address, params, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result().body());
            } else {
                promise.fail(ar.cause());
            }
        });
        
        return promise.future();
    }
    
    /**
     * Call a tool with streaming event publishing
     */
    private Future<JsonObject> callToolWithEvents(MilestoneContext context, String clientName, 
                                                  String toolName, JsonObject params, String description) {
        if (context.isStreaming() && context.getSessionId() != null) {
            // Publish tool start event
            publishToolStartEvent(context.getConversationId(), toolName, description);
            
            return callTool(clientName, toolName, params)
                .map(result -> {
                    // Publish tool complete event AND return result
                    publishToolCompleteEvent(context.getConversationId(), toolName, true);
                    return result;  // MUST return the result!
                })
                .recover(err -> {
                    // Publish tool error event AND re-throw
                    publishToolCompleteEvent(context.getConversationId(), toolName, false);
                    return Future.failedFuture(err);  // MUST propagate the error!
                });
        } else {
            return callTool(clientName, toolName, params);
        }
    }
    
    @Override
    public JsonObject getShareableResult(MilestoneContext context) {
        return new JsonObject()
            .put("milestone", 1)
            .put("milestone_name", "Intent Extraction")
            .put("intent", context.getIntent())
            .put("intent_type", context.getIntentType())
            .put("confidence", context.getIntentDetails() != null ? 
                 context.getIntentDetails().getDouble("confidence", 0.5) : 0.5)
            .put("message", "I understand you want to: " + context.getIntent());
    }
    
    /**
     * Build a clear, user-friendly intent statement
     */
    private String buildIntentStatement(String primaryIntent, String intentType,
                                       JsonObject extractedIntent, JsonObject evaluation, String query) {
        if (primaryIntent == null || primaryIntent.isEmpty()) {
            // Try to extract from the intent result or use a better fallback
            if (extractedIntent != null && extractedIntent.getJsonObject("result") != null) {
                String fallbackIntent = extractedIntent.getJsonObject("result").getString("primary_intent");
                if (fallbackIntent != null && !fallbackIntent.isEmpty()) {
                    primaryIntent = fallbackIntent;
                }
            }
            
            // If still empty, use the simple intent extraction
            if (primaryIntent == null || primaryIntent.isEmpty()) {
                return extractSimpleIntent(query);
            }
        }
        
        // Clean up and format the primary intent
        String intent = primaryIntent.toLowerCase();
        
        // Add context based on intent type
        switch (intentType.toLowerCase()) {
            case "query":
            case "data_retrieval":
                if (!intent.startsWith("get") && !intent.startsWith("find") && 
                    !intent.startsWith("show") && !intent.startsWith("list")) {
                    intent = "retrieve " + intent;
                }
                break;
                
            case "aggregation":
                if (!intent.contains("count") && !intent.contains("sum") && 
                    !intent.contains("average") && !intent.contains("total")) {
                    intent = "calculate " + intent;
                }
                break;
                
            case "exploration":
                if (!intent.startsWith("explore")) {
                    intent = "explore " + intent;
                }
                break;
                
            case "sql_generation":
                if (!intent.contains("sql") && !intent.contains("query")) {
                    intent = "generate SQL for " + intent;
                }
                break;
        }
        
        // Ensure first letter is capitalized
        if (!intent.isEmpty()) {
            intent = Character.toUpperCase(intent.charAt(0)) + intent.substring(1);
        }
        
        return intent;
    }
    
    /**
     * Simple fallback intent extraction
     */
    private String extractSimpleIntent(String query) {
        String lower = query.toLowerCase();
        
        if (lower.contains("how many")) {
            return "Count items based on your criteria";
        } else if (lower.contains("show") || lower.contains("list")) {
            return "Display data from the database";
        } else if (lower.contains("what") || lower.contains("which")) {
            return "Answer your question using database information";
        } else if (lower.contains("sum") || lower.contains("total")) {
            return "Calculate totals from the data";
        } else if (lower.contains("average") || lower.contains("avg")) {
            return "Calculate averages from the data";
        } else if (lower.contains("find")) {
            return "Find specific information in the database";
        } else {
            return "Process your database query";
        }
    }
    
    @Override
    public Future<Void> cleanup() {
        List<Future> undeployFutures = new ArrayList<>();
        
        // Undeploy all clients
        for (Map.Entry<String, String> entry : deploymentIds.entrySet()) {
            Promise<Void> promise = Promise.promise();
            vertx.undeploy(entry.getValue(), ar -> {
                if (ar.succeeded()) {
                    log("Undeployed " + entry.getKey() + " clients", 3);
                    promise.complete();
                } else {
                    promise.fail(ar.cause());
                }
            });
            undeployFutures.add(promise.future());
        }
        
        // Also call parent cleanup
        undeployFutures.add(super.cleanup());
        
        return CompositeFuture.all(undeployFutures).mapEmpty();
    }
}
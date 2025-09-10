package agents.director.hosts.base.managers;

import agents.director.mcp.client.QueryIntentEvaluationClient;
import agents.director.mcp.client.IntentAnalysisClient;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;

import java.util.Arrays;
import java.util.List;

/**
 * Manager for intent analysis clients.
 * Coordinates between QueryIntentEvaluation and IntentAnalysis clients
 * to provide comprehensive understanding of user intent.
 */
public class IntentAnalysisManager extends MCPClientManager {
    
    private static final String EVALUATION_CLIENT = "evaluation";
    private static final String ANALYSIS_CLIENT = "analysis";
    
    public IntentAnalysisManager(Vertx vertx, String baseUrl) {
        super(vertx, baseUrl);
    }
    
    @Override
    public Future<Void> initialize() {
        List<Future> deploymentFutures = Arrays.asList(
            deployClient(EVALUATION_CLIENT, new QueryIntentEvaluationClient(baseUrl)),
            deployClient(ANALYSIS_CLIENT, new IntentAnalysisClient(baseUrl))
        );
        
        return CompositeFuture.all(deploymentFutures).mapEmpty();
    }
    
    /**
     * Perform full intent analysis on a query
     * @param query The user's query
     * @param context Optional conversation context
     * @return Future with comprehensive intent analysis
     */
    public Future<JsonObject> fullIntentAnalysis(String query, JsonObject context) {
        // Extract primary and secondary intents
        Future<JsonObject> intentExtractFuture = callClientTool(ANALYSIS_CLIENT, 
            "intent_analysis__extract_intent",
            new JsonObject()
                .put("query", query)
                .put("conversation_history", context != null ? 
                    context.getJsonArray("history", new JsonArray()) : new JsonArray()));
        
        // Evaluate query intent
        Future<JsonObject> evaluationFuture = callClientTool(EVALUATION_CLIENT, 
            "evaluate_query_intent",
            new JsonObject()
                .put("query", query)
                .put("context", context != null ? context : new JsonObject()));
        
        // Determine output format
        Future<JsonObject> outputFormatFuture = callClientTool(ANALYSIS_CLIENT, 
            "intent_analysis__determine_output_format",
            new JsonObject()
                .put("query", query)
                .put("user_profile", context != null ? 
                    context.getJsonObject("userProfile", new JsonObject()) : new JsonObject()));
        
        return CompositeFuture.all(intentExtractFuture, evaluationFuture, outputFormatFuture)
            .map(composite -> new JsonObject()
                .put("extractedIntent", composite.resultAt(0))
                .put("intentEvaluation", composite.resultAt(1))
                .put("outputFormat", composite.resultAt(2))
                .put("query", query));
    }
    
    /**
     * Determine the best tool strategy based on intent
     * @param intent The analyzed intent
     * @return Future with tool strategy
     */
    public Future<JsonObject> determineToolStrategy(JsonObject intent) {
        return callClientTool(EVALUATION_CLIENT, "select_tool_strategy",
            new JsonObject().put("intent", intent));
    }
    
    /**
     * Suggest interaction style based on user profile
     * @param userProfile The user's profile information
     * @param query The current query
     * @return Future with interaction style suggestions
     */
    public Future<JsonObject> suggestInteractionStyle(JsonObject userProfile, String query) {
        return callClientTool(ANALYSIS_CLIENT, "intent_analysis__suggest_interaction_style",
            new JsonObject()
                .put("user_profile", userProfile)
                .put("query", query));
    }
    
    /**
     * Learn from successful query execution
     * @param query The original query
     * @param intent The analyzed intent
     * @param result The execution result
     * @return Future indicating learning completion
     */
    public Future<JsonObject> learnFromSuccess(String query, JsonObject intent, JsonObject result) {
        return callClientTool(EVALUATION_CLIENT, "learn_from_success",
            new JsonObject()
                .put("query", query)
                .put("intent", intent)
                .put("result", result)
                .put("timestamp", System.currentTimeMillis()));
    }
    
    /**
     * Analyze intent with confidence scoring
     * @param query The query to analyze
     * @param previousQueries Previous queries for context
     * @return Future with intent and confidence scores
     */
    public Future<JsonObject> analyzeWithConfidence(String query, JsonArray previousQueries) {
        // Get intent extraction
        Future<JsonObject> intentFuture = callClientTool(ANALYSIS_CLIENT, 
            "intent_analysis__extract_intent",
            new JsonObject()
                .put("query", query)
                .put("conversation_history", previousQueries != null ? 
                    previousQueries : new JsonArray()));
        
        // Get evaluation
        Future<JsonObject> evaluationFuture = callClientTool(EVALUATION_CLIENT, 
            "evaluate_query_intent",
            new JsonObject().put("query", query));
        
        return CompositeFuture.all(intentFuture, evaluationFuture)
            .map(composite -> {
                JsonObject intent = composite.resultAt(0);
                JsonObject evaluation = composite.resultAt(1);
                
                // Calculate overall confidence
                double intentConfidence = intent.getDouble("confidence", 0.5);
                double evaluationConfidence = evaluation.getDouble("confidence", 0.5);
                double overallConfidence = (intentConfidence + evaluationConfidence) / 2;
                
                return new JsonObject()
                    .put("intent", intent)
                    .put("evaluation", evaluation)
                    .put("overallConfidence", overallConfidence)
                    .put("requiresClarification", overallConfidence < 0.7);
            });
    }
    
    /**
     * Get adaptive response format based on query complexity
     * @param query The query
     * @param complexity Query complexity assessment
     * @return Future with adaptive format recommendations
     */
    public Future<JsonObject> getAdaptiveFormat(String query, JsonObject complexity) {
        // Determine output format
        Future<JsonObject> formatFuture = callClientTool(ANALYSIS_CLIENT, 
            "intent_analysis__determine_output_format",
            new JsonObject()
                .put("query", query)
                .put("complexity", complexity));
        
        // Suggest interaction style
        Future<JsonObject> styleFuture = callClientTool(ANALYSIS_CLIENT, 
            "intent_analysis__suggest_interaction_style",
            new JsonObject()
                .put("query", query)
                .put("complexity", complexity));
        
        return CompositeFuture.all(formatFuture, styleFuture)
            .map(composite -> new JsonObject()
                .put("outputFormat", composite.resultAt(0))
                .put("interactionStyle", composite.resultAt(1))
                .put("isComplex", complexity.getBoolean("isComplex", false)));
    }
}
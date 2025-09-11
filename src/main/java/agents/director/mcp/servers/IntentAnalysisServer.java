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
import static agents.director.Driver.logLevel;

/**
 * MCP Server for analyzing user intent and determining desired output formats.
 * Provides deep understanding of what users want to achieve with their queries.
 */
public class IntentAnalysisServer extends MCPServerBase {
    
    private LlmAPIService llmService;
    
    // Common intent patterns
    private static final Map<String, List<String>> INTENT_KEYWORDS = Map.of(
        "get_sql_only", Arrays.asList("show sql", "generate query", "create sql", "write query", 
                                      "build query", "sql for", "query to"),
        "answer_question", Arrays.asList("how many", "what is", "show me", "list all", "find", 
                                        "get", "count", "sum", "average", "total"),
        "explore_schema", Arrays.asList("what tables", "show schema", "describe", "structure", 
                                       "columns in", "fields", "database layout"),
        "debug_query", Arrays.asList("why", "error", "wrong", "fix", "debug", "issue", "problem"),
        "performance_analysis", Arrays.asList("optimize", "slow", "performance", "faster", "speed up", 
                                            "improve", "index")
    );
    
    // Output format preferences based on intent
    private static final Map<String, JsonObject> DEFAULT_OUTPUT_FORMATS = Map.of(
        "get_sql_only", new JsonObject()
            .put("primary", "sql_only")
            .put("include_sql", true)
            .put("verbosity", "normal")
            .put("include_explanations", true)
            .put("include_alternatives", false),
        "answer_question", new JsonObject()
            .put("primary", "narrative")
            .put("include_sql", true)
            .put("verbosity", "normal")
            .put("include_explanations", false)
            .put("include_alternatives", false),
        "explore_schema", new JsonObject()
            .put("primary", "structured")
            .put("include_sql", false)
            .put("verbosity", "detailed")
            .put("include_explanations", true)
            .put("include_alternatives", true)
    );
    
    public IntentAnalysisServer() {
        super("IntentAnalysisServer", "/mcp/servers/intent-analysis");
    }
    
    @Override
    public void start(Promise<Void> startPromise) {
        llmService = LlmAPIService.getInstance();
        
        if (!llmService.isInitialized()) {
            if (logLevel >= 1) vertx.eventBus().publish("log", "LLM service not initialized - will use keyword-based analysis only,1,IntentAnalysisServer,Init,Warning");
        }
        
        super.start(startPromise);
    }
    
    @Override
    protected void initializeTools() {
        // Register extract_intent tool
        registerTool(new MCPTool(
            "intent_analysis__extract_intent",
            "Extract primary and secondary intents from user query with confidence scoring",
            new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("query", new JsonObject()
                        .put("type", "string")
                        .put("description", "The user's query to analyze"))
                    .put("conversation_history", new JsonObject()
                        .put("type", "array")
                        .put("description", "Previous messages in the conversation")
                        .put("items", new JsonObject()
                            .put("type", "object")
                            .put("properties", new JsonObject()
                                .put("role", new JsonObject().put("type", "string"))
                                .put("content", new JsonObject().put("type", "string")))))
                    .put("user_profile", new JsonObject()
                        .put("type", "object")
                        .put("description", "Optional user profile information")
                        .put("properties", new JsonObject()
                            .put("expertise_level", new JsonObject()
                                .put("type", "string")
                                .put("enum", new JsonArray().add("beginner").add("intermediate").add("expert")))
                            .put("preferred_style", new JsonObject()
                                .put("type", "string")
                                .put("enum", new JsonArray().add("technical").add("business").add("casual"))))))
                .put("required", new JsonArray().add("query"))
        ));
        
        // Register determine_output_format tool
        registerTool(new MCPTool(
            "intent_analysis__determine_output_format",
            "Determine desired output format based on intent analysis",
            new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("intent", new JsonObject()
                        .put("type", "object")
                        .put("description", "Intent analysis result from extract_intent"))
                    .put("query", new JsonObject()
                        .put("type", "string")
                        .put("description", "The original query"))
                    .put("user_preferences", new JsonObject()
                        .put("type", "object")
                        .put("description", "Optional user preferences")
                        .put("properties", new JsonObject()
                            .put("always_include_sql", new JsonObject().put("type", "boolean"))
                            .put("preferred_verbosity", new JsonObject()
                                .put("type", "string")
                                .put("enum", new JsonArray().add("minimal").add("normal").add("detailed"))))))
                .put("required", new JsonArray().add("intent").add("query"))
        ));
        
        // Register suggest_interaction_style tool
        registerTool(new MCPTool(
            "intent_analysis__suggest_interaction_style",
            "Suggest how the system should interact with the user based on intent and expertise",
            new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("intent", new JsonObject()
                        .put("type", "object")
                        .put("description", "Intent analysis result"))
                    .put("user_expertise", new JsonObject()
                        .put("type", "string")
                        .put("description", "User's expertise level")
                        .put("enum", new JsonArray().add("beginner").add("intermediate").add("expert")))
                    .put("query_complexity", new JsonObject()
                        .put("type", "number")
                        .put("description", "Complexity score from 0-1")))
                .put("required", new JsonArray().add("intent"))
        ));
    }
    
    @Override
    protected void executeTool(RoutingContext ctx, String requestId, String toolName, JsonObject arguments) {
        switch (toolName) {
            case "intent_analysis__extract_intent":
                extractIntent(ctx, requestId, arguments);
                break;
            case "intent_analysis__determine_output_format":
                determineOutputFormat(ctx, requestId, arguments);
                break;
            case "intent_analysis__suggest_interaction_style":
                suggestInteractionStyle(ctx, requestId, arguments);
                break;
            default:
                sendError(ctx, requestId, MCPResponse.ErrorCodes.METHOD_NOT_FOUND, 
                    "Unknown tool: " + toolName);
        }
    }
    
    private void extractIntent(RoutingContext ctx, String requestId, JsonObject arguments) {
        String query = arguments.getString("query");
        JsonArray conversationHistory = arguments.getJsonArray("conversation_history", new JsonArray());
        JsonObject userProfile = arguments.getJsonObject("user_profile", new JsonObject());
        
        // Validate required query parameter
        if (query == null || query.trim().isEmpty()) {
            sendError(ctx, requestId, MCPResponse.ErrorCodes.INVALID_PARAMS, 
                "Query parameter is required and cannot be empty");
            return;
        }
        
        if (logLevel >= 3) vertx.eventBus().publish("log", "Extracting intent from query: " + query + ",3,IntentAnalysisServer,Intent,Extract");
        
        // If LLM is available, use it for deep analysis
        if (llmService.isInitialized()) {
            analyzeIntentWithLLM(query, conversationHistory, userProfile)
                .onComplete(ar -> {
                    if (ar.succeeded()) {
                        sendSuccess(ctx, requestId, new JsonObject().put("result", ar.result()));
                    } else {
                        // Fallback to keyword-based analysis
                        JsonObject fallbackResult = analyzeIntentWithKeywords(query, conversationHistory);
                        sendSuccess(ctx, requestId, new JsonObject().put("result", fallbackResult));
                    }
                });
        } else {
            // Use keyword-based analysis
            JsonObject result = analyzeIntentWithKeywords(query, conversationHistory);
            sendSuccess(ctx, requestId, new JsonObject().put("result", result));
        }
    }
    
    private Future<JsonObject> analyzeIntentWithLLM(String query, JsonArray conversationHistory, JsonObject userProfile) {
        Promise<JsonObject> promise = Promise.promise();
        
        String prompt = buildIntentAnalysisPrompt(query, conversationHistory, userProfile);
        
        // Convert prompt to messages array for chatCompletion
        JsonArray messages = new JsonArray()
            .add(new JsonObject()
                .put("role", "system")
                .put("content", "You are an intent analysis assistant. Analyze user queries and return structured JSON responses."))
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
                        JsonObject intent = parseIntentFromLLM(content);
                        promise.complete(intent);
                    } catch (Exception e) {
                        vertx.eventBus().publish("log", "Failed to parse intent from LLM: " + e.getMessage() + ",0,IntentAnalysisServer,Intent,Parse");
                        promise.fail(e);
                    }
                } else {
                    promise.fail(ar.cause());
                }
            });
        
        return promise.future();
    }
    
    private String buildIntentAnalysisPrompt(String query, JsonArray conversationHistory, JsonObject userProfile) {
        StringBuilder historyStr = new StringBuilder();
        if (!conversationHistory.isEmpty()) {
            historyStr.append("\nConversation context:\n");
            for (int i = Math.max(0, conversationHistory.size() - 3); i < conversationHistory.size(); i++) {
                JsonObject msg = conversationHistory.getJsonObject(i);
                historyStr.append(msg.getString("role")).append(": ").append(msg.getString("content")).append("\n");
            }
        }
        
        return String.format("""
            Analyze this database query and extract the user's intent.
            
            Query: %s%s
            
            User expertise: %s
            
            Categorize the PRIMARY intent as exactly one of these:
            - get_sql_only: User explicitly wants the SQL query without execution (keywords: "show sql", "generate query")
            - answer_question: User wants data-based answers (keywords: "how many", "what is", "show me")
            - explore_schema: User wants to understand database structure (keywords: "what tables", "describe")
            - debug_query: User needs help fixing issues (keywords: "error", "wrong", "fix")
            - performance_analysis: User wants optimization (keywords: "slow", "optimize", "performance")
            
            Also identify:
            - Secondary intents if multiple purposes are detected
            - Confidence level (0.0 to 1.0) based on clarity
            - Whether the query is ambiguous and needs clarification
            - Specific clarifications that would help
            
            Output ONLY this JSON structure:
            {
              "primary_intent": "one of the categories above",
              "confidence": 0.0-1.0,
              "reasoning": "brief explanation of why this intent was chosen",
              "secondary_intents": ["array of other detected intents"],
              "ambiguity_level": 0.0-1.0,
              "clarification_needed": true/false,
              "suggested_clarifications": ["array of questions to ask user"]
            }
            """, 
            query,
            historyStr.toString(),
            userProfile.getString("expertise_level", "unknown"));
    }
    
    private JsonObject parseIntentFromLLM(String llmResponse) {
        // Extract JSON from response
        String response = llmResponse.trim();
        int start = response.indexOf("{");
        int end = response.lastIndexOf("}");
        
        if (start >= 0 && end > start) {
            String jsonStr = response.substring(start, end + 1);
            return new JsonObject(jsonStr);
        }
        
        throw new IllegalArgumentException("No valid JSON found in LLM response");
    }
    
    private JsonObject analyzeIntentWithKeywords(String query, JsonArray conversationHistory) {
        String lowerQuery = query.toLowerCase();
        Map<String, Double> intentScores = new HashMap<>();
        
        // Score each intent based on keyword matches
        for (Map.Entry<String, List<String>> entry : INTENT_KEYWORDS.entrySet()) {
            String intent = entry.getKey();
            List<String> keywords = entry.getValue();
            
            double score = 0.0;
            int matches = 0;
            for (String keyword : keywords) {
                if (lowerQuery.contains(keyword)) {
                    score += 1.0;
                    matches++;
                }
            }
            
            // Normalize by number of keywords to avoid bias
            if (matches > 0) {
                score = score / keywords.size();
                intentScores.put(intent, score);
            }
        }
        
        // Find primary intent
        String primaryIntent = "answer_question"; // default
        double maxScore = 0.0;
        for (Map.Entry<String, Double> entry : intentScores.entrySet()) {
            if (entry.getValue() > maxScore) {
                maxScore = entry.getValue();
                primaryIntent = entry.getKey();
            }
        }
        
        // Calculate confidence based on score strength and uniqueness
        double confidence = Math.min(maxScore * 2, 1.0); // Scale up since keyword matching is less precise
        
        // Find secondary intents
        JsonArray secondaryIntents = new JsonArray();
        for (Map.Entry<String, Double> entry : intentScores.entrySet()) {
            if (!entry.getKey().equals(primaryIntent) && entry.getValue() > 0.3) {
                secondaryIntents.add(entry.getKey());
            }
        }
        
        // Check for ambiguity
        boolean clarificationNeeded = confidence < 0.6 || secondaryIntents.size() > 1;
        double ambiguityLevel = clarificationNeeded ? 1.0 - confidence : 0.0;
        
        // Generate clarification suggestions
        JsonArray clarifications = new JsonArray();
        if (clarificationNeeded) {
            clarifications.add("Are you looking for just the SQL query or do you want me to execute it?");
            if (secondaryIntents.contains("explore_schema")) {
                clarifications.add("Do you need information about the database structure?");
            }
        }
        
        return new JsonObject()
            .put("primary_intent", primaryIntent)
            .put("confidence", confidence)
            .put("reasoning", "Based on keyword analysis: found " + 
                (int)(maxScore * INTENT_KEYWORDS.get(primaryIntent).size()) + " matching keywords")
            .put("secondary_intents", secondaryIntents)
            .put("ambiguity_level", ambiguityLevel)
            .put("clarification_needed", clarificationNeeded)
            .put("suggested_clarifications", clarifications);
    }
    
    private void determineOutputFormat(RoutingContext ctx, String requestId, JsonObject arguments) {
        JsonObject intent = arguments.getJsonObject("intent");
        String query = arguments.getString("query");
        JsonObject userPreferences = arguments.getJsonObject("user_preferences", new JsonObject());
        
        // Validate required parameters
        if (intent == null) {
            sendError(ctx, requestId, MCPResponse.ErrorCodes.INVALID_PARAMS, 
                "Intent parameter is required for determining output format");
            return;
        }
        
        if (query == null || query.trim().isEmpty()) {
            sendError(ctx, requestId, MCPResponse.ErrorCodes.INVALID_PARAMS, 
                "Query parameter is required and cannot be empty");
            return;
        }
        
        String primaryIntent = intent.getString("primary_intent", "answer_question");
        
        if (logLevel >= 3) vertx.eventBus().publish("log", "Determining output format for intent: " + primaryIntent + ",3,IntentAnalysisServer,Format,Determine");
        
        // Start with default format for the intent
        JsonObject format = DEFAULT_OUTPUT_FORMATS.getOrDefault(primaryIntent, 
            DEFAULT_OUTPUT_FORMATS.get("answer_question")).copy();
        
        // Apply user preferences
        if (userPreferences != null && !userPreferences.isEmpty()) {
            if (userPreferences.containsKey("always_include_sql")) {
                format.put("include_sql", userPreferences.getBoolean("always_include_sql"));
            }
            if (userPreferences.containsKey("preferred_verbosity")) {
                format.put("verbosity", userPreferences.getString("preferred_verbosity"));
            }
        }
        
        // Adjust based on query characteristics
        if (query.toLowerCase().contains("detail") || query.toLowerCase().contains("explain")) {
            format.put("verbosity", "detailed");
            format.put("include_explanations", true);
        }
        
        if (query.toLowerCase().contains("summary") || query.toLowerCase().contains("brief")) {
            format.put("verbosity", "minimal");
            format.put("include_explanations", false);
        }
        
        if (query.toLowerCase().contains("alternative") || query.toLowerCase().contains("other way")) {
            format.put("include_alternatives", true);
        }
        
        // Special handling for SQL-only intent
        if (primaryIntent.equals("get_sql_only")) {
            format.put("include_sql", true);
            format.put("primary", "sql_only");
        }
        
        JsonObject result = new JsonObject().put("format", format);
        sendSuccess(ctx, requestId, new JsonObject().put("result", result));
    }
    
    private void suggestInteractionStyle(RoutingContext ctx, String requestId, JsonObject arguments) {
        JsonObject intent = arguments.getJsonObject("intent");
        String userExpertise = arguments.getString("user_expertise", "intermediate");
        Float queryComplexity = arguments.getFloat("query_complexity", 0.5f);
        
        // Validate intent parameter
        if (intent == null) {
            vertx.eventBus().publish("log", "Intent parameter is missing in suggestInteractionStyle,1,IntentAnalysisServer,Style,Warning");
            intent = new JsonObject().put("primary_intent", "answer_question");
        }
        
        if (logLevel >= 3) vertx.eventBus().publish("log", "Suggesting interaction style for expertise: " + userExpertise + ",3,IntentAnalysisServer,Style,Suggest");
        
        JsonObject interactionStyle = new JsonObject();
        JsonArray confirmationPoints = new JsonArray();
        
        // Determine guidance level
        String guidanceLevel;
        String explanationDepth;
        
        if (userExpertise.equals("beginner")) {
            guidanceLevel = "high";
            explanationDepth = "detailed";
            confirmationPoints.add("Before executing complex queries");
            confirmationPoints.add("When multiple tables are involved");
            confirmationPoints.add("Before any data modifications");
        } else if (userExpertise.equals("expert")) {
            guidanceLevel = "minimal";
            explanationDepth = "concise";
            confirmationPoints.add("Only for data modifications");
        } else {
            guidanceLevel = "moderate";
            explanationDepth = "normal";
            confirmationPoints.add("For complex multi-table queries");
            confirmationPoints.add("Before data modifications");
        }
        
        // Adjust based on query complexity
        if (queryComplexity > 0.7) {
            if (!guidanceLevel.equals("high")) {
                guidanceLevel = "moderate";
            }
            confirmationPoints.add("After query generation for review");
        }
        
        // Check if intent suggests need for interaction
        String primaryIntent = intent.getString("primary_intent");
        if (primaryIntent.equals("debug_query") || primaryIntent.equals("explore_schema")) {
            guidanceLevel = "interactive";
            explanationDepth = "detailed";
            confirmationPoints.add("At each discovery step");
        }
        
        interactionStyle
            .put("guidance_level", guidanceLevel)
            .put("explanation_depth", explanationDepth)
            .put("confirmation_points", confirmationPoints)
            .put("progressive_disclosure", userExpertise.equals("beginner"))
            .put("show_intermediate_results", queryComplexity > 0.5)
            .put("offer_alternatives", intent.getBoolean("clarification_needed", false));
        
        JsonObject result = new JsonObject().put("interaction_style", interactionStyle);
        sendSuccess(ctx, requestId, new JsonObject().put("result", result));
    }
}
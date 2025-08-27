package agents.director.mcp.servers;

import agents.director.mcp.base.MCPServerBase;
import agents.director.mcp.base.MCPTool;
import agents.director.mcp.base.MCPResponse;
import agents.director.services.LlmAPIService;
import io.vertx.core.Promise;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.web.RoutingContext;


import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

/**
 * MCP Server for natural language query analysis.
 * Provides tools for semantic analysis and token extraction.
 * The tools themselves are database-agnostic, though the server is part of the Oracle pipeline.
 * Deployed as a Worker Verticle due to potentially blocking LLM operations.
 */
public class OracleQueryAnalysisServer extends MCPServerBase {
    
    
    
    private LlmAPIService llmService;
    
    // Common English stop words
    private static final Set<String> STOP_WORDS = new HashSet<>(Arrays.asList(
        "a", "an", "and", "are", "as", "at", "be", "been", "by", "for", "from",
        "has", "have", "in", "is", "it", "its", "of", "on", "that", "the", "to",
        "was", "were", "will", "with", "what", "when", "where", "which", "who",
        "how", "many", "much", "all", "some", "any", "each", "every"
    ));
    
    public OracleQueryAnalysisServer() {
        super("OracleQueryAnalysisServer", "/mcp/servers/oracle-query-analysis");
    }
    
    @Override
    protected void onServerReady() {
        // Initialize LLM service
        llmService = LlmAPIService.getInstance();
        if (!llmService.isInitialized()) {
            vertx.eventBus().publish("log", "LLM service not initialized - analysis capabilities will be limited,1,OracleQueryAnalysisServer,MCP,System");
        }
    }
    
    @Override
    protected void initializeTools() {
        // Register the analyze_query tool (DB-agnostic)
        registerTool(new MCPTool(
            "analyze_query",
            "Perform semantic analysis on a natural language query (no direct DB access).",
            new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("query", new JsonObject()
                        .put("type", "string")
                        .put("description", "The user's natural language question."))
                    .put("context", new JsonObject()
                        .put("type", "array")
                        .put("description", "Optional conversation history for context")
                        .put("items", new JsonObject()
                            .put("type", "object")
                            .put("properties", new JsonObject()
                                .put("role", new JsonObject()
                                    .put("type", "string")
                                    .put("description", "speaker role (user/assistant)"))
                                .put("content", new JsonObject()
                                    .put("type", "string")
                                    .put("description", "utterance text"))))))
                .put("required", new JsonArray().add("query"))
        ));
        
        // Register the extract_query_tokens tool (DB-agnostic)
        registerTool(new MCPTool(
            "extract_query_tokens",
            "Extract key tokens (keywords, potential column or table names) from the query.",
            new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("query", new JsonObject()
                        .put("type", "string")
                        .put("description", "The query to tokenize."))
                    .put("includeStopWords", new JsonObject()
                        .put("type", "boolean")
                        .put("description", "Whether to include common stop words in tokens.")
                        .put("default", false)))
                .put("required", new JsonArray().add("query"))
        ));
    }
    
    @Override
    protected void executeTool(RoutingContext ctx, String requestId, String toolName, JsonObject arguments) {
        switch (toolName) {
            case "analyze_query":
                analyzeQuery(ctx, requestId, arguments);
                break;
            case "extract_query_tokens":
                extractTokens(ctx, requestId, arguments);
                break;
            default:
                sendError(ctx, requestId, MCPResponse.ErrorCodes.METHOD_NOT_FOUND, 
                    "Unknown tool: " + toolName);
        }
    }
    
    private void analyzeQuery(RoutingContext ctx, String requestId, JsonObject arguments) {
        String query = arguments.getString("query");
        JsonArray context = arguments.getJsonArray("context", new JsonArray());
        
        if (query == null || query.trim().isEmpty()) {
            sendError(ctx, requestId, MCPResponse.ErrorCodes.INVALID_PARAMS, "Query is required");
            return;
        }
        
        // If LLM service is not available, fall back to rule-based analysis
        if (!llmService.isInitialized()) {
            performRuleBasedAnalysis(ctx, requestId, query);
            return;
        }
        
        // Execute LLM analysis as blocking operation
        executeBlocking(Promise.<JsonObject>promise(), promise -> {
            try {
                // Prepare the prompt for structured analysis
                String systemPrompt = """
                    You are a query analysis expert. Analyze the user's natural language query and extract:
                    1. The primary intent (what the user wants to know)
                    2. Key entities mentioned (people, places, things, time periods)
                    3. The query type (retrieval, aggregation, comparison, etc.)
                    4. Any filters or conditions
                    5. The expected result format
                    
                    Respond in JSON format with these fields:
                    {
                      "intent": "brief description of what user wants",
                      "queryType": "retrieval|aggregation|comparison|calculation|other",
                      "entities": ["list", "of", "entities"],
                      "timeframe": "any time-related constraints",
                      "filters": ["any specific conditions"],
                      "aggregations": ["count", "sum", "average", etc. if applicable],
                      "orderBy": "how results should be sorted if mentioned",
                      "confidence": 0.0-1.0
                    }
                    """;
                
                // Build messages including context if provided
                List<JsonObject> messages = new ArrayList<>();
                messages.add(new JsonObject()
                    .put("role", "system")
                    .put("content", systemPrompt));
                
                // Add conversation history if provided
                for (int i = 0; i < context.size(); i++) {
                    JsonObject msg = context.getJsonObject(i);
                    messages.add(msg);
                }
                
                // Add the current query
                messages.add(new JsonObject()
                    .put("role", "user")
                    .put("content", query));
                
                // Call LLM
                JsonObject llmResponse = llmService.chatCompletion(
                    messages.stream()
                        .map(JsonObject::encode)
                        .collect(Collectors.toList()),
                    0.0, // temperature 0 for consistent analysis
                    1000 // max tokens
                ).join(); // Block and wait for result
                
                // Parse the LLM response
                String content = llmResponse.getJsonArray("choices")
                    .getJsonObject(0)
                    .getJsonObject("message")
                    .getString("content");
                
                // Extract JSON from the response
                JsonObject analysis = extractJsonFromContent(content);
                
                // Add the original query to the result
                analysis.put("originalQuery", query);
                
                promise.complete(analysis);
                
            } catch (Exception e) {
                vertx.eventBus().publish("log", "LLM analysis failed" + ",0,OracleQueryAnalysisServer,MCP,System");
                promise.fail(e);
            }
        }, res -> {
            if (res.succeeded()) {
                sendSuccess(ctx, requestId, res.result());
            } else {
                // Fall back to rule-based analysis
                performRuleBasedAnalysis(ctx, requestId, query);
            }
        });
    }
    
    private void extractTokens(RoutingContext ctx, String requestId, JsonObject arguments) {
        String query = arguments.getString("query");
        boolean includeStopWords = arguments.getBoolean("includeStopWords", false);
        
        if (query == null || query.trim().isEmpty()) {
            sendError(ctx, requestId, MCPResponse.ErrorCodes.INVALID_PARAMS, "Query is required");
            return;
        }
        
        JsonObject result = new JsonObject();
        
        // Convert to lowercase for processing
        String processedQuery = query.toLowerCase();
        
        // Extract different types of tokens
        JsonArray allTokens = new JsonArray();
        JsonArray keywords = new JsonArray();
        JsonArray potentialIdentifiers = new JsonArray();
        JsonArray numbers = new JsonArray();
        JsonArray quotedPhrases = new JsonArray();
        
        // Extract quoted phrases first
        Pattern quotedPattern = Pattern.compile("\"([^\"]+)\"|'([^']+)'");
        Matcher quotedMatcher = quotedPattern.matcher(query);
        while (quotedMatcher.find()) {
            String phrase = quotedMatcher.group(1) != null ? quotedMatcher.group(1) : quotedMatcher.group(2);
            quotedPhrases.add(phrase);
            // Remove quoted phrases from query to avoid duplicate tokenization
            processedQuery = processedQuery.replace("\"" + phrase.toLowerCase() + "\"", "");
            processedQuery = processedQuery.replace("'" + phrase.toLowerCase() + "'", "");
        }
        
        // Tokenize remaining text
        String[] tokens = processedQuery.split("\\s+");
        
        for (String token : tokens) {
            // Clean token of punctuation at edges
            token = token.replaceAll("^[^a-zA-Z0-9_]+|[^a-zA-Z0-9_]+$", "");
            
            if (token.isEmpty()) continue;
            
            // Add to all tokens
            allTokens.add(token);
            
            // Check if it's a number
            if (token.matches("\\d+(\\.\\d+)?")) {
                numbers.add(token);
            }
            // Check if it's a potential identifier (contains underscore or mixed case in original)
            else if (token.contains("_") || token.matches(".*[A-Z].*")) {
                potentialIdentifiers.add(token);
            }
            // Check if it's a keyword (not a stop word)
            else if (includeStopWords || !STOP_WORDS.contains(token)) {
                keywords.add(token);
            }
        }
        
        // Also extract potential SQL-like patterns
        JsonArray sqlPatterns = extractSqlPatterns(query);
        
        result.put("allTokens", allTokens);
        result.put("keywords", keywords);
        result.put("potentialIdentifiers", potentialIdentifiers);
        result.put("numbers", numbers);
        result.put("quotedPhrases", quotedPhrases);
        result.put("sqlPatterns", sqlPatterns);
        result.put("tokenCount", allTokens.size());
        
        sendSuccess(ctx, requestId, result);
    }
    
    private void performRuleBasedAnalysis(RoutingContext ctx, String requestId, String query) {
        JsonObject analysis = new JsonObject();
        
        String lowerQuery = query.toLowerCase();
        
        // Determine query type
        String queryType = "retrieval"; // default
        if (lowerQuery.contains("how many") || lowerQuery.contains("count") || 
            lowerQuery.contains("total") || lowerQuery.contains("sum")) {
            queryType = "aggregation";
        } else if (lowerQuery.contains("compare") || lowerQuery.contains("versus") || 
                   lowerQuery.contains("difference")) {
            queryType = "comparison";
        } else if (lowerQuery.contains("calculate") || lowerQuery.contains("compute")) {
            queryType = "calculation";
        }
        
        // Extract potential timeframes
        String timeframe = null;
        if (lowerQuery.contains("today")) timeframe = "today";
        else if (lowerQuery.contains("yesterday")) timeframe = "yesterday";
        else if (lowerQuery.contains("last week")) timeframe = "last week";
        else if (lowerQuery.contains("last month")) timeframe = "last month";
        else if (lowerQuery.contains("last year")) timeframe = "last year";
        else if (lowerQuery.contains("this week")) timeframe = "this week";
        else if (lowerQuery.contains("this month")) timeframe = "this month";
        else if (lowerQuery.contains("this year")) timeframe = "this year";
        
        // Extract aggregations
        JsonArray aggregations = new JsonArray();
        if (lowerQuery.contains("count")) aggregations.add("count");
        if (lowerQuery.contains("sum")) aggregations.add("sum");
        if (lowerQuery.contains("average") || lowerQuery.contains("avg")) aggregations.add("average");
        if (lowerQuery.contains("maximum") || lowerQuery.contains("max")) aggregations.add("max");
        if (lowerQuery.contains("minimum") || lowerQuery.contains("min")) aggregations.add("min");
        
        // Simple intent extraction
        String intent = "Retrieve information";
        if (queryType.equals("aggregation")) {
            intent = "Calculate aggregate values";
        } else if (queryType.equals("comparison")) {
            intent = "Compare data points";
        }
        
        // Extract entities (simple approach - proper nouns)
        JsonArray entities = new JsonArray();
        String[] words = query.split("\\s+");
        for (String word : words) {
            if (word.length() > 2 && Character.isUpperCase(word.charAt(0)) && 
                !word.equals("I") && !STOP_WORDS.contains(word.toLowerCase())) {
                entities.add(word);
            }
        }
        
        analysis.put("intent", intent);
        analysis.put("queryType", queryType);
        analysis.put("entities", entities);
        analysis.put("timeframe", timeframe);
        analysis.put("filters", new JsonArray());
        analysis.put("aggregations", aggregations);
        analysis.put("orderBy", null);
        analysis.put("confidence", 0.6); // Lower confidence for rule-based
        analysis.put("originalQuery", query);
        analysis.put("analysisMethod", "rule-based");
        
        sendSuccess(ctx, requestId, analysis);
    }
    
    private JsonArray extractSqlPatterns(String query) {
        JsonArray patterns = new JsonArray();
        String lowerQuery = query.toLowerCase();
        
        // Common SQL-like patterns in natural language
        if (lowerQuery.contains("group by")) patterns.add("GROUP BY");
        if (lowerQuery.contains("order by")) patterns.add("ORDER BY");
        if (lowerQuery.contains("where")) patterns.add("WHERE");
        if (lowerQuery.contains("having")) patterns.add("HAVING");
        if (lowerQuery.contains("join")) patterns.add("JOIN");
        if (lowerQuery.contains("between")) patterns.add("BETWEEN");
        if (lowerQuery.contains("like")) patterns.add("LIKE");
        if (lowerQuery.contains("in (") || lowerQuery.contains("in(")) patterns.add("IN");
        
        return patterns;
    }
    
    private JsonObject extractJsonFromContent(String content) {
        try {
            // Try to find JSON in the content
            int startIdx = content.indexOf("{");
            int endIdx = content.lastIndexOf("}");
            
            if (startIdx != -1 && endIdx != -1 && endIdx > startIdx) {
                String jsonStr = content.substring(startIdx, endIdx + 1);
                return new JsonObject(jsonStr);
            }
        } catch (Exception e) {
            vertx.eventBus().publish("log", "Failed to extract JSON from LLM response" + ",1,OracleQueryAnalysisServer,MCP,System");
        }
        
        // Return a default structure if parsing fails
        return new JsonObject()
            .put("intent", "Unable to parse LLM response")
            .put("queryType", "unknown")
            .put("entities", new JsonArray())
            .put("confidence", 0.0)
            .put("error", "Failed to parse LLM response");
    }
    
    /**
     * Get deployment options for this server (Worker Verticle)
     */
    public static DeploymentOptions getDeploymentOptions() {
        return new DeploymentOptions()
            .setWorker(true)
            .setWorkerPoolSize(3); // Allow 3 concurrent LLM operations
    }
}
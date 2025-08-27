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


import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;

/**
 * MCP Server for query intent evaluation and strategy selection.
 * This server is database-agnostic and helps hosts decide how to handle queries.
 * It loads orchestration strategies from configuration and uses LLM for intelligent routing.
 */
public class QueryIntentEvaluationServer extends MCPServerBase {
    
    
    
    private LlmAPIService llmService;
    private JsonObject orchestrationConfig;
    private JsonObject strategies;
    private JsonObject strategyRules;
    private JsonObject hosts;
    
    public QueryIntentEvaluationServer() {
        super("QueryIntentEvaluationServer", "/mcp/servers/query-intent");
    }
    
    @Override
    protected void onServerReady() {
        // Initialize LLM service
        llmService = LlmAPIService.getInstance();
        if (!llmService.isInitialized()) {
            vertx.eventBus().publish("log", "LLM service not initialized - intent evaluation will use rules only,1,QueryIntentEvaluationServer,MCP,System");
        }
        
        // Load orchestration strategies
        loadOrchestrationStrategies();
    }
    
    @Override
    protected void initializeTools() {
        // Register evaluate_query_intent tool
        registerTool(new MCPTool(
            "evaluate_query_intent",
            "Analyze a user's query to determine complexity and required capabilities.",
            new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("query", new JsonObject()
                        .put("type", "string")
                        .put("description", "The user's question or command."))
                    .put("conversationHistory", new JsonObject()
                        .put("type", "array")
                        .put("description", "Prior messages in the conversation, if any (for context).")
                        .put("items", new JsonObject().put("type", "object"))))
                .put("required", new JsonArray().add("query"))
        ));
        
        // Register suggest_strategy tool
        registerTool(new MCPTool(
            "suggest_strategy",
            "Recommend which host or orchestration strategy to use for a given intent.",
            new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("intentAnalysis", new JsonObject()
                        .put("type", "object")
                        .put("description", "Output from evaluate_query_intent."))
                    .put("availableStrategies", new JsonObject()
                        .put("type", "array")
                        .put("items", new JsonObject().put("type", "string"))
                        .put("description", "List of possible strategy names or host types to choose from.")))
                .put("required", new JsonArray().add("intentAnalysis"))
        ));
        
        // Register select_tool_strategy tool - comprehensive tool/strategy selection
        registerTool(new MCPTool(
            "select_tool_strategy",
            "Intelligently select the best tool or orchestration strategy for a query based on comprehensive analysis.",
            new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("query", new JsonObject()
                        .put("type", "string")
                        .put("description", "The user's natural language query to analyze."))
                    .put("conversationHistory", new JsonObject()
                        .put("type", "array")
                        .put("description", "Recent conversation messages for context.")
                        .put("items", new JsonObject()
                            .put("type", "object")
                            .put("properties", new JsonObject()
                                .put("role", new JsonObject().put("type", "string"))
                                .put("content", new JsonObject().put("type", "string")))))
                    .put("intentAnalysis", new JsonObject()
                        .put("type", "object")
                        .put("description", "Pre-computed intent analysis from evaluate_query_intent (optional).")))
                .put("required", new JsonArray().add("query"))
        ));
    }
    
    @Override
    protected void executeTool(RoutingContext ctx, String requestId, String toolName, JsonObject arguments) {
        switch (toolName) {
            case "evaluate_query_intent":
                evaluateIntent(ctx, requestId, arguments);
                break;
            case "suggest_strategy":
                suggestStrategy(ctx, requestId, arguments);
                break;
            case "select_tool_strategy":
                selectToolStrategy(ctx, requestId, arguments);
                break;
            default:
                sendError(ctx, requestId, MCPResponse.ErrorCodes.METHOD_NOT_FOUND, 
                    "Unknown tool: " + toolName);
        }
    }
    
    private void evaluateIntent(RoutingContext ctx, String requestId, JsonObject arguments) {
        String query = arguments.getString("query");
        JsonArray conversationHistory = arguments.getJsonArray("conversationHistory", new JsonArray());
        
        if (query == null || query.trim().isEmpty()) {
            sendError(ctx, requestId, MCPResponse.ErrorCodes.INVALID_PARAMS, "Query is required");
            return;
        }
        
        if (llmService.isInitialized() && orchestrationConfig != null) {
            // Use LLM for intelligent intent evaluation
            executeBlocking(Promise.<JsonObject>promise(), promise -> {
                try {
                    JsonObject intent = evaluateWithLLM(query, conversationHistory);
                    promise.complete(intent);
                } catch (Exception e) {
                    vertx.eventBus().publish("log", "LLM intent evaluation failed" + ",0,QueryIntentEvaluationServer,MCP,System");
                    // Fall back to rule-based
                    JsonObject fallback = evaluateWithRules(query);
                    promise.complete(fallback);
                }
            }, res -> {
                if (res.succeeded()) {
                    sendSuccess(ctx, requestId, res.result());
                } else {
                    sendError(ctx, requestId, MCPResponse.ErrorCodes.INTERNAL_ERROR, 
                        "Intent evaluation failed: " + res.cause().getMessage());
                }
            });
        } else {
            // Use rule-based evaluation
            JsonObject result = evaluateWithRules(query);
            sendSuccess(ctx, requestId, result);
        }
    }
    
    private void suggestStrategy(RoutingContext ctx, String requestId, JsonObject arguments) {
        JsonObject intentAnalysis = arguments.getJsonObject("intentAnalysis");
        JsonArray availableStrategies = arguments.getJsonArray("availableStrategies");
        
        if (intentAnalysis == null) {
            sendError(ctx, requestId, MCPResponse.ErrorCodes.INVALID_PARAMS, "Intent analysis is required");
            return;
        }
        
        // If no available strategies provided, use all from config
        if (availableStrategies == null || availableStrategies.isEmpty()) {
            availableStrategies = new JsonArray();
            if (strategies != null) {
                for (String key : strategies.fieldNames()) {
                    availableStrategies.add(key);
                }
            }
        }
        
        final JsonArray finalAvailableStrategies = availableStrategies;
        
        if (llmService.isInitialized() && orchestrationConfig != null) {
            // Use LLM for strategy selection
            executeBlocking(Promise.<JsonObject>promise(), promise -> {
                try {
                    JsonObject suggestion = selectStrategyWithLLM(intentAnalysis, finalAvailableStrategies);
                    promise.complete(suggestion);
                } catch (Exception e) {
                    vertx.eventBus().publish("log", "LLM strategy selection failed" + ",0,QueryIntentEvaluationServer,MCP,System");
                    // Fall back to rule-based
                    JsonObject fallback = selectStrategyWithRules(intentAnalysis, finalAvailableStrategies);
                    promise.complete(fallback);
                }
            }, res -> {
                if (res.succeeded()) {
                    sendSuccess(ctx, requestId, res.result());
                } else {
                    sendError(ctx, requestId, MCPResponse.ErrorCodes.INTERNAL_ERROR, 
                        "Strategy selection failed: " + res.cause().getMessage());
                }
            });
        } else {
            // Use rule-based selection
            JsonObject result = selectStrategyWithRules(intentAnalysis, finalAvailableStrategies);
            sendSuccess(ctx, requestId, result);
        }
    }
    
    private JsonObject evaluateWithLLM(String query, JsonArray history) throws Exception {
        String systemPrompt = """
            You are a query intent analyzer. Evaluate the user's query and determine:
            1. The primary intent (what the user wants to achieve)
            2. Complexity level (simple, medium, complex)
            3. Required capabilities (database_access, llm_analysis, schema_intelligence, sql_generation, llm_only)
            4. Whether it requires data retrieval, analysis, or just information
            5. Confidence in your assessment (0.0-1.0)
            
            Consider the orchestration strategies and their capabilities:
            """ + orchestrationConfig.encodePrettily() + """
            
            Respond in JSON format:
            {
              "intent": "brief description",
              "complexity": "simple|medium|complex",
              "requiresDatabase": true/false,
              "requiresTools": true/false,
              "capabilities": ["list", "of", "required", "capabilities"],
              "queryType": "data_retrieval|analysis|information|schema_exploration|sql_generation",
              "confidence": 0.0-1.0,
              "reasoning": "brief explanation"
            }
            """;
        
        List<JsonObject> messages = new ArrayList<>();
        messages.add(new JsonObject().put("role", "system").put("content", systemPrompt));
        
        // Add conversation history
        for (int i = 0; i < history.size(); i++) {
            messages.add(history.getJsonObject(i));
        }
        
        messages.add(new JsonObject().put("role", "user").put("content", query));
        
        JsonObject response = llmService.chatCompletion(
            messages.stream().map(JsonObject::encode).collect(Collectors.toList()),
            0.0, // temperature 0 for consistent analysis
            500
        ).join();
        
        String content = response.getJsonArray("choices")
            .getJsonObject(0)
            .getJsonObject("message")
            .getString("content");
        
        // Parse JSON response
        JsonObject intent = parseJsonFromContent(content);
        
        // Validate required fields
        if (!intent.containsKey("intent") || !intent.containsKey("complexity") || 
            !intent.containsKey("capabilities") || intent.containsKey("error")) {
            vertx.eventBus().publish("log", "LLM returned incomplete intent analysis: " + intent.encode() + "" + ",1,QueryIntentEvaluationServer,MCP,System");
            // Add defaults for missing fields
            if (!intent.containsKey("intent")) {
                intent.put("intent", "Process user query");
            }
            if (!intent.containsKey("complexity")) {
                intent.put("complexity", "medium");
            }
            if (!intent.containsKey("requiresDatabase")) {
                intent.put("requiresDatabase", true);
            }
            if (!intent.containsKey("requiresTools")) {
                intent.put("requiresTools", true);
            }
            if (!intent.containsKey("capabilities")) {
                intent.put("capabilities", new JsonArray()
                    .add("database_access")
                    .add("llm_analysis")
                    .add("sql_generation"));
            }
            if (!intent.containsKey("queryType")) {
                intent.put("queryType", "data_retrieval");
            }
            if (!intent.containsKey("confidence")) {
                intent.put("confidence", 0.5);
            }
            if (!intent.containsKey("reasoning")) {
                intent.put("reasoning", "Incomplete LLM response - using defaults");
            }
        }
        
        intent.put("method", "llm_analysis");
        intent.put("originalQuery", query);
        
        return intent;
    }
    
    private JsonObject evaluateWithRules(String query) {
        JsonObject intent = new JsonObject();
        String lowerQuery = query.toLowerCase();
        
        // Determine complexity
        String complexity = "simple";
        int conditionCount = countConditions(lowerQuery);
        boolean hasAggregation = hasAggregation(lowerQuery);
        boolean hasMultipleTables = hasMultipleTables(lowerQuery);
        
        if (hasMultipleTables || conditionCount > 5 || (hasAggregation && conditionCount > 2)) {
            complexity = "complex";
        } else if (hasAggregation || conditionCount > 2) {
            complexity = "medium";
        }
        
        // Determine query type and capabilities
        String queryType = "data_retrieval";
        List<String> capabilities = new ArrayList<>();
        boolean requiresDatabase = true;
        boolean requiresTools = true;
        
        // Check keyword hints from config
        if (strategyRules != null) {
            JsonObject keywordHints = strategyRules.getJsonObject("keyword_hints", new JsonObject());
            
            // Check direct LLM keywords
            if (matchesKeywords(lowerQuery, keywordHints.getJsonArray("direct_llm", new JsonArray()))) {
                queryType = "information";
                requiresDatabase = false;
                requiresTools = false;
                capabilities.add("llm_only");
            }
            // Check SQL generation keywords
            else if (matchesKeywords(lowerQuery, keywordHints.getJsonArray("sql_generation", new JsonArray()))) {
                queryType = "sql_generation";
                requiresDatabase = false;
                requiresTools = true;
                capabilities.addAll(Arrays.asList("llm_analysis", "schema_intelligence", "sql_generation"));
            }
            // Check schema exploration keywords
            else if (matchesKeywords(lowerQuery, keywordHints.getJsonArray("schema_exploration", new JsonArray()))) {
                queryType = "schema_exploration";
                capabilities.addAll(Arrays.asList("database_access", "schema_intelligence"));
            }
            // Default to full pipeline
            else {
                queryType = hasAggregation ? "analysis" : "data_retrieval";
                capabilities.addAll(Arrays.asList("database_access", "llm_analysis", 
                    "schema_intelligence", "sql_generation"));
            }
        }
        
        intent.put("intent", extractIntent(query));
        intent.put("complexity", complexity);
        intent.put("requiresDatabase", requiresDatabase);
        intent.put("requiresTools", requiresTools);
        intent.put("capabilities", new JsonArray(capabilities));
        intent.put("queryType", queryType);
        intent.put("confidence", 0.7); // Lower confidence for rule-based
        intent.put("reasoning", "Rule-based analysis");
        intent.put("method", "rule_based");
        intent.put("originalQuery", query);
        
        return intent;
    }
    
    private JsonObject selectStrategyWithLLM(JsonObject intentAnalysis, JsonArray availableStrategies) 
            throws Exception {
        
        // Build strategy descriptions
        JsonObject strategyDescriptions = new JsonObject();
        for (int i = 0; i < availableStrategies.size(); i++) {
            String strategyName = availableStrategies.getString(i);
            JsonObject strategy = strategies.getJsonObject(strategyName);
            if (strategy != null) {
                strategyDescriptions.put(strategyName, strategy);
            }
        }
        
        String systemPrompt = """
            You are a strategy selector for database query processing.
            Based on the query intent analysis, select the most appropriate strategy.
            
            Available strategies:
            """ + strategyDescriptions.encodePrettily() + """
            
            Selection criteria:
            1. Match required capabilities with strategy capabilities
            2. Choose simplest strategy that meets the requirements
            3. Consider query complexity and type
            4. Prioritize strategies that avoid unnecessary steps
            
            Respond in JSON format:
            {
              "selectedStrategy": "strategy_name",
              "selectedHost": "host_name",
              "confidence": 0.0-1.0,
              "reasoning": "why this strategy was chosen",
              "alternativeStrategy": "backup_strategy_if_any"
            }
            """;
        
        JsonObject promptData = new JsonObject()
            .put("intentAnalysis", intentAnalysis)
            .put("availableStrategies", availableStrategies);
        
        List<JsonObject> messages = Arrays.asList(
            new JsonObject().put("role", "system").put("content", systemPrompt),
            new JsonObject().put("role", "user").put("content", promptData.encodePrettily())
        );
        
        JsonObject response = llmService.chatCompletion(
            messages.stream().map(JsonObject::encode).collect(Collectors.toList()),
            0.0,
            500
        ).join();
        
        String content = response.getJsonArray("choices")
            .getJsonObject(0)
            .getJsonObject("message")
            .getString("content");
        
        JsonObject selection = parseJsonFromContent(content);
        
        // Validate required fields
        if (!selection.containsKey("selectedStrategy") || selection.containsKey("error")) {
            vertx.eventBus().publish("log", "LLM returned incomplete strategy selection: " + selection.encode() + "" + ",1,QueryIntentEvaluationServer,MCP,System");
            // Use rule-based fallback
            return selectStrategyWithRules(intentAnalysis, availableStrategies);
        }
        
        // Add defaults for optional fields
        if (!selection.containsKey("confidence")) {
            selection.put("confidence", 0.7);
        }
        if (!selection.containsKey("reasoning")) {
            selection.put("reasoning", "LLM selection");
        }
        
        selection.put("method", "llm_selection");
        
        // Add strategy details
        String selectedStrategy = selection.getString("selectedStrategy");
        if (selectedStrategy != null && strategies.containsKey(selectedStrategy)) {
            selection.put("strategyDetails", strategies.getJsonObject(selectedStrategy));
        } else if (selectedStrategy != null) {
            // Strategy not found in config - log warning and use default
            vertx.eventBus().publish("log", "LLM selected unknown strategy: " + selectedStrategy + "" + ",1,QueryIntentEvaluationServer,MCP,System");
            String defaultStrategy = strategyRules.getString("default_strategy", "oracle_full_pipeline");
            selection.put("selectedStrategy", defaultStrategy);
            selection.put("confidence", 0.5);
            selection.put("reasoning", "Unknown strategy selected by LLM, using default");
            if (strategies.containsKey(defaultStrategy)) {
                selection.put("strategyDetails", strategies.getJsonObject(defaultStrategy));
            }
        }
        
        return selection;
    }
    
    private JsonObject selectStrategyWithRules(JsonObject intentAnalysis, JsonArray availableStrategies) {
        JsonObject selection = new JsonObject();
        
        String queryType = intentAnalysis.getString("queryType", "data_retrieval");
        String complexity = intentAnalysis.getString("complexity", "medium");
        boolean requiresDatabase = intentAnalysis.getBoolean("requiresDatabase", true);
        boolean requiresTools = intentAnalysis.getBoolean("requiresTools", true);
        JsonArray requiredCapabilities = intentAnalysis.getJsonArray("capabilities", new JsonArray());
        
        // Score each available strategy
        String bestStrategy = null;
        String bestHost = null;
        double bestScore = -1;
        String reasoning = "";
        
        for (int i = 0; i < availableStrategies.size(); i++) {
            String strategyName = availableStrategies.getString(i);
            JsonObject strategy = strategies.getJsonObject(strategyName);
            
            if (strategy == null) continue;
            
            double score = 0;
            
            // Check if strategy capabilities match requirements
            JsonArray strategyCapabilities = strategy.getJsonArray("capabilities_required", new JsonArray());
            int matchedCapabilities = 0;
            
            for (int j = 0; j < requiredCapabilities.size(); j++) {
                String required = requiredCapabilities.getString(j);
                for (int k = 0; k < strategyCapabilities.size(); k++) {
                    if (strategyCapabilities.getString(k).equals(required)) {
                        matchedCapabilities++;
                        break;
                    }
                }
            }
            
            // Score based on capability match
            if (requiredCapabilities.size() > 0) {
                score += (double) matchedCapabilities / requiredCapabilities.size() * 0.5;
            }
            
            // Score based on complexity match
            String strategyComplexity = strategy.getString("complexity", "medium");
            if (strategyComplexity.equals(complexity)) {
                score += 0.2;
            }
            
            // Specific scoring for query types
            switch (queryType) {
                case "information":
                    if (strategyName.contains("direct_llm")) score += 0.3;
                    break;
                case "sql_generation":
                    if (strategyName.contains("sql_only")) score += 0.3;
                    break;
                case "schema_exploration":
                    if (strategyName.contains("schema_exploration")) score += 0.3;
                    break;
                case "data_retrieval":
                case "analysis":
                    if (strategyName.contains("full_pipeline")) score += 0.3;
                    break;
            }
            
            // Penalty for overengineering
            if (!requiresDatabase && strategyName.contains("full_pipeline")) {
                score -= 0.2;
            }
            if (!requiresTools && !strategyName.contains("direct_llm")) {
                score -= 0.2;
            }
            
            if (score > bestScore) {
                bestScore = score;
                bestStrategy = strategyName;
                bestHost = strategy.getString("host");
                reasoning = buildReasoningForStrategy(strategyName, score, intentAnalysis);
            }
        }
        
        // Default if no good match
        if (bestStrategy == null && strategyRules != null) {
            bestStrategy = strategyRules.getString("default_strategy", "oracle_full_pipeline");
            bestHost = "oracledbanswerer";
            reasoning = "Using default strategy";
        }
        
        selection.put("selectedStrategy", bestStrategy);
        selection.put("selectedHost", bestHost);
        selection.put("confidence", Math.min(bestScore, 1.0));
        selection.put("reasoning", reasoning);
        selection.put("method", "rule_based");
        
        // Add strategy details
        if (bestStrategy != null && strategies.containsKey(bestStrategy)) {
            selection.put("strategyDetails", strategies.getJsonObject(bestStrategy));
        }
        
        return selection;
    }
    
    private void loadOrchestrationStrategies() {
        try {
            // Load from resources
            InputStream is = getClass().getResourceAsStream("/orchestration-strategies.json");
            if (is != null) {
                String content = new String(is.readAllBytes());
                orchestrationConfig = new JsonObject(content);
                strategies = orchestrationConfig.getJsonObject("strategies", new JsonObject());
                strategyRules = orchestrationConfig.getJsonObject("strategy_selection_rules", new JsonObject());
                hosts = orchestrationConfig.getJsonObject("hosts", new JsonObject());
                
                vertx.eventBus().publish("log", "Loaded " + strategies.size() + " orchestration strategies" + ",2,QueryIntentEvaluationServer,MCP,System");
            } else {
                vertx.eventBus().publish("log", "orchestration-strategies.json not found in resources,0,QueryIntentEvaluationServer,MCP,System");
                orchestrationConfig = new JsonObject();
                strategies = new JsonObject();
                strategyRules = new JsonObject();
                hosts = new JsonObject();
            }
        } catch (Exception e) {
            vertx.eventBus().publish("log", "Failed to load orchestration strategies" + ",0,QueryIntentEvaluationServer,MCP,System");
            orchestrationConfig = new JsonObject();
            strategies = new JsonObject();
            strategyRules = new JsonObject();
            hosts = new JsonObject();
        }
    }
    
    // Utility methods
    
    private int countConditions(String query) {
        int count = 0;
        String[] conditionWords = {"where", "and", "or", "having", "when", "if"};
        for (String word : conditionWords) {
            count += countOccurrences(query, word);
        }
        return count;
    }
    
    private int countOccurrences(String text, String word) {
        int count = 0;
        int index = 0;
        while ((index = text.indexOf(word, index)) != -1) {
            // Check word boundaries
            boolean validStart = index == 0 || !Character.isLetterOrDigit(text.charAt(index - 1));
            boolean validEnd = index + word.length() >= text.length() || 
                               !Character.isLetterOrDigit(text.charAt(index + word.length()));
            if (validStart && validEnd) {
                count++;
            }
            index += word.length();
        }
        return count;
    }
    
    private boolean hasAggregation(String query) {
        String[] aggregations = {"count", "sum", "avg", "average", "max", "min", 
                                "total", "group by", "having"};
        for (String agg : aggregations) {
            if (query.contains(agg)) {
                return true;
            }
        }
        return false;
    }
    
    private boolean hasMultipleTables(String query) {
        return query.contains("join") || 
               query.contains("from") && query.contains(",") ||
               countOccurrences(query, "table") > 1;
    }
    
    private boolean matchesKeywords(String query, JsonArray keywords) {
        if (keywords == null) return false;
        
        for (int i = 0; i < keywords.size(); i++) {
            String keyword = keywords.getString(i);
            if (query.contains(keyword)) {
                return true;
            }
        }
        return false;
    }
    
    private String extractIntent(String query) {
        // Simple intent extraction
        if (query.toLowerCase().startsWith("how many")) {
            return "Count items matching criteria";
        } else if (query.toLowerCase().startsWith("what is")) {
            return "Get information or definition";
        } else if (query.toLowerCase().startsWith("show me")) {
            return "Retrieve and display data";
        } else if (query.toLowerCase().startsWith("list")) {
            return "List items matching criteria";
        } else {
            return "Process user query";
        }
    }
    
    private String buildReasoningForStrategy(String strategy, double score, JsonObject intent) {
        StringBuilder reasoning = new StringBuilder();
        reasoning.append("Selected ").append(strategy).append(" based on: ");
        
        List<String> reasons = new ArrayList<>();
        if (intent.getString("queryType", "").equals("information") && strategy.contains("direct_llm")) {
            reasons.add("query seeks information without data retrieval");
        }
        if (intent.getString("queryType", "").equals("sql_generation") && strategy.contains("sql_only")) {
            reasons.add("user wants SQL generation only");
        }
        if (intent.getString("complexity", "").equals("complex") && strategy.contains("full_pipeline")) {
            reasons.add("complex query requires full processing");
        }
        if (!intent.getBoolean("requiresDatabase", true) && strategy.contains("direct_llm")) {
            reasons.add("no database access needed");
        }
        
        if (reasons.isEmpty()) {
            reasons.add("best match for requirements (score: " + String.format("%.2f", score) + ")");
        }
        
        reasoning.append(String.join(", ", reasons));
        return reasoning.toString();
    }
    
    private JsonObject parseJsonFromContent(String content) {
        try {
            // Try to find JSON in the content
            int startIdx = content.indexOf("{");
            int endIdx = content.lastIndexOf("}");
            
            if (startIdx != -1 && endIdx != -1 && endIdx > startIdx) {
                String jsonStr = content.substring(startIdx, endIdx + 1);
                return new JsonObject(jsonStr);
            }
        } catch (Exception e) {
            vertx.eventBus().publish("log", "Failed to extract JSON from content" + ",1,QueryIntentEvaluationServer,MCP,System");
        }
        
        // Return a default structure if parsing fails
        return new JsonObject()
            .put("error", "Failed to parse response")
            .put("confidence", 0.0);
    }
    
    /**
     * Select the best tool or orchestration strategy for a query
     * This is the comprehensive tool selection logic ported from ToolSelection.java
     */
    private void selectToolStrategy(RoutingContext ctx, String requestId, JsonObject arguments) {
        String query = arguments.getString("query");
        JsonArray conversationHistory = arguments.getJsonArray("conversationHistory", new JsonArray());
        JsonObject providedIntent = arguments.getJsonObject("intentAnalysis");
        
        if (query == null || query.trim().isEmpty()) {
            sendError(ctx, requestId, MCPResponse.ErrorCodes.INVALID_PARAMS, "Query is required");
            return;
        }
        
        // Step 1: Get or compute intent analysis
        executeBlocking(Promise.<JsonObject>promise(), promise -> {
            try {
                JsonObject intentAnalysis;
                if (providedIntent != null) {
                    intentAnalysis = providedIntent;
                } else {
                    // Compute intent analysis
                    if (llmService.isInitialized() && orchestrationConfig != null) {
                        intentAnalysis = evaluateWithLLM(query, conversationHistory);
                    } else {
                        intentAnalysis = evaluateWithRules(query);
                    }
                }
                
                // Step 2: Analyze query patterns for tool selection
                JsonObject queryPatterns = analyzeQueryPatterns(query, intentAnalysis);
                
                // Step 3: Determine strategy
                JsonObject decision = determineToolStrategy(query, intentAnalysis, queryPatterns);
                
                promise.complete(decision);
            } catch (Exception e) {
                vertx.eventBus().publish("log", "Tool strategy selection failed" + ",0,QueryIntentEvaluationServer,MCP,System");
                promise.fail(e);
            }
        }, res -> {
            if (res.succeeded()) {
                sendSuccess(ctx, requestId, res.result());
            } else {
                sendError(ctx, requestId, MCPResponse.ErrorCodes.INTERNAL_ERROR, 
                    "Tool strategy selection failed: " + res.cause().getMessage());
            }
        });
    }
    
    /**
     * Analyze query patterns for enhanced tool selection
     */
    private JsonObject analyzeQueryPatterns(String query, JsonObject intentAnalysis) {
        JsonObject patterns = new JsonObject();
        String lowerQuery = query.toLowerCase();
        
        // Enhanced entity detection
        JsonArray entities = new JsonArray();
        JsonArray businessTerms = new JsonArray();
        
        // Business status terms
        if (lowerQuery.contains("pending")) businessTerms.add("pending");
        if (lowerQuery.contains("active")) businessTerms.add("active");
        if (lowerQuery.contains("cancelled")) businessTerms.add("cancelled");
        if (lowerQuery.contains("completed")) businessTerms.add("completed");
        
        // Priority terms
        if (lowerQuery.contains("high priority") || lowerQuery.contains("urgent")) {
            businessTerms.add("high_priority");
        }
        if (lowerQuery.contains("low priority")) businessTerms.add("low_priority");
        
        // Location terms
        if (lowerQuery.contains("california") || lowerQuery.contains(" ca ")) {
            entities.add("california");
        }
        
        // Database entities
        if (lowerQuery.contains("order")) entities.add("orders");
        if (lowerQuery.contains("customer")) entities.add("customers");
        if (lowerQuery.contains("product")) entities.add("products");
        if (lowerQuery.contains("employee")) entities.add("employees");
        if (lowerQuery.contains("supplier")) entities.add("suppliers");
        
        // Detect query characteristics
        boolean hasBusinessTerms = !businessTerms.isEmpty();
        boolean hasMultipleEntities = entities.size() > 1;
        boolean hasAggregation = lowerQuery.contains("count") || lowerQuery.contains("sum") || 
                                lowerQuery.contains("average") || lowerQuery.contains("total") ||
                                lowerQuery.contains("group by");
        boolean hasRelationship = lowerQuery.contains("relationship") || lowerQuery.contains("between") ||
                                 lowerQuery.contains("join") || lowerQuery.contains("connected") ||
                                 lowerQuery.contains("related");
        boolean hasDiscovery = lowerQuery.contains("what kind") || lowerQuery.contains("show me data") ||
                              lowerQuery.contains("what data") || lowerQuery.contains("explore");
        boolean hasPerformance = lowerQuery.contains("top ") || lowerQuery.contains("bottom ") ||
                                lowerQuery.contains("largest") || lowerQuery.contains("smallest") ||
                                lowerQuery.contains("highest") || lowerQuery.contains("lowest");
        
        patterns.put("entities", entities);
        patterns.put("businessTerms", businessTerms);
        patterns.put("hasBusinessTerms", hasBusinessTerms);
        patterns.put("hasMultipleEntities", hasMultipleEntities);
        patterns.put("hasAggregation", hasAggregation);
        patterns.put("hasRelationship", hasRelationship);
        patterns.put("hasDiscovery", hasDiscovery);
        patterns.put("hasPerformance", hasPerformance);
        patterns.put("complexity", determineComplexity(intentAnalysis, patterns));
        
        return patterns;
    }
    
    /**
     * Determine complexity based on intent and patterns
     */
    private String determineComplexity(JsonObject intentAnalysis, JsonObject patterns) {
        String baseComplexity = intentAnalysis.getString("complexity", "simple");
        
        // Upgrade complexity based on patterns
        if (patterns.getBoolean("hasMultipleEntities", false) ||
            patterns.getBoolean("hasBusinessTerms", false) ||
            patterns.getBoolean("hasRelationship", false) ||
            (patterns.getBoolean("hasAggregation", false) && !baseComplexity.equals("simple"))) {
            return "complex";
        }
        
        return baseComplexity;
    }
    
    /**
     * Determine the best tool strategy based on comprehensive analysis
     */
    private JsonObject determineToolStrategy(String query, JsonObject intentAnalysis, JsonObject patterns) {
        JsonObject decision = new JsonObject();
        String lowerQuery = query.toLowerCase();
        
        // Check if this is a database-related query
        boolean isDatabaseQuery = false;
        JsonArray entities = patterns.getJsonArray("entities", new JsonArray());
        for (int i = 0; i < entities.size(); i++) {
            String entity = entities.getString(i);
            if (entity.contains("order") || entity.contains("customer") || 
                entity.contains("product") || entity.contains("table") ||
                entity.contains("database") || lowerQuery.contains("sql")) {
                isDatabaseQuery = true;
                break;
            }
        }
        
        // Non-database queries
        if (!isDatabaseQuery && !patterns.getBoolean("hasBusinessTerms", false)) {
            decision.put("strategy", "STANDARD_LLM");
            decision.put("confidence", 0.9);
            decision.put("reasoning", "Non-database query, using standard LLM");
            return decision;
        }
        
        // Database queries - select appropriate orchestration
        String orchestrationName = selectOracleOrchestrationPipeline(query, intentAnalysis, patterns);
        
        decision.put("strategy", "ORCHESTRATION");
        decision.put("orchestrationName", orchestrationName);
        decision.put("confidence", calculateConfidence(intentAnalysis, patterns));
        decision.put("reasoning", buildStrategyReasoning(orchestrationName, patterns));
        
        return decision;
    }
    
    /**
     * Select the appropriate Oracle orchestration pipeline
     * This is the intelligent selection logic from ToolSelection.java
     */
    private String selectOracleOrchestrationPipeline(String query, JsonObject analysis, JsonObject patterns) {
        String lowerQuery = query.toLowerCase();
        String intent = analysis.getString("intent", "");
        JsonArray entities = patterns.getJsonArray("entities", new JsonArray());
        
        // Discovery queries
        if (patterns.getBoolean("hasDiscovery", false) ||
            intent.equals("explore") || intent.equals("discover") ||
            lowerQuery.contains("what tables") || lowerQuery.contains("describe")) {
            return "oracle_discovery_first";
        }
        
        // Performance-critical queries
        if (patterns.getBoolean("hasPerformance", false) ||
            (patterns.getBoolean("hasAggregation", false) && 
             patterns.getString("complexity", "").equals("complex"))) {
            return "oracle_performance_focused";
        }
        
        // Simple queries
        if (patterns.getString("complexity", "").equals("simple") && 
            !patterns.getBoolean("hasMultipleEntities", false) &&
            !patterns.getBoolean("hasAggregation", false) &&
            !patterns.getBoolean("hasBusinessTerms", false)) {
            return "oracle_simple_query";
        }
        
        // Relationship queries
        if (patterns.getBoolean("hasRelationship", false) ||
            patterns.getBoolean("hasMultipleEntities", false)) {
            return "oracle_adaptive_pipeline";
        }
        
        // Business term queries needing semantic understanding
        if (patterns.getBoolean("hasBusinessTerms", false) ||
            lowerQuery.contains("category") || lowerQuery.contains("type") ||
            (entities.contains("california") && patterns.getString("complexity", "").equals("complex"))) {
            return "oracle_truly_intelligent";
        }
        
        // Complex queries benefit from intelligent handling
        if (patterns.getString("complexity", "").equals("complex")) {
            return "oracle_truly_intelligent";
        }
        
        // Default to validated pipeline
        return "oracle_validated_pipeline";
    }
    
    /**
     * Calculate confidence based on analysis quality
     */
    private double calculateConfidence(JsonObject intentAnalysis, JsonObject patterns) {
        double baseConfidence = intentAnalysis.getDouble("confidence", 0.7);
        
        // Boost confidence for clear patterns
        if (patterns.getBoolean("hasBusinessTerms", false)) {
            baseConfidence *= 1.1;
        }
        if (patterns.getBoolean("hasDiscovery", false) || 
            patterns.getBoolean("hasPerformance", false)) {
            baseConfidence *= 1.1;
        }
        
        return Math.min(0.95, baseConfidence);
    }
    
    /**
     * Build reasoning for strategy selection
     */
    private String buildStrategyReasoning(String strategyName, JsonObject patterns) {
        StringBuilder reasoning = new StringBuilder();
        reasoning.append("Selected ").append(strategyName).append(" because: ");
        
        List<String> reasons = new ArrayList<>();
        
        switch (strategyName) {
            case "oracle_discovery_first":
                reasons.add("query seeks to explore or discover data");
                break;
            case "oracle_performance_focused":
                reasons.add("performance-critical query with aggregations or limits");
                break;
            case "oracle_simple_query":
                reasons.add("simple single-table query");
                break;
            case "oracle_adaptive_pipeline":
                reasons.add("involves relationships between multiple entities");
                break;
            case "oracle_truly_intelligent":
                if (patterns.getBoolean("hasBusinessTerms", false)) {
                    reasons.add("contains business terms requiring semantic understanding");
                } else {
                    reasons.add("complex query benefits from intelligent processing");
                }
                break;
            case "oracle_validated_pipeline":
                reasons.add("standard query with validation steps");
                break;
        }
        
        reasoning.append(String.join(", ", reasons));
        return reasoning.toString();
    }
    
    /**
     * Get deployment options for this server (Regular Verticle)
     */
    public static DeploymentOptions getDeploymentOptions() {
        return new DeploymentOptions()
            .setWorker(false); // Not a worker verticle
    }
}
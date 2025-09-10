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
import java.util.regex.Pattern;
import static agents.director.Driver.logLevel;
import java.util.regex.Matcher;

/**
 * MCP Server for dynamic orchestration strategy generation.
 * Generates custom strategies using LLM intelligence with validation and fallback.
 */
public class StrategyGenerationServer extends MCPServerBase {
    
    private LlmAPIService llmService;
    
    // Validation patterns
    private static final Pattern TOOL_NAME_PATTERN = Pattern.compile("^[a-z_]+(__[a-z_]+)?$");
    
    // Pre-defined fallback strategies
    private static final JsonObject FALLBACK_SIMPLE_STRATEGY = new JsonObject()
        .put("name", "Fallback Simple Pipeline")
        .put("description", "Basic query processing pipeline")
        .put("method", "fallback")
        .put("steps", new JsonArray()
            .add(createStep(1, "analyze_query", "OracleQueryAnalysis", "Analyze query"))
            .add(createStep(2, "match_oracle_schema", "OracleSchemaIntelligence", "Find tables"))
            .add(createStep(3, "generate_oracle_sql", "OracleSQLGeneration", "Generate SQL"))
            .add(createStep(4, "run_oracle_query", "OracleQueryExecution", "Execute query")))
        .put("decision_points", new JsonArray())
        .put("adaptation_rules", new JsonObject()
            .put("allow_runtime_modification", false)
            .put("max_retries_per_step", 1));
    
    private static final JsonObject FALLBACK_COMPLEX_STRATEGY = new JsonObject()
        .put("name", "Fallback Complex Pipeline")
        .put("description", "Comprehensive query processing with validation")
        .put("method", "fallback")
        .put("steps", new JsonArray()
            .add(createStep(1, "evaluate_query_intent", "QueryIntentEvaluation", "Deep intent analysis"))
            .add(createStep(2, "analyze_query", "OracleQueryAnalysis", "Analyze query structure"))
            .add(createStep(3, "match_oracle_schema", "OracleSchemaIntelligence", "Find all related tables"))
            .add(createStep(4, "infer_table_relationships", "OracleSchemaIntelligence", "Discover relationships", true))
            .add(createStep(5, "map_business_terms", "BusinessMapping", "Map business terminology", true))
            .add(createStep(6, "generate_oracle_sql", "OracleSQLGeneration", "Generate complex SQL"))
            .add(createStep(7, "optimize_oracle_sql", "OracleSQLGeneration", "Optimize for performance", true))
            .add(createStep(8, "validate_oracle_sql", "OracleSQLValidation", "Validate SQL"))
            .add(createStep(9, "run_oracle_query", "OracleQueryExecution", "Execute with monitoring"))
            .add(createStep(10, "format_results", "OracleQueryExecution", "Format response", true)))
        .put("decision_points", new JsonArray())
        .put("adaptation_rules", new JsonObject()
            .put("allow_runtime_modification", true)
            .put("max_retries_per_step", 2));
    
    private static final JsonObject FALLBACK_SQL_ONLY_STRATEGY = new JsonObject()
        .put("name", "Fallback SQL Generation Only")
        .put("description", "Generate SQL without execution")
        .put("method", "fallback")
        .put("steps", new JsonArray()
            .add(createStep(1, "analyze_query", "OracleQueryAnalysis", "Analyze query"))
            .add(createStep(2, "match_oracle_schema", "OracleSchemaIntelligence", "Find schema elements"))
            .add(createStep(3, "generate_oracle_sql", "OracleSQLGeneration", "Generate SQL"))
            .add(createStep(4, "validate_oracle_sql", "OracleSQLValidation", "Validate SQL")))
        .put("decision_points", new JsonArray())
        .put("adaptation_rules", new JsonObject()
            .put("allow_runtime_modification", false)
            .put("max_retries_per_step", 1));
    
    public StrategyGenerationServer() {
        super("StrategyGenerationServer", "/mcp/servers/strategy-gen");
    }
    
    @Override
    public void start(Promise<Void> startPromise) {
        llmService = LlmAPIService.getInstance();
        
        if (!llmService.isInitialized()) {
            if (logLevel >= 1) vertx.eventBus().publish("log", "LLM service not initialized - will use fallback strategies only,1,StrategyGenerationServer,Init,Warning");
        }
        
        super.start(startPromise);
    }
    
    @Override
    protected void initializeTools() {
        // Register analyze_complexity tool
        registerTool(new MCPTool(
            "strategy_generation__analyze_complexity",
            "Analyzes query complexity to inform strategy generation",
            new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("query", new JsonObject()
                        .put("type", "string")
                        .put("description", "The user's query to analyze"))
                    .put("context", new JsonObject()
                        .put("type", "object")
                        .put("description", "Optional context including previous queries")
                        .put("properties", new JsonObject()
                            .put("previous_queries", new JsonObject()
                                .put("type", "array")
                                .put("items", new JsonObject().put("type", "string")))
                            .put("user_expertise_level", new JsonObject()
                                .put("type", "string")
                                .put("enum", new JsonArray().add("beginner").add("intermediate").add("expert"))))))
                .put("required", new JsonArray().add("query"))
        ));
        
        // Register create_strategy tool
        registerTool(new MCPTool(
            "strategy_generation__create_strategy",
            "Generate a custom orchestration strategy using LLM intelligence",
            new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("query", new JsonObject()
                        .put("type", "string")
                        .put("description", "The user's query"))
                    .put("intent", new JsonObject()
                        .put("type", "object")
                        .put("description", "Intent analysis result"))
                    .put("complexity_analysis", new JsonObject()
                        .put("type", "object")
                        .put("description", "Complexity analysis result"))
                    .put("available_tools", new JsonObject()
                        .put("type", "array")
                        .put("description", "List of available tools with their servers")
                        .put("items", new JsonObject()
                            .put("type", "object")
                            .put("properties", new JsonObject()
                                .put("name", new JsonObject().put("type", "string"))
                                .put("server", new JsonObject().put("type", "string")))))
                    .put("constraints", new JsonObject()
                        .put("type", "object")
                        .put("description", "Optional constraints")
                        .put("properties", new JsonObject()
                            .put("max_steps", new JsonObject()
                                .put("type", "integer")
                                .put("default", 15))
                            .put("timeout_seconds", new JsonObject()
                                .put("type", "integer")
                                .put("default", 120))
                            .put("required_validations", new JsonObject()
                                .put("type", "array")
                                .put("items", new JsonObject().put("type", "string"))))))
                .put("required", new JsonArray().add("query").add("intent").add("complexity_analysis").add("available_tools"))
        ));
        
        // Register optimize_strategy tool
        registerTool(new MCPTool(
            "strategy_generation__optimize_strategy",
            "Optimize an existing strategy for better performance",
            new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("strategy", new JsonObject()
                        .put("type", "object")
                        .put("description", "The strategy to optimize"))
                    .put("optimization_goals", new JsonObject()
                        .put("type", "array")
                        .put("description", "Goals like 'speed', 'accuracy', 'minimal_steps'")
                        .put("items", new JsonObject().put("type", "string")))
                    .put("performance_history", new JsonObject()
                        .put("type", "object")
                        .put("description", "Optional performance metrics from previous executions")))
                .put("required", new JsonArray().add("strategy"))
        ));
    }
    
    @Override
    protected void executeTool(RoutingContext ctx, String requestId, String toolName, JsonObject arguments) {
        switch (toolName) {
            case "strategy_generation__analyze_complexity":
                analyzeComplexity(ctx, requestId, arguments);
                break;
            case "strategy_generation__create_strategy":
                createStrategy(ctx, requestId, arguments);
                break;
            case "strategy_generation__optimize_strategy":
                optimizeStrategy(ctx, requestId, arguments);
                break;
            default:
                sendError(ctx, requestId, MCPResponse.ErrorCodes.METHOD_NOT_FOUND, 
                    "Unknown tool: " + toolName);
        }
    }
    
    private void analyzeComplexity(RoutingContext ctx, String requestId, JsonObject arguments) {
        String query = arguments.getString("query");
        JsonObject context = arguments.getJsonObject("context", new JsonObject());
        
        if (logLevel >= 3) vertx.eventBus().publish("log", "Analyzing complexity for query: " + query + ",3,StrategyGenerationServer,Complexity,Analysis");
        
        // Analyze various complexity factors
        float complexityScore = 0.0f;
        JsonObject factors = new JsonObject();
        
        // Count potential joins (words like "and", "with", "including", "related")
        int joinIndicators = countOccurrences(query.toLowerCase(), 
            Arrays.asList("and", "with", "including", "related", "between", "across"));
        factors.put("join_count", Math.min(joinIndicators, 5));
        complexityScore += joinIndicators * 0.15f;
        
        // Count aggregation indicators
        int aggregationIndicators = countOccurrences(query.toLowerCase(),
            Arrays.asList("sum", "total", "average", "count", "group", "by", "max", "min", "aggregate"));
        factors.put("aggregation_depth", Math.min(aggregationIndicators, 3));
        complexityScore += aggregationIndicators * 0.2f;
        
        // Check for subquery indicators
        int subqueryIndicators = countOccurrences(query.toLowerCase(),
            Arrays.asList("where", "having", "exists", "in", "not in", "all", "any"));
        factors.put("subquery_count", Math.min(subqueryIndicators, 4));
        complexityScore += subqueryIndicators * 0.15f;
        
        // Check for time-based queries
        boolean hasTimeComponent = query.toLowerCase().matches(".*\\b(year|month|day|date|time|period|quarter)\\b.*");
        if (hasTimeComponent) {
            complexityScore += 0.1f;
            factors.put("has_time_component", true);
        }
        
        // Check for comparison/calculation
        boolean hasCalculations = query.toLowerCase().matches(".*\\b(calculate|compare|versus|vs|difference|change|growth)\\b.*");
        if (hasCalculations) {
            complexityScore += 0.15f;
            factors.put("requires_calculations", true);
        }
        
        // Estimate data volume based on keywords
        String volumeEstimate = "medium";
        if (query.toLowerCase().contains("all") || query.toLowerCase().contains("every")) {
            volumeEstimate = "large";
            complexityScore += 0.1f;
        } else if (query.toLowerCase().contains("top") || query.toLowerCase().contains("first")) {
            volumeEstimate = "small";
        }
        factors.put("estimated_data_volume", volumeEstimate);
        
        // Calculate business logic complexity
        float businessComplexity = 0.0f;
        if (query.length() > 100) businessComplexity += 0.3f;
        if (context.getJsonArray("previous_queries", new JsonArray()).size() > 2) businessComplexity += 0.2f;
        factors.put("business_logic_complexity", businessComplexity);
        complexityScore += businessComplexity * 0.15f;
        
        // Normalize score
        complexityScore = Math.min(complexityScore, 1.0f);
        
        // Generate recommendations
        JsonArray recommendations = new JsonArray();
        if (complexityScore > 0.7) {
            recommendations.add("Consider breaking down into multiple sub-queries");
            recommendations.add("Use incremental validation steps");
        } else if (complexityScore < 0.3) {
            recommendations.add("Simple direct execution recommended");
            recommendations.add("Minimal validation needed");
        }
        
        JsonObject result = new JsonObject()
            .put("complexity_score", complexityScore)
            .put("factors", factors)
            .put("recommendations", recommendations)
            .put("suggested_strategy_type", complexityScore > 0.6 ? "complex" : 
                                          complexityScore > 0.3 ? "medium" : "simple");
        
        sendSuccess(ctx, requestId, new JsonObject().put("result", result));
    }
    
    private void createStrategy(RoutingContext ctx, String requestId, JsonObject arguments) {
        // Validate required parameters
        String query = arguments.getString("query");
        if (query == null || query.trim().isEmpty()) {
            sendError(ctx, requestId, MCPResponse.ErrorCodes.INVALID_PARAMS, "Missing required parameter: query");
            return;
        }
        
        // Get parameters with defaults
        JsonObject intent = arguments.getJsonObject("intent", new JsonObject());
        JsonObject complexityAnalysis = arguments.getJsonObject("complexity_analysis", new JsonObject());
        JsonArray availableTools = arguments.getJsonArray("available_tools");
        JsonObject constraints = arguments.getJsonObject("constraints", new JsonObject());
        
        // If available_tools is not provided, use default tool set
        if (availableTools == null || availableTools.isEmpty()) {
            availableTools = getDefaultAvailableTools();
        }
        
        if (logLevel >= 1) vertx.eventBus().publish("log", "Creating dynamic strategy for query: " + query + ",1,StrategyGenerationServer,Strategy,Create");
        
        // If LLM is not available, use fallback immediately
        if (!llmService.isInitialized()) {
            if (logLevel >= 1) vertx.eventBus().publish("log", "Using fallback strategy due to: LLM service not initialized,1,StrategyGenerationServer,Strategy,Fallback");
            JsonObject fallback = selectFallbackStrategy(intent, complexityAnalysis);
            fallback.put("generation_method", "fallback_no_llm");
            fallback.put("fallback_reason", "LLM service not initialized");
            sendSuccess(ctx, requestId, new JsonObject().put("result", fallback));
            return;
        }
        
        // Try LLM generation once only - no retries for reliability
        String prompt = buildStrategyPrompt(query, intent, complexityAnalysis, availableTools);
        
        // Convert prompt to messages array for chatCompletion
        JsonArray messages = new JsonArray()
            .add(new JsonObject()
                .put("role", "system")
                .put("content", "You are a strategy generation assistant. Generate structured JSON strategies for database queries."))
            .add(new JsonObject()
                .put("role", "user")
                .put("content", prompt));
        
        if (logLevel >= 2) vertx.eventBus().publish("log", "Attempting LLM strategy generation (single attempt),2,StrategyGenerationServer,Strategy,LLM");
        
        llmService.chatCompletion(messages)
            .onComplete(ar -> {
                boolean useFallback = false;
                String fallbackReason = null;
                
                if (ar.succeeded()) {
                    try {
                        JsonObject response = ar.result();
                        JsonArray choices = response.getJsonArray("choices", new JsonArray());
                        
                        if (choices.isEmpty()) {
                            useFallback = true;
                            fallbackReason = "LLM returned empty choices array";
                        } else {
                            JsonObject firstChoice = choices.getJsonObject(0);
                            JsonObject message = firstChoice.getJsonObject("message", new JsonObject());
                            String content = message.getString("content", "");
                            
                            if (content.isEmpty()) {
                                useFallback = true;
                                fallbackReason = "LLM returned empty content";
                            } else {
                                JsonObject strategy = parseStrategyFromLLM(content);
                                ValidationResult validation = validateStrategySchema(strategy);
                                
                                if (validation.isValid() && strategy.containsKey("name") && !strategy.getString("name", "").isEmpty()) {
                                    if (logLevel >= 1) vertx.eventBus().publish("log", "Successfully generated valid strategy via LLM: " + strategy.getString("name") + ",1,StrategyGenerationServer,Strategy,Valid");
                                    strategy.put("generation_method", "dynamic_llm");
                                    sendSuccess(ctx, requestId, new JsonObject().put("result", strategy));
                                } else {
                                    useFallback = true;
                                    if (!validation.isValid()) {
                                        fallbackReason = "Strategy validation failed: " + String.join("; ", validation.getErrors());
                                    } else {
                                        fallbackReason = "Strategy missing or has empty name field";
                                    }
                                }
                            }
                        }
                    } catch (Exception e) {
                        useFallback = true;
                        fallbackReason = "Failed to parse strategy JSON: " + e.getMessage();
                    }
                } else {
                    useFallback = true;
                    fallbackReason = "LLM API call failed: " + ar.cause().getMessage();
                }
                
                if (useFallback) {
                    // Use fallback strategy immediately
                    if (logLevel >= 1) vertx.eventBus().publish("log", "Using fallback strategy due to: " + fallbackReason + ",1,StrategyGenerationServer,Strategy,Fallback");
                    JsonObject fallback = selectFallbackStrategy(intent, complexityAnalysis);
                    
                    // Double-check fallback has a name
                    if (!fallback.containsKey("name") || fallback.getString("name", "").isEmpty()) {
                        fallback.put("name", "Fallback Strategy - " + intent.getString("primary_intent", "general"));
                        vertx.eventBus().publish("log", "WARNING: Had to add name to fallback strategy,0,StrategyGenerationServer,Strategy,Warning");
                    }
                    
                    fallback.put("generation_method", "fallback_after_error");
                    fallback.put("fallback_reason", fallbackReason);
                    sendSuccess(ctx, requestId, new JsonObject().put("result", fallback));
                }
            });
    }
    
    
    private String buildStrategyPrompt(String query, JsonObject intent, JsonObject complexityAnalysis, JsonArray availableTools) {
        // Build a formatted list of available tools
        StringBuilder toolsList = new StringBuilder();
        toolsList.append("AVAILABLE TOOLS (You MUST only use these tools):\n");
        for (int i = 0; i < availableTools.size(); i++) {
            JsonObject tool = availableTools.getJsonObject(i);
            String toolName = tool.getString("name");
            String server = tool.getString("server");
            toolsList.append(String.format("- Tool: %s, Server: %s\n", toolName, server));
        }
        
        return String.format("""
            Generate a dynamic orchestration strategy for this database query.
            
            Query: %s
            Intent: %s
            Complexity: %s
            
            %s
            
            IMPORTANT: Output ONLY valid JSON matching the schema below. No explanations, no markdown, just JSON.
            CRITICAL: You MUST only use tools from the AVAILABLE TOOLS list above. Do NOT invent or guess tool names.
            
            Required JSON Schema:
            {
              "name": "string - descriptive name for the strategy",
              "description": "string - brief description of approach",
              "steps": [
                {
                  "step": 1,
                  "tool": "tool_name",
                  "server": "server_name",
                  "description": "what this step does",
                  "optional": false
                }
              ],
              "adaptation_rules": {
                "allow_runtime_modification": false,
                "max_retries_per_step": 2
              }
            }
            
            Optional fields for steps:
            - "optional": true/false (default: false)
            - "depends_on": [1, 2] (array of step numbers this depends on)
            - "parallel_allowed": true/false (can run in parallel with other steps)
            
            Optional fields for strategy:
            - "decision_points": array of decision points (see examples below)
            
            Example strategies for different scenarios:
            
            1. SIMPLE QUERY (single table, basic filters):
            {
              "name": "Simple Query Pipeline",
              "description": "Streamlined pipeline for straightforward single-table queries",
              "steps": [
                {"step": 1, "tool": "analyze_query", "server": "OracleQueryAnalysis", "description": "Quick semantic analysis"},
                {"step": 2, "tool": "get_oracle_schema", "server": "OracleQueryExecution", "description": "Get actual database schema"},
                {"step": 3, "tool": "match_oracle_schema", "server": "OracleSchemaIntelligence", "description": "Find the target table"},
                {"step": 4, "tool": "generate_oracle_sql", "server": "OracleSQLGeneration", "description": "Generate simple SQL"},
                {"step": 5, "tool": "run_oracle_query", "server": "OracleQueryExecution", "description": "Execute and return results"}
              ],
              "adaptation_rules": {"allow_runtime_modification": false, "max_retries_per_step": 1}
            }
            
            2. COMPLEX ANALYTICAL (multiple joins, aggregations):
            {
              "name": "Complex Analytical Pipeline",
              "description": "Comprehensive pipeline for multi-table queries with aggregations",
              "steps": [
                {"step": 1, "tool": "evaluate_query_intent", "server": "QueryIntentEvaluation", "description": "Deep intent and requirement analysis"},
                {"step": 2, "tool": "analyze_query", "server": "OracleQueryAnalysis", "description": "Extract all entities and relationships"},
                {"step": 3, "tool": "match_oracle_schema", "server": "OracleSchemaIntelligence", "description": "Find all related tables and columns"},
                {"step": 4, "tool": "infer_table_relationships", "server": "OracleSchemaIntelligence", "description": "Discover join paths", "parallel_allowed": true},
                {"step": 5, "tool": "map_business_terms", "server": "BusinessMapping", "description": "Map business terminology", "optional": true},
                {"step": 6, "tool": "generate_oracle_sql", "server": "OracleSQLGeneration", "description": "Generate complex SQL with joins", "depends_on": [4, 5]},
                {"step": 7, "tool": "optimize_oracle_sql", "server": "OracleSQLGeneration", "description": "Optimize for performance", "optional": true},
                {"step": 8, "tool": "validate_oracle_sql", "server": "OracleSQLValidation", "description": "Comprehensive validation"},
                {"step": 9, "tool": "run_oracle_query", "server": "OracleQueryExecution", "description": "Execute with performance monitoring"},
                {"step": 10, "tool": "format_results", "server": "OracleQueryExecution", "description": "Format into readable response", "optional": true}
              ],
              "decision_points": [
                {
                  "after_step": 3,
                  "condition": "more than 5 tables matched",
                  "action": "insert_step",
                  "parameters": {"tool": "refine_schema_matches", "server": "OracleSchemaIntelligence"}
                }
              ],
              "adaptation_rules": {"allow_runtime_modification": true, "max_retries_per_step": 2}
            }
            
            3. EXPLORATORY/DISCOVERY (unknown schema):
            {
              "name": "Schema Discovery Pipeline",
              "description": "Exploration-first approach for unfamiliar data",
              "steps": [
                {"step": 1, "tool": "analyze_query", "server": "OracleQueryAnalysis", "description": "Understand exploration intent"},
                {"step": 2, "tool": "get_oracle_schema", "server": "OracleQueryExecution", "description": "Retrieve complete schema information"},
                {"step": 3, "tool": "match_oracle_schema", "server": "OracleSchemaIntelligence", "description": "Fuzzy match relevant tables"},
                {"step": 4, "tool": "discover_sample_data", "server": "OracleSchemaIntelligence", "description": "Get sample data", "parallel_allowed": true},
                {"step": 5, "tool": "discover_column_semantics", "server": "OracleSchemaIntelligence", "description": "Analyze column meanings", "optional": true},
                {"step": 6, "tool": "generate_exploration_suggestions", "server": "OracleSQLGeneration", "description": "Suggest queries", "depends_on": [4, 5]}
              ],
              "adaptation_rules": {"allow_runtime_modification": true, "max_retries_per_step": 3}
            }
            
            NOTE: decision_points are optional and only needed for complex conditional logic.
            
            4. FILTERED QUERY (with status/location filters):
            {
              "name": "Filtered Query Pipeline",
              "description": "For queries with specific filters like status or location",
              "steps": [
                {"step": 1, "tool": "analyze_query", "server": "OracleQueryAnalysis", "description": "Extract filters and conditions"},
                {"step": 2, "tool": "get_oracle_schema", "server": "OracleQueryExecution", "description": "Retrieve actual table schemas"},
                {"step": 3, "tool": "map_business_terms", "server": "BusinessMapping", "description": "Map filter terms to database columns"},
                {"step": 4, "tool": "match_oracle_schema", "server": "OracleSchemaIntelligence", "description": "Match tables with mapped terms"},
                {"step": 5, "tool": "generate_oracle_sql", "server": "OracleSQLGeneration", "description": "Generate SQL with proper columns"},
                {"step": 6, "tool": "validate_oracle_sql", "server": "OracleSQLValidation", "description": "Validate column names"},
                {"step": 7, "tool": "run_oracle_query", "server": "OracleQueryExecution", "description": "Execute validated query"},
                {"step": 8, "tool": "format_results", "server": "OracleQueryExecution", "description": "Format the results"}
              ],
              "adaptation_rules": {"allow_runtime_modification": true, "max_retries_per_step": 2}
            }
            
            Now generate an optimal strategy for the given query. Consider:
            - The specific intent type: %s
            - Complexity score of %s suggests a %s approach
            - Whether validation steps are critical for this query type
            - Which steps can run in parallel to improve performance
            - Appropriate decision points for runtime adaptation
            - FOR QUERIES WITH FILTERS: Always include get_oracle_schema early to ensure correct column names
            
            Remember: Output ONLY the JSON strategy, no other text.
            """, 
            query, 
            intent.encode(), 
            complexityAnalysis.encode(),
            toolsList.toString(),
            intent.getString("primary_intent", "unknown"),
            complexityAnalysis.getFloat("complexity_score", 0.5f),
            complexityAnalysis.getString("suggested_strategy_type", "medium"));
    }
    
    private JsonObject parseStrategyFromLLM(String llmResponse) {
        // Extract JSON from the response
        String response = llmResponse.trim();
        
        // Find the first { and last }
        int start = response.indexOf("{");
        int end = response.lastIndexOf("}");
        
        if (start >= 0 && end > start) {
            String jsonStr = response.substring(start, end + 1);
            return new JsonObject(jsonStr);
        }
        
        throw new IllegalArgumentException("No valid JSON found in LLM response");
    }
    
    private ValidationResult validateStrategySchema(JsonObject strategy) {
        List<String> errors = new ArrayList<>();
        
        // Required top-level fields
        if (!strategy.containsKey("name") || strategy.getString("name", "").isEmpty()) {
            errors.add("Missing or empty 'name' field");
        }
        if (!strategy.containsKey("description") || strategy.getString("description", "").isEmpty()) {
            errors.add("Missing or empty 'description' field");
        }
        if (!strategy.containsKey("steps")) {
            errors.add("Missing 'steps' field");
        }
        
        // Validate steps array
        JsonArray steps = strategy.getJsonArray("steps");
        if (steps == null || steps.isEmpty()) {
            errors.add("Steps array is empty or null");
        } else {
            Set<Integer> stepNumbers = new HashSet<>();
            for (int i = 0; i < steps.size(); i++) {
                try {
                    JsonObject step = steps.getJsonObject(i);
                    validateStep(step, i, stepNumbers, errors);
                } catch (ClassCastException e) {
                    errors.add("Step " + i + " is not a valid JSON object");
                }
            }
        }
        
        // Validate optional decision_points if present
        if (strategy.containsKey("decision_points")) {
            JsonArray decisionPoints = strategy.getJsonArray("decision_points");
            if (decisionPoints != null) {
                for (int i = 0; i < decisionPoints.size(); i++) {
                    try {
                        JsonObject dp = decisionPoints.getJsonObject(i);
                        validateDecisionPoint(dp, i, errors);
                    } catch (ClassCastException e) {
                        errors.add("Decision point " + i + " is not a valid JSON object");
                    }
                }
            }
        }
        
        return new ValidationResult(errors.isEmpty(), errors);
    }
    
    private void validateStep(JsonObject step, int index, Set<Integer> stepNumbers, List<String> errors) {
        String prefix = "Step " + index + ": ";
        
        // Required fields
        if (!step.containsKey("step")) {
            errors.add(prefix + "Missing 'step' number");
        } else {
            Integer stepNum = step.getInteger("step");
            if (stepNum == null || stepNum <= 0) {
                errors.add(prefix + "Invalid step number");
            } else if (!stepNumbers.add(stepNum)) {
                errors.add(prefix + "Duplicate step number: " + stepNum);
            }
        }
        
        if (!step.containsKey("tool") || step.getString("tool", "").isEmpty()) {
            errors.add(prefix + "Missing or empty 'tool'");
        } else {
            String tool = step.getString("tool");
            if (!TOOL_NAME_PATTERN.matcher(tool).matches()) {
                errors.add(prefix + "Invalid tool name format: " + tool);
            }
        }
        
        if (!step.containsKey("server") || step.getString("server", "").isEmpty()) {
            errors.add(prefix + "Missing or empty 'server'");
        }
        
        if (!step.containsKey("description") || step.getString("description", "").isEmpty()) {
            errors.add(prefix + "Missing or empty 'description'");
        }
        
        // Optional fields validation
        if (step.containsKey("depends_on")) {
            JsonArray deps = step.getJsonArray("depends_on");
            if (deps != null) {
                for (int i = 0; i < deps.size(); i++) {
                    Object value = deps.getValue(i);
                    if (!(value instanceof Integer)) {
                        errors.add(prefix + "depends_on must contain integers (step numbers), found: " + 
                                  (value == null ? "null" : value.getClass().getSimpleName()));
                        break;
                    }
                }
            }
        }
        
        // Validate optional parallel_allowed field
        if (step.containsKey("parallel_allowed")) {
            Object value = step.getValue("parallel_allowed");
            if (!(value instanceof Boolean)) {
                errors.add(prefix + "parallel_allowed must be a boolean");
            }
        }
    }
    
    private void validateDecisionPoint(JsonObject dp, int index, List<String> errors) {
        String prefix = "Decision point " + index + ": ";
        
        if (!dp.containsKey("after_step")) {
            errors.add(prefix + "Missing 'after_step'");
        }
        if (!dp.containsKey("condition") || dp.getString("condition", "").isEmpty()) {
            errors.add(prefix + "Missing or empty 'condition'");
        }
        if (!dp.containsKey("action")) {
            errors.add(prefix + "Missing 'action'");
        } else {
            String action = dp.getString("action");
            if (!Arrays.asList("insert_step", "skip_to_step", "branch").contains(action)) {
                errors.add(prefix + "Invalid action: " + action);
            }
        }
    }
    
    private JsonObject selectFallbackStrategy(JsonObject intent, JsonObject complexityAnalysis) {
        String primaryIntent = intent.getString("primary_intent", "unknown");
        float complexityScore = complexityAnalysis.getFloat("complexity_score", 0.5f);
        
        if (logLevel >= 1) vertx.eventBus().publish("log", "Selecting fallback strategy for intent: " + primaryIntent + 
            "; complexity: " + complexityScore + ",1,StrategyGenerationServer,Fallback,Select");
        
        JsonObject selectedStrategy = null;
        
        // Select based on intent first, then complexity
        if (primaryIntent.equals("get_sql_only")) {
            selectedStrategy = FALLBACK_SQL_ONLY_STRATEGY.copy();
        } else if (complexityScore < 0.3) {
            selectedStrategy = FALLBACK_SIMPLE_STRATEGY.copy();
        } else if (complexityScore > 0.7) {
            selectedStrategy = FALLBACK_COMPLEX_STRATEGY.copy();
        } else {
            // Medium complexity - use complex but mark some steps as optional
            selectedStrategy = FALLBACK_COMPLEX_STRATEGY.copy();
            selectedStrategy.put("name", "Fallback Medium Pipeline");
        }
        
        // CRITICAL: Ensure the selected strategy has a valid name field
        if (!selectedStrategy.containsKey("name") || selectedStrategy.getString("name", "").isEmpty()) {
            // This should never happen with properly defined fallback strategies, but add safety check
            selectedStrategy.put("name", "Fallback Strategy - " + primaryIntent);
            vertx.eventBus().publish("log", "WARNING: Fallback strategy missing name field - added default,0,StrategyGenerationServer,Fallback,Warning");
        }
        
        // Ensure all other required fields are present
        if (!selectedStrategy.containsKey("description")) {
            selectedStrategy.put("description", "Fallback strategy for query processing");
        }
        if (!selectedStrategy.containsKey("steps") || selectedStrategy.getJsonArray("steps").isEmpty()) {
            // Use the complex strategy steps as default
            selectedStrategy.put("steps", FALLBACK_COMPLEX_STRATEGY.getJsonArray("steps").copy());
            vertx.eventBus().publish("log", "WARNING: Fallback strategy missing steps - using complex strategy steps,0,StrategyGenerationServer,Fallback,Warning");
        }
        if (!selectedStrategy.containsKey("method")) {
            selectedStrategy.put("method", "fallback");
        }
        
        // Double-check that the name is not "Unknown Strategy"
        if (selectedStrategy.getString("name", "").equals("Unknown Strategy")) {
            selectedStrategy.put("name", "Fallback " + primaryIntent.substring(0, 1).toUpperCase() + primaryIntent.substring(1) + " Pipeline");
            vertx.eventBus().publish("log", "WARNING: Detected 'Unknown Strategy' name - replaced with proper fallback name,0,StrategyGenerationServer,Fallback,Warning");
        }
        
        // Log the selected strategy name for debugging
        if (logLevel >= 2) vertx.eventBus().publish("log", "Selected fallback strategy: " + selectedStrategy.getString("name") + ",2,StrategyGenerationServer,Fallback,Selected");
        
        return selectedStrategy;
    }
    
    private JsonArray getDefaultAvailableTools() {
        // Return default set of available tools for Oracle DB operations
        return new JsonArray()
            .add(new JsonObject()
                .put("name", "evaluate_query_intent")
                .put("server", "QueryIntentEvaluation"))
            .add(new JsonObject()
                .put("name", "analyze_query")
                .put("server", "OracleQueryAnalysis"))
            .add(new JsonObject()
                .put("name", "match_oracle_schema")
                .put("server", "OracleSchemaIntelligence"))
            .add(new JsonObject()
                .put("name", "infer_table_relationships")
                .put("server", "OracleSchemaIntelligence"))
            .add(new JsonObject()
                .put("name", "map_business_terms")
                .put("server", "BusinessMapping"))
            .add(new JsonObject()
                .put("name", "generate_oracle_sql")
                .put("server", "OracleSQLGeneration"))
            .add(new JsonObject()
                .put("name", "optimize_oracle_sql")
                .put("server", "OracleSQLGeneration"))
            .add(new JsonObject()
                .put("name", "validate_oracle_sql")
                .put("server", "OracleSQLValidation"))
            .add(new JsonObject()
                .put("name", "run_oracle_query")
                .put("server", "OracleQueryExecution"))
            .add(new JsonObject()
                .put("name", "format_results")
                .put("server", "OracleQueryExecution"));
    }
    
    private void optimizeStrategy(RoutingContext ctx, String requestId, JsonObject arguments) {
        JsonObject strategy = arguments.getJsonObject("strategy");
        JsonArray optimizationGoals = arguments.getJsonArray("optimization_goals", new JsonArray());
        JsonObject performanceHistory = arguments.getJsonObject("performance_history", new JsonObject());
        
        if (logLevel >= 3) vertx.eventBus().publish("log", "Optimizing strategy: " + strategy.getString("name") + ",3,StrategyGenerationServer,Strategy,Optimize");
        
        // Simple optimization logic for now
        JsonObject optimized = strategy.copy();
        JsonArray improvements = new JsonArray();
        
        // Check for speed optimization
        if (optimizationGoals.contains("speed")) {
            // Mark non-critical steps as optional
            JsonArray steps = optimized.getJsonArray("steps");
            for (int i = 0; i < steps.size(); i++) {
                JsonObject step = steps.getJsonObject(i);
                String tool = step.getString("tool");
                if (tool.contains("validate") || tool.contains("format") || tool.contains("optimize")) {
                    step.put("optional", true);
                    improvements.add("Made " + tool + " optional for speed");
                }
            }
        }
        
        // Check for accuracy optimization
        if (optimizationGoals.contains("accuracy")) {
            // Ensure all validation steps are required
            JsonArray steps = optimized.getJsonArray("steps");
            for (int i = 0; i < steps.size(); i++) {
                JsonObject step = steps.getJsonObject(i);
                String tool = step.getString("tool");
                if (tool.contains("validate")) {
                    step.put("optional", false);
                    improvements.add("Made " + tool + " required for accuracy");
                }
            }
        }
        
        JsonObject result = new JsonObject()
            .put("optimized_strategy", optimized)
            .put("improvements", improvements)
            .put("estimated_speedup", optimizationGoals.contains("speed") ? 1.3f : 1.0f);
        
        sendSuccess(ctx, requestId, new JsonObject().put("result", result));
    }
    
    // Helper methods
    private static JsonObject createStep(int stepNum, String tool, String server, String description) {
        return createStep(stepNum, tool, server, description, false);
    }
    
    private static JsonObject createStep(int stepNum, String tool, String server, String description, boolean optional) {
        return new JsonObject()
            .put("step", stepNum)
            .put("tool", tool)
            .put("server", server)
            .put("description", description)
            .put("optional", optional);
    }
    
    private int countOccurrences(String text, List<String> words) {
        int count = 0;
        for (String word : words) {
            Pattern pattern = Pattern.compile("\\b" + word + "\\b");
            Matcher matcher = pattern.matcher(text);
            while (matcher.find()) {
                count++;
            }
        }
        return count;
    }
    
    // Inner classes
    private static class ValidationResult {
        private final boolean valid;
        private final List<String> errors;
        
        ValidationResult(boolean valid, List<String> errors) {
            this.valid = valid;
            this.errors = errors;
        }
        
        boolean isValid() {
            return valid;
        }
        
        List<String> getErrors() {
            return errors;
        }
    }
}
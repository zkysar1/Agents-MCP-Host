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
    
    private static final int MAX_GENERATION_ATTEMPTS = 2;
    private LlmAPIService llmService;
    
    // Validation patterns
    private static final Pattern TOOL_NAME_PATTERN = Pattern.compile("^[a-z_]+(__[a-z_]+)?$");
    
    // Pre-defined fallback strategies
    private static final JsonObject FALLBACK_SIMPLE_STRATEGY = new JsonObject()
        .put("name", "Fallback Simple Pipeline")
        .put("description", "Basic query processing pipeline")
        .put("method", "fallback")
        .put("steps", new JsonArray()
            .add(createStep(1, "analyze_query", "oracle-query-analysis", "Analyze query"))
            .add(createStep(2, "match_oracle_schema", "oracle-schema-intel", "Find tables"))
            .add(createStep(3, "generate_oracle_sql", "oracle-sql-gen", "Generate SQL"))
            .add(createStep(4, "run_oracle_query", "oracle-db", "Execute query")))
        .put("decision_points", new JsonArray())
        .put("adaptation_rules", new JsonObject()
            .put("allow_runtime_modification", false)
            .put("max_retries_per_step", 1));
    
    private static final JsonObject FALLBACK_COMPLEX_STRATEGY = new JsonObject()
        .put("name", "Fallback Complex Pipeline")
        .put("description", "Comprehensive query processing with validation")
        .put("method", "fallback")
        .put("steps", new JsonArray()
            .add(createStep(1, "evaluate_query_intent", "query-intent", "Deep intent analysis"))
            .add(createStep(2, "analyze_query", "oracle-query-analysis", "Analyze query structure"))
            .add(createStep(3, "match_oracle_schema", "oracle-schema-intel", "Find all related tables"))
            .add(createStep(4, "infer_table_relationships", "oracle-schema-intel", "Discover relationships", true))
            .add(createStep(5, "map_business_terms", "business-map", "Map business terminology", true))
            .add(createStep(6, "generate_oracle_sql", "oracle-sql-gen", "Generate complex SQL"))
            .add(createStep(7, "optimize_oracle_sql", "oracle-sql-gen", "Optimize for performance", true))
            .add(createStep(8, "validate_oracle_sql", "oracle-sql-val", "Validate SQL"))
            .add(createStep(9, "run_oracle_query", "oracle-db", "Execute with monitoring"))
            .add(createStep(10, "format_results", "oracle-db", "Format response", true)))
        .put("decision_points", new JsonArray())
        .put("adaptation_rules", new JsonObject()
            .put("allow_runtime_modification", true)
            .put("max_retries_per_step", 2));
    
    private static final JsonObject FALLBACK_SQL_ONLY_STRATEGY = new JsonObject()
        .put("name", "Fallback SQL Generation Only")
        .put("description", "Generate SQL without execution")
        .put("method", "fallback")
        .put("steps", new JsonArray()
            .add(createStep(1, "analyze_query", "oracle-query-analysis", "Analyze query"))
            .add(createStep(2, "match_oracle_schema", "oracle-schema-intel", "Find schema elements"))
            .add(createStep(3, "generate_oracle_sql", "oracle-sql-gen", "Generate SQL"))
            .add(createStep(4, "validate_oracle_sql", "oracle-sql-val", "Validate SQL")))
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
                .put("required", new JsonArray().add("query").add("intent").add("complexity_analysis"))
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
        String query = arguments.getString("query");
        JsonObject intent = arguments.getJsonObject("intent");
        JsonObject complexityAnalysis = arguments.getJsonObject("complexity_analysis");
        JsonObject constraints = arguments.getJsonObject("constraints", new JsonObject());
        
        if (logLevel >= 1) vertx.eventBus().publish("log", "Creating dynamic strategy for query: " + query + ",1,StrategyGenerationServer,Strategy,Create");
        
        // If LLM is not available, use fallback immediately
        if (!llmService.isInitialized()) {
            if (logLevel >= 1) vertx.eventBus().publish("log", "LLM service not available - using fallback strategy,1,StrategyGenerationServer,Strategy,Fallback");
            JsonObject fallback = selectFallbackStrategy(intent, complexityAnalysis);
            fallback.put("generation_method", "fallback_no_llm");
            sendSuccess(ctx, requestId, new JsonObject().put("result", fallback));
            return;
        }
        
        // Attempt to generate strategy with LLM
        generateStrategyWithRetry(query, intent, complexityAnalysis, constraints, 0)
            .onComplete(ar -> {
                if (ar.succeeded()) {
                    sendSuccess(ctx, requestId, new JsonObject().put("result", ar.result()));
                } else {
                    vertx.eventBus().publish("log", "Strategy generation failed after retries: " + ar.cause().getMessage() + ",0,StrategyGenerationServer,Strategy,Failed");
                    
                    // Use fallback strategy
                    JsonObject fallback = selectFallbackStrategy(intent, complexityAnalysis);
                    fallback.put("generation_method", "fallback_after_error");
                    fallback.put("fallback_reason", ar.cause().getMessage());
                    sendSuccess(ctx, requestId, new JsonObject().put("result", fallback));
                }
            });
    }
    
    private Future<JsonObject> generateStrategyWithRetry(String query, JsonObject intent, 
                                                       JsonObject complexityAnalysis, 
                                                       JsonObject constraints, int attempt) {
        Promise<JsonObject> promise = Promise.promise();
        
        if (attempt >= MAX_GENERATION_ATTEMPTS) {
            promise.fail("Max generation attempts exceeded");
            return promise.future();
        }
        
        String prompt = buildStrategyPrompt(query, intent, complexityAnalysis);
        
        // Convert prompt to messages array for chatCompletion
        JsonArray messages = new JsonArray()
            .add(new JsonObject()
                .put("role", "system")
                .put("content", "You are a strategy generation assistant. Generate structured JSON strategies for database queries."))
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
                        JsonObject strategy = parseStrategyFromLLM(content);
                        ValidationResult validation = validateStrategySchema(strategy);
                        
                        if (validation.isValid()) {
                            if (logLevel >= 1) vertx.eventBus().publish("log", "Successfully generated valid strategy on attempt " + (attempt + 1) + ",1,StrategyGenerationServer,Strategy,Valid");
                            strategy.put("generation_method", "dynamic_llm");
                            promise.complete(strategy);
                        } else {
                            if (logLevel >= 1) vertx.eventBus().publish("log", "Strategy validation failed on attempt " + (attempt + 1) + ": " + 
                                String.join("; ", validation.getErrors()) + ",1,StrategyGenerationServer,Strategy,Invalid");
                            
                            // Retry
                            generateStrategyWithRetry(query, intent, complexityAnalysis, constraints, attempt + 1)
                                .onComplete(promise);
                        }
                    } catch (Exception e) {
                        vertx.eventBus().publish("log", "Failed to parse strategy JSON: " + e.getMessage() + ",0,StrategyGenerationServer,Strategy,Parse");
                        
                        // Retry
                        generateStrategyWithRetry(query, intent, complexityAnalysis, constraints, attempt + 1)
                            .onComplete(promise);
                    }
                } else {
                    promise.fail(ar.cause());
                }
            });
        
        return promise.future();
    }
    
    private String buildStrategyPrompt(String query, JsonObject intent, JsonObject complexityAnalysis) {
        return String.format("""
            Generate a dynamic orchestration strategy for this database query.
            
            Query: %s
            Intent: %s
            Complexity: %s
            
            IMPORTANT: Output ONLY valid JSON matching the schema below. No explanations, no markdown, just JSON.
            
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
                {"step": 1, "tool": "analyze_query", "server": "oracle-query-analysis", "description": "Quick semantic analysis"},
                {"step": 2, "tool": "match_oracle_schema", "server": "oracle-schema-intel", "description": "Find the target table"},
                {"step": 3, "tool": "generate_oracle_sql", "server": "oracle-sql-gen", "description": "Generate simple SQL"},
                {"step": 4, "tool": "run_oracle_query", "server": "oracle-db", "description": "Execute and return results"}
              ],
              "adaptation_rules": {"allow_runtime_modification": false, "max_retries_per_step": 1}
            }
            
            2. COMPLEX ANALYTICAL (multiple joins, aggregations):
            {
              "name": "Complex Analytical Pipeline",
              "description": "Comprehensive pipeline for multi-table queries with aggregations",
              "steps": [
                {"step": 1, "tool": "evaluate_query_intent", "server": "query-intent", "description": "Deep intent and requirement analysis"},
                {"step": 2, "tool": "analyze_query", "server": "oracle-query-analysis", "description": "Extract all entities and relationships"},
                {"step": 3, "tool": "match_oracle_schema", "server": "oracle-schema-intel", "description": "Find all related tables and columns"},
                {"step": 4, "tool": "infer_table_relationships", "server": "oracle-schema-intel", "description": "Discover join paths", "parallel_allowed": true},
                {"step": 5, "tool": "map_business_terms", "server": "business-map", "description": "Map business terminology", "optional": true},
                {"step": 6, "tool": "generate_oracle_sql", "server": "oracle-sql-gen", "description": "Generate complex SQL with joins", "depends_on": [4, 5]},
                {"step": 7, "tool": "optimize_oracle_sql", "server": "oracle-sql-gen", "description": "Optimize for performance", "optional": true},
                {"step": 8, "tool": "validate_oracle_sql", "server": "oracle-sql-val", "description": "Comprehensive validation"},
                {"step": 9, "tool": "run_oracle_query", "server": "oracle-db", "description": "Execute with performance monitoring"},
                {"step": 10, "tool": "format_results", "server": "oracle-db", "description": "Format into readable response", "optional": true}
              ],
              "decision_points": [
                {
                  "after_step": 3,
                  "condition": "more than 5 tables matched",
                  "action": "insert_step",
                  "parameters": {"tool": "refine_schema_matches", "server": "oracle-schema-intel"}
                }
              ],
              "adaptation_rules": {"allow_runtime_modification": true, "max_retries_per_step": 2}
            }
            
            3. EXPLORATORY/DISCOVERY (unknown schema):
            {
              "name": "Schema Discovery Pipeline",
              "description": "Exploration-first approach for unfamiliar data",
              "steps": [
                {"step": 1, "tool": "analyze_query", "server": "oracle-query-analysis", "description": "Understand exploration intent"},
                {"step": 2, "tool": "get_oracle_schema", "server": "oracle-db", "description": "Retrieve complete schema information"},
                {"step": 3, "tool": "match_oracle_schema", "server": "oracle-schema-intel", "description": "Fuzzy match relevant tables"},
                {"step": 4, "tool": "discover_sample_data", "server": "oracle-schema-intel", "description": "Get sample data", "parallel_allowed": true},
                {"step": 5, "tool": "discover_column_semantics", "server": "oracle-schema-intel", "description": "Analyze column meanings", "optional": true},
                {"step": 6, "tool": "generate_exploration_suggestions", "server": "oracle-sql-gen", "description": "Suggest queries", "depends_on": [4, 5]}
              ],
              "adaptation_rules": {"allow_runtime_modification": true, "max_retries_per_step": 3}
            }
            
            NOTE: decision_points are optional and only needed for complex conditional logic.
            
            Now generate an optimal strategy for the given query. Consider:
            - The specific intent type: %s
            - Complexity score of %s suggests a %s approach
            - Whether validation steps are critical for this query type
            - Which steps can run in parallel to improve performance
            - Appropriate decision points for runtime adaptation
            
            Remember: Output ONLY the JSON strategy, no other text.
            """, 
            query, 
            intent.encode(), 
            complexityAnalysis.encode(),
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
        
        // Select based on intent first, then complexity
        if (primaryIntent.equals("get_sql_only")) {
            return FALLBACK_SQL_ONLY_STRATEGY.copy();
        } else if (complexityScore < 0.3) {
            return FALLBACK_SIMPLE_STRATEGY.copy();
        } else if (complexityScore > 0.7) {
            return FALLBACK_COMPLEX_STRATEGY.copy();
        } else {
            // Medium complexity - use complex but mark some steps as optional
            JsonObject medium = FALLBACK_COMPLEX_STRATEGY.copy();
            medium.put("name", "Fallback Medium Pipeline");
            return medium;
        }
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
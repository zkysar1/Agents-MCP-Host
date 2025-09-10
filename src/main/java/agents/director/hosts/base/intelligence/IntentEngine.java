package agents.director.hosts.base.intelligence;

import agents.director.services.LlmAPIService;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;

/**
 * Intent Engine - Fixed pipeline depth determination based on intent analysis.
 * 
 * This class analyzes user queries, backstory, and guidance to determine
 * how deep into the FIXED 10-level pipeline to execute. It no longer selects
 * managers dynamically - instead it determines execution depth (1-10).
 * 
 * Key responsibilities:
 * - Analyze user intent and classify query type
 * - Determine complexity and execution requirements
 * - Calculate appropriate pipeline execution depth (1-10)
 * - Provide context for the fixed pipeline execution
 */
public class IntentEngine {
    
    private final LlmAPIService llmService;
    
    public IntentEngine(LlmAPIService llmService) {
        this.llmService = llmService;
    }
    
    /**
     * Analyze backstory, guidance, and query to determine pipeline execution depth.
     * This uses LLM to understand intent and determine how deep into the 
     * fixed 10-level pipeline to execute.
     * 
     * @param backstory The agent's backstory/context
     * @param guidance The user's guidance/requirements  
     * @param query The user's actual query
     * @return Future with depth analysis results
     */
    public Future<JsonObject> analyzeExecutionDepth(String backstory, String guidance, String query) {
        Promise<JsonObject> promise = Promise.promise();
        
        if (!llmService.isInitialized()) {
            promise.complete(createFallbackDepthAnalysis(query, backstory, guidance));
            return promise.future();
        }
        
        // Build prompt for depth analysis
        String systemPrompt = "You are an AI pipeline depth analyzer. Based on the provided context, determine how deep into a fixed 10-level pipeline to execute.\n\n" +
            "The fixed pipeline levels are:\n" +
            "1. Intent Analysis - Understanding what the user wants\n" +
            "2. Schema Intelligence - Map business terms to database schema\n" +
            "3. SQL Analysis - Analyze query requirements\n" +
            "4. SQL Generation - Generate SQL query\n" +
            "5. SQL Validation - Validate generated SQL\n" +
            "6. SQL Optimization - Optimize SQL performance\n" +
            "7. Query Execution - Execute query against database\n" +
            "8. Result Formatting - Format results for presentation\n" +
            "9. Strategy Learning - Learn from execution\n" +
            "10. Full Orchestration - Complex multi-step orchestration\n\n" +
            "Return a JSON object with:\n" +
            "- 'execution_depth': integer 1-10 indicating how deep to execute\n" +
            "- 'query_type': classification of the query type\n" +
            "- 'complexity': object with score (0-1) and reasoning\n" +
            "- 'requires_execution': boolean if database execution is needed\n" +
            "- 'confidence': confidence score 0-1\n" +
            "- 'reasoning': explanation of depth decision";
        
        String userPrompt = "Backstory: " + (backstory != null ? backstory : "General assistant") + "\n" +
            "Guidance: " + (guidance != null ? guidance : "Help the user with their request") + "\n" +
            "User Query: " + query + "\n\n" +
            "Analyze this request and determine the appropriate pipeline execution depth (1-10).";
        
        java.util.List<String> messages = java.util.Arrays.asList(
            new JsonObject().put("role", "system").put("content", systemPrompt).encode(),
            new JsonObject().put("role", "user").put("content", userPrompt).encode()
        );
        
        // Get the current Vert.x context to ensure callbacks run on the correct thread
        io.vertx.core.Context vertxContext = io.vertx.core.Vertx.currentContext();
        
        llmService.chatCompletion(messages, 0.2, 1200)
            .whenComplete((result, error) -> {
                // Run callback on the original Vert.x context
                Runnable handler = () -> {
                    // Check if promise is already completed
                    if (promise.future().isComplete()) {
                        return;
                    }
                    
                    if (error != null) {
                        promise.complete(createFallbackDepthAnalysis(query, backstory, guidance));
                        return;
                    }
                    
                    try {
                        String content = result.getJsonArray("choices")
                            .getJsonObject(0)
                            .getJsonObject("message")
                            .getString("content");
                        
                        // Try to parse the JSON response
                        JsonObject analysis;
                        try {
                            analysis = new JsonObject(content);
                            
                            // Validate and sanitize the response
                            analysis = validateDepthAnalysis(analysis, query, backstory, guidance);
                            
                        } catch (Exception e) {
                            // If parsing fails, create a fallback
                            analysis = createFallbackDepthAnalysis(query, backstory, guidance);
                        }
                        
                        if (!promise.future().isComplete()) {
                            promise.complete(analysis);
                        }
                    } catch (Exception e) {
                        if (!promise.future().isComplete()) {
                            promise.complete(createFallbackDepthAnalysis(query, backstory, guidance));
                        }
                    }
                };
                
                // Execute on the proper context
                if (vertxContext != null) {
                    vertxContext.runOnContext(v -> handler.run());
                } else {
                    handler.run();
                }
            });
        
        return promise.future();
    }
    
    /**
     * Validate and sanitize LLM depth analysis response
     */
    private JsonObject validateDepthAnalysis(JsonObject analysis, String query, String backstory, String guidance) {
        // Ensure execution_depth is valid
        int depth = analysis.getInteger("execution_depth", 5);
        depth = Math.max(1, Math.min(10, depth));
        analysis.put("execution_depth", depth);
        
        // Ensure query_type is present
        if (!analysis.containsKey("query_type") || analysis.getString("query_type").isEmpty()) {
            analysis.put("query_type", classifyQueryType(query));
        }
        
        // Ensure complexity object is present
        if (!analysis.containsKey("complexity")) {
            analysis.put("complexity", new JsonObject()
                .put("score", 0.5)
                .put("reasoning", "Default complexity assessment"));
        }
        
        // Ensure requires_execution is boolean
        if (!analysis.containsKey("requires_execution")) {
            analysis.put("requires_execution", requiresExecution(query));
        }
        
        // Ensure confidence is present and valid
        double confidence = analysis.getDouble("confidence", 0.7);
        confidence = Math.max(0.1, Math.min(1.0, confidence));
        analysis.put("confidence", confidence);
        
        // Add metadata
        analysis.put("analysis_method", "llm_with_validation");
        analysis.put("timestamp", System.currentTimeMillis());
        
        return analysis;
    }
    
    /**
     * Create fallback depth analysis when LLM is unavailable or fails
     */
    private JsonObject createFallbackDepthAnalysis(String query, String backstory, String guidance) {
        String queryType = classifyQueryType(query);
        boolean requiresExecution = requiresExecution(query);
        JsonObject complexity = assessComplexity(query, backstory, guidance);
        
        // Determine depth based on fallback rules
        int depth = getFallbackDepth(queryType, requiresExecution, backstory, guidance, complexity);
        
        return new JsonObject()
            .put("execution_depth", depth)
            .put("query_type", queryType)
            .put("complexity", complexity)
            .put("requires_execution", requiresExecution)
            .put("confidence", 0.6)
            .put("reasoning", "Fallback analysis using keyword detection and heuristics")
            .put("analysis_method", "fallback_heuristics")
            .put("timestamp", System.currentTimeMillis());
    }
    
    /**
     * Classify query type using keyword analysis
     */
    private String classifyQueryType(String query) {
        if (query == null || query.trim().isEmpty()) return "general_assistance";
        
        String lowerQuery = query.toLowerCase();
        
        // SQL-related queries
        if (lowerQuery.contains("select") || lowerQuery.contains("insert") || 
            lowerQuery.contains("update") || lowerQuery.contains("delete")) {
            return lowerQuery.contains("run") || lowerQuery.contains("execute") ? 
                "sql_execution" : "sql_generation";
        }
        
        // Schema-related queries
        if (lowerQuery.contains("schema") || lowerQuery.contains("table") || 
            lowerQuery.contains("column") || lowerQuery.contains("structure")) {
            return "schema_inquiry";
        }
        
        // Data-related queries
        if (lowerQuery.contains("data") || lowerQuery.contains("records") || 
            lowerQuery.contains("rows") || lowerQuery.contains("find")) {
            return "data_retrieval";
        }
        
        // Report/formatting queries
        if (lowerQuery.contains("report") || lowerQuery.contains("format") || 
            lowerQuery.contains("present") || lowerQuery.contains("display")) {
            return "formatted_results";
        }
        
        // Learning/analysis queries
        if (lowerQuery.contains("learn") || lowerQuery.contains("pattern") || 
            lowerQuery.contains("analyze") || lowerQuery.contains("insight")) {
            return "learning_query";
        }
        
        // Complex orchestration
        if (lowerQuery.contains("complex") || lowerQuery.contains("multi") || 
            lowerQuery.contains("orchestrat") || lowerQuery.contains("pipeline")) {
            return "complex_orchestration";
        }
        
        // SQL help queries
        if (lowerQuery.contains("sql") || lowerQuery.contains("query")) {
            return "sql_help";
        }
        
        // Default
        return "general_assistance";
    }
    
    /**
     * Determine if query requires database execution
     */
    private boolean requiresExecution(String query) {
        if (query == null || query.trim().isEmpty()) return false;
        
        String lowerQuery = query.toLowerCase();
        
        return lowerQuery.contains("run") || lowerQuery.contains("execute") || 
               lowerQuery.contains("data") || lowerQuery.contains("results") ||
               lowerQuery.contains("show") || lowerQuery.contains("find") ||
               lowerQuery.contains("get") || lowerQuery.contains("fetch") ||
               lowerQuery.contains("retrieve");
    }
    
    /**
     * Assess query complexity using heuristics
     */
    private JsonObject assessComplexity(String query, String backstory, String guidance) {
        if (query == null || query.trim().isEmpty()) query = "";
        
        double score = 0.3; // Base score
        String reasoning = "Base complexity assessment";
        
        String lowerQuery = query.toLowerCase();
        
        // Increase complexity for SQL keywords
        if (lowerQuery.contains("join") || lowerQuery.contains("group by") || 
            lowerQuery.contains("having") || lowerQuery.contains("subquery")) {
            score += 0.3;
            reasoning += "; Contains complex SQL constructs";
        }
        
        // Increase complexity for multiple operations
        long operationCount = lowerQuery.chars()
            .filter(ch -> ch == '(' || ch == ')').count();
        if (operationCount > 4) {
            score += 0.2;
            reasoning += "; Contains multiple nested operations";
        }
        
        // Increase complexity for orchestration keywords
        if (backstory != null && backstory.toLowerCase().contains("orchestration")) {
            score += 0.3;
            reasoning += "; Orchestration context indicated";
        }
        
        // Increase complexity for learning contexts
        if (guidance != null && guidance.toLowerCase().contains("learn")) {
            score += 0.2;
            reasoning += "; Learning context indicated";
        }
        
        score = Math.max(0.1, Math.min(1.0, score));
        
        return new JsonObject()
            .put("score", score)
            .put("reasoning", reasoning);
    }
    
    /**
     * Get fallback execution depth using heuristics
     */
    private int getFallbackDepth(String queryType, boolean requiresExecution, 
                                String backstory, String guidance, JsonObject complexity) {
        
        // Start with base depth for query type
        int depth = getBaseDepthForQueryType(queryType);
        
        // Adjust for complexity
        double complexityScore = complexity.getDouble("score", 0.5);
        if (complexityScore > 0.8) {
            depth = Math.min(10, depth + 2);
        } else if (complexityScore > 0.6) {
            depth = Math.min(10, depth + 1);
        }
        
        // Ensure execution depth if required
        if (requiresExecution && depth < 7) {
            depth = 7;
        }
        
        // Adjust for context keywords
        if (backstory != null && backstory.toLowerCase().contains("learning")) {
            depth = Math.max(depth, 9);
        }
        
        if (backstory != null && backstory.toLowerCase().contains("orchestration")) {
            depth = 10;
        }
        
        if (guidance != null && guidance.toLowerCase().contains("format")) {
            depth = Math.max(depth, 8);
        }
        
        return Math.max(1, Math.min(10, depth));
    }
    
    /**
     * Get base execution depth for query types
     */
    private int getBaseDepthForQueryType(String queryType) {
        switch (queryType) {
            case "general_assistance": return 1;
            case "schema_inquiry": return 2;
            case "sql_help": return 3;
            case "sql_generation": return 5;
            case "sql_execution": case "data_retrieval": return 7;
            case "formatted_results": return 8;
            case "learning_query": return 9;
            case "complex_orchestration": return 10;
            default: return 5;
        }
    }
}
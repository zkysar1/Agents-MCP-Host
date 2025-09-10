package agents.director.hosts.base.intelligence;

import agents.director.services.LlmAPIService;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;

/**
 * Strategy Picker - Fixed pipeline architecture with depth-based execution.
 * 
 * This class implements the new fixed pipeline architecture where there is a 
 * HARDCODED sequence of 10 levels, and the strategy picker only determines
 * how deep into this pipeline to execute (depth 1-10).
 * 
 * The fixed pipeline sequence is:
 * 1. Intent Analysis - Understanding what the user wants
 * 2. Schema Intelligence - Map business terms to database schema
 * 3. SQL Analysis - Analyze the query requirements
 * 4. SQL Generation - Generate the SQL query
 * 5. SQL Validation - Validate the generated SQL
 * 6. SQL Optimization - Optimize the SQL if needed
 * 7. Query Execution - Execute the query
 * 8. Result Formatting - Format the results
 * 9. Strategy Learning - Learn from the execution
 * 10. Full Orchestration - Complex multi-step orchestration
 */
public class StrategyPicker {
    
    // Fixed pipeline levels - NEVER changes
    private static final JsonArray FIXED_PIPELINE = new JsonArray()
        .add(createPipelineLevel(1, "intent_analysis", "IntentAnalysisManager", 
            "intent_analysis__extract_intent", "IntentAnalysis",
            "Analyze user intent and extract primary goals"))
        .add(createPipelineLevel(2, "schema_intelligence", "SchemaIntelligenceManager", 
            "match_oracle_schema", "OracleSchemaIntelligence",
            "Map business terms to database schema"))
        .add(createPipelineLevel(3, "sql_analysis", "SQLPipelineManager", 
            "analyze_query", "OracleQueryAnalysis",
            "Analyze query structure and requirements"))
        .add(createPipelineLevel(4, "sql_generation", "SQLPipelineManager", 
            "generate_oracle_sql", "OracleSQLGeneration",
            "Generate SQL query from natural language"))
        .add(createPipelineLevel(5, "sql_validation", "SQLPipelineManager", 
            "validate_oracle_sql", "OracleSQLValidation",
            "Validate generated SQL for correctness"))
        .add(createPipelineLevel(6, "sql_optimization", "SQLPipelineManager", 
            "optimize_oracle_sql", "OracleSQLGeneration",
            "Optimize SQL for better performance"))
        .add(createPipelineLevel(7, "query_execution", "OracleExecutionManager", 
            "run_oracle_query", "OracleQueryExecution",
            "Execute SQL query against database"))
        .add(createPipelineLevel(8, "result_formatting", "OracleExecutionManager", 
            "format_results", "OracleQueryExecution",
            "Format query results for presentation"))
        .add(createPipelineLevel(9, "strategy_learning", "StrategyOrchestrationManager", 
            "strategy_learning__record_execution", "StrategyLearning",
            "Learn from execution for future improvements"))
        .add(createPipelineLevel(10, "full_orchestration", "StrategyOrchestrationManager", 
            "strategy_orchestrator__execute_step", "StrategyOrchestrator",
            "Complex multi-step strategy orchestration"));
    
    public StrategyPicker() {
        // Fixed pipeline architecture - no initialization needed
    }
    
    /**
     * Determine execution depth based on query complexity and requirements.
     * Returns a depth level (1-10) that indicates how far down the fixed
     * pipeline to execute.
     * 
     * @param queryType The type of query analyzed by IntentEngine
     * @param complexity Query complexity assessment
     * @param requiresExecution Whether database execution is needed
     * @param backstory The agent's backstory for context
     * @return Execution depth level (1-10)
     */
    public int determineExecutionDepth(String queryType, JsonObject complexity, 
                                     boolean requiresExecution, String backstory) {
        
        // Null safety checks
        if (queryType == null || queryType.trim().isEmpty()) {
            queryType = "general_assistance";
        }
        
        if (complexity == null) {
            complexity = new JsonObject().put("score", 0.5).put("reasoning", "Default complexity");
        }
        
        // Start with baseline depth based on query type
        int depth = getBaseDepthForQueryType(queryType);
        
        // Adjust based on complexity
        double complexityScore = complexity.getDouble("score", 0.5);
        if (complexityScore > 0.8) {
            depth = Math.min(10, depth + 2); // Very complex queries need more levels
        } else if (complexityScore > 0.6) {
            depth = Math.min(10, depth + 1); // Moderate complexity
        }
        
        // Adjust based on execution requirements
        if (requiresExecution && depth < 7) {
            depth = 7; // Minimum depth for execution
        }
        
        // Adjust based on backstory context
        if (backstory != null && backstory.toLowerCase().contains("learning")) {
            depth = Math.max(depth, 9); // Learning contexts need strategy learning
        }
        
        if (backstory != null && backstory.toLowerCase().contains("orchestration")) {
            depth = 10; // Full orchestration requested
        }
        
        return Math.max(1, Math.min(10, depth));
    }
    
    /**
     * Create execution strategy with fixed pipeline up to specified depth
     * 
     * @param executionDepth How deep into the pipeline to execute (1-10)
     * @param queryContext Context about the query for metadata
     * @return JsonObject with fixed pipeline strategy
     */
    public JsonObject createFixedPipelineStrategy(int executionDepth, JsonObject queryContext) {
        // Ensure depth is within valid range
        executionDepth = Math.max(1, Math.min(10, executionDepth));
        
        // Ensure queryContext is not null
        if (queryContext == null) {
            queryContext = new JsonObject();
        }
        
        JsonArray steps = new JsonArray();
        JsonArray requiredManagers = new JsonArray();
        
        // Add steps up to the specified depth
        for (int i = 0; i < executionDepth; i++) {
            JsonObject level = FIXED_PIPELINE.getJsonObject(i);
            steps.add(level.getJsonObject("step"));
            
            // Track required managers
            String manager = level.getJsonObject("step").getString("manager");
            if (!containsManager(requiredManagers, manager)) {
                requiredManagers.add(manager);
            }
        }
        
        return new JsonObject()
            .put("strategy", new JsonObject()
                .put("name", "fixed_pipeline")
                .put("type", "fixed_depth")
                .put("execution_depth", executionDepth)
                .put("max_depth", 10)
                .put("steps", steps)
                .put("required_managers", requiredManagers)
                .put("timestamp", System.currentTimeMillis())
                .put("architecture_version", "2.0_fixed_pipeline"))
            .put("metadata", new JsonObject()
                .put("pipeline_description", "Fixed 10-level pipeline architecture")
                .put("depth_strategy", "Execute levels 1-" + executionDepth)
                .put("query_context", queryContext != null ? queryContext : new JsonObject()));
    }
    
    /**
     * Get base execution depth for different query types
     */
    private int getBaseDepthForQueryType(String queryType) {
        if (queryType == null || queryType.trim().isEmpty()) {
            return 5; // Default depth
        }
        
        switch (queryType.toLowerCase()) {
            case "simple_question":
            case "general_assistance":
                return 1; // Just intent analysis
                
            case "schema_inquiry":
            case "table_structure":
                return 2; // Intent + Schema intelligence
                
            case "sql_help":
            case "query_explanation":
                return 3; // Through SQL analysis
                
            case "sql_generation":
            case "data_query":
                return 5; // Through SQL validation
                
            case "sql_execution":
            case "data_retrieval":
                return 7; // Through query execution
                
            case "report_generation":
            case "formatted_results":
                return 8; // Through result formatting
                
            case "learning_query":
            case "pattern_analysis":
                return 9; // Through strategy learning
                
            case "complex_orchestration":
            case "multi_step_analysis":
                return 10; // Full pipeline
                
            default:
                return 5; // Default to SQL validation level
        }
    }
    
    /**
     * Create a pipeline level definition
     */
    private static JsonObject createPipelineLevel(int level, String name, String manager, 
                                                String tool, String server, String description) {
        return new JsonObject()
            .put("level", level)
            .put("name", name)
            .put("step", new JsonObject()
                .put("tool", tool)
                .put("server", server)
                .put("manager", manager)
                .put("description", description)
                .put("level", level)
                .put("optional", level > 7) // Levels 8-10 are optional
                .put("streaming_event", "pipeline_level_" + level + "_complete"));
    }
    
    /**
     * Check if manager array contains a specific manager
     */
    private boolean containsManager(JsonArray managers, String manager) {
        for (int i = 0; i < managers.size(); i++) {
            if (manager.equals(managers.getString(i))) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Get the fixed pipeline definition (for debugging/inspection)
     */
    public JsonArray getFixedPipeline() {
        return FIXED_PIPELINE.copy();
    }
    
    /**
     * Get description of what each pipeline level does
     */
    public JsonObject getPipelineDescription() {
        JsonObject description = new JsonObject();
        for (int i = 0; i < FIXED_PIPELINE.size(); i++) {
            JsonObject level = FIXED_PIPELINE.getJsonObject(i);
            description.put("level_" + level.getInteger("level"), 
                level.getString("name") + ": " + level.getJsonObject("step").getString("description"));
        }
        return description;
    }
}
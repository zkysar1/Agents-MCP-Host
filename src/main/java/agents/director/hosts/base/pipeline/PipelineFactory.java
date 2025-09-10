package agents.director.hosts.base.pipeline;

import agents.director.hosts.base.managers.*;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;

import java.util.Arrays;
import java.util.List;

/**
 * Factory class for creating common pipeline configurations and steps.
 * Provides pre-built pipelines for common Oracle database operations.
 */
public class PipelineFactory {
    
    /**
     * Create a complete Oracle database query pipeline
     * This pipeline handles the full flow: intent → schema → SQL → validation → execution → formatting
     */
    public static PipelineConfiguration createCompleteOracleQueryPipeline() {
        return new PipelineConfiguration.Builder("complete_oracle_query")
            .name("Complete Oracle Query Pipeline")
            .description("Full pipeline from user query to formatted results")
            .executionPolicy(PipelineConfiguration.ExecutionPolicy.SEQUENTIAL)
            .continueOnOptionalFailure(true)
            .enableFallback(true)
            .steps(Arrays.asList(
                // Step 1: Analyze user intent
                createIntentAnalysisStep(),
                
                // Step 2: Schema resolution and business term mapping
                createSchemaResolutionStep(),
                createBusinessMappingStep(), // Optional
                
                // Step 3: Query analysis and SQL generation
                createQueryAnalysisStep(),
                createSQLGenerationStep(),
                
                // Step 4: SQL optimization and validation
                createSQLOptimizationStep(), // Optional
                createSQLValidationStep(),
                
                // Step 5: Query execution
                createQueryExecutionStep(),
                
                // Step 6: Result formatting
                createResultFormattingStep() // Optional
            ))
            .build();
    }
    
    /**
     * Create a schema exploration pipeline
     * This pipeline focuses on understanding database structure
     */
    public static PipelineConfiguration createSchemaExplorationPipeline() {
        return new PipelineConfiguration.Builder("schema_exploration")
            .name("Schema Exploration Pipeline")
            .description("Pipeline for exploring and understanding database schema")
            .executionPolicy(PipelineConfiguration.ExecutionPolicy.DEPENDENCY_BASED)
            .steps(Arrays.asList(
                createSchemaDiscoveryStep(),
                createRelationshipInferenceStep(),
                createBusinessTermMappingStep(),
                createSchemaIntelligenceStep()
            ))
            .build();
    }
    
    /**
     * Create an SQL-only pipeline (no execution)
     * This pipeline generates and validates SQL but doesn't execute it
     */
    public static PipelineConfiguration createSQLOnlyPipeline() {
        return new PipelineConfiguration.Builder("sql_only")
            .name("SQL Generation Only Pipeline")
            .description("Generate and validate SQL without execution")
            .executionPolicy(PipelineConfiguration.ExecutionPolicy.SEQUENTIAL)
            .steps(Arrays.asList(
                createIntentAnalysisStep(),
                createSchemaResolutionStep(),
                createQueryAnalysisStep(),
                createSQLGenerationStep(),
                createSQLValidationStep()
            ))
            .build();
    }
    
    /**
     * Create an adaptive pipeline that can modify itself during execution
     */
    public static PipelineConfiguration createAdaptivePipeline() {
        return new PipelineConfiguration.Builder("adaptive_oracle_query")
            .name("Adaptive Oracle Query Pipeline")
            .description("Self-adapting pipeline based on execution feedback")
            .executionPolicy(PipelineConfiguration.ExecutionPolicy.DEPENDENCY_BASED)
            .enableAdaptation(true)
            .adaptationCheckInterval(2)
            .adaptationThreshold(0.8)
            .steps(Arrays.asList(
                createIntentAnalysisStep(),
                createComplexityAnalysisStep(),
                createAdaptiveSchemaResolutionStep(),
                createAdaptiveSQLGenerationStep(),
                createSQLValidationStep(),
                createQueryExecutionStep(),
                createResultFormattingStep()
            ))
            .build();
    }
    
    // Individual step creation methods
    
    private static PipelineStep createIntentAnalysisStep() {
        return PipelineStep.intentStep("intent_analysis", "full_intent_analysis")
            .name("Intent Analysis")
            .description("Analyze user query intent and determine output format")
            .priority(10)
            .argumentBuilder(context -> new JsonObject()
                .put("query", context.getOriginalQuery())
                .put("context", new JsonObject()
                    .put("conversationHistory", context.getRecentHistory(5))
                    .put("userProfile", new JsonObject())))
            .build();
    }
    
    private static PipelineStep createSchemaResolutionStep() {
        return PipelineStep.schemaStep("schema_resolution", "match_oracle_schema")
            .name("Schema Resolution")
            .description("Find relevant database tables and columns")
            .addDependency("intent_analysis")
            .priority(9)
            .argumentBuilder(context -> {
                JsonObject intentResult = context.getStepResultSafe("intent_analysis");
                JsonObject analysis = intentResult.getJsonObject("extractedIntent", new JsonObject());
                
                return new JsonObject()
                    .put("analysis", analysis)
                    .put("maxSuggestions", 10)
                    .put("confidenceThreshold", 0.6);
            })
            .build();
    }
    
    private static PipelineStep createBusinessMappingStep() {
        return PipelineStep.schemaStep("business_mapping", "map_business_terms")
            .name("Business Term Mapping")
            .description("Map business terminology to technical database terms")
            .addDependency("schema_resolution")
            .optional()
            .priority(8)
            .argumentBuilder(context -> {
                JsonObject intentResult = context.getStepResultSafe("intent_analysis");
                JsonObject schemaResult = context.getStepResultSafe("schema_resolution");
                
                // Extract entities or terms from intent analysis
                JsonObject analysis = intentResult.getJsonObject("extractedIntent", new JsonObject());
                JsonArray entities = analysis.getJsonArray("entities", new JsonArray());
                
                if (entities.isEmpty()) {
                    // Use the original query as a fallback
                    entities.add(context.getOriginalQuery());
                }
                
                return new JsonObject()
                    .put("terms", entities)
                    .put("context", new JsonObject()
                        .put("schemaMatches", schemaResult)
                        .put("queryAnalysis", analysis));
            })
            .build();
    }
    
    private static PipelineStep createQueryAnalysisStep() {
        return PipelineStep.sqlStep("query_analysis", "analyze_query", "OracleQueryAnalysis")
            .name("Query Analysis")
            .description("Analyze query structure and extract components")
            .addDependency("intent_analysis")
            .priority(7)
            .argumentBuilder(context -> new JsonObject()
                .put("query", context.getOriginalQuery())
                .put("context", context.getRecentHistory(3)))
            .build();
    }
    
    private static PipelineStep createSQLGenerationStep() {
        return PipelineStep.sqlStep("sql_generation", "generate_oracle_sql", "OracleSQLGeneration")
            .name("SQL Generation")
            .description("Generate Oracle SQL based on analysis and schema")
            .dependencies("query_analysis", "schema_resolution")
            .priority(6)
            .argumentBuilder(context -> {
                JsonObject analysis = context.getStepResultSafe("query_analysis");
                JsonObject schemaMatches = context.getStepResultSafe("schema_resolution");
                JsonObject businessMapping = context.getStepResultSafe("business_mapping");
                
                JsonObject args = new JsonObject()
                    .put("analysis", analysis)
                    .put("schemaMatches", schemaMatches)
                    .put("includeEnums", true)
                    .put("maxComplexity", determineComplexity(context));
                
                if (businessMapping != null && !businessMapping.isEmpty()) {
                    args.put("businessMapping", businessMapping);
                }
                
                return args;
            })
            .build();
    }
    
    private static PipelineStep createSQLOptimizationStep() {
        return PipelineStep.sqlStep("sql_optimization", "optimize_oracle_sql", "OracleSQLGeneration")
            .name("SQL Optimization")
            .description("Optimize SQL for better performance")
            .addDependency("sql_generation")
            .optional()
            .priority(5)
            .argumentBuilder(context -> {
                JsonObject sqlResult = context.getStepResultSafe("sql_generation");
                String sql = sqlResult.getString("sql", "");
                
                return new JsonObject()
                    .put("sql", sql.replaceAll(";\\s*$", "")) // Remove trailing semicolon
                    .put("applyHints", true);
            })
            .build();
    }
    
    private static PipelineStep createSQLValidationStep() {
        return PipelineStep.sqlStep("sql_validation", "validate_oracle_sql", "OracleSQLValidation")
            .name("SQL Validation")
            .description("Validate generated SQL for correctness")
            .addDependency("sql_generation")
            .priority(4)
            .argumentBuilder(context -> {
                JsonObject optimized = context.getStepResultSafe("sql_optimization");
                JsonObject generated = context.getStepResultSafe("sql_generation");
                
                String sql;
                if (!optimized.isEmpty() && optimized.containsKey("optimizedSQL")) {
                    sql = optimized.getString("optimizedSQL");
                } else {
                    sql = generated.getString("sql", "");
                }
                
                return new JsonObject()
                    .put("sql", sql)
                    .put("checkPermissions", true)
                    .put("suggestFixes", true);
            })
            .build();
    }
    
    private static PipelineStep createQueryExecutionStep() {
        return PipelineStep.executionStep("query_execution", "execute_with_session_context")
            .name("Query Execution")
            .description("Execute SQL query against Oracle database")
            .addDependency("sql_validation")
            .priority(3)
            .argumentBuilder(context -> {
                JsonObject validated = context.getStepResultSafe("sql_validation");
                String sql = null;
                
                if (validated.getBoolean("valid", false)) {
                    sql = validated.getString("sql");
                } else if (validated.containsKey("suggestedSQL")) {
                    sql = validated.getString("suggestedSQL");
                } else {
                    JsonObject generated = context.getStepResultSafe("sql_generation");
                    sql = generated.getString("sql", "");
                }
                
                JsonObject args = new JsonObject()
                    .put("sql", sql)
                    .put("maxRows", 1000);
                
                if (context.getSessionId() != null) {
                    args.put("sessionId", context.getSessionId());
                }
                
                return args;
            })
            .condition(context -> {
                // Only execute if we have valid SQL
                JsonObject validated = context.getStepResultSafe("sql_validation");
                JsonObject generated = context.getStepResultSafe("sql_generation");
                
                return (validated.getBoolean("valid", false) || 
                        validated.containsKey("suggestedSQL") ||
                        generated.containsKey("sql"));
            })
            .build();
    }
    
    private static PipelineStep createResultFormattingStep() {
        return PipelineStep.executionStep("result_formatting", "format_results")
            .name("Result Formatting")
            .description("Format query results into natural language")
            .addDependency("query_execution")
            .optional()
            .priority(2)
            .argumentBuilder(context -> {
                JsonObject queryResults = context.getStepResultSafe("query_execution");
                String executedSQL = context.getExecutedSQL();
                
                return new JsonObject()
                    .put("original_query", context.getOriginalQuery())
                    .put("sql_executed", executedSQL)
                    .put("results", queryResults)
                    .put("format", "narrative");
            })
            .build();
    }
    
    // Steps for schema exploration pipeline
    
    private static PipelineStep createSchemaDiscoveryStep() {
        return PipelineStep.executionStep("schema_discovery", "get_oracle_schema")
            .name("Schema Discovery")
            .description("Discover available database schemas and tables")
            .priority(10)
            .argumentBuilder(context -> new JsonObject()
                .put("schemaName", null)) // Get all schemas
            .build();
    }
    
    private static PipelineStep createRelationshipInferenceStep() {
        return PipelineStep.schemaStep("relationship_inference", "infer_table_relationships")
            .name("Relationship Inference")
            .description("Infer relationships between database tables")
            .addDependency("schema_discovery")
            .priority(8)
            .argumentBuilder(context -> {
                JsonObject schemaInfo = context.getStepResultSafe("schema_discovery");
                JsonArray tables = schemaInfo.getJsonArray("tables", new JsonArray());
                
                // Limit to first 20 tables for performance
                JsonArray limitedTables = new JsonArray();
                for (int i = 0; i < Math.min(tables.size(), 20); i++) {
                    limitedTables.add(tables.getValue(i));
                }
                
                return new JsonObject()
                    .put("tables", limitedTables)
                    .put("includeIndirect", false);
            })
            .build();
    }
    
    private static PipelineStep createBusinessTermMappingStep() {
        return PipelineStep.schemaStep("business_term_mapping", "map_business_terms")
            .name("Business Term Mapping")
            .description("Map common business terms to database objects")
            .addDependency("schema_discovery")
            .optional()
            .priority(7)
            .argumentBuilder(context -> {
                // Use common business terms for exploration
                JsonArray commonTerms = new JsonArray()
                    .add("customer")
                    .add("order")
                    .add("product")
                    .add("sales")
                    .add("invoice")
                    .add("employee")
                    .add("department");
                
                return new JsonObject()
                    .put("terms", commonTerms)
                    .put("context", context.getStepResultSafe("schema_discovery"));
            })
            .build();
    }
    
    private static PipelineStep createSchemaIntelligenceStep() {
        return PipelineStep.schemaStep("schema_intelligence", "get_comprehensive_intelligence")
            .name("Comprehensive Schema Intelligence")
            .description("Generate comprehensive schema understanding")
            .dependencies("schema_discovery", "relationship_inference")
            .priority(6)
            .argumentBuilder(context -> new JsonObject()
                .put("query", "Provide comprehensive schema overview"))
            .build();
    }
    
    // Steps for adaptive pipeline
    
    private static PipelineStep createComplexityAnalysisStep() {
        return PipelineStep.strategyStep("complexity_analysis", "strategy_generation__analyze_complexity")
            .name("Complexity Analysis")
            .description("Analyze query complexity for adaptive execution")
            .addDependency("intent_analysis")
            .priority(9)
            .argumentBuilder(context -> {
                JsonObject intentResult = context.getStepResultSafe("intent_analysis");
                
                return new JsonObject()
                    .put("query", context.getOriginalQuery())
                    .put("intent_analysis", intentResult)
                    .put("context", new JsonObject()
                        .put("conversation_history", context.getRecentHistory(3)));
            })
            .build();
    }
    
    private static PipelineStep createAdaptiveSchemaResolutionStep() {
        return PipelineStep.schemaStep("adaptive_schema_resolution", "suggest_optimal_strategy")
            .name("Adaptive Schema Resolution")
            .description("Schema resolution with strategy optimization")
            .dependencies("intent_analysis", "complexity_analysis")
            .priority(8)
            .argumentBuilder(context -> {
                JsonObject complexityResult = context.getStepResultSafe("complexity_analysis");
                
                return new JsonObject()
                    .put("query", context.getOriginalQuery())
                    .put("schemaContext", complexityResult);
            })
            .build();
    }
    
    private static PipelineStep createAdaptiveSQLGenerationStep() {
        return PipelineStep.sqlStep("adaptive_sql_generation", "generate_oracle_sql", "OracleSQLGeneration")
            .name("Adaptive SQL Generation")
            .description("SQL generation with complexity-aware optimization")
            .dependencies("query_analysis", "adaptive_schema_resolution", "complexity_analysis")
            .priority(6)
            .argumentBuilder(context -> {
                JsonObject analysis = context.getStepResultSafe("query_analysis");
                JsonObject schemaStrategy = context.getStepResultSafe("adaptive_schema_resolution");
                JsonObject complexity = context.getStepResultSafe("complexity_analysis");
                
                return new JsonObject()
                    .put("analysis", analysis)
                    .put("schemaMatches", schemaStrategy)
                    .put("complexity", complexity)
                    .put("includeEnums", true)
                    .put("adaptive", true);
            })
            .build();
    }
    
    // Helper methods
    
    private static String determineComplexity(ExecutionContext context) {
        JsonObject intentResult = context.getStepResultSafe("intent_analysis");
        JsonObject complexity = context.getStepResultSafe("complexity_analysis");
        
        if (complexity.containsKey("complexity_score")) {
            int score = complexity.getInteger("complexity_score", 3);
            if (score <= 2) return "simple";
            if (score >= 4) return "complex";
            return "medium";
        }
        
        // Fallback complexity determination
        JsonObject extractedIntent = intentResult.getJsonObject("extractedIntent", new JsonObject());
        JsonArray entities = extractedIntent.getJsonArray("entities", new JsonArray());
        JsonArray aggregations = extractedIntent.getJsonArray("aggregations", new JsonArray());
        
        if (entities.size() > 3 || aggregations.size() > 1) {
            return "complex";
        } else if (entities.size() > 1 || aggregations.size() > 0) {
            return "medium";
        }
        
        return "simple";
    }
    
    /**
     * Create a pipeline configured for a specific strategy picker decision
     */
    public static PipelineConfiguration createStrategyBasedPipeline(String strategyName, JsonObject strategyConfig) {
        PipelineConfiguration.Builder builder = new PipelineConfiguration.Builder("strategy_" + strategyName)
            .name("Strategy-Based Pipeline: " + strategyName)
            .description("Pipeline configured based on strategy picker decision");
        
        // Configure based on strategy
        switch (strategyName.toLowerCase()) {
            case "sql_only":
            case "generate_sql_only":
                return builder.steps(createSQLOnlySteps()).build();
                
            case "schema_exploration":
            case "explore_schema":
                return builder.steps(createSchemaExplorationSteps()).build();
                
            case "full_execution":
            case "complete_query":
                return builder.steps(createCompleteQuerySteps()).build();
                
            case "adaptive":
            case "intelligent":
                return builder.enableAdaptation(true).steps(createAdaptiveSteps()).build();
                
            default:
                return createCompleteOracleQueryPipeline();
        }
    }
    
    private static List<PipelineStep> createSQLOnlySteps() {
        return Arrays.asList(
            createIntentAnalysisStep(),
            createSchemaResolutionStep(),
            createQueryAnalysisStep(),
            createSQLGenerationStep(),
            createSQLValidationStep()
        );
    }
    
    private static List<PipelineStep> createSchemaExplorationSteps() {
        return Arrays.asList(
            createSchemaDiscoveryStep(),
            createRelationshipInferenceStep(),
            createBusinessTermMappingStep(),
            createSchemaIntelligenceStep()
        );
    }
    
    private static List<PipelineStep> createCompleteQuerySteps() {
        return Arrays.asList(
            createIntentAnalysisStep(),
            createSchemaResolutionStep(),
            createBusinessMappingStep(),
            createQueryAnalysisStep(),
            createSQLGenerationStep(),
            createSQLOptimizationStep(),
            createSQLValidationStep(),
            createQueryExecutionStep(),
            createResultFormattingStep()
        );
    }
    
    private static List<PipelineStep> createAdaptiveSteps() {
        return Arrays.asList(
            createIntentAnalysisStep(),
            createComplexityAnalysisStep(),
            createAdaptiveSchemaResolutionStep(),
            createQueryAnalysisStep(),
            createAdaptiveSQLGenerationStep(),
            createSQLValidationStep(),
            createQueryExecutionStep(),
            createResultFormattingStep()
        );
    }
}
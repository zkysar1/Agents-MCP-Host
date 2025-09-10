package agents.director.hosts.base.managers;

import agents.director.mcp.client.OracleQueryAnalysisClient;
import agents.director.mcp.client.OracleSQLGenerationClient;
import agents.director.mcp.client.OracleSQLValidationClient;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;

import java.util.ArrayList;
import java.util.List;

import java.util.Arrays;
import java.util.List;

/**
 * Manager for SQL pipeline clients.
 * Coordinates between Analysis, Generation, and Validation clients
 * to provide a complete SQL generation pipeline.
 */
public class SQLPipelineManager extends MCPClientManager {
    
    private static final String ANALYSIS_CLIENT = "analysis";
    private static final String GENERATION_CLIENT = "generation";
    private static final String VALIDATION_CLIENT = "validation";
    
    public SQLPipelineManager(Vertx vertx, String baseUrl) {
        super(vertx, baseUrl);
    }
    
    @Override
    public Future<Void> initialize() {
        List<Future> deploymentFutures = Arrays.asList(
            deployClient(ANALYSIS_CLIENT, new OracleQueryAnalysisClient(baseUrl)),
            deployClient(GENERATION_CLIENT, new OracleSQLGenerationClient(baseUrl)),
            deployClient(VALIDATION_CLIENT, new OracleSQLValidationClient(baseUrl))
        );
        
        return CompositeFuture.all(deploymentFutures).mapEmpty();
    }
    
    /**
     * Generate and validate SQL from natural language query
     * @param naturalLanguageQuery The user's query in natural language
     * @param schemaContext Optional schema context
     * @return Future with validated SQL and metadata
     */
    public Future<JsonObject> generateValidatedSQL(String naturalLanguageQuery, JsonObject schemaContext) {
        // Step 1: Analyze the query
        return callClientTool(ANALYSIS_CLIENT, "analyze_query",
            new JsonObject()
                .put("query", naturalLanguageQuery)
                .put("schemaContext", schemaContext != null ? schemaContext : new JsonObject()))
            .compose(analysis -> {
                // Step 2: Generate SQL based on analysis
                return callClientTool(GENERATION_CLIENT, "generate_oracle_sql",
                    new JsonObject()
                        .put("query", naturalLanguageQuery)
                        .put("analysis", analysis)
                        .put("schemaContext", schemaContext != null ? schemaContext : new JsonObject()))
                    .map(sqlResult -> {
                        // Attach analysis to result
                        sqlResult.put("analysis", analysis);
                        return sqlResult;
                    });
            })
            .compose(sqlWithAnalysis -> {
                String sql = sqlWithAnalysis.getString("sql");
                
                // Step 3: Validate the generated SQL
                return callClientTool(VALIDATION_CLIENT, "validate_oracle_sql",
                    new JsonObject()
                        .put("sql", sql)
                        .put("schemaContext", schemaContext != null ? schemaContext : new JsonObject()))
                    .map(validation -> {
                        // Combine all results
                        return new JsonObject()
                            .put("sql", sql)
                            .put("analysis", sqlWithAnalysis.getJsonObject("analysis"))
                            .put("validation", validation)
                            .put("isValid", validation.getBoolean("valid", false));
                    });
            });
    }
    
    /**
     * Analyze and optimize existing SQL
     * @param sql The SQL to optimize
     * @return Future with optimized SQL and analysis
     */
    public Future<JsonObject> analyzeAndOptimizeSQL(String sql) {
        // Extract tokens from the SQL
        Future<JsonObject> tokensFuture = callClientTool(ANALYSIS_CLIENT, "extract_query_tokens",
            new JsonObject().put("sql", sql));
        
        // Optimize the SQL
        Future<JsonObject> optimizeFuture = callClientTool(GENERATION_CLIENT, "optimize_oracle_sql",
            new JsonObject().put("sql", sql));
        
        // Get execution plan
        Future<JsonObject> planFuture = callClientTool(VALIDATION_CLIENT, "explain_plan",
            new JsonObject().put("sql", sql));
        
        return CompositeFuture.all(tokensFuture, optimizeFuture, planFuture)
            .map(composite -> new JsonObject()
                .put("originalSql", sql)
                .put("tokens", composite.resultAt(0))
                .put("optimizedSql", composite.resultAt(1))
                .put("executionPlan", composite.resultAt(2)));
    }
    
    /**
     * Validate SQL with detailed error explanation
     * @param sql The SQL to validate
     * @return Future with validation results and error explanations
     */
    public Future<JsonObject> validateWithExplanation(String sql) {
        return callClientTool(VALIDATION_CLIENT, "validate_oracle_sql",
            new JsonObject().put("sql", sql))
            .compose(validation -> {
                if (!validation.getBoolean("valid", false)) {
                    // Get error explanation if validation failed
                    String errorCode = validation.getString("errorCode");
                    if (errorCode != null) {
                        return callClientTool(VALIDATION_CLIENT, "explain_oracle_error",
                            new JsonObject()
                                .put("errorCode", errorCode)
                                .put("context", sql))
                            .map(explanation -> {
                                validation.put("errorExplanation", explanation);
                                return validation;
                            });
                    }
                }
                return Future.succeededFuture(validation);
            });
    }
    
    /**
     * Generate multiple SQL variations for a query
     * @param naturalLanguageQuery The query
     * @param count Number of variations to generate
     * @return Future with SQL variations
     */
    public Future<JsonArray> generateSQLVariations(String naturalLanguageQuery, int count) {
        List<Future> generationFutures = new ArrayList<>();
        
        for (int i = 0; i < count; i++) {
            Future<JsonObject> genFuture = callClientTool(GENERATION_CLIENT, "generate_oracle_sql",
                new JsonObject()
                    .put("query", naturalLanguageQuery)
                    .put("variation", i + 1));
            generationFutures.add(genFuture);
        }
        
        return CompositeFuture.all(generationFutures)
            .map(composite -> {
                JsonArray variations = new JsonArray();
                for (int i = 0; i < composite.size(); i++) {
                    variations.add(composite.resultAt(i));
                }
                return variations;
            });
    }
}
package agents.director.hosts.base.pipeline;

import agents.director.hosts.base.managers.*;
import agents.director.hosts.base.intelligence.StrategyPicker;
import io.vertx.core.*;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

/**
 * Example integration showing how to use the Manager Pipeline system
 * in place of the current manual step execution in OracleDBAnswererHost.
 * 
 * This demonstrates the benefits of the pipeline approach:
 * - Cleaner separation of concerns
 * - Better error handling and fallbacks
 * - Consistent streaming events
 * - Easier testing and maintenance
 * - Strategy-driven pipeline configuration
 */
public class PipelineIntegrationExample {
    
    private final Vertx vertx;
    private final EventBus eventBus;
    
    // Managers (same as current host)
    private final OracleExecutionManager executionManager;
    private final SQLPipelineManager sqlPipelineManager;
    private final SchemaIntelligenceManager schemaIntelligenceManager;
    private final IntentAnalysisManager intentAnalysisManager;
    private final StrategyOrchestrationManager strategyOrchestrationManager;
    
    // New pipeline components
    private final StrategyPicker strategyPicker;
    private final Map<String, ManagerPipeline> pipelines = new ConcurrentHashMap<>();
    
    public PipelineIntegrationExample(Vertx vertx, String baseUrl) {
        this.vertx = vertx;
        this.eventBus = vertx.eventBus();
        
        // Initialize managers (same as current approach)
        this.executionManager = new OracleExecutionManager(vertx, baseUrl);
        this.sqlPipelineManager = new SQLPipelineManager(vertx, baseUrl);
        this.schemaIntelligenceManager = new SchemaIntelligenceManager(vertx, baseUrl);
        this.intentAnalysisManager = new IntentAnalysisManager(vertx, baseUrl);
        this.strategyOrchestrationManager = new StrategyOrchestrationManager(vertx, baseUrl);
        this.strategyPicker = new StrategyPicker();
    }
    
    /**
     * Initialize the pipeline system - replaces the current manual initialization
     */
    public Future<Void> initialize() {
        // Initialize all managers first
        CompositeFuture managersInit = CompositeFuture.all(
            executionManager.initialize(),
            sqlPipelineManager.initialize(),
            schemaIntelligenceManager.initialize(),
            intentAnalysisManager.initialize(),
            strategyOrchestrationManager.initialize()
        );
        
        return managersInit.compose(v -> {
            // Create and configure standard pipelines
            createStandardPipelines();
            
            eventBus.publish("log", "Pipeline system initialized with " + 
                pipelines.size() + " configured pipelines,2,PipelineIntegration,System,Info");
            
            return Future.succeededFuture();
        });
    }
    
    /**
     * Process query using the pipeline system - replaces the current processQuery method
     */
    public Future<JsonObject> processQuery(JsonObject request) {
        String query = request.getString("query");
        String conversationId = request.getString("conversationId");
        String sessionId = request.getString("sessionId");
        JsonArray history = request.getJsonArray("history", new JsonArray());
        boolean streaming = request.getBoolean("streaming", true);
        
        // Create execution context
        ExecutionContext context = new ExecutionContext(conversationId, sessionId, query, history);
        context.setStreaming(streaming);
        
        // Use strategy picker to determine the best pipeline approach
        return selectOptimalPipeline(context)
            .compose(pipelineName -> {
                context.setCurrentStrategy(pipelineName);
                
                // Get the configured pipeline
                ManagerPipeline pipeline = pipelines.get(pipelineName);
                if (pipeline == null) {
                    return Future.failedFuture("Pipeline not found: " + pipelineName);
                }
                
                publishStreamingEvent(context, "pipeline_selected", new JsonObject()
                    .put("pipeline", pipelineName)
                    .put("reason", "Strategy picker decision"));
                
                // Execute the pipeline
                return pipeline.execute(context);
            })
            .compose(result -> {
                // Compose final response (similar to current approach but cleaner)
                return composeFinalResponse(context, result);
            });
    }
    
    /**
     * Select optimal pipeline based on query analysis and strategy picker
     */
    private Future<String> selectOptimalPipeline(ExecutionContext context) {
        // Quick intent analysis to determine pipeline strategy
        JsonArray availableManagers = new JsonArray()
            .add("OracleExecutionManager")
            .add("SQLPipelineManager")
            .add("SchemaIntelligenceManager")
            .add("IntentAnalysisManager")
            .add("StrategyOrchestrationManager");
        
        JsonObject agentConfig = new JsonObject()
            .put("enabledManagers", availableManagers)
            .put("maxComplexity", 5)
            .put("requireValidation", true)
            .put("allowExecution", true)
            .put("dynamicStrategies", true);
        
        // Use strategy picker to analyze intent and select strategy
        return intentAnalysisManager.analyzeWithConfidence(
            context.getOriginalQuery(), 
            context.getRecentHistory(3)
        ).compose(intentAnalysis -> {
            // Use fixed pipeline strategy creation
            String queryType = intentAnalysis.getString("queryType", "data_query");
            JsonObject complexity = intentAnalysis.getJsonObject("complexity", new JsonObject().put("score", 0.5));
            boolean requiresExecution = true;
            
            // Determine execution depth based on query analysis
            int depth = strategyPicker.determineExecutionDepth(queryType, complexity, requiresExecution, "");
            
            // Create fixed pipeline strategy
            JsonObject strategy = strategyPicker.createFixedPipelineStrategy(depth, intentAnalysis);
            
            // Map strategy to pipeline name
            String pipelineName = mapStrategyToPipeline(queryType, strategy);
            
            return Future.succeededFuture(pipelineName);
        }).recover(error -> {
            // Fallback to default pipeline on strategy selection failure
            eventBus.publish("log", "Strategy selection failed, using default pipeline: " + 
                error.getMessage() + ",1,PipelineIntegration,System,Warning");
            return Future.succeededFuture("complete_oracle_query");
        });
    }
    
    /**
     * Map strategy picker results to pipeline names
     */
    private String mapStrategyToPipeline(String strategy, JsonObject strategySelection) {
        JsonObject strategyObj = strategySelection.getJsonObject("strategy");
        
        if (strategyObj != null) {
            JsonArray steps = strategyObj.getJsonArray("steps", new JsonArray());
            boolean hasExecution = steps.toString().contains("run_oracle_query");
            boolean hasValidation = steps.toString().contains("validate_oracle_sql");
            
            if (!hasExecution && hasValidation) {
                return "sql_only";
            } else if (steps.size() <= 5) {
                return "simple_oracle_query";
            } else if (strategyObj.getBoolean("adaptive", false)) {
                return "adaptive_oracle_query";
            }
        }
        
        // Default mapping
        switch (strategy.toLowerCase()) {
            case "sql_only":
            case "generate_sql_only":
                return "sql_only";
            case "schema_exploration":
            case "explore_schema":
                return "schema_exploration";
            case "simple_query":
            case "basic_pipeline":
                return "simple_oracle_query";
            case "adaptive":
            case "intelligent":
                return "adaptive_oracle_query";
            default:
                return "complete_oracle_query";
        }
    }
    
    /**
     * Create standard pipeline configurations
     */
    private void createStandardPipelines() {
        // Complete Oracle query pipeline
        PipelineConfiguration completeConfig = PipelineFactory.createCompleteOracleQueryPipeline();
        ManagerPipeline completePipeline = createRegisteredPipeline(completeConfig);
        pipelines.put("complete_oracle_query", completePipeline);
        
        // SQL-only pipeline
        PipelineConfiguration sqlOnlyConfig = PipelineFactory.createSQLOnlyPipeline();
        ManagerPipeline sqlOnlyPipeline = createRegisteredPipeline(sqlOnlyConfig);
        pipelines.put("sql_only", sqlOnlyPipeline);
        
        // Schema exploration pipeline
        PipelineConfiguration schemaConfig = PipelineFactory.createSchemaExplorationPipeline();
        ManagerPipeline schemaPipeline = createRegisteredPipeline(schemaConfig);
        pipelines.put("schema_exploration", schemaPipeline);
        
        // Adaptive pipeline
        PipelineConfiguration adaptiveConfig = PipelineFactory.createAdaptivePipeline();
        ManagerPipeline adaptivePipeline = createRegisteredPipeline(adaptiveConfig);
        pipelines.put("adaptive_oracle_query", adaptivePipeline);
        
        // Simple pipeline for basic queries
        PipelineConfiguration simpleConfig = new PipelineConfiguration.Builder("simple_oracle_query")
            .name("Simple Oracle Query Pipeline")
            .description("Streamlined pipeline for simple queries")
            .steps(
                PipelineStep.intentStep("intent", "full_intent_analysis").build(),
                PipelineStep.schemaStep("schema", "match_oracle_schema")
                    .addDependency("intent").build(),
                PipelineStep.sqlStep("generate", "generate_oracle_sql", "OracleSQLGeneration")
                    .dependencies("intent", "schema").build(),
                PipelineStep.sqlStep("validate", "validate_oracle_sql", "OracleSQLValidation")
                    .addDependency("generate").build(),
                PipelineStep.executionStep("execute", "execute_with_session_context")
                    .addDependency("validate").build()
            )
            .build();
        ManagerPipeline simplePipeline = createRegisteredPipeline(simpleConfig);
        pipelines.put("simple_oracle_query", simplePipeline);
    }
    
    /**
     * Create a pipeline with all managers registered
     */
    private ManagerPipeline createRegisteredPipeline(PipelineConfiguration config) {
        return new ManagerPipeline(vertx, config)
            .registerManager("OracleExecutionManager", executionManager)
            .registerManager("SQLPipelineManager", sqlPipelineManager)
            .registerManager("SchemaIntelligenceManager", schemaIntelligenceManager)
            .registerManager("IntentAnalysisManager", intentAnalysisManager)
            .registerManager("StrategyOrchestrationManager", strategyOrchestrationManager);
    }
    
    /**
     * Compose final response from pipeline execution results
     */
    private Future<JsonObject> composeFinalResponse(ExecutionContext context, JsonObject pipelineResult) {
        JsonObject response = new JsonObject()
            .put("conversationId", context.getConversationId())
            .put("success", pipelineResult.getBoolean("success", false))
            .put("confidence", context.calculateConfidence())
            .put("strategy", context.getCurrentStrategy())
            .put("duration", context.getTotalDuration())
            .put("completedSteps", context.getCompletedSteps());
        
        // Extract answer from step results
        JsonObject stepResults = pipelineResult.getJsonObject("stepResults", new JsonObject());
        
        // Check for formatted results first
        JsonObject formatted = stepResults.getJsonObject("result_formatting");
        if (formatted != null && formatted.containsKey("formatted")) {
            response.put("answer", formatted.getString("formatted"));
        } else {
            // Check for query execution results
            JsonObject execution = stepResults.getJsonObject("query_execution");
            if (execution != null && execution.containsKey("rows")) {
                // Use simple formatting for results
                String answer = formatQueryResults(execution, context.getOriginalQuery());
                response.put("answer", answer);
                response.put("data", execution);
            } else {
                // Check for SQL generation results
                JsonObject sqlGen = stepResults.getJsonObject("sql_generation");
                if (sqlGen != null && sqlGen.containsKey("sql")) {
                    String sql = sqlGen.getString("sql");
                    response.put("answer", "I generated the following SQL query for your request:\n\n```sql\n" + sql + "\n```");
                    response.put("sql", sql);
                } else {
                    // Compose from available results
                    response.put("answer", composeAnswerFromResults(stepResults, context));
                }
            }
        }
        
        // Add execution metadata
        response.put("executedSQL", context.getExecutedSQL());
        if (pipelineResult.containsKey("stepDurations")) {
            response.put("stepDurations", pipelineResult.getJsonObject("stepDurations"));
        }
        
        return Future.succeededFuture(response);
    }
    
    private String formatQueryResults(JsonObject queryResult, String originalQuery) {
        JsonArray rows = queryResult.getJsonArray("rows", new JsonArray());
        
        if (rows.isEmpty()) {
            return "No results found for your query.";
        } else if (rows.size() == 1) {
            return "Found 1 result for your query.";
        } else {
            return String.format("Found %d results for your query.", rows.size());
        }
    }
    
    private String composeAnswerFromResults(JsonObject stepResults, ExecutionContext context) {
        StringBuilder answer = new StringBuilder();
        answer.append("I processed your query and completed the following steps:\n\n");
        
        if (stepResults.containsKey("intent_analysis")) {
            answer.append("✓ Analyzed your intent and requirements\n");
        }
        if (stepResults.containsKey("schema_resolution")) {
            answer.append("✓ Found relevant database tables and columns\n");
        }
        if (stepResults.containsKey("sql_generation")) {
            answer.append("✓ Generated SQL query\n");
        }
        if (stepResults.containsKey("sql_validation")) {
            answer.append("✓ Validated SQL syntax and structure\n");
        }
        
        answer.append("\nCompleted ").append(context.getCompletedSteps())
              .append(" steps with confidence ").append(String.format("%.1f", context.calculateConfidence() * 100))
              .append("%.");
        
        return answer.toString();
    }
    
    /**
     * Publish streaming event (same as current approach)
     */
    private void publishStreamingEvent(ExecutionContext context, String eventType, JsonObject data) {
        if (context.isStreaming() && context.getSessionId() != null) {
            String address = "streaming." + context.getConversationId() + "." + eventType;
            data.put("timestamp", System.currentTimeMillis());
            data.put("source", "PipelineIntegration");
            eventBus.publish(address, data);
        }
    }
    
    /**
     * Shutdown all pipelines and managers
     */
    public Future<Void> shutdown() {
        // Shutdown all pipelines
        pipelines.values().forEach(ManagerPipeline::shutdown);
        pipelines.clear();
        
        // Shutdown managers
        return CompositeFuture.all(
            executionManager.shutdown(),
            sqlPipelineManager.shutdown(),
            schemaIntelligenceManager.shutdown(),
            intentAnalysisManager.shutdown(),
            strategyOrchestrationManager.shutdown(),
            Future.succeededFuture() // StrategyPicker is now a simple utility class
        ).mapEmpty();
    }
    
    /**
     * Get pipeline system status
     */
    public JsonObject getStatus() {
        return new JsonObject()
            .put("pipelinesConfigured", pipelines.size())
            .put("managersRegistered", 5)
            .put("availablePipelines", pipelines.keySet())
            .put("status", "ready");
    }
    
    /**
     * Example of how this would replace the current processQuery method in OracleDBAnswererHost
     */
    public static void demonstrateReplacement() {
        /*
         * BEFORE (current approach in OracleDBAnswererHost):
         * - Manual step execution with complex nested promises
         * - Hardcoded step dependencies and error handling
         * - Repetitive argument building and context management
         * - Scattered streaming event publishing
         * - Complex fallback logic intermixed with main flow
         * 
         * AFTER (with pipeline system):
         * - Clean separation of pipeline configuration and execution
         * - Declarative step dependencies and conditions
         * - Centralized error handling and retry logic
         * - Consistent streaming events across all pipelines
         * - Strategy-driven pipeline selection
         * - Easy testing and maintenance
         * 
         * Key benefits:
         * 1. The current 400+ line processQuery method becomes ~50 lines
         * 2. Pipeline configurations are reusable and testable
         * 3. New execution strategies can be added without code changes
         * 4. Error handling and fallbacks are consistent
         * 5. Performance monitoring is built-in
         * 6. Streaming events are standardized
         */
    }
}
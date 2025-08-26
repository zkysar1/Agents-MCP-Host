package AgentsMCPHost.mcp.servers.oracle.orchestration;

import AgentsMCPHost.mcp.core.orchestration.OrchestrationStrategy;
import AgentsMCPHost.mcp.core.orchestration.OrchestrationContext;

import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;

/**
 * Oracle Orchestration Strategy - Coordinates the full Oracle query pipeline.
 * 
 * This replaces the monolithic OracleAgentLoop. All business logic has been
 * moved to individual tools in OracleServer. This orchestrator
 * simply coordinates the tool calls according to the strategy configuration.
 * 
 * The entire complex Oracle flow is now just configuration in orchestration-strategies.json
 * under "oracle_full_pipeline". This orchestrator adds no logic - it's pure coordination.
 */
public class OracleOrchestrationStrategy extends OrchestrationStrategy {
    
    @Override
    protected String getStrategyName() {
        return "oracle_full_pipeline";
    }
    
    @Override
    public void start(Promise<Void> startPromise) {
        super.start(startPromise);
        
        // Log that Oracle orchestration is ready
        vertx.eventBus().publish("oracle.orchestration.ready", new JsonObject()
            .put("strategy", getStrategyName())
            .put("message", "Oracle orchestration strategy ready - all logic in tools"));
        
        System.out.println("[OracleOrchestration] Initialized - coordinating tool calls only");
        System.out.println("[OracleOrchestration] No business logic here - all in tools!");
    }
    
    /**
     * Override to add Oracle-specific step skipping logic if needed
     */
    @Override
    protected boolean shouldSkipStep(OrchestrationContext context, JsonObject step) {
        String stepName = step.getString("name");
        
        // Skip data discovery if we already have good schema matches
        if ("Discover Data".equals(stepName)) {
            JsonObject stepResults = context.getStepResults();
            JsonObject schemaMatches = stepResults.getJsonObject("Match Schema");
            if (schemaMatches != null && schemaMatches.getDouble("confidence", 0.0) > 0.8) {
                System.out.println("[OracleOrchestration] Skipping data discovery - high confidence schema match");
                return true;
            }
        }
        
        // Skip optimization if query is simple
        if ("Optimize SQL".equals(stepName)) {
            JsonObject stepResults = context.getStepResults();
            JsonObject analysis = stepResults.getJsonObject("Analyze Query");
            if (analysis != null && "simple".equals(analysis.getString("complexity"))) {
                System.out.println("[OracleOrchestration] Skipping optimization - simple query");
                return true;
            }
        }
        
        return false;
    }
}
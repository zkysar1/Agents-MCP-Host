package AgentsMCPHost.mcp.core.orchestration;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.core.Future;
import java.util.HashMap;
import java.util.Map;

/**
 * Generic Orchestration Verticle that dynamically loads and registers all orchestration strategies
 * from the orchestration-strategies.json configuration file.
 * 
 * This replaces the need for individual strategy verticles and ensures all strategies
 * defined in the configuration are available for use.
 */
public class GenericOrchestrationVerticle extends AbstractVerticle {
    
    private JsonObject strategies;
    private final Map<String, OrchestrationStrategyWrapper> activeStrategies = new HashMap<>();
    
    @Override
    public void start(Promise<Void> startPromise) {
        System.out.println("[GenericOrchestration] Starting Generic Orchestration Verticle...");
        
        // Load orchestration strategies from JSON
        try {
            String strategyJson = vertx.fileSystem()
                .readFileBlocking("src/main/resources/orchestration-strategies.json")
                .toString();
            JsonObject config = new JsonObject(strategyJson);
            strategies = config.getJsonObject("strategies", new JsonObject());
            
            System.out.println("[GenericOrchestration] Loaded " + strategies.fieldNames().size() + " strategies from configuration");
            
            // Deploy wrapper verticles for each strategy
            deployAllStrategies()
                .onSuccess(count -> {
                    System.out.println("[GenericOrchestration] Successfully deployed " + count + " orchestration strategies");
                    
                    // Register for configuration reload events
                    vertx.eventBus().consumer("orchestration.reload", msg -> {
                        System.out.println("[GenericOrchestration] Reload request received");
                        reloadStrategies();
                        msg.reply(new JsonObject().put("status", "reloaded").put("strategies", activeStrategies.size()));
                    });
                    
                    // Log deployment
                    if (vertx.eventBus() != null) {
                        vertx.eventBus().publish("log",
                            "GenericOrchestration deployed " + count + " strategies,2,GenericOrchestration,StartUp,Orchestration");
                    }
                    
                    startPromise.complete();
                })
                .onFailure(err -> {
                    System.err.println("[GenericOrchestration] Failed to deploy strategies: " + err.getMessage());
                    startPromise.fail(err);
                });
                
        } catch (Exception e) {
            System.err.println("[GenericOrchestration] Failed to load orchestration strategies: " + e.getMessage());
            e.printStackTrace();
            startPromise.fail(e);
        }
    }
    
    /**
     * Deploy all strategies as individual verticles
     */
    private Future<Integer> deployAllStrategies() {
        Promise<Integer> promise = Promise.promise();
        int totalStrategies = strategies.fieldNames().size();
        
        if (totalStrategies == 0) {
            promise.fail("No orchestration strategies found in configuration");
            return promise.future();
        }
        
        // Deploy each strategy
        int[] deployedCount = {0};
        int[] processedCount = {0};
        
        for (String strategyName : strategies.fieldNames()) {
            // Skip if already deployed
            if (activeStrategies.containsKey(strategyName)) {
                System.err.println("[GenericOrchestration] Strategy already deployed: " + strategyName);
                processedCount[0]++;
                if (processedCount[0] == totalStrategies) {
                    promise.complete(deployedCount[0]);
                }
                continue;
            }
            
            // Create and deploy wrapper verticle
            OrchestrationStrategyWrapper wrapper = new OrchestrationStrategyWrapper(strategyName);
            
            vertx.deployVerticle(wrapper, res -> {
                if (res.succeeded()) {
                    activeStrategies.put(strategyName, wrapper);
                    deployedCount[0]++;
                    System.out.println("[GenericOrchestration] Deployed strategy: " + strategyName);
                } else {
                    System.err.println("[GenericOrchestration] Failed to deploy strategy " + strategyName + ": " + res.cause().getMessage());
                }
                
                processedCount[0]++;
                if (processedCount[0] == totalStrategies) {
                    if (deployedCount[0] == 0) {
                        promise.fail("No strategies were successfully deployed");
                    } else {
                        promise.complete(deployedCount[0]);
                    }
                }
            });
        }
        
        return promise.future();
    }
    
    /**
     * Reload strategies without disrupting active orchestrations
     */
    private void reloadStrategies() {
        System.out.println("[GenericOrchestration] Reloading orchestration strategies...");
        
        try {
            // Reload configuration
            String strategyJson = vertx.fileSystem()
                .readFileBlocking("src/main/resources/orchestration-strategies.json")
                .toString();
            JsonObject config = new JsonObject(strategyJson);
            JsonObject newStrategies = config.getJsonObject("strategies", new JsonObject());
            
            // Deploy only new strategies (don't disrupt existing ones)
            for (String strategyName : newStrategies.fieldNames()) {
                if (!activeStrategies.containsKey(strategyName)) {
                    System.out.println("[GenericOrchestration] Found new strategy: " + strategyName);
                    OrchestrationStrategyWrapper wrapper = new OrchestrationStrategyWrapper(strategyName);
                    vertx.deployVerticle(wrapper, res -> {
                        if (res.succeeded()) {
                            activeStrategies.put(strategyName, wrapper);
                            System.out.println("[GenericOrchestration] Deployed new strategy: " + strategyName);
                        }
                    });
                }
            }
            
            // Update strategies reference
            strategies = newStrategies;
            
        } catch (Exception e) {
            System.err.println("[GenericOrchestration] Failed to reload strategies: " + e.getMessage());
        }
    }
    
    @Override
    public void stop() {
        System.out.println("[GenericOrchestration] Stopping Generic Orchestration Verticle");
        activeStrategies.clear();
    }
    
    /**
     * Inner class that wraps the base OrchestrationStrategy for a specific strategy
     */
    private class OrchestrationStrategyWrapper extends OrchestrationStrategy {
        private final String strategyName;
        
        public OrchestrationStrategyWrapper(String strategyName) {
            this.strategyName = strategyName;
        }
        
        @Override
        protected String getStrategyName() {
            return strategyName;
        }
        
        @Override
        public void start(Promise<Void> startPromise) {
            // Initialize with the specific strategy configuration
            super.start(startPromise);
            
            System.out.println("[GenericOrchestration] Strategy wrapper started for: " + strategyName);
            
            // Publish ready event
            vertx.eventBus().publish("orchestration." + strategyName + ".ready", new JsonObject()
                .put("strategy", strategyName)
                .put("message", strategyName + " orchestration ready"));
        }
        
        @Override
        protected boolean shouldSkipStep(ExecutionContext context, JsonObject step) {
            // Add strategy-specific skip logic if needed
            String stepName = step.getString("name");
            
            // Oracle-specific logic (from OracleOrchestrationStrategy)
            if (strategyName.startsWith("oracle_")) {
                // Skip data discovery if we already have good schema matches
                if ("Discover Data".equals(stepName)) {
                    JsonObject schemaMatches = context.getStepResults().get("Match Schema");
                    if (schemaMatches != null && schemaMatches.getDouble("confidence", 0.0) > 0.8) {
                        System.out.println("[" + strategyName + "] Skipping data discovery - high confidence schema match");
                        return true;
                    }
                }
                
                // Skip optimization if query is simple
                if ("Optimize SQL".equals(stepName)) {
                    JsonObject analysis = context.getStepResults().get("Analyze Query");
                    if (analysis != null && "simple".equals(analysis.getString("complexity"))) {
                        System.out.println("[" + strategyName + "] Skipping optimization - simple query");
                        return true;
                    }
                }
            }
            
            return super.shouldSkipStep(context, step);
        }
    }
}
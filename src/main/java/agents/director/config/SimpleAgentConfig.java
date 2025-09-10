package agents.director.config;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;

/**
 * Simple Agent Configuration - Just backstory and guidance strings.
 * 
 * This replaces the complex AgentConfiguration system with a simple
 * approach that takes backstory and guidance from the frontend and
 * uses LLM to interpret what managers and strategies to use.
 */
public class SimpleAgentConfig {
    
    private final String backstory;
    private final String guidance;
    private final JsonArray managers;
    private final String strategy;
    private final boolean requiresExecution;
    private final double confidence;
    
    public SimpleAgentConfig(String backstory, String guidance) {
        this.backstory = backstory;
        this.guidance = guidance;
        this.managers = new JsonArray();
        this.strategy = "general-assistance";
        this.requiresExecution = false;
        this.confidence = 0.5;
    }
    
    public SimpleAgentConfig(String backstory, String guidance, JsonArray managers, 
                           String strategy, boolean requiresExecution, double confidence) {
        this.backstory = backstory;
        this.guidance = guidance;
        this.managers = managers;
        this.strategy = strategy;
        this.requiresExecution = requiresExecution;
        this.confidence = confidence;
    }
    
    public String getBackstory() {
        return backstory;
    }
    
    public String getGuidance() {
        return guidance;
    }
    
    public JsonArray getManagers() {
        return managers;
    }
    
    public String getStrategy() {
        return strategy;
    }
    
    public boolean requiresExecution() {
        return requiresExecution;
    }
    
    public double getConfidence() {
        return confidence;
    }
    
    public JsonObject toJsonObject() {
        return new JsonObject()
            .put("backstory", backstory)
            .put("guidance", guidance)
            .put("managers", managers)
            .put("strategy", strategy)
            .put("requiresExecution", requiresExecution)
            .put("confidence", confidence);
    }
    
    public static SimpleAgentConfig fromJsonObject(JsonObject json) {
        return new SimpleAgentConfig(
            json.getString("backstory", ""),
            json.getString("guidance", ""),
            json.getJsonArray("managers", new JsonArray()),
            json.getString("strategy", "general-assistance"),
            json.getBoolean("requiresExecution", false),
            json.getDouble("confidence", 0.5)
        );
    }
}
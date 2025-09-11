package agents.director.hosts.base;

import io.vertx.core.*;
import io.vertx.core.json.JsonObject;

/**
 * Base class for milestone managers in the simplified 6-milestone architecture.
 * Each milestone represents a clear step in processing a user's query.
 * 
 * Unlike the complex pipeline system, milestones are:
 * - Sequential (1-6, cannot skip)
 * - Self-contained (each does one clear thing)
 * - User-facing (results are shared at each step)
 * - Simple (no complex strategies or depth analysis)
 * 
 * Each milestone now deploys MCP clients directly without manager abstractions.
 */
public abstract class MilestoneManager {
    
    protected final Vertx vertx;
    protected final String baseUrl;
    protected final int milestoneNumber;
    protected final String milestoneName;
    protected final String description;
    
    /**
     * Constructor for milestone manager
     * @param vertx The Vert.x instance
     * @param baseUrl The base URL for MCP servers
     * @param milestoneNumber The milestone number (1-6)
     * @param milestoneName The name of this milestone
     * @param description What this milestone does
     */
    protected MilestoneManager(Vertx vertx, String baseUrl, int milestoneNumber, 
                               String milestoneName, String description) {
        this.vertx = vertx;
        this.baseUrl = baseUrl;
        this.milestoneNumber = milestoneNumber;
        this.milestoneName = milestoneName;
        this.description = description;
    }
    
    /**
     * Initialize the managers needed for this milestone
     * @return Future that completes when initialization is done
     */
    public abstract Future<Void> initialize();
    
    /**
     * Execute this milestone with the given context
     * @param context The milestone context containing all data
     * @return Future with the updated context
     */
    public abstract Future<MilestoneContext> execute(MilestoneContext context);
    
    /**
     * Get the shareable result from this milestone to show the user
     * @param context The current context
     * @return JsonObject with user-friendly results
     */
    public abstract JsonObject getShareableResult(MilestoneContext context);
    
    /**
     * Check if this milestone should be skipped based on context
     * @param context The current context
     * @return true if milestone should be skipped
     */
    public boolean shouldSkip(MilestoneContext context) {
        // By default, never skip milestones
        return false;
    }
    
    /**
     * Get milestone metadata
     */
    public JsonObject getMetadata() {
        return new JsonObject()
            .put("number", milestoneNumber)
            .put("name", milestoneName)
            .put("description", description);
    }
    
    /**
     * Publish a streaming event for this milestone
     */
    protected void publishStreamingEvent(String conversationId, String eventType, JsonObject data) {
        String address;
        
        // Route milestone events to progress events that frontend expects
        if (eventType.startsWith("milestone.")) {
            // Convert milestone.* events to progress events with phase info
            address = "streaming." + conversationId + ".progress";
            data.put("phase", eventType.replace("milestone.", ""))
                .put("step", milestoneName);
            // Don't override message if already set
            if (!data.containsKey("message")) {
                data.put("message", "Milestone " + milestoneNumber + " completed");
            }
        } else {
            // Keep standard event types as-is (tool.start, tool.complete, etc.)
            address = "streaming." + conversationId + "." + eventType;
        }
        
        data.put("milestone", milestoneNumber)
            .put("milestone_name", milestoneName)
            .put("timestamp", System.currentTimeMillis());
        vertx.eventBus().publish(address, data);
    }
    
    /**
     * Publish a progress event with standard structure
     */
    protected void publishProgressEvent(String conversationId, String step, String message, JsonObject details) {
        JsonObject progressData = new JsonObject()
            .put("step", step)
            .put("message", message)
            .put("details", details != null ? details : new JsonObject());
        publishStreamingEvent(conversationId, "progress", progressData);
    }
    
    /**
     * Publish tool start event
     */
    protected void publishToolStartEvent(String conversationId, String toolName, String description) {
        JsonObject toolData = new JsonObject()
            .put("tool", toolName)
            .put("description", description != null ? description : "Calling " + toolName);
        publishStreamingEvent(conversationId, "tool.start", toolData);
    }
    
    /**
     * Publish tool complete event
     */
    protected void publishToolCompleteEvent(String conversationId, String toolName, boolean success) {
        JsonObject toolData = new JsonObject()
            .put("tool", toolName)
            .put("success", success);
        publishStreamingEvent(conversationId, "tool.complete", toolData);
    }
    
    /**
     * Publish degradation event when falling back to degraded mode
     */
    protected void publishDegradationEvent(MilestoneContext context, String operation, String reason) {
        if (context.isStreaming() && context.getSessionId() != null) {
            JsonObject degradationData = new JsonObject()
                .put("milestone", milestoneNumber)
                .put("milestone_name", milestoneName)
                .put("operation", operation)
                .put("reason", reason)
                .put("degradation_level", context.getDegradationLevel())
                .put("confidence", context.getOverallConfidence())
                .put("severity", "WARNING")
                .put("message", "Using degraded mode for " + operation);
            
            publishStreamingEvent(context.getConversationId(), "degradation_warning", degradationData);
            
            // Also log at warning level
            log("DEGRADED MODE: " + operation + " - " + reason, 1);
        }
    }
    
    /**
     * Log milestone activity
     */
    protected void log(String message, int level) {
        vertx.eventBus().publish("log", 
            message + "," + level + "," + milestoneName + ",Milestone,System");
    }
    
    /**
     * Clean up resources
     * Each milestone should override this to undeploy its MCP clients
     */
    public Future<Void> cleanup() {
        // Default implementation - subclasses should override to clean up their clients
        return Future.succeededFuture();
    }
    
    // Getters
    public int getMilestoneNumber() { return milestoneNumber; }
    public String getMilestoneName() { return milestoneName; }
    public String getDescription() { return description; }
}
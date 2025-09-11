package agents.director.hosts;

import agents.director.hosts.base.MilestoneContext;
import agents.director.hosts.base.MilestoneDecider;
import agents.director.hosts.base.MilestoneManager;
import agents.director.hosts.milestones.*;
import agents.director.services.InterruptManager;
import io.vertx.core.*;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Simplified Universal Host using the 6-milestone architecture.
 * 
 * DRASTICALLY SIMPLIFIED from 1000+ lines to ~300 lines!
 * 
 * Takes backstory + guidance → Determines target milestone → Executes sequentially
 */
public class UniversalHost extends AbstractVerticle {
    
    private EventBus eventBus;
    private MilestoneDecider milestoneDecider;
    private InterruptManager interruptManager;
    
    // The 6 milestone managers
    private final Map<Integer, MilestoneManager> milestones = new HashMap<>();
    
    // Active conversations
    private final Map<String, MilestoneContext> activeConversations = new ConcurrentHashMap<>();
    
    @Override
    public void start(Promise<Void> startPromise) {
        eventBus = vertx.eventBus();
        milestoneDecider = new MilestoneDecider();
        interruptManager = new InterruptManager(vertx);
        
        // Initialize all 6 milestones at startup
        initializeMilestones()
            .onSuccess(v -> {
                // Register message handlers
                eventBus.consumer("host.universal.process", this::processQuery);
                eventBus.consumer("host.universal.status", this::handleStatus);
                
                log("UniversalHost (Simplified) started with 6-milestone architecture", 1);
                startPromise.complete();
            })
            .onFailure(err -> {
                log("Failed to initialize milestones: " + err.getMessage(), 0);
                startPromise.fail(err);
            });
        
        // Periodic cleanup of old conversations
        vertx.setPeriodic(60000, id -> cleanupOldConversations());
    }
    
    /**
     * Initialize all 6 milestone managers
     */
    private Future<Void> initializeMilestones() {
        List<Future> initFutures = new ArrayList<>();
        String baseUrl = agents.director.Driver.BASE_URL;
        
        // Create and initialize each milestone
        milestones.put(1, new IntentMilestone(vertx, baseUrl));
        milestones.put(2, new SchemaMilestone(vertx, baseUrl));
        milestones.put(3, new DataStatsMilestone(vertx, baseUrl));
        milestones.put(4, new SQLGenerationMilestone(vertx, baseUrl));
        milestones.put(5, new ExecutionMilestone(vertx, baseUrl));
        milestones.put(6, new NaturalResponseMilestone(vertx, baseUrl));
        
        for (MilestoneManager milestone : milestones.values()) {
            initFutures.add(milestone.initialize());
        }
        
        return CompositeFuture.all(initFutures).mapEmpty();
    }
    
    /**
     * Main query processing - SIMPLIFIED!
     */
    private void processQuery(Message<JsonObject> message) {
        JsonObject request = message.body();
        
        // Extract parameters
        String query = request.getString("query");
        String conversationId = request.getString("conversationId", UUID.randomUUID().toString());
        String sessionId = request.getString("sessionId");
        boolean streaming = request.getBoolean("streaming", true);
        
        // Extract backstory and guidance from options
        JsonObject options = request.getJsonObject("options", new JsonObject());
        String backstory = options.getString("backstory", "General assistant");
        String guidance = options.getString("guidance", "Help the user with their request");
        
        log("Processing query: " + query + " (conversation: " + conversationId + ")", 2);
        
        // Create context
        MilestoneContext context = new MilestoneContext(
            backstory, guidance, query, conversationId, sessionId, streaming
        );
        
        activeConversations.put(conversationId, context);
        
        // Step 1: Decide target milestone
        milestoneDecider.decideTargetMilestone(backstory, guidance, query)
            .compose(targetMilestone -> {
                context.setTargetMilestone(targetMilestone);
                log("Target milestone determined: " + targetMilestone, 2);
                
                // Publish milestone decision if streaming
                if (streaming && sessionId != null) {
                    // Publish as both milestone_decision and progress event
                    publishStreamingEvent(conversationId, "milestone_decision", new JsonObject()
                        .put("target_milestone", targetMilestone)
                        .put("description", MilestoneDecider.getMilestoneDescription(targetMilestone)));
                    
                    // Also publish as progress event so frontend sees it
                    publishStreamingEvent(conversationId, "progress", new JsonObject()
                        .put("step", "Processing Strategy")
                        .put("message", "Selected milestone " + targetMilestone + ": " + 
                            MilestoneDecider.getMilestoneDescription(targetMilestone))
                        .put("details", new JsonObject()
                            .put("phase", "milestone_decision")
                            .put("target_milestone", targetMilestone)));
                }
                
                // Step 2: Execute milestones sequentially
                return executeMilestones(context, 1);
            })
            .onSuccess(finalContext -> {
                // Build response based on target milestone
                JsonObject response = buildResponse(finalContext);
                
                // Publish final event if streaming
                if (streaming && sessionId != null) {
                    publishStreamingEvent(conversationId, "final", response);
                }
                
                log("Query processing complete for: " + conversationId, 2);
                message.reply(response);
                
                // Clean up after a delay
                vertx.setTimer(5000, id -> activeConversations.remove(conversationId));
            })
            .onFailure(err -> {
                log("Query processing failed: " + err.getMessage(), 0);
                
                // Publish error if streaming
                if (streaming && sessionId != null) {
                    publishStreamingEvent(conversationId, "error", new JsonObject()
                        .put("error", err.getMessage())
                        .put("severity", "ERROR"));
                }
                
                message.fail(500, err.getMessage());
                activeConversations.remove(conversationId);
            });
    }
    
    /**
     * Execute milestones sequentially up to target
     */
    private Future<MilestoneContext> executeMilestones(MilestoneContext context, int currentMilestone) {
        // Check if we've reached the target
        if (currentMilestone > context.getTargetMilestone()) {
            return Future.succeededFuture(context);
        }
        
        // Check for interrupts
        if (context.isStreaming() && context.getSessionId() != null) {
            if (interruptManager.isInterrupted(context.getSessionId())) {
                return Future.failedFuture("User interrupted execution");
            }
        }
        
        // Get the milestone to execute
        MilestoneManager milestone = milestones.get(currentMilestone);
        if (milestone == null) {
            return Future.failedFuture("Milestone " + currentMilestone + " not found");
        }
        
        log("Executing milestone " + currentMilestone + ": " + milestone.getMilestoneName(), 3);
        
        // Publish milestone start event
        if (context.isStreaming() && context.getSessionId() != null) {
            publishStreamingEvent(context.getConversationId(), "progress", new JsonObject()
                .put("step", "Milestone " + currentMilestone)
                .put("message", "Starting: " + milestone.getMilestoneName())
                .put("details", new JsonObject()
                    .put("phase", "milestone_start")
                    .put("milestone", currentMilestone)
                    .put("milestone_name", milestone.getMilestoneName())));
        }
        
        // Execute the milestone
        return milestone.execute(context)
            .compose(updatedContext -> {
                // Continue to next milestone
                return executeMilestones(updatedContext, currentMilestone + 1);
            });
    }
    
    /**
     * Build final response based on what milestones were executed
     */
    private JsonObject buildResponse(MilestoneContext context) {
        JsonObject response = new JsonObject()
            .put("conversationId", context.getConversationId())
            .put("success", true)
            .put("milestones_executed", context.getCurrentMilestone())
            .put("target_milestone", context.getTargetMilestone())
            .put("total_time_ms", context.getTotalTime());
        
        // Add the key result based on target milestone
        switch (context.getTargetMilestone()) {
            case 1:
                response.put("answer", context.getIntent());
                response.put("type", "intent");
                break;
                
            case 2:
                response.put("answer", "Found " + context.getRelevantTables().size() + " relevant tables");
                response.put("tables", new JsonArray(context.getRelevantTables()));
                response.put("type", "schema");
                break;
                
            case 3:
                int totalColumns = context.getTableColumns().values().stream()
                    .mapToInt(List::size).sum();
                response.put("answer", "Analyzed " + totalColumns + " columns across tables");
                response.put("type", "data_stats");
                break;
                
            case 4:
                response.put("answer", context.getGeneratedSql());
                response.put("sql", context.getGeneratedSql());
                response.put("type", "sql");
                break;
                
            case 5:
                response.put("answer", "Query returned " + context.getRowCount() + " rows");
                response.put("data", context.getQueryResults());
                response.put("row_count", context.getRowCount());
                response.put("type", "data");
                break;
                
            case 6:
                response.put("answer", context.getNaturalResponse());
                response.put("type", "natural_response");
                if (context.getRowCount() > 0) {
                    response.put("data_points", context.getRowCount());
                }
                break;
        }
        
        // Add context summary
        response.put("context", context.toJson());
        
        return response;
    }
    
    /**
     * Handle status request
     */
    private void handleStatus(Message<JsonObject> message) {
        JsonObject status = new JsonObject()
            .put("status", "running")
            .put("architecture", "6-milestone-simplified")
            .put("active_conversations", activeConversations.size())
            .put("milestones_initialized", milestones.size());
        
        // Add milestone status
        JsonArray milestoneStatus = new JsonArray();
        for (Map.Entry<Integer, MilestoneManager> entry : milestones.entrySet()) {
            milestoneStatus.add(entry.getValue().getMetadata());
        }
        status.put("milestones", milestoneStatus);
        
        message.reply(status);
    }
    
    /**
     * Publish streaming event
     */
    private void publishStreamingEvent(String conversationId, String eventType, JsonObject data) {
        String address = "streaming." + conversationId + "." + eventType;
        eventBus.publish(address, data);
    }
    
    /**
     * Clean up old conversations
     */
    private void cleanupOldConversations() {
        long cutoff = System.currentTimeMillis() - (30 * 60 * 1000); // 30 minutes
        
        activeConversations.entrySet().removeIf(entry -> {
            MilestoneContext context = entry.getValue();
            return context.getTotalTime() > (30 * 60 * 1000);
        });
    }
    
    /**
     * Simple logging
     */
    private void log(String message, int level) {
        eventBus.publish("log", message + "," + level + ",UniversalHost,Host,System");
    }
    
    @Override
    public void stop(Promise<Void> stopPromise) {
        // Clean up milestones
        List<Future> cleanupFutures = new ArrayList<>();
        for (MilestoneManager milestone : milestones.values()) {
            cleanupFutures.add(milestone.cleanup());
        }
        
        CompositeFuture.all(cleanupFutures)
            .onComplete(ar -> {
                activeConversations.clear();
                log("UniversalHost (Simplified) stopped", 1);
                stopPromise.complete();
            });
    }
}
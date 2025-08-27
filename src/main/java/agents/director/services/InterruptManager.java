package agents.director.services;

import io.vertx.core.Vertx;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.core.json.JsonObject;

/**
 * Manages interrupt states for conversations using Vert.x SharedData.
 * Allows users to stop agent execution and provide feedback.
 */
public class InterruptManager {
    private final Vertx vertx;
    private static final String INTERRUPT_MAP = "conversation-interrupts";
    private static final String STATE_MAP = "conversation-states";
    private static final String FEEDBACK_MAP = "conversation-feedback";
    
    public enum ConversationState {
        PLANNING,
        EXECUTING,
        PAUSED,
        COMPLETED,
        CANCELLED
    }
    
    public InterruptManager(Vertx vertx) {
        this.vertx = vertx;
    }
    
    /**
     * Mark a conversation as interrupted
     */
    public void interrupt(String streamId, String reason) {
        LocalMap<String, JsonObject> interrupts = vertx.sharedData().getLocalMap(INTERRUPT_MAP);
        interrupts.put(streamId, new JsonObject()
            .put("interrupted", true)
            .put("reason", reason)
            .put("timestamp", System.currentTimeMillis()));
            
        // Update state to paused
        setState(streamId, ConversationState.PAUSED);
    }
    
    /**
     * Check if a conversation is interrupted
     */
    public boolean isInterrupted(String streamId) {
        LocalMap<String, JsonObject> interrupts = vertx.sharedData().getLocalMap(INTERRUPT_MAP);
        JsonObject interrupt = interrupts.get(streamId);
        return interrupt != null && interrupt.getBoolean("interrupted", false);
    }
    
    /**
     * Clear interrupt state
     */
    public void clearInterrupt(String streamId) {
        LocalMap<String, JsonObject> interrupts = vertx.sharedData().getLocalMap(INTERRUPT_MAP);
        interrupts.remove(streamId);
    }
    
    /**
     * Set conversation state
     */
    public void setState(String streamId, ConversationState state) {
        LocalMap<String, String> states = vertx.sharedData().getLocalMap(STATE_MAP);
        states.put(streamId, state.name());
    }
    
    /**
     * Get conversation state
     */
    public ConversationState getState(String streamId) {
        LocalMap<String, String> states = vertx.sharedData().getLocalMap(STATE_MAP);
        String state = states.get(streamId);
        return state != null ? ConversationState.valueOf(state) : ConversationState.PLANNING;
    }
    
    /**
     * Store user feedback
     */
    public void storeFeedback(String streamId, JsonObject feedback) {
        LocalMap<String, JsonObject> feedbackMap = vertx.sharedData().getLocalMap(FEEDBACK_MAP);
        feedbackMap.put(streamId, feedback);
    }
    
    /**
     * Get and clear user feedback
     */
    public JsonObject getFeedback(String streamId) {
        LocalMap<String, JsonObject> feedbackMap = vertx.sharedData().getLocalMap(FEEDBACK_MAP);
        return feedbackMap.remove(streamId);
    }
    
    /**
     * Clean up all data for a stream
     */
    public void cleanup(String streamId) {
        clearInterrupt(streamId);
        vertx.sharedData().getLocalMap(STATE_MAP).remove(streamId);
        vertx.sharedData().getLocalMap(FEEDBACK_MAP).remove(streamId);
    }
}
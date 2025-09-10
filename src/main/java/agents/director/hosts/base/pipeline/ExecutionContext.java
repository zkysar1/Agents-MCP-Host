package agents.director.hosts.base.pipeline;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Execution context that maintains shared state between pipeline steps.
 * This context is passed through all pipeline steps to maintain continuity
 * and allow steps to access results from previous steps.
 */
public class ExecutionContext {
    
    private final String contextId;
    private final String conversationId;
    private final String sessionId;
    private final long startTime;
    private final String originalQuery;
    private final JsonArray conversationHistory;
    
    // Step results storage
    private final Map<String, JsonObject> stepResults = new ConcurrentHashMap<>();
    private final Map<String, Long> stepTimings = new ConcurrentHashMap<>();
    private final Map<String, Exception> stepErrors = new ConcurrentHashMap<>();
    
    // Pipeline execution state
    private volatile int currentStepIndex = 0;
    private volatile int completedSteps = 0;
    private volatile boolean streaming = true;
    private volatile String currentStrategy = "unknown";
    
    // Metadata
    private final Map<String, Object> metadata = new ConcurrentHashMap<>();
    
    /**
     * Create a new execution context
     * @param conversationId The conversation identifier
     * @param sessionId The session identifier for streaming
     * @param originalQuery The original user query
     * @param conversationHistory The conversation history
     */
    public ExecutionContext(String conversationId, String sessionId, String originalQuery, JsonArray conversationHistory) {
        this.contextId = UUID.randomUUID().toString();
        this.conversationId = conversationId;
        this.sessionId = sessionId;
        this.originalQuery = originalQuery;
        this.conversationHistory = conversationHistory != null ? conversationHistory.copy() : new JsonArray();
        this.startTime = System.currentTimeMillis();
    }
    
    /**
     * Create execution context with minimal parameters
     */
    public ExecutionContext(String conversationId, String originalQuery) {
        this(conversationId, null, originalQuery, new JsonArray());
    }
    
    // Getters for basic context info
    public String getContextId() { return contextId; }
    public String getConversationId() { return conversationId; }
    public String getSessionId() { return sessionId; }
    public long getStartTime() { return startTime; }
    public String getOriginalQuery() { return originalQuery; }
    public JsonArray getConversationHistory() { return conversationHistory.copy(); }
    
    // Pipeline execution state
    public int getCurrentStepIndex() { return currentStepIndex; }
    public void setCurrentStepIndex(int index) { this.currentStepIndex = index; }
    
    public int getCompletedSteps() { return completedSteps; }
    public void incrementCompletedSteps() { this.completedSteps++; }
    
    public boolean isStreaming() { return streaming; }
    public void setStreaming(boolean streaming) { this.streaming = streaming; }
    
    public String getCurrentStrategy() { return currentStrategy; }
    public void setCurrentStrategy(String strategy) { this.currentStrategy = strategy; }
    
    // Step result management
    public void storeStepResult(String stepName, JsonObject result) {
        stepResults.put(stepName, result != null ? result.copy() : new JsonObject());
        
        // Also store with normalized key for common lookups
        String normalizedKey = normalizeStepName(stepName);
        if (!normalizedKey.equals(stepName)) {
            stepResults.put(normalizedKey, result != null ? result.copy() : new JsonObject());
        }
    }
    
    public JsonObject getStepResult(String stepName) {
        JsonObject result = stepResults.get(stepName);
        if (result == null) {
            // Try normalized version
            result = stepResults.get(normalizeStepName(stepName));
        }
        return result != null ? result.copy() : null;
    }
    
    public JsonObject getStepResultSafe(String stepName) {
        return getStepResultSafe(stepName, new JsonObject());
    }
    
    public JsonObject getStepResultSafe(String stepName, JsonObject defaultValue) {
        JsonObject result = getStepResult(stepName);
        return result != null ? result : (defaultValue != null ? defaultValue.copy() : new JsonObject());
    }
    
    public boolean hasStepResult(String stepName) {
        return stepResults.containsKey(stepName) || stepResults.containsKey(normalizeStepName(stepName));
    }
    
    public Set<String> getCompletedStepNames() {
        return Set.copyOf(stepResults.keySet());
    }
    
    // Step timing tracking
    public void recordStepTiming(String stepName, long duration) {
        stepTimings.put(stepName, duration);
    }
    
    public Long getStepTiming(String stepName) {
        return stepTimings.get(stepName);
    }
    
    public long getTotalDuration() {
        return System.currentTimeMillis() - startTime;
    }
    
    // Error tracking
    public void recordStepError(String stepName, Exception error) {
        stepErrors.put(stepName, error);
    }
    
    public Exception getStepError(String stepName) {
        return stepErrors.get(stepName);
    }
    
    public boolean hasErrors() {
        return !stepErrors.isEmpty();
    }
    
    public Set<String> getStepsWithErrors() {
        return Set.copyOf(stepErrors.keySet());
    }
    
    // Metadata management
    public void setMetadata(String key, Object value) {
        metadata.put(key, value);
    }
    
    public Object getMetadata(String key) {
        return metadata.get(key);
    }
    
    public Object getMetadata(String key, Object defaultValue) {
        return metadata.getOrDefault(key, defaultValue);
    }
    
    // Helper methods for common data access patterns
    
    /**
     * Get the most recent user message from conversation history
     */
    public String getLatestUserMessage() {
        for (int i = conversationHistory.size() - 1; i >= 0; i--) {
            JsonObject message = conversationHistory.getJsonObject(i);
            if ("user".equals(message.getString("role"))) {
                return message.getString("content");
            }
        }
        return originalQuery; // fallback to original query
    }
    
    /**
     * Get recent conversation history up to maxMessages
     */
    public JsonArray getRecentHistory(int maxMessages) {
        int start = Math.max(0, conversationHistory.size() - maxMessages);
        JsonArray recent = new JsonArray();
        for (int i = start; i < conversationHistory.size(); i++) {
            recent.add(conversationHistory.getValue(i));
        }
        return recent;
    }
    
    /**
     * Add a message to conversation history
     */
    public void addMessage(String role, String content) {
        conversationHistory.add(new JsonObject()
            .put("role", role)
            .put("content", content)
            .put("timestamp", System.currentTimeMillis()));
    }
    
    /**
     * Get the executed SQL from various possible sources
     */
    public String getExecutedSQL() {
        // Try validated SQL first
        JsonObject validated = getStepResultSafe("validate_oracle_sql");
        if (validated.containsKey("sql")) {
            return validated.getString("sql");
        }
        
        // Then optimized
        JsonObject optimized = getStepResultSafe("optimize_oracle_sql");
        if (optimized.containsKey("optimizedSQL")) {
            return optimized.getString("optimizedSQL");
        }
        if (optimized.containsKey("sql")) {
            return optimized.getString("sql");
        }
        
        // Then generated
        JsonObject generated = getStepResultSafe("generate_oracle_sql");
        if (generated.containsKey("sql")) {
            return generated.getString("sql");
        }
        
        return "";
    }
    
    /**
     * Calculate confidence based on pipeline execution
     */
    public double calculateConfidence() {
        double confidence = 1.0;
        
        // Reduce confidence if optional steps failed
        if (!hasStepResult("map_business_terms")) {
            confidence *= 0.95;
        }
        if (!hasStepResult("optimize_oracle_sql")) {
            confidence *= 0.95;
        }
        
        // Reduce confidence if validation had issues
        JsonObject validation = getStepResult("validate_oracle_sql");
        if (validation != null && !validation.getBoolean("valid", true)) {
            confidence *= 0.8;
        }
        
        // Reduce confidence for errors
        if (hasErrors()) {
            confidence *= 0.7;
        }
        
        return Math.max(0.1, confidence);
    }
    
    /**
     * Get summary of execution state
     */
    public JsonObject getExecutionSummary() {
        return new JsonObject()
            .put("contextId", contextId)
            .put("conversationId", conversationId)
            .put("currentStep", currentStepIndex)
            .put("completedSteps", completedSteps)
            .put("totalDuration", getTotalDuration())
            .put("hasErrors", hasErrors())
            .put("errorCount", stepErrors.size())
            .put("resultCount", stepResults.size())
            .put("strategy", currentStrategy)
            .put("confidence", calculateConfidence());
    }
    
    /**
     * Normalize step names for consistent lookups
     */
    private String normalizeStepName(String stepName) {
        // Map tool names to simplified keys
        switch (stepName) {
            case "strategy_generation__analyze_complexity":
                return "complexity_analysis";
            case "strategy_generation__create_strategy":
                return "strategy_creation";
            case "intent_analysis__extract_intent":
                return "intent_extraction";
            case "strategy_orchestrator__evaluate_progress":
                return "progress_evaluation";
            case "strategy_orchestrator__execute_step":
                return "step_execution";
            case "strategy_orchestrator__adapt_strategy":
                return "strategy_adaptation";
            case "strategy_learning__record_execution":
                return "execution_recording";
            case "strategy_learning__analyze_patterns":
                return "pattern_analysis";
            case "strategy_learning__suggest_improvements":
                return "improvement_suggestions";
            default:
                return stepName;
        }
    }
    
    @Override
    public String toString() {
        return String.format("ExecutionContext[id=%s, conversation=%s, steps=%d/%d, duration=%dms, errors=%d]",
            contextId, conversationId, completedSteps, currentStepIndex + 1, getTotalDuration(), stepErrors.size());
    }
}
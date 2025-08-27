package agents.director.mcp.servers;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import java.util.UUID;

/**
 * Enhanced orchestration context that travels through the Intent Engine.
 * 
 * This context object contains all accumulated knowledge and state during
 * an orchestration execution. Tools that need context receive this object
 * and can access whatever information they need from it.
 * 
 * Key principle: No mapping or resolution needed. Data is simply stored
 * here and tools access it directly.
 */
public class OrchestrationContext {
    
    // Core identity
    private final String id;
    private final String originalQuery;
    private final String sessionId;
    private final String streamId;
    private final long startTime;
    
    // Current execution state
    private int currentStep;
    private String currentTool;
    
    // Accumulated understanding
    private JsonObject deepAnalysis;
    private JsonObject schemaKnowledge;
    private JsonObject businessTerms;
    private JsonObject sampleData;
    private JsonObject relationships;
    
    // Execution history
    private final JsonArray executionHistory;
    private final JsonObject toolResults;
    
    // Error handling and validation
    private final JsonArray errors;
    private final JsonArray warnings;
    
    // Metadata
    private final JsonObject confidenceScores;
    private JsonObject lastGeneratedSql;
    private JsonObject lastSchemaValidation;
    
    // Semantic understanding tracking
    private final JsonObject semanticUnderstandings;
    private final JsonObject conceptMappings;
    private final JsonObject explorationHistory;
    private final JsonArray stepRetries;
    
    public OrchestrationContext(String originalQuery, String sessionId, String streamId) {
        this.id = UUID.randomUUID().toString();
        this.originalQuery = originalQuery;
        this.sessionId = sessionId;
        this.streamId = streamId;
        this.startTime = System.currentTimeMillis();
        this.currentStep = 0;
        
        this.executionHistory = new JsonArray();
        this.toolResults = new JsonObject();
        this.errors = new JsonArray();
        this.warnings = new JsonArray();
        this.confidenceScores = new JsonObject();
        
        this.semanticUnderstandings = new JsonObject();
        this.conceptMappings = new JsonObject();
        this.explorationHistory = new JsonObject();
        this.stepRetries = new JsonArray();
    }
    
    /**
     * Record a tool execution
     */
    public void recordExecution(String toolName, JsonObject arguments, JsonObject result, long duration) {
        // Create execution record as JsonObject
        JsonObject execution = new JsonObject()
            .put("toolName", toolName)
            .put("arguments", arguments)
            .put("result", result)
            .put("timestamp", System.currentTimeMillis())
            .put("duration", duration)
            .put("stepNumber", currentStep);
        
        executionHistory.add(execution);
        
        // Store with step number to avoid overwrites
        String resultKey = currentStep + "_" + toolName;
        toolResults.put(resultKey, result);
        
        // Update specific knowledge areas based on tool
        updateKnowledgeFromTool(toolName, result);
        
        // Record errors if present
        if (result.containsKey("error") || result.getBoolean("isError", false)) {
            errors.add(new JsonObject()
                .put("tool", toolName)
                .put("error", result.getString("error", "Unknown error"))
                .put("step", currentStep)
                .put("timestamp", System.currentTimeMillis()));
        }
    }
    
    /**
     * Update accumulated knowledge based on tool results
     */
    private void updateKnowledgeFromTool(String toolName, JsonObject result) {
        // Don't skip errors - we still want to track what happened
        boolean isError = result.containsKey("error") || result.getBoolean("isError", false);
        
        // Only update knowledge if not an error
        if (!isError) {
            switch (toolName) {
            case "deep_analyze_query":
            case "analyze_query":
                this.deepAnalysis = result;
                break;
                
            case "smart_schema_match":
            case "match_schema":
                this.schemaKnowledge = result;
                break;
                
            case "discover_sample_data":
                this.sampleData = result;
                break;
                
            case "map_business_terms":
                this.businessTerms = result;
                break;
                
            case "infer_relationships":
                this.relationships = result;
                break;
                
            case "generate_sql":
                this.lastGeneratedSql = result;
                break;
                
            case "validate_schema_sql":
                this.lastSchemaValidation = result;
                break;
            }
        }
    }
    
    /**
     * Get the context as a JsonObject for tools that need it
     */
    public JsonObject toJson() {
        JsonObject json = new JsonObject()
            .put("id", id)
            .put("originalQuery", originalQuery)
            .put("sessionId", sessionId)
            .put("streamId", streamId)
            .put("currentStep", currentStep)
            .put("elapsedMs", System.currentTimeMillis() - startTime);
        
        // Add accumulated knowledge (only non-null values)
        if (deepAnalysis != null) {
            json.put("deepAnalysis", deepAnalysis);
        }
        if (schemaKnowledge != null) {
            json.put("schemaKnowledge", schemaKnowledge);
        }
        if (businessTerms != null) {
            json.put("businessTerms", businessTerms);
        }
        if (sampleData != null) {
            json.put("sampleData", sampleData);
        }
        if (relationships != null) {
            json.put("relationships", relationships);
        }
        if (lastGeneratedSql != null) {
            json.put("lastGeneratedSql", lastGeneratedSql);
        }
        if (lastSchemaValidation != null) {
            json.put("lastSchemaValidation", lastSchemaValidation);
        }
        
        // Add execution history (limit to last 20 to avoid bloat)
        JsonArray limitedHistory = new JsonArray();
        int historySize = executionHistory.size();
        int startIndex = Math.max(0, historySize - 20);
        for (int i = startIndex; i < historySize; i++) {
            limitedHistory.add(executionHistory.getJsonObject(i));
        }
        json.put("executionHistory", limitedHistory);
        
        // Add confidence scores
        if (confidenceScores.size() > 0) {
            json.put("confidenceScores", confidenceScores);
        }
        
        // Add semantic understanding data
        if (semanticUnderstandings.size() > 0) {
            json.put("semanticUnderstandings", semanticUnderstandings);
        }
        if (conceptMappings.size() > 0) {
            json.put("conceptMappings", conceptMappings);
        }
        
        // Add errors and warnings if any
        if (errors.size() > 0) {
            json.put("errors", errors);
        }
        if (warnings.size() > 0) {
            json.put("warnings", warnings);
        }
        
        return json;
    }
    
    // Getters
    public String getId() { return id; }
    public String getOriginalQuery() { return originalQuery; }
    public String getSessionId() { return sessionId; }
    public String getStreamId() { return streamId; }
    public long getStartTime() { return startTime; }
    public int getCurrentStep() { return currentStep; }
    public String getCurrentTool() { return currentTool; }
    public JsonObject getDeepAnalysis() { return deepAnalysis; }
    public JsonObject getSchemaKnowledge() { return schemaKnowledge; }
    public JsonObject getSampleData() { return sampleData; }
    public JsonObject getLastGeneratedSql() { return lastGeneratedSql; }
    public JsonArray getExecutionHistory() { return executionHistory; }
    public JsonObject getToolResults() { return toolResults; }
    public JsonArray getErrors() { return errors; }
    
    /**
     * Get results organized by step name for backward compatibility
     * This maps step names to their tool results
     */
    public JsonObject getStepResults() {
        JsonObject stepResults = new JsonObject();
        
        // Map common tool names to step names
        if (deepAnalysis != null) {
            stepResults.put("Analyze Query", deepAnalysis);
        }
        if (schemaKnowledge != null) {
            stepResults.put("Match Schema", schemaKnowledge);
        }
        if (sampleData != null) {
            stepResults.put("Discover Data", sampleData);
        }
        if (businessTerms != null) {
            stepResults.put("Map Business Terms", businessTerms);
        }
        if (relationships != null) {
            stepResults.put("Infer Relationships", relationships);
        }
        if (lastGeneratedSql != null) {
            stepResults.put("Generate SQL", lastGeneratedSql);
        }
        if (lastSchemaValidation != null) {
            stepResults.put("Validate SQL", lastSchemaValidation);
        }
        
        return stepResults;
    }
    
    // Setters for state
    public void setCurrentStep(int step) { this.currentStep = step; }
    public void setCurrentTool(String tool) { this.currentTool = tool; }
    public void addError(JsonObject error) { this.errors.add(error); }
    public void addWarning(JsonObject warning) { this.warnings.add(warning); }
    public void setConfidence(String key, double confidence) { 
        this.confidenceScores.put(key, confidence); 
    }
    
    /**
     * Record semantic learning from error analysis
     */
    public void recordSemanticLearning(String concept, JsonObject understanding) {
        semanticUnderstandings.put(concept, understanding);
    }
    
    /**
     * Update schema knowledge from exploration
     */
    public void updateSchemaKnowledge(JsonObject newKnowledge) {
        if (schemaKnowledge == null) {
            schemaKnowledge = new JsonObject();
        }
        schemaKnowledge.mergeIn(newKnowledge);
    }
    
    /**
     * Get step retry count
     */
    public int getStepRetries(int stepIndex) {
        // Ensure array is large enough
        while (stepRetries.size() <= stepIndex) {
            stepRetries.add(0);
        }
        return stepRetries.getInteger(stepIndex);
    }
    
    /**
     * Increment step retry count
     */
    public void incrementStepRetry(int stepIndex) {
        // Ensure array is large enough
        while (stepRetries.size() <= stepIndex) {
            stepRetries.add(0);
        }
        int current = stepRetries.getInteger(stepIndex);
        stepRetries.set(stepIndex, current + 1);
    }
    
    /**
     * Get semantic understandings
     */
    public JsonObject getSemanticUnderstandings() {
        return semanticUnderstandings;
    }
    
    /**
     * Get a compact version of context with only essential data
     * Used for tools that need minimal context
     */
    public JsonObject toCompactJson() {
        JsonObject compact = new JsonObject()
            .put("originalQuery", originalQuery)
            .put("currentStep", currentStep);
        
        // Only include key knowledge fields if they exist
        if (deepAnalysis != null) {
            compact.put("hasAnalysis", true);
        }
        if (schemaKnowledge != null) {
            compact.put("hasSchemaKnowledge", true);
        }
        if (lastGeneratedSql != null && lastGeneratedSql.containsKey("sql")) {
            compact.put("lastSql", lastGeneratedSql.getString("sql"));
        }
        
        // Include error count if any
        if (errors.size() > 0) {
            compact.put("errorCount", errors.size());
        }
        
        return compact;
    }
}
package agents.director.hosts.base;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import java.util.*;

/**
 * Simple context object that flows through the milestone pipeline.
 * Each milestone adds its results to this context for the next milestone to use.
 * 
 * This replaces the complex JsonObject merging and step results management
 * with a clear, typed structure that's easy to understand and debug.
 */
public class MilestoneContext {
    
    // Core inputs
    private final String backstory;
    private final String guidance;
    private final String query;
    private final String conversationId;
    private final String sessionId;
    private final boolean streaming;
    
    // Milestone 1: Intent Analysis
    private String intent;
    private String intentType;
    private JsonObject intentDetails;
    
    // Milestone 2: Schema Discovery
    private List<String> relevantTables;
    private Map<String, String> tableDescriptions;
    private JsonObject schemaDetails;
    
    // Milestone 3: Data Statistics
    private Map<String, JsonArray> tableColumns;  // JsonArray of full column objects
    private Map<String, JsonObject> columnStats;
    private JsonObject dataProfile;
    
    // Milestone 4: SQL Generation
    private String generatedSql;
    private String sqlExplanation;
    private JsonObject sqlMetadata;
    
    // Milestone 5: Execution
    private JsonArray queryResults;
    private int rowCount;
    private long executionTime;
    private JsonObject executionMetadata;
    
    // Milestone 6: Natural Response
    private String naturalResponse;
    private JsonObject responseMetadata;
    
    // Tracking
    private int currentMilestone = 0;
    private int targetMilestone;
    private long startTime;
    private Map<Integer, Long> milestoneTimes = new HashMap<>();
    private Map<String, Object> customData = new HashMap<>();
    
    /**
     * Constructor with required fields
     */
    public MilestoneContext(String backstory, String guidance, String query,
                           String conversationId, String sessionId, boolean streaming) {
        this.backstory = backstory != null ? backstory : "General assistant";
        this.guidance = guidance != null ? guidance : "Help the user with their request";
        this.query = query;
        this.conversationId = conversationId;
        this.sessionId = sessionId;
        this.streaming = streaming;
        this.startTime = System.currentTimeMillis();
        
        // Initialize collections
        this.relevantTables = new ArrayList<>();
        this.tableDescriptions = new HashMap<>();
        this.tableColumns = new HashMap<>();
        this.columnStats = new HashMap<>();
    }
    
    /**
     * Record milestone completion
     */
    public void completeMilestone(int milestone) {
        this.currentMilestone = milestone;
        this.milestoneTimes.put(milestone, System.currentTimeMillis() - startTime);
    }
    
    /**
     * Get total execution time
     */
    public long getTotalTime() {
        return System.currentTimeMillis() - startTime;
    }
    
    /**
     * Get milestone execution time
     */
    public long getMilestoneTime(int milestone) {
        return milestoneTimes.getOrDefault(milestone, 0L);
    }
    
    /**
     * Convert context to JsonObject for logging/debugging
     */
    public JsonObject toJson() {
        return new JsonObject()
            .put("conversationId", conversationId)
            .put("query", query)
            .put("backstory", backstory)
            .put("guidance", guidance)
            .put("currentMilestone", currentMilestone)
            .put("targetMilestone", targetMilestone)
            .put("intent", intent)
            .put("relevantTables", new JsonArray(relevantTables))
            .put("generatedSql", generatedSql)
            .put("rowCount", rowCount)
            .put("naturalResponse", naturalResponse)
            .put("totalTime", getTotalTime());
    }
    
    // Getters and Setters
    public String getBackstory() { return backstory; }
    public String getGuidance() { return guidance; }
    public String getQuery() { return query; }
    public String getConversationId() { return conversationId; }
    public String getSessionId() { return sessionId; }
    public boolean isStreaming() { return streaming; }
    
    public String getIntent() { return intent; }
    public void setIntent(String intent) { this.intent = intent; }
    
    public String getIntentType() { return intentType; }
    public void setIntentType(String intentType) { this.intentType = intentType; }
    
    public JsonObject getIntentDetails() { return intentDetails; }
    public void setIntentDetails(JsonObject details) { this.intentDetails = details; }
    
    public List<String> getRelevantTables() { return relevantTables; }
    public void addRelevantTable(String table) { this.relevantTables.add(table); }
    public void setRelevantTables(List<String> tables) { this.relevantTables = tables; }
    
    public Map<String, String> getTableDescriptions() { return tableDescriptions; }
    public void setTableDescription(String table, String description) { 
        this.tableDescriptions.put(table, description); 
    }
    
    public JsonObject getSchemaDetails() { return schemaDetails; }
    public void setSchemaDetails(JsonObject details) { this.schemaDetails = details; }
    
    public Map<String, JsonArray> getTableColumns() { return tableColumns; }
    public void setTableColumns(String table, JsonArray columns) { 
        this.tableColumns.put(table, columns); 
    }
    
    public Map<String, JsonObject> getColumnStats() { return columnStats; }
    public void setColumnStats(String column, JsonObject stats) { 
        this.columnStats.put(column, stats); 
    }
    
    public JsonObject getDataProfile() { return dataProfile; }
    public void setDataProfile(JsonObject profile) { this.dataProfile = profile; }
    
    public String getGeneratedSql() { return generatedSql; }
    public void setGeneratedSql(String sql) { this.generatedSql = sql; }
    
    public String getSqlExplanation() { return sqlExplanation; }
    public void setSqlExplanation(String explanation) { this.sqlExplanation = explanation; }
    
    public JsonObject getSqlMetadata() { return sqlMetadata; }
    public void setSqlMetadata(JsonObject metadata) { this.sqlMetadata = metadata; }
    
    public JsonArray getQueryResults() { return queryResults; }
    public void setQueryResults(JsonArray results) { 
        this.queryResults = results;
        this.rowCount = results != null ? results.size() : 0;
    }
    
    public int getRowCount() { return rowCount; }
    public void setRowCount(int count) { this.rowCount = count; }
    
    public long getExecutionTime() { return executionTime; }
    public void setExecutionTime(long time) { this.executionTime = time; }
    
    public JsonObject getExecutionMetadata() { return executionMetadata; }
    public void setExecutionMetadata(JsonObject metadata) { this.executionMetadata = metadata; }
    
    public String getNaturalResponse() { return naturalResponse; }
    public void setNaturalResponse(String response) { this.naturalResponse = response; }
    
    public JsonObject getResponseMetadata() { return responseMetadata; }
    public void setResponseMetadata(JsonObject metadata) { this.responseMetadata = metadata; }
    
    public int getCurrentMilestone() { return currentMilestone; }
    public void setCurrentMilestone(int milestone) { this.currentMilestone = milestone; }
    
    public int getTargetMilestone() { return targetMilestone; }
    public void setTargetMilestone(int target) { this.targetMilestone = target; }
    
    public Map<String, Object> getCustomData() { return customData; }
    public void setCustomData(String key, Object value) { this.customData.put(key, value); }
    public Object getCustomData(String key) { return customData.get(key); }
}
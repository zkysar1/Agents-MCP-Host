package agents.director.hosts.base;

/**
 * Enumeration of the 6 milestone types in the simplified architecture.
 * Each milestone represents a clear, sequential step in processing a query.
 * 
 * This replaces the complex ManagerType enum with a simpler, more intuitive structure.
 */
public enum MilestoneType {
    
    /**
     * Milestone 1: Extract and understand user intent
     */
    INTENT_EXTRACTION(1, "IntentMilestone", 
        "Extract user intent and share understanding with user"),
    
    /**
     * Milestone 2: Explore relevant database schema
     */
    SCHEMA_EXPLORATION(2, "SchemaMilestone", 
        "Explore database schema and identify relevant tables"),
    
    /**
     * Milestone 3: Analyze table columns and data statistics
     */
    DATA_STATS_ANALYSIS(3, "DataStatsMilestone", 
        "Analyze table columns and data statistics"),
    
    /**
     * Milestone 4: Generate SQL statement
     */
    SQL_GENERATION(4, "SQLGenerationMilestone", 
        "Create SQL statement based on gathered information"),
    
    /**
     * Milestone 5: Execute SQL and get results
     */
    SQL_EXECUTION(5, "ExecutionMilestone", 
        "Execute SQL query and retrieve results"),
    
    /**
     * Milestone 6: Generate natural language response
     */
    NATURAL_RESPONSE(6, "NaturalResponseMilestone", 
        "Generate natural language response to user's question");
    
    private final int milestoneNumber;
    private final String className;
    private final String description;
    
    MilestoneType(int milestoneNumber, String className, String description) {
        this.milestoneNumber = milestoneNumber;
        this.className = className;
        this.description = description;
    }
    
    /**
     * Gets the milestone number (1-6)
     */
    public int getMilestoneNumber() {
        return milestoneNumber;
    }
    
    /**
     * Gets the class name of the milestone
     */
    public String getClassName() {
        return className;
    }
    
    /**
     * Gets the description of what this milestone does
     */
    public String getDescription() {
        return description;
    }
    
    /**
     * Find a MilestoneType by its number
     */
    public static MilestoneType fromNumber(int number) {
        for (MilestoneType type : values()) {
            if (type.milestoneNumber == number) {
                return type;
            }
        }
        return null;
    }
    
    /**
     * Find a MilestoneType by its class name
     */
    public static MilestoneType fromClassName(String className) {
        for (MilestoneType type : values()) {
            if (type.className.equals(className)) {
                return type;
            }
        }
        return null;
    }
    
    /**
     * Get all milestone numbers as a simple string for prompts
     */
    public static String getMilestoneDescriptions() {
        StringBuilder sb = new StringBuilder();
        for (MilestoneType type : values()) {
            sb.append(type.milestoneNumber)
              .append(". ")
              .append(type.description)
              .append("\n");
        }
        return sb.toString();
    }
    
    @Override
    public String toString() {
        return milestoneNumber + "-" + className;
    }
}
package agents.director.config;

/**
 * Enumeration of available manager types in the UniversalHost architecture.
 * Each manager type represents a specialized functionality that can be
 * enabled or configured for an agent.
 */
public enum ManagerType {
    /**
     * Manages Oracle database execution operations
     */
    ORACLE_EXECUTION("OracleExecutionManager", "Manages Oracle database execution operations"),
    
    /**
     * Manages SQL pipeline operations and query processing
     */
    SQL_PIPELINE("SQLPipelineManager", "Manages SQL pipeline operations and query processing"),
    
    /**
     * Manages schema intelligence and database structure analysis
     */
    SCHEMA_INTELLIGENCE("SchemaIntelligenceManager", "Manages schema intelligence and database structure analysis"),
    
    /**
     * Manages intent analysis and natural language processing
     */
    INTENT_ANALYSIS("IntentAnalysisManager", "Manages intent analysis and natural language processing"),
    
    /**
     * Manages strategy orchestration and dynamic planning
     */
    STRATEGY_ORCHESTRATION("StrategyOrchestrationManager", "Manages strategy orchestration and dynamic planning"),
    
    /**
     * Base MCP client management functionality
     */
    MCP_CLIENT("MCPClientManager", "Base MCP client management functionality"),
    
    /**
     * Manages conversation interruption and control flow
     */
    INTERRUPT("InterruptManager", "Manages conversation interruption and control flow"),
    
    /**
     * Manages Oracle database connections
     */
    ORACLE_CONNECTION("OracleConnectionManager", "Manages Oracle database connections"),
    
    /**
     * Intent Engine - Intelligent query intent analysis and interpretation
     */
    INTENT_ENGINE("IntentEngine", "Intelligent query intent analysis and interpretation"),
    
    /**
     * Strategy Picker - Intelligent strategy selection and orchestration planning
     */
    STRATEGY_PICKER("StrategyPicker", "Intelligent strategy selection and orchestration planning");

    private final String className;
    private final String description;

    ManagerType(String className, String description) {
        this.className = className;
        this.description = description;
    }

    /**
     * Gets the class name of the manager
     * @return the manager class name
     */
    public String getClassName() {
        return className;
    }

    /**
     * Gets the description of what this manager does
     * @return the manager description
     */
    public String getDescription() {
        return description;
    }

    /**
     * Finds a ManagerType by its class name
     * @param className the class name to search for
     * @return the matching ManagerType, or null if not found
     */
    public static ManagerType fromClassName(String className) {
        for (ManagerType type : values()) {
            if (type.className.equals(className)) {
                return type;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return className;
    }
}
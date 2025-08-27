package agents.director.mcp.servers;

/**
 * Configuration constants for intelligent Oracle tools.
 * 
 * Centralizes all limits, thresholds, and timeouts to make them
 * easily adjustable without searching through code.
 */
public class IntelligenceConfig {
    
    // Analysis limits
    public static final int MAX_TABLES_TO_ANALYZE = 5;
    public static final int MAX_ENUM_TABLES_TO_PROCESS = 10;
    public static final int MAX_RELATIONSHIPS_PER_TABLE = 5;
    public static final int MAX_TABLES_TO_COMPARE = 10;
    
    // Sample sizes
    public static final int DEFAULT_SAMPLE_SIZE = 10;
    public static final int MAX_SAMPLE_SIZE = 100;
    
    // Timeouts (milliseconds)
    public static final long METADATA_TIMEOUT_MS = 5000;
    public static final long QUERY_TIMEOUT_MS = 10000;
    public static final long LLM_TIMEOUT_MS = 30000;
    
    // Cache settings
    public static final long PLAN_CACHE_TTL_MS = 10 * 60 * 1000; // 10 minutes
    public static final long SCHEMA_CACHE_TTL_MS = 30 * 60 * 1000; // 30 minutes
    public static final int MAX_CACHE_SIZE = 1000;
    
    // Complexity thresholds
    public static final double DEFAULT_OPTIMIZATION_THRESHOLD = 0.3;
    public static final double SIMPLE_QUERY_THRESHOLD = 0.2;
    public static final double COMPLEX_QUERY_THRESHOLD = 0.6;
    
    // Confidence thresholds
    public static final double MIN_MATCH_CONFIDENCE = 0.5;
    public static final double HIGH_RELEVANCE_THRESHOLD = 0.7;
    public static final double TERM_MAPPING_THRESHOLD = 0.6;
    
    // Token limits for LLM
    public static final int MAX_PROMPT_TOKENS = 1000;
    public static final int MAX_RESPONSE_TOKENS = 2000;
    
    // Performance settings
    public static final boolean ENABLE_PARALLEL_ANALYSIS = true;
    public static final int MAX_PARALLEL_OPERATIONS = 3;
    
    // Logging levels
    public static final boolean DEBUG_MODE = false;
    public static final boolean LOG_PERFORMANCE_METRICS = true;
    
    // Private constructor to prevent instantiation
    private IntelligenceConfig() {}
}
package agents.director.mcp.clients;

import agents.director.mcp.base.MCPClientBase;

/**
 * MCP Client for Oracle Query Analysis Server.
 * Provides tools for analyzing SQL queries and extracting query components.
 * 
 * Available tools:
 * - analyze_query: Analyze a natural language query for SQL generation
 * - extract_query_tokens: Extract key tokens and patterns from queries
 * 
 * This clients maintains a 1:1 relationship with the Oracle Query Analysis Server.
 */
public class OracleQueryAnalysisClient extends MCPClientBase {
    
    private static final String SERVER_PATH = "/mcp/servers/oracle-query-analysis";
    
    /**
     * Create a new Oracle Query Analysis clients
     * @param baseUrl The base URL (e.g., http://localhost:8080)
     */
    public OracleQueryAnalysisClient(String baseUrl) {
        super("OracleQueryAnalysis", baseUrl + SERVER_PATH);
    }
    
    @Override
    protected void onClientReady() {
        // Log available tools for verification
        if (tools.size() > 0) {
            vertx.eventBus().publish("log", 
                "OracleQueryAnalysisClient ready with tools: " + tools.keySet() + 
                ",2,OracleQueryAnalysisClient,MCP,System");
        }
    }
}
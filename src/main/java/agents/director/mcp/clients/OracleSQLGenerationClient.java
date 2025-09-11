package agents.director.mcp.clients;

import agents.director.mcp.base.MCPClientBase;

/**
 * MCP Client for Oracle SQL Generation Server.
 * Provides tools for generating and optimizing Oracle SQL queries.
 * 
 * Available tools:
 * - generate_oracle_sql: Generate SQL from natural language queries
 * - optimize_oracle_sql: Optimize existing SQL queries
 * 
 * This clients maintains a 1:1 relationship with the Oracle SQL Generation Server.
 */
public class OracleSQLGenerationClient extends MCPClientBase {
    
    private static final String SERVER_PATH = "/mcp/servers/oracle-sql-gen";
    
    /**
     * Create a new Oracle SQL Generation clients
     * @param baseUrl The base URL (e.g., http://localhost:8080)
     */
    public OracleSQLGenerationClient(String baseUrl) {
        super("OracleSQLGeneration", baseUrl + SERVER_PATH);
    }
    
    @Override
    protected void onClientReady() {
        // Log available tools for verification
        if (tools.size() > 0) {
            vertx.eventBus().publish("log", 
                "OracleSQLGenerationClient ready with tools: " + tools.keySet() + 
                ",2,OracleSQLGenerationClient,MCP,System");
        }
    }
}
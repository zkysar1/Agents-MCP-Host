package agents.director.mcp.clients;

import agents.director.mcp.base.MCPClientBase;

/**
 * MCP Client for Oracle SQL Validation Server.
 * Provides tools for validating SQL syntax and explaining errors.
 * 
 * Available tools:
 * - validate_oracle_sql: Validate SQL syntax and semantics
 * - explain_oracle_error: Explain Oracle error codes
 * - explain_plan: Generate and explain query execution plans
 * 
 * This clients maintains a 1:1 relationship with the Oracle SQL Validation Server.
 */
public class OracleSQLValidationClient extends MCPClientBase {
    
    private static final String SERVER_PATH = "/mcp/servers/oracle-sql-val";
    
    /**
     * Create a new Oracle SQL Validation clients
     * @param baseUrl The base URL (e.g., http://localhost:8080)
     */
    public OracleSQLValidationClient(String baseUrl) {
        super("OracleSQLValidation", baseUrl + SERVER_PATH);
    }
    
    @Override
    protected void onClientReady() {
        // Log available tools for verification
        if (tools.size() > 0) {
            vertx.eventBus().publish("log", 
                "OracleSQLValidationClient ready with tools: " + tools.keySet() + 
                ",2,OracleSQLValidationClient,MCP,System");
        }
    }
}
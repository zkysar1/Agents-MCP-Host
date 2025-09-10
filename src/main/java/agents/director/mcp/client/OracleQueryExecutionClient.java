package agents.director.mcp.client;

import agents.director.mcp.base.MCPClientBase;
import io.vertx.core.Vertx;

/**
 * MCP Client for Oracle Query Execution Server.
 * Provides tools for executing SQL queries against Oracle database and formatting results.
 * 
 * Available tools:
 * - run_oracle_query: Execute SQL queries with optional row limits
 * - get_oracle_schema: Retrieve database schema information
 * - format_results: Convert query results to natural language
 * 
 * This client maintains a 1:1 relationship with the Oracle Query Execution Server.
 */
public class OracleQueryExecutionClient extends MCPClientBase {
    
    private static final String SERVER_PATH = "/mcp/servers/oracle-db";
    
    /**
     * Create a new Oracle Query Execution client
     * @param baseUrl The base URL (e.g., http://localhost:8080)
     */
    public OracleQueryExecutionClient(String baseUrl) {
        super("OracleQueryExecution", baseUrl + SERVER_PATH);
    }
    
    @Override
    protected void onClientReady() {
        // Log available tools for verification
        if (tools.size() > 0) {
            vertx.eventBus().publish("log", 
                "OracleQueryExecutionClient ready with tools: " + tools.keySet() + 
                ",2,OracleQueryExecutionClient,MCP,System");
        }
    }
}
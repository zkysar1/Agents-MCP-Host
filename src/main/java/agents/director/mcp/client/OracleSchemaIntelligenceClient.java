package agents.director.mcp.client;

import agents.director.mcp.base.MCPClientBase;
import io.vertx.core.Vertx;

/**
 * MCP Client for Oracle Schema Intelligence Server.
 * Provides tools for understanding database schema, relationships, and strategies.
 * 
 * Available tools:
 * - match_oracle_schema: Match user query to database schema elements
 * - infer_table_relationships: Discover relationships between tables
 * - suggest_strategy: Suggest optimal query strategies
 * 
 * This client maintains a 1:1 relationship with the Oracle Schema Intelligence Server.
 */
public class OracleSchemaIntelligenceClient extends MCPClientBase {
    
    private static final String SERVER_PATH = "/mcp/servers/oracle-schema-intel";
    
    /**
     * Create a new Oracle Schema Intelligence client
     * @param baseUrl The base URL (e.g., http://localhost:8080)
     */
    public OracleSchemaIntelligenceClient(String baseUrl) {
        super("OracleSchemaIntelligence", baseUrl + SERVER_PATH);
    }
    
    @Override
    protected void onClientReady() {
        // Log available tools for verification
        if (tools.size() > 0) {
            vertx.eventBus().publish("log", 
                "OracleSchemaIntelligenceClient ready with tools: " + tools.keySet() + 
                ",2,OracleSchemaIntelligenceClient,MCP,System");
        }
    }
}
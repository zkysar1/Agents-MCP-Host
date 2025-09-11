package agents.director.mcp.clients;

import agents.director.mcp.base.MCPClientBase;

/**
 * MCP Client for Business Mapping Server.
 * Provides tools for mapping business terminology to database entities.
 * 
 * Available tools:
 * - map_business_terms: Map business terms to database columns/tables
 * - translate_enum: Translate enumerated values between business and technical terms
 * 
 * This clients maintains a 1:1 relationship with the Business Mapping Server.
 */
public class BusinessMappingClient extends MCPClientBase {
    
    private static final String SERVER_PATH = "/mcp/servers/business-map";
    
    /**
     * Create a new Business Mapping clients
     * @param baseUrl The base URL (e.g., http://localhost:8080)
     */
    public BusinessMappingClient(String baseUrl) {
        super("BusinessMapping", baseUrl + SERVER_PATH);
    }
    
    @Override
    protected void onClientReady() {
        // Log available tools for verification
        if (tools.size() > 0) {
            vertx.eventBus().publish("log", 
                "BusinessMappingClient ready with tools: " + tools.keySet() + 
                ",2,BusinessMappingClient,MCP,System");
        }
    }
}
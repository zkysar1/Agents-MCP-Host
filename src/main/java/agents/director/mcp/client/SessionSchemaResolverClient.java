package agents.director.mcp.client;

import agents.director.mcp.base.MCPClientBase;
import io.vertx.core.Vertx;

/**
 * MCP Client for Session Schema Resolver Server.
 * Provides tools for resolving session-specific schema information and patterns.
 * 
 * Available tools:
 * - resolve_table_schema: Resolve table schemas for specific sessions
 * - get_session_patterns: Get query patterns from session history
 * - discover_available_schemas: Discover available database schemas
 * - discover_column_semantics: Discover column semantic information
 * - discover_sample_data: Get sample data for understanding
 * 
 * This client maintains a 1:1 relationship with the Session Schema Resolver Server.
 */
public class SessionSchemaResolverClient extends MCPClientBase {
    
    private static final String SERVER_PATH = "/mcp/servers/session-schema-resolver";
    
    /**
     * Create a new Session Schema Resolver client
     * @param baseUrl The base URL (e.g., http://localhost:8080)
     */
    public SessionSchemaResolverClient(String baseUrl) {
        super("SessionSchemaResolver", baseUrl + SERVER_PATH);
    }
    
    @Override
    protected void onClientReady() {
        // Log available tools for verification
        if (tools.size() > 0) {
            vertx.eventBus().publish("log", 
                "SessionSchemaResolverClient ready with tools: " + tools.keySet() + 
                ",2,SessionSchemaResolverClient,MCP,System");
        }
    }
}
package AgentsMCPHost.mcp.clients;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.core.eventbus.Message;

import java.util.UUID;

/**
 * MCP Client wrapper for Oracle Tools Server.
 * 
 * This verticle acts as an MCP client that bridges the gap between
 * McpHostManager and the oracle-tools event bus handlers.
 * 
 * It maintains the single routing path:
 * Request → McpHostManager → OracleToolsClient → Event Bus Handler
 */
public class OracleToolsClientVerticle extends AbstractVerticle {
    
    private final String clientId = UUID.randomUUID().toString();
    private final String clientName = "oracle-tools";
    
    // List of tools exposed by oracle-tools server
    private final String[] toolNames = {
        "analyze_query",
        "match_schema", 
        "discover_enums",
        "discover_sample_data",
        "generate_sql",
        "optimize_sql",
        "validate_sql",
        "execute_query",
        "explain_plan",
        "format_results",
        "summarize_data"
    };
    
    @Override
    public void start(Promise<Void> startPromise) {
        // Wait for the corresponding server to be ready
        vertx.eventBus().consumer("mcp.server.ready", msg -> {
            JsonObject serverInfo = (JsonObject) msg.body();
            String serverName = serverInfo.getString("server");
            
            // Check if this is our server
            if ("oracle-tools".equals(serverName)) {
                System.out.println("[OracleToolsClient] Server oracle-tools is ready, initializing client...");
                
                // Register as MCP client
                registerWithMcpHost();
                
                // Register handler for tool calls from McpHostManager
                vertx.eventBus().consumer("mcp.client." + clientName + ".call", this::handleToolCall);
                
                // Publish tool discovery
                publishToolDiscovery();
                
                // Log through event bus to LoggerVerticle
                vertx.eventBus().publish("log", 
                    "OracleToolsClient started as MCP client,1,OracleToolsClient,StartUp,MCP");
                
                System.out.println("[OracleToolsClient] Started as MCP client for oracle-tools");
                System.out.println("[OracleToolsClient] Bridging " + toolNames.length + " tools to MCP infrastructure");
            }
        });
        
        startPromise.complete();
    }
    
    /**
     * Register this client with McpHostManager
     */
    private void registerWithMcpHost() {
        JsonObject registration = new JsonObject()
            .put("clientId", clientName)  // Use clientName as the ID
            .put("type", "local")
            .put("toolCount", toolNames.length)  // Add toolCount like other clients
            .put("ready", true);
        
        vertx.eventBus().publish("mcp.client.ready", registration);
        
        // Add extensive logging
        System.out.println("[OracleToolsClient] Publishing client ready event:");
        System.out.println("[OracleToolsClient]   clientId: " + clientName);
        System.out.println("[OracleToolsClient]   toolCount: " + toolNames.length);
        System.out.println("[OracleToolsClient] Registered with McpHostManager as: " + clientName);
        
        // Log to CSV
        vertx.eventBus().publish("log",
            "OracleToolsClient publishing client.ready with " + toolNames.length + " tools,2,OracleToolsClient,Registration,MCP");
    }
    
    /**
     * Publish tool discovery event for McpHostManager
     */
    private void publishToolDiscovery() {
        JsonArray tools = new JsonArray();
        
        for (String toolName : toolNames) {
            tools.add(new JsonObject()
                .put("name", toolName)
                .put("description", getToolDescription(toolName)));
        }
        
        JsonObject discovery = new JsonObject()
            .put("client", clientName)
            .put("server", clientName)  // Acts as both client and server
            .put("tools", tools);
        
        // Publish tool discovery
        vertx.eventBus().publish("mcp.tools.discovered", discovery);
        
        // Add extensive logging for debugging
        System.out.println("[OracleToolsClient] Publishing tools.discovered event:");
        System.out.println("[OracleToolsClient]   client: " + clientName);
        System.out.println("[OracleToolsClient]   server: " + clientName);
        System.out.println("[OracleToolsClient]   tools: " + tools.size());
        
        // Log discovery
        vertx.eventBus().publish("log",
            "OracleToolsClient published " + tools.size() + " tools,2,OracleToolsClient,Discovery,MCP");
        
        System.out.println("[OracleToolsClient] Published tool discovery: " + tools.size() + " tools");
    }
    
    /**
     * Handle tool call from McpHostManager
     */
    private void handleToolCall(Message<JsonObject> msg) {
        JsonObject request = msg.body();
        String toolName = request.getString("tool");
        JsonObject arguments = request.getJsonObject("arguments", new JsonObject());
        
        // Log the tool call
        vertx.eventBus().publish("log",
            "OracleToolsClient routing " + toolName + ",3,OracleToolsClient,Routing,Tool");
        
        System.out.println("[OracleToolsClient] Routing tool call: " + toolName);
        
        // Route to event bus handler
        String eventBusAddress = "tool." + clientName + "__" + toolName;
        
        vertx.eventBus().<JsonObject>request(eventBusAddress, 
            new JsonObject()
                .put("arguments", arguments)
                .put("tool", toolName))
            .onSuccess(reply -> {
                JsonObject result = reply.body();
                
                // Ensure proper MCP response format
                JsonObject mcpResponse = formatMcpResponse(result);
                
                // Log success
                vertx.eventBus().publish("log",
                    "Tool " + toolName + " completed,2,OracleToolsClient,Success,Tool");
                
                msg.reply(mcpResponse);
            })
            .onFailure(err -> {
                // Log error
                vertx.eventBus().publish("log",
                    "Tool " + toolName + " failed: " + err.getMessage() + ",0,OracleToolsClient,Error,Tool");
                
                // Return MCP error response
                JsonObject errorResponse = new JsonObject()
                    .put("isError", true)
                    .put("error", err.getMessage())
                    .put("content", new JsonArray()
                        .add(new JsonObject()
                            .put("type", "text")
                            .put("text", "Error: " + err.getMessage())));
                
                msg.reply(errorResponse);
            });
    }
    
    /**
     * Format response to match MCP protocol
     */
    private JsonObject formatMcpResponse(JsonObject rawResponse) {
        // If already in MCP format, return as-is
        if (rawResponse.containsKey("content") && rawResponse.getValue("content") instanceof JsonArray) {
            return rawResponse;
        }
        
        // Wrap in MCP format
        return new JsonObject()
            .put("content", new JsonArray()
                .add(new JsonObject()
                    .put("type", "text")
                    .put("text", rawResponse.encodePrettily())))
            .put("isError", false);
    }
    
    /**
     * Get tool description
     */
    private String getToolDescription(String toolName) {
        switch (toolName) {
            case "analyze_query":
                return "Analyze natural language query to extract intent and entities";
            case "match_schema":
                return "Match query entities to database schema";
            case "discover_enums":
                return "Discover enumeration tables and mappings";
            case "discover_sample_data":
                return "Get sample data from tables";
            case "generate_sql":
                return "Generate SQL from natural language using LLM";
            case "optimize_sql":
                return "Optimize SQL query for performance";
            case "validate_sql":
                return "Validate SQL syntax without execution";
            case "execute_query":
                return "Execute SQL query and return results";
            case "explain_plan":
                return "Get execution plan for SQL query";
            case "format_results":
                return "Format query results as natural language";
            case "summarize_data":
                return "Create statistical summary of data";
            default:
                return "Oracle tool: " + toolName;
        }
    }
    
    @Override
    public void stop(Promise<Void> stopPromise) {
        vertx.eventBus().publish("log",
            "OracleToolsClient stopped,1,OracleToolsClient,Shutdown,MCP");
        
        System.out.println("[OracleToolsClient] Stopped");
        stopPromise.complete();
    }
}
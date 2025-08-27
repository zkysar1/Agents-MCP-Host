package agents.director.mcp.clients;

import agents.director.services.VertxStreamableHttpTransport;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.core.eventbus.Message;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * MCP Client for Oracle Server.
 * 
 * This is a TRUE MCP client that connects to Oracle Server via HTTP on port 8086.
 * It follows the standard MCP protocol, enabling external applications like
 * Claude Desktop, VS Code, or other AI tools to also connect to the Oracle server.
 * 
 * Architecture:
 * - Connects via HTTP using VertxStreamableHttpTransport
 * - Discovers tools dynamically via MCP protocol
 * - Routes tool calls through HTTP, not event bus
 * - Enables true server-client separation
 */
public class OracleSQLClient extends AbstractVerticle {
    
    private VertxStreamableHttpTransport oracleTransport;
    
    // Track available tools discovered from server
    private final Map<String, JsonObject> availableTools = new ConcurrentHashMap<>();
    
    // Client ID for event bus addressing
    private static final String CLIENT_ID = "oracle";
    private static final int ORACLE_PORT = 8086;
    
    @Override
    public void start(Promise<Void> startPromise) {
        // Initialize HTTP transport to Oracle server
        oracleTransport = new VertxStreamableHttpTransport(vertx, "localhost", ORACLE_PORT);
        
        // Connect to Oracle server via HTTP
        connectToOracleServer()
            .onSuccess(v -> {
                // Register event bus consumers for host requests
                vertx.eventBus().consumer("mcp.client." + CLIENT_ID + ".call", this::handleToolCall);
                vertx.eventBus().consumer("mcp.client." + CLIENT_ID + ".status", this::handleStatusRequest);
                
                // Notify host that client is ready
                publishClientReady();
                
                vertx.eventBus().publish("log", "[OracleSQLClient] Connected to Oracle server via HTTP on port " + ORACLE_PORT + ",2,OracleSQLClient,MCP,System");
                vertx.eventBus().publish("log", "[OracleSQLClient] Discovered " + availableTools.size() + " tools" + ",2,OracleSQLClient,MCP,System");
                
                vertx.eventBus().publish("log", 
                    "OracleSQLClient ready with " + availableTools.size() + " tools,1,OracleSQLClient,StartUp,MCP");
                
                startPromise.complete();
            })
            .onFailure(err -> {
                vertx.eventBus().publish("log", "[OracleSQLClient] Failed to connect: " + err.getMessage() + ",0,OracleSQLClient,MCP,System");
                startPromise.fail(err);
            });
    }
    
    /**
     * Connect to Oracle server via HTTP and discover its tools
     */
    private Future<Void> connectToOracleServer() {
        Promise<Void> promise = Promise.promise();
        
        // Initialize connection (MCP protocol handshake)
        oracleTransport.initialize()
            .compose(initResponse -> {
                vertx.eventBus().publish("log", "[OracleSQLClient] Initialized MCP connection to Oracle server" + ",2,OracleSQLClient,MCP,System");
                // Protocol version might be a string or object
                Object protocolVersion = initResponse.getValue("protocolVersion");
                if (protocolVersion != null) {
                    vertx.eventBus().publish("log", "[OracleSQLClient] Protocol version: " + protocolVersion + ",2,OracleSQLClient,MCP,System");
                }
                
                // Request tools list via MCP protocol
                return oracleTransport.listTools();
            })
            .onSuccess(tools -> {
                // Store discovered tools WITHOUT any prefix manipulation
                for (int i = 0; i < tools.size(); i++) {
                    JsonObject tool = tools.getJsonObject(i);
                    String toolName = tool.getString("name");
                    
                    // Store tool as-is - no prefixing!
                    availableTools.put(toolName, tool);
                    
                    vertx.eventBus().publish("log", "[OracleSQLClient]   Discovered tool: " + toolName + ",3,OracleSQLClient,MCP,System");
                }
                
                // Publish discovered tools to event bus for HostManager
                publishToolsDiscovered(tools);
                
                // Start SSE stream for server-initiated messages (optional)
                oracleTransport.startSseStream(event -> {
                    handleSseEvent(event);
                });
                
                promise.complete();
            })
            .onFailure(err -> {
                vertx.eventBus().publish("log", "[OracleSQLClient] Failed to connect to Oracle server: " + err.getMessage() + ",0,OracleSQLClient,MCP,System");
                vertx.eventBus().publish("log", "[OracleSQLClient] Is Oracle server running on port " + ORACLE_PORT + "?" + ",0,OracleSQLClient,MCP,System");
                
                // Retry connection after delay
                vertx.setTimer(5000, id -> {
                    vertx.eventBus().publish("log", "[OracleSQLClient] Retrying connection..." + ",1,OracleSQLClient,MCP,System");
                    connectToOracleServer();
                });
                
                promise.fail(err);
            });
            
        return promise.future();
    }
    
    /**
     * Handle tool call requests from host via event bus
     */
    private void handleToolCall(Message<JsonObject> msg) {
        JsonObject request = msg.body();
        String toolName = request.getString("tool");
        JsonObject arguments = request.getJsonObject("arguments", new JsonObject());
        String streamId = request.getString("streamId"); // Extract streamId if present
        
        vertx.eventBus().publish("log", "[OracleSQLClient] Received tool call request: " + toolName + ",2,OracleSQLClient,MCP,System");
        
        // No more prefix manipulation - send tool name as-is!
        String serverToolName = toolName;
        
        // Add streamId to arguments if present
        if (streamId != null) {
            arguments.put("_streamId", streamId);
        }
        
        // Call tool via HTTP transport using MCP protocol
        oracleTransport.callTool(serverToolName, arguments)
            .onSuccess(result -> {
                // Check if result contains an error
                if (result.getBoolean("isError", false)) {
                    String errorMessage = result.getString("error", "Unknown error");
                    vertx.eventBus().publish("log", "[OracleSQLClient] Tool returned error: " + errorMessage + ",0,OracleSQLClient,MCP,System");
                    
                    // Log error
                    vertx.eventBus().publish("log",
                        "Tool " + toolName + " returned error: " + errorMessage + ",0,OracleSQLClient,Error,Tool");
                    
                    // Add metadata about which client handled it
                    result.put("_client", CLIENT_ID);
                    result.put("_transport", "HTTP");
                    
                    // Reply with error result (don't fail the message)
                    msg.reply(result);
                } else {
                    // Add metadata about which client handled it
                    result.put("_client", CLIENT_ID);
                    result.put("_transport", "HTTP");
                    
                    vertx.eventBus().publish("log", "[OracleSQLClient] Tool call succeeded: " + toolName + ",2,OracleSQLClient,MCP,System");
                    
                    // Log success
                    vertx.eventBus().publish("log",
                        "Tool " + toolName + " completed via HTTP,2,OracleSQLClient,Success,Tool");
                    
                    msg.reply(result);
                }
            })
            .onFailure(err -> {
                vertx.eventBus().publish("log", "[OracleSQLClient] Tool call failed: " + err.getMessage() + ",0,OracleSQLClient,MCP,System");
                
                // Log error
                vertx.eventBus().publish("log",
                    "Tool " + toolName + " failed: " + err.getMessage() + ",0,OracleSQLClient,Error,Tool");
                
                // Return error in MCP format
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
     * Handle status requests
     */
    private void handleStatusRequest(Message<JsonObject> msg) {
        JsonObject status = new JsonObject()
            .put("client", CLIENT_ID)
            .put("connected", oracleTransport != null)
            .put("server", "Oracle Server")
            .put("port", ORACLE_PORT)
            .put("transport", "HTTP")
            .put("tools", availableTools.size())
            .put("toolList", new JsonArray(availableTools.keySet().stream().toList()));
        
        msg.reply(status);
    }
    
    /**
     * Publish tool discovery to HostManager
     */
    private void publishToolsDiscovered(JsonArray tools) {
        // Send tools without prefix - HostManager will handle prefixing
        JsonObject discovery = new JsonObject()
            .put("client", CLIENT_ID)
            .put("server", "oracle")
            .put("tools", tools);
        
        vertx.eventBus().publish("mcp.tools.discovered", discovery);
        
        vertx.eventBus().publish("log", "[OracleSQLClient] Published tool discovery: " + tools.size() + " tools" + ",2,OracleSQLClient,MCP,System");
        
        vertx.eventBus().publish("log",
            "OracleSQLClient discovered " + tools.size() + " tools via HTTP,2,OracleSQLClient,Discovery,MCP");
    }
    
    /**
     * Publish client ready event
     */
    private void publishClientReady() {
        JsonObject registration = new JsonObject()
            .put("clientId", CLIENT_ID)
            .put("type", "http")
            .put("port", ORACLE_PORT)
            .put("toolCount", availableTools.size())
            .put("ready", true);
        
        vertx.eventBus().publish("mcp.client.ready", registration);
        
        vertx.eventBus().publish("log", "[OracleSQLClient] Published client ready event" + ",2,OracleSQLClient,MCP,System");
        
        vertx.eventBus().publish("log",
            "OracleSQLClient ready with HTTP transport,2,OracleSQLClient,Registration,MCP");
    }
    
    /**
     * Handle server-sent events (optional, for real-time updates)
     */
    private void handleSseEvent(JsonObject event) {
        String eventType = event.getString("type");
        
        if ("tool_update".equals(eventType)) {
            // Server notified us of tool changes, re-discover
            vertx.eventBus().publish("log", "[OracleSQLClient] Received tool update notification, refreshing tools..." + ",2,OracleSQLClient,MCP,System");
            oracleTransport.listTools()
                .onSuccess(tools -> {
                    availableTools.clear();
                    for (int i = 0; i < tools.size(); i++) {
                        JsonObject tool = tools.getJsonObject(i);
                        String toolName = tool.getString("name");
                        availableTools.put(toolName, tool);
                    }
                    publishToolsDiscovered(tools);
                });
        }
    }
    
    @Override
    public void stop() {
        if (oracleTransport != null) {
            // Clean shutdown of HTTP connection
            vertx.eventBus().publish("log", "[OracleSQLClient] Shutting down HTTP connection" + ",2,OracleSQLClient,MCP,System");
        }
        
        vertx.eventBus().publish("log", 
            "OracleSQLClient stopped,1,OracleSQLClient,Shutdown,MCP");
    }
}
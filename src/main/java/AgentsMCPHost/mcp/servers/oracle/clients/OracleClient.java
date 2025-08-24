package AgentsMCPHost.mcp.servers.oracle.clients;

import AgentsMCPHost.mcp.core.transport.VertxStreamableHttpTransport;
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
public class OracleClient extends AbstractVerticle {
    
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
                
                System.out.println("[OracleClient] Connected to Oracle server via HTTP on port " + ORACLE_PORT);
                System.out.println("[OracleClient] Discovered " + availableTools.size() + " tools");
                
                vertx.eventBus().publish("log", 
                    "OracleClient ready with " + availableTools.size() + " tools,1,OracleClient,StartUp,MCP");
                
                startPromise.complete();
            })
            .onFailure(err -> {
                System.err.println("[OracleClient] Failed to connect: " + err.getMessage());
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
                System.out.println("[OracleClient] Initialized MCP connection to Oracle server");
                // Protocol version might be a string or object
                Object protocolVersion = initResponse.getValue("protocolVersion");
                if (protocolVersion != null) {
                    System.out.println("[OracleClient] Protocol version: " + protocolVersion);
                }
                
                // Request tools list via MCP protocol
                return oracleTransport.listTools();
            })
            .onSuccess(tools -> {
                // Store discovered tools with oracle__ prefix for namespacing
                for (int i = 0; i < tools.size(); i++) {
                    JsonObject tool = tools.getJsonObject(i);
                    String originalName = tool.getString("name");
                    String prefixedName = "oracle__" + originalName;
                    
                    // Store with prefixed name for McpHostManager
                    JsonObject toolWithPrefix = tool.copy();
                    toolWithPrefix.put("name", prefixedName);
                    toolWithPrefix.put("originalName", originalName);
                    availableTools.put(prefixedName, toolWithPrefix);
                    
                    System.out.println("[OracleClient]   Discovered tool: " + prefixedName);
                }
                
                // Publish discovered tools to event bus for McpHostManager
                publishToolsDiscovered(tools);
                
                // Start SSE stream for server-initiated messages (optional)
                oracleTransport.startSseStream(event -> {
                    handleSseEvent(event);
                });
                
                promise.complete();
            })
            .onFailure(err -> {
                System.err.println("[OracleClient] Failed to connect to Oracle server: " + err.getMessage());
                System.err.println("[OracleClient] Is Oracle server running on port " + ORACLE_PORT + "?");
                
                // Retry connection after delay
                vertx.setTimer(5000, id -> {
                    System.out.println("[OracleClient] Retrying connection...");
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
        
        System.out.println("[OracleClient] Received tool call request: " + toolName);
        
        // Remove prefix for server call (server expects unprefixed names)
        String serverToolName = toolName.startsWith("oracle__") 
            ? toolName.substring("oracle__".length()) 
            : toolName;
        
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
                    System.err.println("[OracleClient] Tool returned error: " + errorMessage);
                    
                    // Log error
                    vertx.eventBus().publish("log",
                        "Tool " + toolName + " returned error: " + errorMessage + ",0,OracleClient,Error,Tool");
                    
                    // Add metadata about which client handled it
                    result.put("_client", CLIENT_ID);
                    result.put("_transport", "HTTP");
                    
                    // Reply with error result (don't fail the message)
                    msg.reply(result);
                } else {
                    // Add metadata about which client handled it
                    result.put("_client", CLIENT_ID);
                    result.put("_transport", "HTTP");
                    
                    System.out.println("[OracleClient] Tool call succeeded: " + toolName);
                    
                    // Log success
                    vertx.eventBus().publish("log",
                        "Tool " + toolName + " completed via HTTP,2,OracleClient,Success,Tool");
                    
                    msg.reply(result);
                }
            })
            .onFailure(err -> {
                System.err.println("[OracleClient] Tool call failed: " + err.getMessage());
                
                // Log error
                vertx.eventBus().publish("log",
                    "Tool " + toolName + " failed: " + err.getMessage() + ",0,OracleClient,Error,Tool");
                
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
     * Publish tool discovery to McpHostManager
     */
    private void publishToolsDiscovered(JsonArray tools) {
        // Send tools without prefix - McpHostManager will handle prefixing
        JsonObject discovery = new JsonObject()
            .put("client", CLIENT_ID)
            .put("server", "oracle")
            .put("tools", tools);
        
        vertx.eventBus().publish("mcp.tools.discovered", discovery);
        
        System.out.println("[OracleClient] Published tool discovery: " + tools.size() + " tools");
        
        vertx.eventBus().publish("log",
            "OracleClient discovered " + tools.size() + " tools via HTTP,2,OracleClient,Discovery,MCP");
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
        
        System.out.println("[OracleClient] Published client ready event");
        
        vertx.eventBus().publish("log",
            "OracleClient ready with HTTP transport,2,OracleClient,Registration,MCP");
    }
    
    /**
     * Handle server-sent events (optional, for real-time updates)
     */
    private void handleSseEvent(JsonObject event) {
        String eventType = event.getString("type");
        
        if ("tool_update".equals(eventType)) {
            // Server notified us of tool changes, re-discover
            System.out.println("[OracleClient] Received tool update notification, refreshing tools...");
            oracleTransport.listTools()
                .onSuccess(tools -> {
                    availableTools.clear();
                    for (int i = 0; i < tools.size(); i++) {
                        JsonObject tool = tools.getJsonObject(i);
                        String prefixedName = "oracle__" + tool.getString("name");
                        availableTools.put(prefixedName, tool);
                    }
                    publishToolsDiscovered(tools);
                });
        }
    }
    
    @Override
    public void stop() {
        if (oracleTransport != null) {
            // Clean shutdown of HTTP connection
            System.out.println("[OracleClient] Shutting down HTTP connection");
        }
        
        vertx.eventBus().publish("log", 
            "OracleClient stopped,1,OracleClient,Shutdown,MCP");
    }
}
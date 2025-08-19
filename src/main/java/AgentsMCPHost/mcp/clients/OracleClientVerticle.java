package AgentsMCPHost.mcp.clients;

import AgentsMCPHost.mcp.transport.VertxStreamableHttpTransport;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.core.eventbus.Message;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.UUID;

/**
 * MCP Client for Oracle Server.
 * Connects to Oracle server on port 8085.
 * Prefixes all tools with oracle__ for clear identification.
 */
public class OracleClientVerticle extends AbstractVerticle {
    
    private VertxStreamableHttpTransport oracleTransport;
    
    // Track available tools
    private final Map<String, JsonObject> availableTools = new ConcurrentHashMap<>();
    
    // Client ID for event bus addressing
    private static final String CLIENT_ID = "oracle";
    private static final int ORACLE_PORT = 8085;
    
    @Override
    public void start(Promise<Void> startPromise) {
        // Initialize transport
        oracleTransport = new VertxStreamableHttpTransport(vertx, "localhost", ORACLE_PORT);
        
        // Connect to Oracle server
        connectToOracleServer()
            .onSuccess(v -> {
                // Register event bus consumers for host requests
                vertx.eventBus().consumer("mcp.client." + CLIENT_ID + ".call", this::handleToolCall);
                vertx.eventBus().consumer("mcp.client." + CLIENT_ID + ".status", this::handleStatusRequest);
                
                // Notify host that client is ready
                publishClientReady();
                
                System.out.println("OracleClient connected to Oracle server");
                vertx.eventBus().publish("log", 
                    "OracleClient ready with " + availableTools.size() + " tools,1,OracleClient,StartUp,MCP");
                
                startPromise.complete();
            })
            .onFailure(err -> {
                System.err.println("Failed to connect OracleClient: " + err.getMessage());
                startPromise.fail(err);
            });
    }
    
    /**
     * Connect to Oracle server and discover its tools
     */
    private Future<Void> connectToOracleServer() {
        Promise<Void> promise = Promise.promise();
        
        // Initialize connection (protocol negotiation)
        oracleTransport.initialize()
            .compose(initResponse -> {
                System.out.println("Initialized connection to oracle server");
                
                // Request tools list
                return oracleTransport.listTools();
            })
            .onSuccess(tools -> {
                // Store tools with oracle__ prefix
                for (int i = 0; i < tools.size(); i++) {
                    JsonObject tool = tools.getJsonObject(i);
                    String originalName = tool.getString("name");
                    String prefixedName = "oracle__" + originalName;
                    
                    // Store with prefixed name
                    JsonObject toolWithPrefix = tool.copy();
                    toolWithPrefix.put("name", prefixedName);
                    toolWithPrefix.put("originalName", originalName);
                    availableTools.put(prefixedName, toolWithPrefix);
                    
                    System.out.println("  - " + prefixedName + ": " + tool.getString("description"));
                }
                
                // Publish discovered tools to event bus
                publishToolsDiscovered("oracle", tools);
                
                // Start SSE stream for server-initiated messages
                oracleTransport.startSseStream(event -> {
                    handleSseEvent("oracle", event);
                });
                
                promise.complete();
            })
            .onFailure(err -> {
                System.err.println("Failed to connect to Oracle server: " + err.getMessage());
                promise.fail(err);
            });
        
        return promise.future();
    }
    
    /**
     * Publish discovered tools to the host manager
     */
    private void publishToolsDiscovered(String serverName, JsonArray tools) {
        System.out.println("[DEBUG] OracleClient.publishToolsDiscovered - Client " + CLIENT_ID + 
                         " discovered " + tools.size() + " tools from " + serverName);
        
        // Log each tool being published
        for (int i = 0; i < tools.size(); i++) {
            JsonObject tool = tools.getJsonObject(i);
            System.out.println("[DEBUG]   Publishing tool: " + tool.getString("name") + 
                             " - " + tool.getString("description"));
        }
        
        JsonObject message = new JsonObject()
            .put("client", CLIENT_ID)
            .put("server", serverName)
            .put("tools", tools);
        
        System.out.println("[DEBUG] Publishing to event bus: mcp.tools.discovered");
        vertx.eventBus().publish("mcp.tools.discovered", message);
        
        // Also try sending with request/reply to verify receipt
        vertx.setTimer(100, id -> {
            System.out.println("[DEBUG] Verifying tools were registered...");
            vertx.eventBus().request("mcp.host.tools", new JsonObject(), ar -> {
                if (ar.succeeded()) {
                    JsonObject result = (JsonObject) ar.result().body();
                    System.out.println("[DEBUG] Host reports " + result.getInteger("count", 0) + " total tools");
                } else {
                    System.out.println("[DEBUG] Failed to verify tool registration");
                }
            });
        });
    }
    
    /**
     * Publish client ready event
     */
    private void publishClientReady() {
        vertx.eventBus().publish("mcp.client.ready", new JsonObject()
            .put("clientId", CLIENT_ID)
            .put("toolCount", availableTools.size()));
        
        System.out.println("Client ready: " + CLIENT_ID + " with " + availableTools.size() + " tools");
    }
    
    /**
     * Handle SSE events from server
     */
    private void handleSseEvent(String serverName, JsonObject event) {
        // Forward SSE events to host via event bus
        vertx.eventBus().publish("mcp.sse." + CLIENT_ID, event
            .put("_server", serverName)
            .put("_client", CLIENT_ID));
    }
    
    /**
     * Handle tool call requests from the host
     */
    private void handleToolCall(Message<JsonObject> msg) {
        JsonObject request = msg.body();
        String toolName = request.getString("tool");
        JsonObject arguments = request.getJsonObject("arguments", new JsonObject());
        
        // Remove oracle__ prefix to get original tool name
        String originalToolName = toolName;
        if (toolName.startsWith("oracle__")) {
            originalToolName = toolName.substring(8);
        }
        
        // Find the tool (look for prefixed name)
        JsonObject tool = availableTools.get(toolName);
        if (tool == null && !toolName.startsWith("oracle__")) {
            // Try with prefix
            tool = availableTools.get("oracle__" + toolName);
        }
        
        if (tool == null) {
            msg.reply(new JsonObject()
                .put("success", false)
                .put("error", "Tool not found: " + toolName));
            return;
        }
        
        // Call the tool using original name
        String finalToolName = tool.getString("originalName", originalToolName);
        oracleTransport.callTool(finalToolName, arguments)
            .onSuccess(result -> {
                msg.reply(new JsonObject()
                    .put("success", true)
                    .put("result", result));
            })
            .onFailure(err -> {
                msg.reply(new JsonObject()
                    .put("success", false)
                    .put("error", err.getMessage()));
            });
    }
    
    /**
     * Handle status requests
     */
    private void handleStatusRequest(Message<JsonObject> msg) {
        JsonArray toolNames = new JsonArray();
        availableTools.forEach((name, tool) -> toolNames.add(name));
        
        msg.reply(new JsonObject()
            .put("client", CLIENT_ID)
            .put("status", "ready")
            .put("connectedServer", "oracle")
            .put("toolCount", availableTools.size())
            .put("tools", toolNames));
    }
    
    @Override
    public void stop(Promise<Void> stopPromise) {
        // Clean up transport
        if (oracleTransport != null) {
            // Transport cleanup if needed
        }
        stopPromise.complete();
    }
}
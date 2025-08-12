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
 * MCP Client that connects to two servers: Calculator and Weather.
 * Runs as a standard verticle on the event loop.
 * Uses VertxStreamableHttpTransport for async HTTP communication.
 */
public class DualServerClientVerticle extends AbstractVerticle {
    
    private VertxStreamableHttpTransport calculatorTransport;
    private VertxStreamableHttpTransport weatherTransport;
    
    // Track available tools from each server
    private final Map<String, JsonArray> serverTools = new ConcurrentHashMap<>();
    private final Map<String, String> toolToServer = new ConcurrentHashMap<>();
    
    // Client ID for event bus addressing
    private static final String CLIENT_ID = "dual";
    
    @Override
    public void start(Promise<Void> startPromise) {
        // Initialize transports
        calculatorTransport = new VertxStreamableHttpTransport(vertx, "localhost", 8081);
        weatherTransport = new VertxStreamableHttpTransport(vertx, "localhost", 8082);
        
        // Connect to both servers asynchronously
        Future<Void> calcConnect = connectToServer(calculatorTransport, "calculator", 8081);
        Future<Void> weatherConnect = connectToServer(weatherTransport, "weather", 8082);
        
        // Wait for both connections to complete
        Future.all(calcConnect, weatherConnect)
            .onSuccess(v -> {
                // Register event bus consumers for host requests
                vertx.eventBus().consumer("mcp.client." + CLIENT_ID + ".call", this::handleToolCall);
                vertx.eventBus().consumer("mcp.client." + CLIENT_ID + ".status", this::handleStatusRequest);
                
                // Notify host that client is ready
                publishClientReady();
                
                System.out.println("DualServerClient connected to Calculator and Weather servers");
                vertx.eventBus().publish("log", 
                    "DualServerClient ready with " + toolToServer.size() + " tools,1,DualServerClient,StartUp,MCP");
                
                startPromise.complete();
            })
            .onFailure(err -> {
                System.err.println("Failed to connect DualServerClient: " + err.getMessage());
                startPromise.fail(err);
            });
    }
    
    /**
     * Connect to a server and discover its tools
     */
    private Future<Void> connectToServer(VertxStreamableHttpTransport transport, String serverName, int port) {
        Promise<Void> promise = Promise.promise();
        
        // Initialize connection (protocol negotiation)
        transport.initialize()
            .compose(initResponse -> {
                System.out.println("Initialized connection to " + serverName + " server");
                
                // Request tools list
                return transport.listTools();
            })
            .onSuccess(tools -> {
                // Store tools and map them to server
                serverTools.put(serverName, tools);
                
                for (int i = 0; i < tools.size(); i++) {
                    JsonObject tool = tools.getJsonObject(i);
                    String toolName = tool.getString("name");
                    toolToServer.put(toolName, serverName);
                }
                
                // Publish discovered tools to event bus
                publishToolsDiscovered(serverName, tools);
                
                // Start SSE stream for server-initiated messages
                transport.startSseStream(event -> {
                    handleSseEvent(serverName, event);
                });
                
                promise.complete();
            })
            .onFailure(err -> {
                System.err.println("Failed to connect to " + serverName + ": " + err.getMessage());
                // Try to reconnect after delay
                vertx.setTimer(5000, id -> {
                    connectToServer(transport, serverName, port);
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
        
        // Determine which server has this tool
        String serverName = toolToServer.get(toolName);
        
        if (serverName == null) {
            msg.fail(404, "Tool not found: " + toolName);
            return;
        }
        
        // Get appropriate transport
        VertxStreamableHttpTransport transport = "calculator".equals(serverName) 
            ? calculatorTransport 
            : weatherTransport;
        
        // Call tool via transport
        transport.callTool(toolName, arguments)
            .onSuccess(result -> {
                // Add metadata about which server handled it
                result.put("_server", serverName);
                result.put("_client", CLIENT_ID);
                msg.reply(result);
            })
            .onFailure(err -> {
                msg.fail(500, "Tool call failed: " + err.getMessage());
            });
    }
    
    /**
     * Handle status requests
     */
    private void handleStatusRequest(Message<JsonObject> msg) {
        JsonObject status = new JsonObject()
            .put("clientId", CLIENT_ID)
            .put("connected", true)
            .put("servers", new JsonArray()
                .add(new JsonObject()
                    .put("name", "calculator")
                    .put("port", 8081)
                    .put("tools", serverTools.getOrDefault("calculator", new JsonArray()).size()))
                .add(new JsonObject()
                    .put("name", "weather")
                    .put("port", 8082)
                    .put("tools", serverTools.getOrDefault("weather", new JsonArray()).size())))
            .put("totalTools", toolToServer.size());
        
        msg.reply(status);
    }
    
    /**
     * Handle SSE events from servers
     */
    private void handleSseEvent(String serverName, JsonObject event) {
        // Forward SSE events to event bus for host processing
        vertx.eventBus().publish("mcp.sse." + CLIENT_ID, new JsonObject()
            .put("server", serverName)
            .put("client", CLIENT_ID)
            .put("event", event));
    }
    
    /**
     * Publish tools discovered from a server
     */
    private void publishToolsDiscovered(String serverName, JsonArray tools) {
        vertx.eventBus().publish("mcp.tools.discovered", new JsonObject()
            .put("client", CLIENT_ID)
            .put("server", serverName)
            .put("tools", tools)
            .put("timestamp", System.currentTimeMillis()));
    }
    
    /**
     * Publish client ready status
     */
    private void publishClientReady() {
        // Aggregate all tools
        JsonArray allTools = new JsonArray();
        serverTools.values().forEach(tools -> {
            for (int i = 0; i < tools.size(); i++) {
                allTools.add(tools.getJsonObject(i));
            }
        });
        
        vertx.eventBus().publish("mcp.client.ready", new JsonObject()
            .put("clientId", CLIENT_ID)
            .put("servers", new JsonArray(serverTools.keySet().stream().toList()))
            .put("tools", allTools)
            .put("toolCount", toolToServer.size()));
    }
    
    @Override
    public void stop(Promise<Void> stopPromise) {
        // Close transports
        if (calculatorTransport != null) {
            calculatorTransport.close();
        }
        if (weatherTransport != null) {
            weatherTransport.close();
        }
        
        stopPromise.complete();
    }
}
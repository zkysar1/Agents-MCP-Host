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
 * MCP Client that connects to a single server: Database.
 * Runs as a standard verticle on the event loop.
 * Uses VertxStreamableHttpTransport for async HTTP communication.
 * Demonstrates single-server client configuration pattern.
 */
public class SingleServerClientVerticle extends AbstractVerticle {
    
    private VertxStreamableHttpTransport databaseTransport;
    
    // Track available tools from the server
    private final Map<String, JsonObject> availableTools = new ConcurrentHashMap<>();
    
    // Client ID for event bus addressing
    private static final String CLIENT_ID = "single-db";
    private static final String SERVER_NAME = "database";
    private static final int SERVER_PORT = 8083;
    
    @Override
    public void start(Promise<Void> startPromise) {
        // Initialize transport
        databaseTransport = new VertxStreamableHttpTransport(vertx, "localhost", SERVER_PORT);
        
        // Connect to database server
        connectToServer()
            .onSuccess(v -> {
                // Register event bus consumers for host requests
                vertx.eventBus().consumer("mcp.client." + CLIENT_ID + ".call", this::handleToolCall);
                vertx.eventBus().consumer("mcp.client." + CLIENT_ID + ".status", this::handleStatusRequest);
                vertx.eventBus().consumer("mcp.client." + CLIENT_ID + ".sampling", this::handleSamplingRequest);
                
                // Notify host that client is ready
                publishClientReady();
                
                System.out.println("SingleServerClient connected to Database server");
                vertx.eventBus().publish("log", 
                    "SingleServerClient ready with " + availableTools.size() + " tools,1,SingleServerClient,StartUp,MCP");
                
                startPromise.complete();
            })
            .onFailure(err -> {
                System.err.println("Failed to connect SingleServerClient: " + err.getMessage());
                startPromise.fail(err);
            });
    }
    
    /**
     * Connect to the database server and discover its tools
     */
    private Future<Void> connectToServer() {
        Promise<Void> promise = Promise.promise();
        
        // Initialize connection (protocol negotiation)
        databaseTransport.initialize()
            .compose(initResponse -> {
                System.out.println("Initialized connection to " + SERVER_NAME + " server");
                
                // Request tools list
                return databaseTransport.listTools();
            })
            .onSuccess(tools -> {
                // Store tools
                for (int i = 0; i < tools.size(); i++) {
                    JsonObject tool = tools.getJsonObject(i);
                    String toolName = tool.getString("name");
                    availableTools.put(toolName, tool);
                }
                
                System.out.println("Client " + CLIENT_ID + " discovered " + tools.size() + " tools from " + SERVER_NAME);
                
                // Publish discovered tools to event bus
                publishToolsDiscovered(tools);
                
                // Start SSE stream for server-initiated messages
                databaseTransport.startSseStream(event -> {
                    handleSseEvent(event);
                });
                
                promise.complete();
            })
            .onFailure(err -> {
                System.err.println("Failed to connect to " + SERVER_NAME + ": " + err.getMessage());
                // Try to reconnect after delay
                vertx.setTimer(5000, id -> {
                    connectToServer();
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
        
        // Verify tool exists
        if (!availableTools.containsKey(toolName)) {
            msg.fail(404, "Tool not found: " + toolName);
            return;
        }
        
        // Call tool via transport
        databaseTransport.callTool(toolName, arguments)
            .onSuccess(result -> {
                // Check if result indicates sampling workflow needed
                if (result.containsKey("requiresSampling") && result.getBoolean("requiresSampling")) {
                    // Trigger sampling workflow
                    vertx.eventBus().publish("mcp.sampling.required", new JsonObject()
                        .put("client", CLIENT_ID)
                        .put("tool", toolName)
                        .put("result", result));
                }
                
                // Add metadata about which server handled it
                result.put("_server", SERVER_NAME);
                result.put("_client", CLIENT_ID);
                msg.reply(result);
            })
            .onFailure(err -> {
                msg.fail(500, "Tool call failed: " + err.getMessage());
            });
    }
    
    /**
     * Handle sampling requests for large result sets
     */
    private void handleSamplingRequest(Message<JsonObject> msg) {
        JsonObject request = msg.body();
        
        // Forward sampling request to database server
        vertx.eventBus().request("mcp.database.sampling", request)
            .onSuccess(reply -> {
                msg.reply(reply.body());
            })
            .onFailure(err -> {
                msg.fail(500, "Sampling request failed: " + err.getMessage());
            });
    }
    
    /**
     * Handle status requests
     */
    private void handleStatusRequest(Message<JsonObject> msg) {
        JsonObject status = new JsonObject()
            .put("clientId", CLIENT_ID)
            .put("connected", true)
            .put("server", new JsonObject()
                .put("name", SERVER_NAME)
                .put("port", SERVER_PORT)
                .put("tools", availableTools.size()))
            .put("totalTools", availableTools.size())
            .put("capabilities", new JsonArray()
                .add("tool-invocation")
                .add("sampling"));
        
        msg.reply(status);
    }
    
    /**
     * Handle SSE events from server
     */
    private void handleSseEvent(JsonObject event) {
        // Forward SSE events to event bus for host processing
        vertx.eventBus().publish("mcp.sse." + CLIENT_ID, new JsonObject()
            .put("server", SERVER_NAME)
            .put("client", CLIENT_ID)
            .put("event", event));
        
        // Handle specific event types
        String eventType = event.getString("type");
        if ("sampling/create_message".equals(eventType)) {
            // Handle sampling request from server
            handleServerSamplingRequest(event);
        }
    }
    
    /**
     * Handle sampling request initiated by server
     */
    private void handleServerSamplingRequest(JsonObject event) {
        // In a real implementation, would forward to LLM for summarization
        System.out.println("Server requested sampling: " + event.encode());
        
        // For now, acknowledge the request
        vertx.eventBus().publish("mcp.sampling.acknowledged", new JsonObject()
            .put("client", CLIENT_ID)
            .put("server", SERVER_NAME)
            .put("event", event));
    }
    
    /**
     * Publish tools discovered from server
     */
    private void publishToolsDiscovered(JsonArray tools) {
        vertx.eventBus().publish("mcp.tools.discovered", new JsonObject()
            .put("client", CLIENT_ID)
            .put("server", SERVER_NAME)
            .put("tools", tools)
            .put("timestamp", System.currentTimeMillis()));
    }
    
    /**
     * Publish client ready status
     */
    private void publishClientReady() {
        // Convert map values to JsonArray
        JsonArray toolsArray = new JsonArray();
        availableTools.values().forEach(toolsArray::add);
        
        vertx.eventBus().publish("mcp.client.ready", new JsonObject()
            .put("clientId", CLIENT_ID)
            .put("servers", new JsonArray().add(SERVER_NAME))
            .put("tools", toolsArray)
            .put("toolCount", availableTools.size()));
    }
    
    @Override
    public void stop(Promise<Void> stopPromise) {
        // Close transport
        if (databaseTransport != null) {
            databaseTransport.close();
        }
        
        stopPromise.complete();
    }
}
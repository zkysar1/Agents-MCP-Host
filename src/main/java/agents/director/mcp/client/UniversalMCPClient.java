package agents.director.mcp.client;

import agents.director.mcp.base.MCPRequest;
import agents.director.mcp.base.MCPResponse;
import agents.director.mcp.base.MCPTool;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.core.eventbus.Message;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.core.buffer.Buffer;


import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import static agents.director.Driver.logLevel;

/**
 * Universal MCP client that uses VertxStreamableHttpTransport for communication.
 * Dynamically discovers tools via tools/list and provides async tool calling.
 * Supports streaming responses via Server-Sent Events (SSE).
 */
public class UniversalMCPClient extends AbstractVerticle {
    
    
    
    // Configuration
    private final String serverUrl;
    private final String serverName;
    private final int port;
    private final String basePath;
    
    // HTTP client
    private WebClient webClient;
    
    // Tool registry
    private final Map<String, MCPTool> tools = new ConcurrentHashMap<>();
    private final AtomicBoolean toolsLoaded = new AtomicBoolean(false);
    
    // Request tracking
    private final AtomicInteger requestCounter = new AtomicInteger(0);
    
    // Retry configuration
    private static final int MAX_RETRIES = 3;
    private static final long INITIAL_RETRY_DELAY = 1000; // 1 second
    
    // Event bus address for this client
    private String eventBusAddress;
    
    // Registry integration
    private final String clientId = UUID.randomUUID().toString();
    private Long heartbeatTimerId;
    
    /**
     * Create a new MCP client for a server
     * @param serverName Friendly name for the server
     * @param serverUrl Full URL including port and path (e.g., http://localhost:8080/mcp/servers/oracle-db)
     */
    public UniversalMCPClient(String serverName, String serverUrl) {
        this.serverName = serverName;
        this.serverUrl = serverUrl;
        
        // Parse URL more robustly
        if (serverUrl.startsWith("http://") || serverUrl.startsWith("https://")) {
            try {
                // Remove protocol
                String afterProtocol = serverUrl.substring(serverUrl.indexOf("://") + 3);
                
                // Find the path start
                int pathStart = afterProtocol.indexOf('/');
                String hostAndPort = pathStart > 0 ? afterProtocol.substring(0, pathStart) : afterProtocol;
                this.basePath = pathStart > 0 ? afterProtocol.substring(pathStart) : "/";
                
                // Parse host and port
                if (hostAndPort.contains(":")) {
                    String[] hostPortParts = hostAndPort.split(":");
                    // host is hostPortParts[0], which we don't need
                    this.port = Integer.parseInt(hostPortParts[1]);
                } else {
                    // Default ports
                    this.port = serverUrl.startsWith("https://") ? 443 : 80;
                }
            } catch (Exception e) {
                throw new IllegalArgumentException("Invalid server URL format: " + serverUrl, e);
            }
        } else {
            throw new IllegalArgumentException("Invalid server URL: " + serverUrl);
        }
        
        this.eventBusAddress = "mcp.client." + serverName.toLowerCase().replace(" ", "_");
    }
    
    @Override
    public void start(Promise<Void> startPromise) {
        // Create web client with appropriate options
        WebClientOptions options = new WebClientOptions()
            .setDefaultHost("localhost")
            .setDefaultPort(port)
            .setConnectTimeout(5000)
            .setIdleTimeout(30)
            .setMaxPoolSize(10)
            .setKeepAlive(true);
        
        webClient = WebClient.create(vertx, options);
        
        // Register event bus consumer
        vertx.eventBus().<JsonObject>consumer(eventBusAddress, this::handleEventBusMessage);
        
        // Register with MCPRegistryService
        JsonObject registration = new JsonObject()
            .put("clientId", clientId)
            .put("serverName", serverName)
            .put("serverUrl", serverUrl)
            .put("eventBusAddress", eventBusAddress)
            .put("metadata", new JsonObject()
                .put("basePath", basePath)
                .put("port", port));
        
        vertx.eventBus().<JsonObject>request("mcp.registry.register", registration, regReply -> {
            if (regReply.succeeded()) {
                vertx.eventBus().publish("log", "Registered " + serverName + " with MCPRegistry (ID: " + clientId + ")" + ",3,UniversalMCPClient,MCP,System");
            } else {
                vertx.eventBus().publish("log", "Failed to register " + serverName + " with MCPRegistry: " + regReply.cause() + ",1,UniversalMCPClient,MCP,System");
            }
        });
        
        // Start periodic heartbeat
        heartbeatTimerId = vertx.setPeriodic(30000, id -> {
            vertx.eventBus().send("mcp.registry.heartbeat", new JsonObject()
                .put("clientId", clientId));
        });
        
        // Discover tools on startup
        discoverTools().onComplete(ar -> {
            if (ar.succeeded()) {
                vertx.eventBus().publish("log", "" + serverName + " client started with " + tools.size() + " tools discovered" + ",2,UniversalMCPClient,MCP,System");
                startPromise.complete();
            } else {
                vertx.eventBus().publish("log", "" + serverName + " client failed to discover tools" + ",0,UniversalMCPClient,MCP,System");
                startPromise.fail(ar.cause());
            }
        });
    }
    
    /**
     * Discover available tools from the server
     */
    public Future<Void> discoverTools() {
        Promise<Void> promise = Promise.promise();
        
        MCPRequest request = new MCPRequest(
            generateRequestId(),
            "tools/list",
            new JsonObject()
        );
        
        vertx.eventBus().publish("log", "Discovering tools from " + serverName + " at " + basePath + "/tools/list" + "" + ",3,UniversalMCPClient,MCP,System");
        
        makeRequest("/tools/list", request.toJson())
            .onComplete(ar -> {
                if (ar.succeeded()) {
                    JsonObject response = ar.result();
                    MCPResponse mcpResponse = MCPResponse.fromJson(response);
                    
                    if (mcpResponse.isSuccess()) {
                        JsonObject result = mcpResponse.getResult();
                        JsonArray toolsArray = result.getJsonArray("tools", new JsonArray());
                        
                        tools.clear();
                        JsonArray discoveredTools = new JsonArray();
                        
                        for (int i = 0; i < toolsArray.size(); i++) {
                            JsonObject toolJson = toolsArray.getJsonObject(i);
                            MCPTool tool = MCPTool.fromJson(toolJson);
                            tools.put(tool.getName(), tool);
                            vertx.eventBus().publish("log", "Discovered tool: " + tool.getName() + " - {}" + ",3,UniversalMCPClient,MCP,System");
                            
                            // Build tool info for registry
                            discoveredTools.add(new JsonObject()
                                .put("name", tool.getName())
                                .put("description", tool.getDescription() != null ? tool.getDescription() : "")
                                .put("inputSchema", tool.getInputSchema() != null ? tool.getInputSchema() : new JsonObject()));
                        }
                        
                        // Report tools to registry
                        vertx.eventBus().send("mcp.registry.tools.discovered", new JsonObject()
                            .put("clientId", clientId)
                            .put("tools", discoveredTools));
                        
                        toolsLoaded.set(true);
                        promise.complete();
                    } else {
                        promise.fail("Failed to list tools: " + mcpResponse.getError());
                    }
                } else {
                    promise.fail(ar.cause());
                }
            });
        
        return promise.future();
    }
    
    /**
     * Call a tool with given arguments
     */
    public Future<JsonObject> callTool(String toolName, JsonObject arguments) {
        if (!toolsLoaded.get()) {
            return Future.failedFuture("Tools not loaded yet");
        }
        
        if (!tools.containsKey(toolName)) {
            return Future.failedFuture("Unknown tool: " + toolName);
        }
        
        MCPRequest request = new MCPRequest(
            generateRequestId(),
            "tools/call",
            new JsonObject()
                .put("name", toolName)
                .put("arguments", arguments)
        );
        
        vertx.eventBus().publish("log", "Calling tool " + toolName + " on " + serverName + " with args: " + arguments.encode() + "" + ",3,UniversalMCPClient,MCP,System");
        
        long startTime = System.currentTimeMillis();
        
        return makeRequest("/tools/call", request.toJson())
            .compose(response -> {
                long duration = System.currentTimeMillis() - startTime;
                MCPResponse mcpResponse = MCPResponse.fromJson(response);
                
                // Report tool usage
                vertx.eventBus().send("mcp.registry.tool.usage", new JsonObject()
                    .put("clientId", clientId)
                    .put("toolName", toolName)
                    .put("success", mcpResponse.isSuccess())
                    .put("duration", duration));
                
                if (mcpResponse.isSuccess()) {
                    return Future.succeededFuture(mcpResponse.getResult());
                } else {
                    JsonObject error = mcpResponse.getError();
                    String errorMsg = String.format("Tool call failed: %s - %s", 
                        error.getInteger("code", -1),
                        error.getString("message", "Unknown error"));
                    return Future.failedFuture(errorMsg);
                }
            });
    }
    
    /**
     * Call a tool with streaming response support
     */
    public Future<Void> callToolStreaming(String toolName, JsonObject arguments, 
                                         Handler<JsonObject> dataHandler,
                                         Handler<Void> endHandler) {
        if (!toolsLoaded.get()) {
            return Future.failedFuture("Tools not loaded yet");
        }
        
        if (!tools.containsKey(toolName)) {
            return Future.failedFuture("Unknown tool: " + toolName);
        }
        
        Promise<Void> promise = Promise.promise();
        
        MCPRequest request = new MCPRequest(
            generateRequestId(),
            "tools/call",
            new JsonObject()
                .put("name", toolName)
                .put("arguments", arguments)
                .put("stream", true) // Indicate we want streaming
        );
        
        vertx.eventBus().publish("log", "Calling tool " + toolName + " (streaming) on " + serverName + " with args: " + arguments.encode() + ",3,UniversalMCPClient,MCP,System");
        
        // Make SSE request
        makeStreamingRequest("/tools/call", request.toJson(), dataHandler, endHandler)
            .onComplete(promise);
        
        return promise.future();
    }
    
    /**
     * Get available tools
     */
    public List<MCPTool> getAvailableTools() {
        return new ArrayList<>(tools.values());
    }
    
    /**
     * Check if a tool is available
     */
    public boolean hasT 

(String toolName) {
        return tools.containsKey(toolName);
    }
    
    /**
     * Get tool definition
     */
    public MCPTool getTool(String toolName) {
        return tools.get(toolName);
    }
    
    // Private helper methods
    
    private Future<JsonObject> makeRequest(String path, JsonObject requestBody) {
        return makeRequestWithRetry(path, requestBody, 0);
    }
    
    private Future<JsonObject> makeRequestWithRetry(String path, JsonObject requestBody, int attempt) {
        Promise<JsonObject> promise = Promise.promise();
        
        HttpRequest<Buffer> request = webClient
            .post(basePath + path)
            .putHeader("Content-Type", "application/json")
            .putHeader("Accept", "application/json");
        
        request.sendJsonObject(requestBody, ar -> {
            if (ar.succeeded()) {
                try {
                    JsonObject response = ar.result().bodyAsJsonObject();
                    promise.complete(response);
                } catch (Exception e) {
                    promise.fail("Invalid JSON response: " + e.getMessage());
                }
            } else {
                if (attempt < MAX_RETRIES) {
                    long delay = INITIAL_RETRY_DELAY * (long) Math.pow(2, attempt);
                    vertx.eventBus().publish("log", serverName + " request failed (attempt " + (attempt + 1) + "), retrying in " + delay + "ms: " + ar.cause().getMessage() + ",1,UniversalMCPClient,MCP,System");
                    
                    vertx.setTimer(delay, id -> {
                        makeRequestWithRetry(path, requestBody, attempt + 1)
                            .onComplete(promise);
                    });
                } else {
                    vertx.eventBus().publish("log", "" + serverName + " request failed after " + MAX_RETRIES + " attempts" + ",0,UniversalMCPClient,MCP,System");
                    promise.fail(ar.cause());
                }
            }
        });
        
        return promise.future();
    }
    
    private Future<Void> makeStreamingRequest(String path, JsonObject requestBody,
                                            Handler<JsonObject> dataHandler,
                                            Handler<Void> endHandler) {
        Promise<Void> promise = Promise.promise();
        
        HttpRequest<Buffer> request = webClient
            .post(basePath + path)
            .putHeader("Content-Type", "application/json")
            .putHeader("Accept", "text/event-stream");
        
        request.as(BodyCodec.string()).sendJsonObject(requestBody, ar -> {
            if (ar.succeeded()) {
                // Parse Server-Sent Events
                String body = ar.result().body();
                parseSSEResponse(body, dataHandler);
                endHandler.handle(null);
                promise.complete();
            } else {
                promise.fail(ar.cause());
            }
        });
        
        return promise.future();
    }
    
    private void parseSSEResponse(String sseData, Handler<JsonObject> dataHandler) {
        // Parse Server-Sent Events format
        String[] lines = sseData.split("\n");
        StringBuilder eventData = new StringBuilder();
        
        for (String line : lines) {
            if (line.startsWith("data: ")) {
                eventData.append(line.substring(6));
            } else if (line.isEmpty() && eventData.length() > 0) {
                // End of event
                try {
                    JsonObject data = new JsonObject(eventData.toString());
                    dataHandler.handle(data);
                } catch (Exception e) {
                    vertx.eventBus().publish("log", "Failed to parse SSE data: " + eventData.toString() + "" + ",0,UniversalMCPClient,MCP,System");
                }
                eventData.setLength(0);
            }
        }
    }
    
    private void handleEventBusMessage(Message<JsonObject> message) {
        JsonObject request = message.body();
        String action = request.getString("action");
        
        switch (action) {
            case "call":
                String toolName = request.getString("tool");
                JsonObject arguments = request.getJsonObject("arguments", new JsonObject());
                
                callTool(toolName, arguments)
                    .onComplete(ar -> {
                        if (ar.succeeded()) {
                            message.reply(new JsonObject()
                                .put("status", "success")
                                .put("result", ar.result()));
                        } else {
                            message.fail(500, ar.cause().getMessage());
                        }
                    });
                break;
                
            case "list":
                message.reply(new JsonObject()
                    .put("status", "success")
                    .put("tools", new JsonArray(new ArrayList<>(tools.keySet()))));
                break;
                
            case "refresh":
                discoverTools().onComplete(ar -> {
                    if (ar.succeeded()) {
                        message.reply(new JsonObject()
                            .put("status", "success")
                            .put("toolCount", tools.size()));
                    } else {
                        message.fail(500, ar.cause().getMessage());
                    }
                });
                break;
                
            default:
                message.fail(400, "Unknown action: " + action);
        }
    }
    
    private String generateRequestId() {
        return serverName + "-" + System.currentTimeMillis() + "-" + requestCounter.incrementAndGet();
    }
    
    @Override
    public void stop(Promise<Void> stopPromise) {
        // Cancel heartbeat timer
        if (heartbeatTimerId != null) {
            vertx.cancelTimer(heartbeatTimerId);
        }
        
        // Deregister from MCPRegistryService
        vertx.eventBus().send("mcp.registry.deregister", new JsonObject()
            .put("clientId", clientId));
        
        if (webClient != null) {
            try {
                webClient.close();
                vertx.eventBus().publish("log", "" + serverName + " client stopped" + ",2,UniversalMCPClient,MCP,System");
                stopPromise.complete();
            } catch (Exception e) {
                stopPromise.fail(e);
            }
        } else {
            stopPromise.complete();
        }
    }
    
    /**
     * Get the event bus address for this client
     */
    public String getEventBusAddress() {
        return eventBusAddress;
    }
    
    /**
     * Get the server name
     */
    public String getServerName() {
        return serverName;
    }
    
    /**
     * Check if tools are loaded
     */
    public boolean isReady() {
        return toolsLoaded.get();
    }
}
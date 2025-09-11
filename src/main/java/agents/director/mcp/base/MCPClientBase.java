package agents.director.mcp.base;

import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.core.buffer.Buffer;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import static agents.director.Driver.logLevel;

/**
 * Base class for all MCP client implementations.
 * Provides core MCP protocol functionality including tool discovery and invocation.
 * Each client maintains a 1:1 relationship with a single MCP server.
 */
public abstract class MCPClientBase extends AbstractVerticle {
    
    // Configuration
    protected final String serverName;
    protected final String serverUrl;
    protected final int port;
    protected final String basePath;
    
    // HTTP client
    protected WebClient webClient;
    
    // Tool registry
    protected final Map<String, MCPTool> tools = new ConcurrentHashMap<>();
    protected final AtomicBoolean toolsLoaded = new AtomicBoolean(false);
    
    // Request tracking
    protected final AtomicInteger requestCounter = new AtomicInteger(0);
    
    // Retry configuration
    protected static final int MAX_RETRIES = 3;
    protected static final long INITIAL_RETRY_DELAY = 1000; // 1 second
    
    // Registry integration
    protected final String clientId = UUID.randomUUID().toString();
    protected Long heartbeatTimerId;
    
    /**
     * Constructor for MCP client base
     * @param serverName Friendly name for the server
     * @param serverUrl Full URL including port and path
     */
    protected MCPClientBase(String serverName, String serverUrl) {
        this.serverName = serverName;
        this.serverUrl = serverUrl;
        
        // Parse URL to extract components
        if (serverUrl.startsWith("http://") || serverUrl.startsWith("https://")) {
            try {
                String afterProtocol = serverUrl.substring(serverUrl.indexOf("://") + 3);
                
                int pathStart = afterProtocol.indexOf('/');
                String hostAndPort = pathStart > 0 ? afterProtocol.substring(0, pathStart) : afterProtocol;
                this.basePath = pathStart > 0 ? afterProtocol.substring(pathStart) : "/";
                
                if (hostAndPort.contains(":")) {
                    String[] hostPortParts = hostAndPort.split(":");
                    this.port = Integer.parseInt(hostPortParts[1]);
                } else {
                    this.port = serverUrl.startsWith("https://") ? 443 : 80;
                }
            } catch (Exception e) {
                throw new IllegalArgumentException("Invalid server URL format: " + serverUrl, e);
            }
        } else {
            throw new IllegalArgumentException("Invalid server URL: " + serverUrl);
        }
    }
    
    @Override
    public void start(Promise<Void> startPromise) {
        // Create web client
        WebClientOptions options = new WebClientOptions()
            .setDefaultHost("localhost")
            .setDefaultPort(port)
            .setConnectTimeout(5000)
            .setIdleTimeout(30)
            .setMaxPoolSize(10)
            .setKeepAlive(true);
        
        webClient = WebClient.create(vertx, options);
        
        // Register with MCPRegistryService
        registerWithRegistry();
        
        // Start heartbeat
        startHeartbeat();
        
        // Discover tools on startup
        discoverTools().onComplete(ar -> {
            if (ar.succeeded()) {
                vertx.eventBus().publish("log", serverName + " client started with " + tools.size() + " tools discovered,2," + getClass().getSimpleName() + ",MCP,System");
                setupEventBusConsumers();
                onClientReady();
                startPromise.complete();
            } else {
                vertx.eventBus().publish("log", serverName + " client failed to discover tools,0," + getClass().getSimpleName() + ",MCP,System");
                startPromise.fail(ar.cause());
            }
        });
    }
    
    /**
     * Called when client is ready with tools loaded.
     * Subclasses can override for additional initialization.
     */
    protected void onClientReady() {
        // Default: no additional action
    }
    
    /**
     * Set up event bus consumers for each discovered tool
     * This allows milestones to call tools via event bus
     */
    private void setupEventBusConsumers() {
        for (String toolName : tools.keySet()) {
            // Create event bus address for this tool
            // Format: mcp.client.<servername>.<toolname>
            String normalizedServerName = serverName.toLowerCase()
                .replace(" ", "")
                .replace("_", "");
            String address = "mcp.client." + normalizedServerName + "." + toolName;
            
            // Set up consumer that forwards to callTool
            vertx.eventBus().<JsonObject>consumer(address, message -> {
                JsonObject params = message.body();
                
                // Call the tool via HTTP using MCP protocol
                callTool(toolName, params)
                    .onSuccess(result -> message.reply(result))
                    .onFailure(err -> message.fail(500, err.getMessage()));
            });
            
            if (logLevel >= 3) {
                vertx.eventBus().publish("log", 
                    "Set up event bus consumer at: " + address + 
                    ",3," + getClass().getSimpleName() + ",MCP,System");
            }
        }
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
        
        if (logLevel >= 3) {
            vertx.eventBus().publish("log", "Discovering tools from " + serverName + " at " + basePath + "/tools/list,3," + getClass().getSimpleName() + ",MCP,System");
        }
        
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
                            
                            if (logLevel >= 3) {
                                vertx.eventBus().publish("log", "Discovered tool: " + tool.getName() + ",3," + getClass().getSimpleName() + ",MCP,System");
                            }
                            
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
        
        if (logLevel >= 3) {
            vertx.eventBus().publish("log", "Calling tool " + toolName + " on " + serverName + ",3," + getClass().getSimpleName() + ",MCP,System");
        }
        
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
     * Check if this client is ready to handle requests
     * @return true if the client is ready with loaded tools
     */
    public boolean isReady() {
        return toolsLoaded.get() && !tools.isEmpty();
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
                .put("stream", true)
        );
        
        if (logLevel >= 3) {
            vertx.eventBus().publish("log", "Calling tool " + toolName + " (streaming) on " + serverName + ",3," + getClass().getSimpleName() + ",MCP,System");
        }
        
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
    public boolean hasTool(String toolName) {
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
                    if (logLevel >= 1) {
                        vertx.eventBus().publish("log", serverName + " request failed (attempt " + (attempt + 1) + "), retrying in " + delay + "ms,1," + getClass().getSimpleName() + ",MCP,System");
                    }
                    
                    vertx.setTimer(delay, id -> {
                        makeRequestWithRetry(path, requestBody, attempt + 1)
                            .onComplete(promise);
                    });
                } else {
                    vertx.eventBus().publish("log", serverName + " request failed after " + MAX_RETRIES + " attempts,0," + getClass().getSimpleName() + ",MCP,System");
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
        
        request.sendJsonObject(requestBody, ar -> {
            if (ar.succeeded()) {
                // Parse SSE response
                String body = ar.result().bodyAsString();
                String[] events = body.split("\n\n");
                
                for (String event : events) {
                    if (event.startsWith("data: ")) {
                        String data = event.substring(6);
                        try {
                            JsonObject jsonData = new JsonObject(data);
                            dataHandler.handle(jsonData);
                        } catch (Exception e) {
                            // Skip malformed data
                        }
                    }
                }
                
                endHandler.handle(null);
                promise.complete();
            } else {
                promise.fail(ar.cause());
            }
        });
        
        return promise.future();
    }
    
    private String generateRequestId() {
        return serverName + "-" + System.currentTimeMillis() + "-" + requestCounter.incrementAndGet();
    }
    
    private void registerWithRegistry() {
        JsonObject registration = new JsonObject()
            .put("clientId", clientId)
            .put("serverName", serverName)
            .put("serverUrl", serverUrl)
            .put("clientType", getClass().getSimpleName())
            .put("metadata", new JsonObject()
                .put("basePath", basePath)
                .put("port", port));
        
        vertx.eventBus().<JsonObject>request("mcp.registry.register", registration, regReply -> {
            if (regReply.succeeded()) {
                if (logLevel >= 3) {
                    vertx.eventBus().publish("log", "Registered " + serverName + " with MCPRegistry,3," + getClass().getSimpleName() + ",MCP,System");
                }
            } else {
                if (logLevel >= 1) {
                    vertx.eventBus().publish("log", "Failed to register with MCPRegistry,1," + getClass().getSimpleName() + ",MCP,System");
                }
            }
        });
    }
    
    private void startHeartbeat() {
        heartbeatTimerId = vertx.setPeriodic(30000, id -> {
            vertx.eventBus().send("mcp.registry.heartbeat", new JsonObject()
                .put("clientId", clientId));
        });
    }
    
    @Override
    public void stop(Promise<Void> stopPromise) {
        if (heartbeatTimerId != null) {
            vertx.cancelTimer(heartbeatTimerId);
        }
        
        // Unregister from registry
        vertx.eventBus().send("mcp.registry.unregister", new JsonObject()
            .put("clientId", clientId));
        
        if (webClient != null) {
            webClient.close();
        }
        
        stopPromise.complete();
    }
}
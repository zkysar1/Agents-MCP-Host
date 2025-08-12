package AgentsMCPHost.mcp.host;

import AgentsMCPHost.mcp.servers.CalculatorServerVerticle;
import AgentsMCPHost.mcp.servers.WeatherServerVerticle;
import AgentsMCPHost.mcp.servers.DatabaseServerVerticle;
import AgentsMCPHost.mcp.servers.FileSystemServerVerticle;
import AgentsMCPHost.mcp.clients.DualServerClientVerticle;
import AgentsMCPHost.mcp.clients.SingleServerClientVerticle;
import AgentsMCPHost.mcp.clients.FileSystemClientVerticle;
import AgentsMCPHost.mcp.clients.LocalServerClientVerticle;
import AgentsMCPHost.mcp.config.McpConfigLoader;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Future;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.core.eventbus.Message;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * MCP Host Manager - Orchestrates all MCP components.
 * Deploys servers and clients, aggregates tools, routes tool calls.
 * Runs as a standard verticle.
 */
public class McpHostManagerVerticle extends AbstractVerticle {
    
    // Track all tools from all clients
    private final Map<String, JsonObject> allTools = new ConcurrentHashMap<>();
    private final Map<String, String> toolToClient = new ConcurrentHashMap<>();
    
    // Track client and server status
    private final Map<String, JsonObject> clientStatus = new ConcurrentHashMap<>();
    private final Map<String, JsonObject> serverStatus = new ConcurrentHashMap<>();
    
    // Deployment IDs for cleanup
    private final List<String> deploymentIds = new ArrayList<>();
    
    // System ready flag
    private boolean systemReady = false;
    
    // Configuration loader
    private McpConfigLoader configLoader;
    private JsonObject mcpConfig;
    
    @Override
    public void start(Promise<Void> startPromise) {
        // Initialize configuration loader
        configLoader = new McpConfigLoader(vertx);
        
        // Load configuration first
        configLoader.loadConfig()
            .compose(config -> {
                mcpConfig = config;
                System.out.println("Loaded MCP configuration");
                
                // Register event bus consumers
                registerEventBusConsumers();
                
                // Deploy all MCP infrastructure
                return deployMcpInfrastructure();
            })
            .onSuccess(v -> {
                systemReady = true;
                System.out.println("=== MCP Host Manager Ready ===");
                System.out.println("Servers: " + serverStatus.size());
                System.out.println("Clients: " + clientStatus.size());
                System.out.println("Total tools: " + allTools.size());
                
                vertx.eventBus().publish("log", 
                    "MCP infrastructure ready,1,McpHostManager,StartUp,MCP");
                
                // Notify system ready
                vertx.eventBus().publish("mcp.system.ready", new JsonObject()
                    .put("servers", serverStatus.size())
                    .put("clients", clientStatus.size())
                    .put("tools", allTools.size()));
                
                startPromise.complete();
            })
            .onFailure(err -> {
                System.err.println("Failed to deploy MCP infrastructure: " + err.getMessage());
                startPromise.fail(err);
            });
    }
    
    /**
     * Register all event bus consumers
     */
    private void registerEventBusConsumers() {
        // Listen for tool discoveries from clients
        vertx.eventBus().consumer("mcp.tools.discovered", this::handleToolsDiscovered);
        
        // Listen for client ready events
        vertx.eventBus().consumer("mcp.client.ready", this::handleClientReady);
        
        // Listen for server ready events
        vertx.eventBus().consumer("mcp.server.ready", this::handleServerReady);
        
        // Listen for tool call routing requests
        vertx.eventBus().consumer("mcp.host.route", this::routeToolCall);
        
        // Listen for status requests
        vertx.eventBus().consumer("mcp.host.status", this::handleStatusRequest);
        
        // Listen for tool list requests
        vertx.eventBus().consumer("mcp.host.tools", this::handleToolsListRequest);
        
        // Listen for SSE events from clients
        vertx.eventBus().consumer("mcp.sse.*", this::handleSseEvent);
    }
    
    /**
     * Deploy all MCP infrastructure components
     */
    private Future<Void> deployMcpInfrastructure() {
        // Deployment options for worker verticles (servers)
        DeploymentOptions workerOpts = new DeploymentOptions()
            .setWorker(true)
            .setWorkerPoolSize(2)
            .setWorkerPoolName("mcp-servers");
        
        // Get server configuration
        JsonObject httpServers = mcpConfig
            .getJsonObject("mcpServers", new JsonObject())
            .getJsonObject("httpServers", new JsonObject());
        
        // Deploy servers as workers (they have blocking tool execution)
        List<Future<String>> serverDeployments = new ArrayList<>();
        
        // Only deploy enabled servers
        if (httpServers.getJsonObject("calculator", new JsonObject()).getBoolean("enabled", true)) {
            serverDeployments.add(deployVerticle(new CalculatorServerVerticle(), workerOpts, "CalculatorServer"));
        }
        if (httpServers.getJsonObject("weather", new JsonObject()).getBoolean("enabled", true)) {
            serverDeployments.add(deployVerticle(new WeatherServerVerticle(), workerOpts, "WeatherServer"));
        }
        if (httpServers.getJsonObject("database", new JsonObject()).getBoolean("enabled", true)) {
            serverDeployments.add(deployVerticle(new DatabaseServerVerticle(), workerOpts, "DatabaseServer"));
        }
        if (httpServers.getJsonObject("filesystem", new JsonObject()).getBoolean("enabled", true)) {
            serverDeployments.add(deployVerticle(new FileSystemServerVerticle(), workerOpts, "FileSystemServer"));
        }
        
        // Wait for servers to be ready before deploying clients
        return Future.all(serverDeployments)
            .compose(v -> {
                // Give servers more time to start their HTTP endpoints
                Promise<Void> waitPromise = Promise.promise();
                vertx.setTimer(2000, id -> waitPromise.complete());
                return waitPromise.future();
            })
            .compose(v -> {
                // Get client configuration
                JsonObject clientConfigs = mcpConfig
                    .getJsonObject("clientConfigurations", new JsonObject());
                
                // Deploy clients as standard verticles (they use async HTTP)
                List<Future<String>> clientDeployments = new ArrayList<>();
                
                // Deploy enabled clients
                if (clientConfigs.getJsonObject("dual", new JsonObject()).getBoolean("enabled", true)) {
                    clientDeployments.add(deployVerticle(new DualServerClientVerticle(), null, "DualServerClient"));
                }
                if (clientConfigs.getJsonObject("single-db", new JsonObject()).getBoolean("enabled", true)) {
                    clientDeployments.add(deployVerticle(new SingleServerClientVerticle(), null, "SingleServerClient"));
                }
                if (clientConfigs.getJsonObject("filesystem", new JsonObject()).getBoolean("enabled", true)) {
                    clientDeployments.add(deployVerticle(new FileSystemClientVerticle(), null, "FileSystemClient"));
                }
                
                // Check if local servers are configured and enabled
                JsonObject localServersConfig = mcpConfig
                    .getJsonObject("mcpServers", new JsonObject())
                    .getJsonObject("localServers", new JsonObject());
                    
                boolean localClientEnabled = clientConfigs
                    .getJsonObject("local", new JsonObject())
                    .getBoolean("enabled", false);
                    
                if (localClientEnabled && !localServersConfig.isEmpty()) {
                    // Filter enabled local servers
                    JsonObject enabledLocalServers = new JsonObject();
                    localServersConfig.forEach(entry -> {
                        JsonObject serverConfig = (JsonObject) entry.getValue();
                        if (serverConfig.getBoolean("enabled", false)) {
                            enabledLocalServers.put(entry.getKey(), serverConfig);
                        }
                    });
                    
                    if (!enabledLocalServers.isEmpty()) {
                        // Deploy LocalServerClient with configuration
                        DeploymentOptions localClientOpts = new DeploymentOptions()
                            .setConfig(new JsonObject().put("servers", enabledLocalServers));
                        clientDeployments.add(
                            deployVerticle(new LocalServerClientVerticle(), localClientOpts, "LocalServerClient")
                        );
                    }
                }
                
                return Future.all(clientDeployments);
            })
            .mapEmpty();
    }
    
    /**
     * Deploy a verticle and track its deployment ID
     */
    private Future<String> deployVerticle(AbstractVerticle verticle, DeploymentOptions options, String name) {
        Promise<String> promise = Promise.promise();
        
        if (options == null) {
            options = new DeploymentOptions();
        }
        
        vertx.deployVerticle(verticle, options, ar -> {
            if (ar.succeeded()) {
                String deploymentId = ar.result();
                deploymentIds.add(deploymentId);
                System.out.println("Deployed " + name + " [" + deploymentId + "]");
                promise.complete(deploymentId);
            } else {
                System.err.println("Failed to deploy " + name + ": " + ar.cause().getMessage());
                promise.fail(ar.cause());
            }
        });
        
        return promise.future();
    }
    
    /**
     * Handle tools discovered by clients
     */
    private void handleToolsDiscovered(Message<JsonObject> msg) {
        JsonObject discovery = msg.body();
        String clientId = discovery.getString("client");
        String serverName = discovery.getString("server");
        JsonArray tools = discovery.getJsonArray("tools");
        
        System.out.println("Client " + clientId + " discovered " + tools.size() + 
                         " tools from " + serverName);
        
        // Aggregate tools with server prefixing (OpenCode pattern)
        for (int i = 0; i < tools.size(); i++) {
            JsonObject tool = tools.getJsonObject(i);
            String originalToolName = tool.getString("name");
            
            // Create prefixed tool name: serverName__toolName
            String prefixedToolName = serverName + "__" + originalToolName;
            
            // Store tool with metadata
            JsonObject toolWithMeta = tool.copy()
                .put("name", prefixedToolName)  // Use prefixed name
                .put("_originalName", originalToolName)  // Store original name
                .put("_client", clientId)
                .put("_server", serverName);
            
            allTools.put(prefixedToolName, toolWithMeta);
            toolToClient.put(prefixedToolName, clientId);
            
            // Also store by original name for backward compatibility
            toolToClient.put(originalToolName, clientId);
        }
        
        // Update aggregated view
        publishAggregatedTools();
    }
    
    /**
     * Handle client ready event
     */
    private void handleClientReady(Message<JsonObject> msg) {
        JsonObject status = msg.body();
        String clientId = status.getString("clientId");
        
        clientStatus.put(clientId, status);
        
        System.out.println("Client ready: " + clientId + 
                         " with " + status.getInteger("toolCount") + " tools");
    }
    
    /**
     * Handle server ready event
     */
    private void handleServerReady(Message<JsonObject> msg) {
        JsonObject status = msg.body();
        String serverName = status.getString("server");
        
        serverStatus.put(serverName, status);
        
        System.out.println("Server ready: " + serverName + 
                         " on port " + status.getInteger("port"));
    }
    
    /**
     * Route tool calls to appropriate client
     */
    private void routeToolCall(Message<JsonObject> msg) {
        JsonObject request = msg.body();
        String toolName = request.getString("tool");
        
        if (!systemReady) {
            msg.fail(503, "MCP system not ready");
            return;
        }
        
        // Try to find client by tool name (supports both prefixed and non-prefixed)
        String clientId = toolToClient.get(toolName);
        
        // If not found and doesn't contain __, try adding server prefixes
        if (clientId == null && !toolName.contains("__")) {
            // Try to find with any server prefix
            for (String key : allTools.keySet()) {
                if (key.endsWith("__" + toolName)) {
                    clientId = toolToClient.get(key);
                    toolName = key; // Use the prefixed name
                    break;
                }
            }
        }
        
        if (clientId == null) {
            msg.fail(404, "Tool not found: " + toolName);
            return;
        }
        
        // Extract original tool name if prefixed
        String originalToolName = toolName;
        if (toolName.contains("__")) {
            originalToolName = toolName.substring(toolName.indexOf("__") + 2);
        }
        
        // Make toolName final for lambda
        final String finalToolName = toolName;
        
        // Update request with original tool name for client
        JsonObject clientRequest = request.copy()
            .put("tool", originalToolName)  // Use original name for client
            .put("_prefixedName", finalToolName);  // Keep prefixed name for reference
        
        // Forward to appropriate client via event bus
        vertx.eventBus().request("mcp.client." + clientId + ".call", clientRequest)
            .onSuccess(reply -> {
                // Add routing metadata
                JsonObject result = (JsonObject) reply.body();
                result.put("_routedBy", "McpHostManager");
                result.put("_toolName", finalToolName);  // Include prefixed name in response
                msg.reply(result);
            })
            .onFailure(err -> {
                msg.fail(500, "Tool execution failed: " + err.getMessage());
            });
    }
    
    /**
     * Handle status request
     */
    private void handleStatusRequest(Message<JsonObject> msg) {
        JsonObject status = new JsonObject()
            .put("ready", systemReady)
            .put("servers", new JsonObject()
                .put("count", serverStatus.size())
                .put("list", new JsonArray(serverStatus.values().stream().toList())))
            .put("clients", new JsonObject()
                .put("count", clientStatus.size())
                .put("list", new JsonArray(clientStatus.values().stream().toList())))
            .put("tools", new JsonObject()
                .put("count", allTools.size())
                .put("names", new JsonArray(allTools.keySet().stream().sorted().toList())));
        
        msg.reply(status);
    }
    
    /**
     * Handle tools list request
     */
    private void handleToolsListRequest(Message<JsonObject> msg) {
        JsonArray toolsList = new JsonArray();
        
        allTools.forEach((name, tool) -> {
            toolsList.add(tool);
        });
        
        msg.reply(new JsonObject()
            .put("tools", toolsList)
            .put("count", toolsList.size()));
    }
    
    /**
     * Handle SSE events from clients
     */
    private void handleSseEvent(Message<JsonObject> msg) {
        JsonObject event = msg.body();
        
        // Log SSE events for monitoring
        System.out.println("SSE Event from " + event.getString("client") + 
                         "/" + event.getString("server") + ": " + 
                         event.getJsonObject("event").encode());
        
        // Forward to any interested parties
        vertx.eventBus().publish("mcp.sse.processed", event);
    }
    
    /**
     * Publish aggregated tools to event bus
     */
    private void publishAggregatedTools() {
        vertx.eventBus().publish("mcp.tools.aggregated", new JsonObject()
            .put("totalTools", allTools.size())
            .put("clients", clientStatus.size())
            .put("servers", serverStatus.size())
            .put("timestamp", System.currentTimeMillis()));
    }
    
    /**
     * Configure router endpoints for HTTP access (following repo pattern)
     */
    public static void setRouter(Router parentRouter) {
        // List all clients
        parentRouter.get("/host/v1/clients").handler(McpHostManagerVerticle::handleGetClients);
        
        // List all tools
        parentRouter.get("/host/v1/tools").handler(McpHostManagerVerticle::handleGetTools);
        
        // Get system status
        parentRouter.get("/host/v1/mcp/status").handler(McpHostManagerVerticle::handleGetStatus);
    }
    
    /**
     * HTTP endpoint: Get all clients
     */
    private static void handleGetClients(RoutingContext ctx) {
        ctx.vertx().eventBus().request("mcp.host.status", new JsonObject())
            .onSuccess(reply -> {
                JsonObject status = (JsonObject) reply.body();
                JsonObject clients = status.getJsonObject("clients");
                
                ctx.response()
                    .putHeader("Content-Type", "application/json")
                    .setStatusCode(200)
                    .end(clients.encode());
            })
            .onFailure(err -> {
                ctx.response()
                    .setStatusCode(500)
                    .end(new JsonObject()
                        .put("error", "Failed to get clients: " + err.getMessage())
                        .encode());
            });
    }
    
    /**
     * HTTP endpoint: Get all tools
     */
    private static void handleGetTools(RoutingContext ctx) {
        ctx.vertx().eventBus().request("mcp.host.tools", new JsonObject())
            .onSuccess(reply -> {
                JsonObject tools = (JsonObject) reply.body();
                
                ctx.response()
                    .putHeader("Content-Type", "application/json")
                    .setStatusCode(200)
                    .end(tools.encode());
            })
            .onFailure(err -> {
                ctx.response()
                    .setStatusCode(500)
                    .end(new JsonObject()
                        .put("error", "Failed to get tools: " + err.getMessage())
                        .encode());
            });
    }
    
    /**
     * HTTP endpoint: Get MCP system status
     */
    private static void handleGetStatus(RoutingContext ctx) {
        ctx.vertx().eventBus().request("mcp.host.status", new JsonObject())
            .onSuccess(reply -> {
                JsonObject status = (JsonObject) reply.body();
                
                ctx.response()
                    .putHeader("Content-Type", "application/json")
                    .setStatusCode(200)
                    .end(status.encode());
            })
            .onFailure(err -> {
                ctx.response()
                    .setStatusCode(500)
                    .end(new JsonObject()
                        .put("error", "Failed to get status: " + err.getMessage())
                        .encode());
            });
    }
    
    @Override
    public void stop(Promise<Void> stopPromise) {
        // Undeploy all managed verticles
        List<Future<Void>> undeploys = deploymentIds.stream()
            .map(id -> {
                Promise<Void> promise = Promise.promise();
                vertx.undeploy(id, promise);
                return promise.future();
            })
            .collect(Collectors.toList());
        
        Future.all(undeploys)
            .onComplete(ar -> {
                System.out.println("MCP Host Manager stopped");
                stopPromise.complete();
            });
    }
}
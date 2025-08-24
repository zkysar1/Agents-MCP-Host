package AgentsMCPHost.mcp.core;

import AgentsMCPHost.mcp.servers.examples.calculator.CalculatorServer;
import AgentsMCPHost.mcp.servers.examples.weather.WeatherServer;
import AgentsMCPHost.mcp.servers.examples.database.DatabaseServer;
import AgentsMCPHost.mcp.servers.examples.filesystem.FileSystemServer;
import AgentsMCPHost.mcp.servers.oracle.servers.OracleServer;
import AgentsMCPHost.mcp.servers.examples.DualServerClient;
import AgentsMCPHost.mcp.servers.examples.SingleServerClient;
import AgentsMCPHost.mcp.servers.oracle.clients.OracleClient;
import AgentsMCPHost.mcp.servers.examples.LocalServerClient;
import AgentsMCPHost.mcp.core.config.McpConfigLoader;
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
public class McpHostManager extends AbstractVerticle {
    
    // Track all tools from all clients
    private final Map<String, JsonObject> allTools = new ConcurrentHashMap<>();
    private final Map<String, String> toolToClient = new ConcurrentHashMap<>();
    
    // Track client and server status
    private final Map<String, JsonObject> clientStatus = new ConcurrentHashMap<>();
    private final Map<String, JsonObject> serverStatus = new ConcurrentHashMap<>();
    
    // Track which servers/clients are actually ready (not just deployed)
    private final Set<String> readyServers = new HashSet<>();
    private final Set<String> readyClients = new HashSet<>();
    private final Set<String> expectedServers = new HashSet<>();
    private final Set<String> expectedClients = new HashSet<>();
    
    // Deployment IDs for cleanup
    private final List<String> deploymentIds = new ArrayList<>();
    
    // System ready flag
    private boolean systemReady = false;
    private boolean infrastructureDeployed = false;
    
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
                infrastructureDeployed = true;
                System.out.println("[McpHostManager] Infrastructure deployed, waiting for components to be ready...");
                System.out.println("[McpHostManager] Expected servers: " + expectedServers);
                System.out.println("[McpHostManager] Expected clients: " + expectedClients);
                
                // Set up a startup timeout handler
                vertx.setTimer(30000, id -> {
                    if (!systemReady) {
                        System.err.println("[McpHostManager] WARNING: Startup timeout after 30 seconds!");
                        System.err.println("[McpHostManager] Missing components:");
                        
                        Set<String> missingServers = new HashSet<>(expectedServers);
                        missingServers.removeAll(readyServers);
                        if (!missingServers.isEmpty()) {
                            System.err.println("  Missing servers: " + missingServers);
                        }
                        
                        Set<String> missingClients = new HashSet<>(expectedClients);
                        missingClients.removeAll(readyClients);
                        if (!missingClients.isEmpty()) {
                            System.err.println("  Missing clients: " + missingClients);
                        }
                        
                        System.err.println("[McpHostManager] Ready servers: " + readyServers);
                        System.err.println("[McpHostManager] Ready clients: " + readyClients);
                        System.err.println("[McpHostManager] Total tools registered: " + allTools.size());
                        
                        // Log to CSV
                        vertx.eventBus().publish("log",
                            "Startup timeout - missing servers: " + missingServers + ", missing clients: " + missingClients + 
                            ",0,McpHostManager,Error,Startup");
                    }
                });
                
                // Check if everything is already ready (unlikely but possible)
                checkIfSystemReady();
                
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
        // Add logging to confirm consumers are registered
        System.out.println("[McpHostManager] Registering event bus consumers...");
        
        // Listen for tool discoveries from clients
        vertx.eventBus().consumer("mcp.tools.discovered", this::handleToolsDiscovered);
        System.out.println("[McpHostManager] Registered consumer for: mcp.tools.discovered");
        
        // Listen for client ready events
        vertx.eventBus().consumer("mcp.client.ready", this::handleClientReady);
        System.out.println("[McpHostManager] Registered consumer for: mcp.client.ready");
        
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
        // Deploy MCP servers as standard verticles (non-blocking HTTP)
        // These servers handle HTTP requests asynchronously on the event loop
        // Any blocking operations should use executeBlocking internally
        DeploymentOptions serverOpts = new DeploymentOptions()
            .setInstances(1);  // Single instance per server
        
        // Get server configuration
        JsonObject httpServers = mcpConfig
            .getJsonObject("mcpServers", new JsonObject())
            .getJsonObject("httpServers", new JsonObject());
        
        // Deploy servers as standard verticles (async HTTP handling)
        List<Future<String>> serverDeployments = new ArrayList<>();
        
        // Deploy standard MCP servers
        if (httpServers.getJsonObject("calculator", new JsonObject()).getBoolean("enabled", true)) {
            expectedServers.add("calculator");
            serverDeployments.add(deployVerticle(new CalculatorServer(), serverOpts, "CalculatorServer"));
        }
        if (httpServers.getJsonObject("weather", new JsonObject()).getBoolean("enabled", true)) {
            expectedServers.add("weather");
            serverDeployments.add(deployVerticle(new WeatherServer(), serverOpts, "WeatherServer"));
        }
        if (httpServers.getJsonObject("database", new JsonObject()).getBoolean("enabled", true)) {
            expectedServers.add("database");
            serverDeployments.add(deployVerticle(new DatabaseServer(), serverOpts, "DatabaseServer"));
        }
        if (httpServers.getJsonObject("filesystem", new JsonObject()).getBoolean("enabled", true)) {
            expectedServers.add("filesystem");
            serverDeployments.add(deployVerticle(new FileSystemServer(), serverOpts, "FileSystemServer"));
        }
        
        // Deploy Oracle servers if enabled
        if (httpServers.getJsonObject("oracle", new JsonObject()).getBoolean("enabled", false)) {
            expectedServers.add("oracle");
            serverDeployments.add(deployVerticle(new OracleServer(), serverOpts, "OracleServer"));
        }
        
        // Wait for servers to be ready before deploying clients
        return Future.all(serverDeployments)
            .compose(v -> {
                // Give servers more time to start their HTTP endpoints
                // Increased from 2000ms to 5000ms to ensure Oracle server is fully ready
                Promise<Void> waitPromise = Promise.promise();
                System.out.println("[McpHostManager] Waiting 5 seconds for servers to fully initialize HTTP endpoints...");
                vertx.setTimer(5000, id -> {
                    System.out.println("[McpHostManager] Server initialization wait complete, deploying clients...");
                    waitPromise.complete();
                });
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
                    expectedClients.add("dual");
                    clientDeployments.add(deployVerticle(new DualServerClient(), null, "DualServerClient"));
                }
                if (clientConfigs.getJsonObject("single-db", new JsonObject()).getBoolean("enabled", true)) {
                    expectedClients.add("single-db");
                    clientDeployments.add(deployVerticle(new SingleServerClient(), null, "SingleServerClient"));
                }
                
                // Deploy Oracle client if enabled
                JsonObject oracleConfig = clientConfigs.getJsonObject("oracle", new JsonObject());
                boolean oracleEnabled = oracleConfig.getBoolean("enabled", false);
                System.out.println("[DEBUG] Oracle client enabled: " + oracleEnabled);
                if (oracleEnabled) {
                    System.out.println("[DEBUG] Deploying OracleClient...");
                    expectedClients.add("oracle");
                    clientDeployments.add(deployVerticle(new OracleClient(), null, "OracleClient"));
                } else {
                    System.out.println("[DEBUG] Oracle client not enabled in config");
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
                            deployVerticle(new LocalServerClient(), localClientOpts, "LocalServerClient")
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
        
        System.out.println("[McpHostManager] Attempting to deploy: " + name);
        
        vertx.deployVerticle(verticle, options, ar -> {
            if (ar.succeeded()) {
                String deploymentId = ar.result();
                deploymentIds.add(deploymentId);
                System.out.println("[McpHostManager] Successfully deployed " + name + " [" + deploymentId + "]");
                
                // Log to CSV
                vertx.eventBus().publish("log",
                    "Deployed " + name + ",2,McpHostManager,Deployment,MCP");
                    
                promise.complete(deploymentId);
            } else {
                System.err.println("[McpHostManager] FAILED to deploy " + name + ": " + ar.cause().getMessage());
                ar.cause().printStackTrace();
                
                // Log failure to CSV
                vertx.eventBus().publish("log",
                    "Failed to deploy " + name + ": " + ar.cause().getMessage() + ",0,McpHostManager,Error,MCP");
                    
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
        
        // Add extensive logging
        System.out.println("[McpHostManager] RECEIVED tools.discovered event:");
        System.out.println("[McpHostManager]   client: " + clientId);
        System.out.println("[McpHostManager]   server: " + serverName);
        System.out.println("[McpHostManager]   tools count: " + (tools != null ? tools.size() : "null"));
        
        // Log to CSV
        vertx.eventBus().publish("log",
            "McpHostManager received tools.discovered from " + clientId + " with " + 
            (tools != null ? tools.size() : 0) + " tools,2,McpHostManager,Discovery,MCP");
        
        System.out.println("[DEBUG] McpHostManager.handleToolsDiscovered - Client " + clientId + 
                         " discovered " + tools.size() + " tools from " + serverName);
        
        // Aggregate tools with server prefixing (OpenCode pattern)
        int toolsAdded = 0;
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
            
            System.out.println("[DEBUG]   Added tool: " + prefixedToolName + 
                             " (original: " + originalToolName + ")");
            toolsAdded++;
        }
        
        System.out.println("[DEBUG] Total tools now registered: " + allTools.size());
        
        // Update aggregated view
        publishAggregatedTools();
        
        // Publish confirmation
        vertx.eventBus().publish("mcp.tools.registered", new JsonObject()
            .put("client", clientId)
            .put("server", serverName)
            .put("toolsAdded", toolsAdded)
            .put("totalTools", allTools.size()));
    }
    
    /**
     * Handle client ready event
     */
    private void handleClientReady(Message<JsonObject> msg) {
        JsonObject status = msg.body();
        String clientId = status.getString("clientId");
        Integer toolCount = status.getInteger("toolCount", 0);
        
        // Add extensive logging
        System.out.println("[McpHostManager] RECEIVED client.ready event:");
        System.out.println("[McpHostManager]   clientId: " + clientId);
        System.out.println("[McpHostManager]   toolCount: " + toolCount);
        System.out.println("[McpHostManager]   type: " + status.getString("type", "unknown"));
        
        // Log to CSV
        vertx.eventBus().publish("log",
            "McpHostManager received client.ready from " + clientId + " with " + 
            toolCount + " tools,2,McpHostManager,Registration,MCP");
        
        clientStatus.put(clientId, status);
        readyClients.add(clientId);
        
        System.out.println("Client ready: " + clientId + 
                         " with " + toolCount + " tools");
        System.out.println("[McpHostManager] Ready clients: " + readyClients.size() + "/" + expectedClients.size());
        
        // Check if all expected components are ready
        checkIfSystemReady();
    }
    
    /**
     * Handle server ready event
     */
    private void handleServerReady(Message<JsonObject> msg) {
        JsonObject status = msg.body();
        String serverName = status.getString("server");
        
        serverStatus.put(serverName, status);
        readyServers.add(serverName);
        
        System.out.println("[McpHostManager] Server ready: " + serverName + 
                         " on port " + status.getInteger("port"));
        System.out.println("[McpHostManager] Ready servers: " + readyServers.size() + "/" + expectedServers.size());
        
        // Check if all expected components are ready
        checkIfSystemReady();
    }
    
    /**
     * Route tool calls to appropriate client
     */
    private void routeToolCall(Message<JsonObject> msg) {
        JsonObject request = msg.body();
        String toolName = request.getString("tool");
        String streamId = request.getString("streamId"); // Optional stream ID for SSE
        
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
            // Publish error to stream if streaming
            if (streamId != null) {
                vertx.eventBus().publish("conversation." + streamId + ".error",
                    new JsonObject().put("error", "Tool not found: " + toolName));
            }
            return;
        }
        
        // Extract original tool name if prefixed
        String originalToolName = toolName;
        if (toolName.contains("__")) {
            originalToolName = toolName.substring(toolName.indexOf("__") + 2);
        }
        
        // Make toolName final for lambda
        final String finalToolName = toolName;
        
        // Publish tool routing event if streaming
        if (streamId != null) {
            vertx.eventBus().publish("mcp.tool.routing",
                new JsonObject()
                    .put("streamId", streamId)
                    .put("tool", finalToolName)
                    .put("client", clientId)
                    .put("status", "routing"));
        }
        
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
                
                // Publish tool completion event if streaming
                if (streamId != null) {
                    vertx.eventBus().publish("mcp.tool.completed",
                        new JsonObject()
                            .put("streamId", streamId)
                            .put("tool", finalToolName)
                            .put("result", result)
                            .put("status", "completed"));
                }
                
                msg.reply(result);
            })
            .onFailure(err -> {
                msg.fail(500, "Tool execution failed: " + err.getMessage());
                
                // Publish error to stream if streaming
                if (streamId != null) {
                    vertx.eventBus().publish("conversation." + streamId + ".error",
                        new JsonObject().put("error", "Tool execution failed: " + err.getMessage()));
                }
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
     * Check if all expected components are ready and publish system ready event
     */
    private void checkIfSystemReady() {
        // Only check if infrastructure has been deployed
        if (!infrastructureDeployed) {
            return;
        }
        
        // Check if we're already ready
        if (systemReady) {
            return;
        }
        
        // Check if all expected servers are ready
        boolean allServersReady = expectedServers.isEmpty() || 
            readyServers.containsAll(expectedServers);
        
        // Check if all expected clients are ready
        boolean allClientsReady = expectedClients.isEmpty() || 
            readyClients.containsAll(expectedClients);
        
        if (allServersReady && allClientsReady) {
            systemReady = true;
            
            System.out.println("=== MCP Host Manager Ready ===");
            System.out.println("Servers ready: " + readyServers.size() + "/" + expectedServers.size());
            System.out.println("Clients ready: " + readyClients.size() + "/" + expectedClients.size());
            System.out.println("Total tools: " + allTools.size());
            
            vertx.eventBus().publish("log", 
                "MCP infrastructure ready,1,McpHostManager,StartUp,MCP");
            
            // Notify that MCP system is ready
            vertx.eventBus().publish("mcp.system.ready", new JsonObject()
                .put("servers", readyServers.size())
                .put("clients", readyClients.size())
                .put("tools", allTools.size())
                .put("timestamp", System.currentTimeMillis()));
            
            System.out.println("MCP System Ready - Servers: " + readyServers.size() + 
                             ", Clients: " + readyClients.size() + 
                             ", Tools: " + allTools.size());
        } else {
            System.out.println("[McpHostManager] Waiting for components:");
            System.out.println("  Ready servers: " + readyServers + " / Expected: " + expectedServers);
            System.out.println("  Ready clients: " + readyClients + " / Expected: " + expectedClients);
            if (!allServersReady) {
                Set<String> missingServers = new HashSet<>(expectedServers);
                missingServers.removeAll(readyServers);
                System.out.println("  Missing servers: " + missingServers);
            }
            if (!allClientsReady) {
                Set<String> missingClients = new HashSet<>(expectedClients);
                missingClients.removeAll(readyClients);
                System.out.println("  Missing clients: " + missingClients);
            }
        }
    }
    
    /**
     * Configure router endpoints for HTTP access (following repo pattern)
     */
    public static void setRouter(Router parentRouter) {
        // List all clients
        parentRouter.get("/host/v1/clients").handler(McpHostManager::handleGetClients);
        
        // List all tools
        parentRouter.get("/host/v1/tools").handler(McpHostManager::handleGetTools);
        
        // Get system status
        parentRouter.get("/host/v1/mcp/status").handler(McpHostManager::handleGetStatus);
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
                if (ar.succeeded()) {
                    System.out.println("MCP Host Manager stopped - all verticles undeployed");
                } else {
                    System.err.println("MCP Host Manager stop had errors: " + ar.cause().getMessage());
                }
                // Always complete the stop promise to avoid hanging
                stopPromise.complete();
            });
    }
}
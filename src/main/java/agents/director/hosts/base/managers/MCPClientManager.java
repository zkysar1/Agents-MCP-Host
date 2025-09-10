package agents.director.hosts.base.managers;

import agents.director.mcp.base.MCPClientBase;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import static agents.director.Driver.logLevel;

/**
 * Base class for MCP client managers.
 * Managers coordinate multiple MCP clients to provide higher-level functionality.
 * Note: Managers are NOT MCP clients themselves - they orchestrate multiple clients
 * while maintaining MCP's 1:1 client-server relationship requirement.
 */
public abstract class MCPClientManager {
    
    protected final Vertx vertx;
    protected final String baseUrl;
    protected final Map<String, MCPClientBase> clients = new ConcurrentHashMap<>();
    protected final Map<String, String> deploymentIds = new ConcurrentHashMap<>();
    private final AtomicBoolean isShuttingDown = new AtomicBoolean(false);
    
    /**
     * Constructor for client manager
     * @param vertx The Vert.x instance
     * @param baseUrl The base URL for all MCP servers
     */
    protected MCPClientManager(Vertx vertx, String baseUrl) {
        this.vertx = vertx;
        this.baseUrl = baseUrl;
    }
    
    /**
     * Initialize all managed clients
     * @return Future that completes when all clients are initialized
     */
    public abstract Future<Void> initialize();
    
    /**
     * Deploy a client and track its deployment ID
     * @param clientName Name to reference this client
     * @param client The MCP client to deploy
     * @return Future with deployment ID
     */
    protected Future<String> deployClient(String clientName, MCPClientBase client) {
        Promise<String> promise = Promise.promise();
        
        vertx.deployVerticle(client, ar -> {
            if (ar.succeeded()) {
                String deploymentId = ar.result();
                clients.put(clientName, client);
                deploymentIds.put(clientName, deploymentId);
                
                if (logLevel >= 2) {
                    vertx.eventBus().publish("log", 
                        "Deployed " + clientName + " client in " + getClass().getSimpleName() +
                        ",2," + getClass().getSimpleName() + ",Manager,System");
                }
                promise.complete(deploymentId);
            } else {
                vertx.eventBus().publish("log", 
                    "Failed to deploy " + clientName + " client: " + ar.cause().getMessage() +
                    ",0," + getClass().getSimpleName() + ",Manager,System");
                promise.fail(ar.cause());
            }
        });
        
        return promise.future();
    }
    
    /**
     * Get a specific client by name
     * @param clientName Name of the client
     * @return The client instance or null if not found
     */
    protected MCPClientBase getClient(String clientName) {
        return clients.get(clientName);
    }
    
    /**
     * Check if all managed clients are ready
     * @return true if all clients are deployed and ready
     */
    public boolean isReady() {
        if (clients.isEmpty() || clients.size() != deploymentIds.size()) {
            return false;
        }
        
        // Check each client's readiness
        for (MCPClientBase client : clients.values()) {
            if (client == null || !client.isReady()) {
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * Shutdown all managed clients
     * @return Future that completes when all clients are shut down
     */
    public Future<Void> shutdown() {
        // Prevent multiple concurrent shutdowns
        if (!isShuttingDown.compareAndSet(false, true)) {
            return Future.succeededFuture(); // Already shutting down
        }
        
        List<Future> undeployFutures = new ArrayList<>();
        
        // Create a copy to avoid concurrent modification
        Map<String, String> deploymentsCopy = new HashMap<>(deploymentIds);
        
        for (Map.Entry<String, String> entry : deploymentsCopy.entrySet()) {
            Promise<Void> promise = Promise.promise();
            vertx.undeploy(entry.getValue(), ar -> {
                if (ar.succeeded()) {
                    if (logLevel >= 3) {
                        vertx.eventBus().publish("log", 
                            "Undeployed " + entry.getKey() + " client" +
                            ",3," + getClass().getSimpleName() + ",Manager,System");
                    }
                    promise.complete();
                } else {
                    // Log error but complete anyway to not block shutdown
                    vertx.eventBus().publish("log", 
                        "Failed to undeploy " + entry.getKey() + ": " + ar.cause().getMessage() +
                        ",1," + getClass().getSimpleName() + ",Manager,System");
                    promise.complete();
                }
            });
            undeployFutures.add(promise.future());
        }
        
        return CompositeFuture.all(undeployFutures)
            .onComplete(ar -> {
                // Clear maps after shutdown completes
                clients.clear();
                deploymentIds.clear();
            })
            .mapEmpty();
    }
    
    /**
     * Get the names of all managed clients
     * @return Set of client names
     */
    public Set<String> getClientNames() {
        return new HashSet<>(clients.keySet());
    }
    
    /**
     * Helper method to call a tool on a specific client
     * @param clientName Name of the client
     * @param toolName Name of the tool
     * @param arguments Tool arguments
     * @return Future with tool result
     */
    public Future<JsonObject> callClientTool(String clientName, String toolName, JsonObject arguments) {
        MCPClientBase client = getClient(clientName);
        if (client == null) {
            return Future.failedFuture("Client not found: " + clientName);
        }
        return client.callTool(toolName, arguments);
    }
}
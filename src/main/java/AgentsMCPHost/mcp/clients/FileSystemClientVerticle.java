package AgentsMCPHost.mcp.clients;

import AgentsMCPHost.mcp.transport.VertxStreamableHttpTransport;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.core.eventbus.Message;

import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.UUID;

/**
 * MCP Client that connects to FileSystem server with special configuration.
 * Runs as a standard verticle on the event loop.
 * Uses VertxStreamableHttpTransport for async HTTP communication.
 * Implements resources and roots workflows for file access control.
 */
public class FileSystemClientVerticle extends AbstractVerticle {
    
    private VertxStreamableHttpTransport fileSystemTransport;
    
    // Track available tools from the server
    private final Map<String, JsonObject> availableTools = new ConcurrentHashMap<>();
    
    // Track resources and roots
    private final Map<String, JsonObject> exposedResources = new ConcurrentHashMap<>();
    private final Set<String> allowedRoots = new HashSet<>();
    
    // Client ID for event bus addressing
    private static final String CLIENT_ID = "filesystem";
    private static final String SERVER_NAME = "filesystem";
    private static final int SERVER_PORT = 8084;
    
    // Default roots for file system access
    private static final String[] DEFAULT_ROOTS = {
        "/tmp/mcp-sandbox",
        "/tmp"
    };
    
    @Override
    public void start(Promise<Void> startPromise) {
        // Initialize transport
        fileSystemTransport = new VertxStreamableHttpTransport(vertx, "localhost", SERVER_PORT);
        
        // Set default allowed roots
        for (String root : DEFAULT_ROOTS) {
            allowedRoots.add(root);
        }
        
        // Connect to filesystem server with roots configuration
        connectToServer()
            .onSuccess(v -> {
                // Register event bus consumers for host requests
                vertx.eventBus().consumer("mcp.client." + CLIENT_ID + ".call", this::handleToolCall);
                vertx.eventBus().consumer("mcp.client." + CLIENT_ID + ".status", this::handleStatusRequest);
                vertx.eventBus().consumer("mcp.client." + CLIENT_ID + ".resources", this::handleResourcesRequest);
                vertx.eventBus().consumer("mcp.client." + CLIENT_ID + ".roots", this::handleRootsRequest);
                vertx.eventBus().consumer("mcp.client." + CLIENT_ID + ".updateRoots", this::handleUpdateRoots);
                
                // Notify host that client is ready
                publishClientReady();
                
                System.out.println("FileSystemClient connected to FileSystem server");
                vertx.eventBus().publish("log", 
                    "FileSystemClient ready with " + availableTools.size() + " tools,1,FileSystemClient,StartUp,MCP");
                
                startPromise.complete();
            })
            .onFailure(err -> {
                System.err.println("Failed to connect FileSystemClient: " + err.getMessage());
                startPromise.fail(err);
            });
    }
    
    /**
     * Connect to the filesystem server and discover its capabilities
     */
    private Future<Void> connectToServer() {
        Promise<Void> promise = Promise.promise();
        
        // Build initialization params with roots
        JsonObject initParams = new JsonObject()
            .put("roots", new JsonArray(allowedRoots.stream().toList()));
        
        // Initialize connection with roots configuration
        fileSystemTransport.initializeWithParams(initParams)
            .compose(initResponse -> {
                System.out.println("Initialized connection to " + SERVER_NAME + " server with " + 
                                 allowedRoots.size() + " roots");
                
                // Request tools list
                return fileSystemTransport.listTools();
            })
            .compose(tools -> {
                // Store tools
                for (int i = 0; i < tools.size(); i++) {
                    JsonObject tool = tools.getJsonObject(i);
                    String toolName = tool.getString("name");
                    availableTools.put(toolName, tool);
                }
                
                System.out.println("Client " + CLIENT_ID + " discovered " + tools.size() + " tools from " + SERVER_NAME);
                
                // Publish discovered tools to event bus
                publishToolsDiscovered(tools);
                
                // Request initial resources list
                return fileSystemTransport.listResources();
            })
            .onSuccess(resources -> {
                // Store resources
                updateResourcesList(resources);
                
                // Start SSE stream for server-initiated messages
                fileSystemTransport.startSseStream(event -> {
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
        
        // Validate file paths are within allowed roots
        if (arguments.containsKey("path")) {
            String path = arguments.getString("path");
            if (!isPathAllowed(path)) {
                msg.fail(403, "Path not allowed: " + path + ". Allowed roots: " + allowedRoots);
                return;
            }
        }
        
        // Call tool via transport
        fileSystemTransport.callTool(toolName, arguments)
            .onSuccess(result -> {
                // Check if result contains resource references
                if (result.containsKey("resourceId")) {
                    // Store resource reference
                    String resourceId = result.getString("resourceId");
                    exposedResources.put(resourceId, new JsonObject()
                        .put("path", arguments.getString("path"))
                        .put("tool", toolName));
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
     * Handle resources list requests
     */
    private void handleResourcesRequest(Message<JsonObject> msg) {
        // Request current resources from server
        fileSystemTransport.listResources()
            .onSuccess(resources -> {
                updateResourcesList(resources);
                
                JsonObject response = new JsonObject()
                    .put("resources", resources)
                    .put("count", resources.size())
                    .put("clientId", CLIENT_ID);
                
                msg.reply(response);
            })
            .onFailure(err -> {
                msg.fail(500, "Failed to list resources: " + err.getMessage());
            });
    }
    
    /**
     * Handle roots list requests
     */
    private void handleRootsRequest(Message<JsonObject> msg) {
        // Request current roots from server
        fileSystemTransport.listRoots()
            .onSuccess(roots -> {
                // Update local roots cache
                allowedRoots.clear();
                for (int i = 0; i < roots.size(); i++) {
                    allowedRoots.add(roots.getString(i));
                }
                
                JsonObject response = new JsonObject()
                    .put("roots", roots)
                    .put("count", roots.size())
                    .put("clientId", CLIENT_ID);
                
                msg.reply(response);
            })
            .onFailure(err -> {
                // Return cached roots on failure
                msg.reply(new JsonObject()
                    .put("roots", new JsonArray(allowedRoots.stream().toList()))
                    .put("count", allowedRoots.size())
                    .put("clientId", CLIENT_ID)
                    .put("cached", true));
            });
    }
    
    /**
     * Handle roots update requests
     */
    private void handleUpdateRoots(Message<JsonObject> msg) {
        JsonObject request = msg.body();
        JsonArray newRoots = request.getJsonArray("roots", new JsonArray());
        
        // Update local roots
        allowedRoots.clear();
        for (int i = 0; i < newRoots.size(); i++) {
            allowedRoots.add(newRoots.getString(i));
        }
        
        // Notify server of roots update
        vertx.eventBus().request("mcp.filesystem.roots", new JsonObject()
            .put("roots", newRoots))
            .onSuccess(reply -> {
                msg.reply(new JsonObject()
                    .put("success", true)
                    .put("updatedRoots", newRoots));
            })
            .onFailure(err -> {
                msg.fail(500, "Failed to update roots: " + err.getMessage());
            });
    }
    
    /**
     * Check if path is within allowed roots
     */
    private boolean isPathAllowed(String path) {
        if (path == null || path.isEmpty()) {
            return false;
        }
        
        // Normalize path
        String normalizedPath = path.replace("\\", "/");
        
        // Check if path starts with any allowed root
        for (String root : allowedRoots) {
            if (normalizedPath.startsWith(root)) {
                return true;
            }
        }
        
        return false;
    }
    
    /**
     * Update resources list from server response
     */
    private void updateResourcesList(JsonArray resources) {
        exposedResources.clear();
        
        for (int i = 0; i < resources.size(); i++) {
            JsonObject resource = resources.getJsonObject(i);
            String uri = resource.getString("uri");
            if (uri != null && uri.startsWith("file://")) {
                String resourceId = UUID.randomUUID().toString();
                exposedResources.put(resourceId, resource);
            }
        }
        
        System.out.println("Updated resources list: " + exposedResources.size() + " resources");
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
            .put("resources", exposedResources.size())
            .put("roots", new JsonArray(allowedRoots.stream().toList()))
            .put("capabilities", new JsonArray()
                .add("tool-invocation")
                .add("resources")
                .add("roots"));
        
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
        if ("resources/updated".equals(eventType)) {
            // Resources have been updated on server
            handleResourcesUpdatedEvent(event);
        } else if ("roots/changed".equals(eventType)) {
            // Roots configuration has changed
            handleRootsChangedEvent(event);
        }
    }
    
    /**
     * Handle resources updated event from server
     */
    private void handleResourcesUpdatedEvent(JsonObject event) {
        // Request updated resources list
        fileSystemTransport.listResources()
            .onSuccess(resources -> {
                updateResourcesList(resources);
                
                // Notify host of resources update
                vertx.eventBus().publish("mcp.resources.updated", new JsonObject()
                    .put("client", CLIENT_ID)
                    .put("resources", resources)
                    .put("count", resources.size()));
            });
    }
    
    /**
     * Handle roots changed event from server
     */
    private void handleRootsChangedEvent(JsonObject event) {
        JsonArray newRoots = event.getJsonArray("roots", new JsonArray());
        
        // Update local roots
        allowedRoots.clear();
        for (int i = 0; i < newRoots.size(); i++) {
            allowedRoots.add(newRoots.getString(i));
        }
        
        // Notify host of roots change
        vertx.eventBus().publish("mcp.roots.changed", new JsonObject()
            .put("client", CLIENT_ID)
            .put("roots", newRoots));
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
            .put("toolCount", availableTools.size())
            .put("resources", exposedResources.size())
            .put("roots", new JsonArray(allowedRoots.stream().toList())));
    }
    
    @Override
    public void stop(Promise<Void> stopPromise) {
        // Close transport
        if (fileSystemTransport != null) {
            fileSystemTransport.close();
        }
        
        stopPromise.complete();
    }
}
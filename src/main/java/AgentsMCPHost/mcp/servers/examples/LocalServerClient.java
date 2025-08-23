package AgentsMCPHost.mcp.servers.examples;

import AgentsMCPHost.mcp.core.transport.VertxStdioTransport;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * MCP client that connects to local servers via stdio transport.
 * This allows running MCP servers as local processes.
 */
public class LocalServerClient extends AbstractVerticle {
    
    private static final String CLIENT_ID = "local";
    private final Map<String, VertxStdioTransport> transports = new HashMap<>();
    private final Map<String, JsonArray> serverTools = new HashMap<>();
    private JsonArray allTools = new JsonArray();
    
    @Override
    public void start(Promise<Void> startPromise) {
        // Load local server configurations from config
        JsonObject config = config();
        JsonObject servers = config.getJsonObject("servers", new JsonObject());
        
        if (servers.isEmpty()) {
            System.out.println("No local MCP servers configured");
            startPromise.complete();
            return;
        }
        
        // Start all configured local servers
        List<Future<Void>> serverStarts = servers.fieldNames().stream()
            .map(serverName -> startLocalServer(serverName, servers.getJsonObject(serverName)))
            .toList();
        
        Future.all(serverStarts)
            .onSuccess(v -> {
                // Register event bus consumers
                vertx.eventBus().consumer("mcp.client." + CLIENT_ID + ".call", this::handleToolCall);
                vertx.eventBus().consumer("mcp.client." + CLIENT_ID + ".status", this::handleStatusRequest);
                
                // Publish client ready
                publishClientReady();
                
                System.out.println("LocalServerClient started with " + transports.size() + " servers");
                startPromise.complete();
            })
            .onFailure(err -> {
                System.err.println("Failed to start local servers: " + err.getMessage());
                startPromise.fail(err);
            });
    }
    
    private Future<Void> startLocalServer(String serverName, JsonObject serverConfig) {
        Promise<Void> promise = Promise.promise();
        
        String command = serverConfig.getString("command");
        JsonArray argsArray = serverConfig.getJsonArray("args", new JsonArray());
        JsonObject env = serverConfig.getJsonObject("environment", new JsonObject());
        
        if (command == null) {
            promise.fail("No command specified for server: " + serverName);
            return promise.future();
        }
        
        // Convert args array to list
        List<String> args = argsArray.stream()
            .map(Object::toString)
            .toList();
        
        // Convert environment object to map
        Map<String, String> environment = new HashMap<>();
        env.forEach(entry -> environment.put(entry.getKey(), entry.getValue().toString()));
        
        // Create and start stdio transport
        VertxStdioTransport transport = new VertxStdioTransport(vertx, command, args, environment);
        
        transport.start()
            .compose(v -> transport.initialize())
            .compose(init -> {
                System.out.println("Initialized local server: " + serverName);
                
                // Set up notification handler
                transport.setNotificationHandler(notification -> {
                    handleServerNotification(serverName, notification);
                });
                
                // Discover tools
                return transport.listTools();
            })
            .onSuccess(tools -> {
                transports.put(serverName, transport);
                serverTools.put(serverName, tools);
                
                // Add tools with server prefix
                for (Object toolObj : tools) {
                    JsonObject tool = (JsonObject) toolObj;
                    // Prefix tool name with server name
                    String originalName = tool.getString("name");
                    tool.put("name", serverName + "__" + originalName);
                    tool.put("_server", serverName);
                    tool.put("_originalName", originalName);
                    allTools.add(tool);
                }
                
                // Publish discovered tools
                vertx.eventBus().publish("mcp.tools.discovered", new JsonObject()
                    .put("client", CLIENT_ID)
                    .put("server", serverName)
                    .put("tools", tools));
                
                System.out.println("Local server " + serverName + " started with " + tools.size() + " tools");
                promise.complete();
            })
            .onFailure(err -> {
                System.err.println("Failed to start local server " + serverName + ": " + err.getMessage());
                promise.fail(err);
            });
        
        return promise.future();
    }
    
    private void handleToolCall(Message<JsonObject> msg) {
        JsonObject request = msg.body();
        String toolName = request.getString("tool");
        JsonObject arguments = request.getJsonObject("arguments", new JsonObject());
        
        // Find which server has this tool
        String serverName = null;
        String originalToolName = toolName;
        
        // Check if tool name includes server prefix
        if (toolName.contains("__")) {
            String[] parts = toolName.split("__", 2);
            serverName = parts[0];
            originalToolName = parts[1];
        } else {
            // Search for tool in all servers
            for (Object toolObj : allTools) {
                JsonObject tool = (JsonObject) toolObj;
                if (tool.getString("name").endsWith("__" + toolName)) {
                    serverName = tool.getString("_server");
                    originalToolName = tool.getString("_originalName");
                    break;
                }
            }
        }
        
        if (serverName == null || !transports.containsKey(serverName)) {
            msg.fail(404, "Tool not found: " + toolName);
            return;
        }
        
        VertxStdioTransport transport = transports.get(serverName);
        
        // Make serverName final for lambda
        final String finalServerName = serverName;
        
        transport.callTool(originalToolName, arguments)
            .onSuccess(result -> {
                result.put("_client", CLIENT_ID);
                result.put("_server", finalServerName);
                msg.reply(result);
            })
            .onFailure(err -> {
                msg.fail(500, "Tool execution failed: " + err.getMessage());
            });
    }
    
    private void handleStatusRequest(Message<JsonObject> msg) {
        JsonObject status = new JsonObject()
            .put("client", CLIENT_ID)
            .put("type", "local")
            .put("servers", new JsonArray(transports.keySet().stream().toList()))
            .put("toolCount", allTools.size())
            .put("tools", allTools);
        
        msg.reply(status);
    }
    
    private void handleServerNotification(String serverName, JsonObject notification) {
        // Forward server notifications via event bus
        vertx.eventBus().publish("mcp.notification." + serverName, notification);
        
        String method = notification.getString("method");
        if (method != null) {
            System.out.println("Notification from " + serverName + ": " + method);
        }
    }
    
    private void publishClientReady() {
        vertx.eventBus().publish("mcp.client.ready", new JsonObject()
            .put("client", CLIENT_ID)
            .put("type", "local")
            .put("servers", new JsonArray(transports.keySet().stream().toList()))
            .put("toolCount", allTools.size()));
    }
    
    @Override
    public void stop(Promise<Void> stopPromise) {
        // Shutdown all local servers
        List<Future<Void>> shutdowns = transports.values().stream()
            .map(VertxStdioTransport::shutdown)
            .toList();
        
        Future.all(shutdowns)
            .onComplete(ar -> {
                transports.clear();
                serverTools.clear();
                allTools.clear();
                stopPromise.complete();
            });
    }
}
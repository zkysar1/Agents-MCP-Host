package AgentsMCPHost.mcp.transport;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.parsetools.JsonParser;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Stdio transport adapter for MCP servers that communicate via standard input/output.
 * This allows running local MCP servers as separate processes.
 */
public class VertxStdioTransport {
    
    private final Vertx vertx;
    private final String command;
    private final List<String> args;
    private final Map<String, String> environment;
    
    private Process process;
    private BufferedWriter stdin;
    private BufferedReader stdout;
    private BufferedReader stderr;
    
    private final AtomicInteger requestId = new AtomicInteger(1);
    private final Map<String, Promise<JsonObject>> pendingRequests = new ConcurrentHashMap<>();
    private String sessionId;
    private boolean initialized = false;
    private Consumer<JsonObject> notificationHandler;
    
    // Protocol constants
    private static final String MCP_PROTOCOL_VERSION = "2024-11-05";
    private static final String JSONRPC_VERSION = "2.0";
    
    public VertxStdioTransport(Vertx vertx, String command, List<String> args, Map<String, String> environment) {
        this.vertx = vertx;
        this.command = command;
        this.args = args != null ? args : List.of();
        this.environment = environment != null ? environment : Map.of();
        this.sessionId = UUID.randomUUID().toString();
    }
    
    /**
     * Start the process and establish stdio communication
     */
    public Future<Void> start() {
        Promise<Void> promise = Promise.promise();
        
        vertx.executeBlocking(blockingPromise -> {
            try {
                ProcessBuilder pb = new ProcessBuilder();
                List<String> fullCommand = new java.util.ArrayList<>();
                fullCommand.add(command);
                fullCommand.addAll(args);
                pb.command(fullCommand);
                
                // Add environment variables
                Map<String, String> env = pb.environment();
                env.putAll(environment);
                
                // Start the process
                process = pb.start();
                
                // Set up stdio streams
                stdin = new BufferedWriter(new OutputStreamWriter(process.getOutputStream()));
                stdout = new BufferedReader(new InputStreamReader(process.getInputStream()));
                stderr = new BufferedReader(new InputStreamReader(process.getErrorStream()));
                
                // Start reading from stdout in background
                startReadingStdout();
                
                // Start reading from stderr for debugging
                startReadingStderr();
                
                blockingPromise.complete();
            } catch (Exception e) {
                blockingPromise.fail(e);
            }
        }, res -> {
            if (res.succeeded()) {
                promise.complete();
            } else {
                promise.fail(res.cause());
            }
        });
        
        return promise.future();
    }
    
    /**
     * Initialize MCP session
     */
    public Future<JsonObject> initialize() {
        return initialize(new JsonObject());
    }
    
    public Future<JsonObject> initialize(JsonObject params) {
        if (initialized) {
            return Future.succeededFuture(new JsonObject()
                .put("protocolVersion", MCP_PROTOCOL_VERSION)
                .put("sessionId", sessionId));
        }
        
        JsonObject request = createRequest("initialize", new JsonObject()
            .put("protocolVersion", MCP_PROTOCOL_VERSION)
            .put("capabilities", new JsonObject()
                .put("roots", new JsonObject().put("listChanged", true))
                .put("sampling", new JsonObject()))
            .mergeIn(params));
        
        return sendRequest(request)
            .map(response -> {
                initialized = true;
                JsonObject result = response.getJsonObject("result", new JsonObject());
                
                // Send initialized notification
                sendNotification("notifications/initialized", new JsonObject());
                
                return result;
            });
    }
    
    /**
     * List available tools
     */
    public Future<JsonArray> listTools() {
        JsonObject request = createRequest("tools/list", new JsonObject());
        
        return sendRequest(request)
            .map(response -> {
                JsonObject result = response.getJsonObject("result", new JsonObject());
                return result.getJsonArray("tools", new JsonArray());
            });
    }
    
    /**
     * Call a tool
     */
    public Future<JsonObject> callTool(String toolName, JsonObject arguments) {
        JsonObject params = new JsonObject()
            .put("name", toolName)
            .put("arguments", arguments != null ? arguments : new JsonObject());
        
        JsonObject request = createRequest("tools/call", params);
        
        return sendRequest(request)
            .map(response -> response.getJsonObject("result", new JsonObject()));
    }
    
    /**
     * List resources (if server supports them)
     */
    public Future<JsonArray> listResources() {
        JsonObject request = createRequest("resources/list", new JsonObject());
        
        return sendRequest(request)
            .map(response -> {
                JsonObject result = response.getJsonObject("result", new JsonObject());
                return result.getJsonArray("resources", new JsonArray());
            });
    }
    
    /**
     * Set notification handler for server-initiated messages
     */
    public void setNotificationHandler(Consumer<JsonObject> handler) {
        this.notificationHandler = handler;
    }
    
    /**
     * Send a JSON-RPC request and wait for response
     */
    private Future<JsonObject> sendRequest(JsonObject request) {
        Promise<JsonObject> promise = Promise.promise();
        String id = request.getValue("id").toString();
        pendingRequests.put(id, promise);
        
        // Send request via stdin
        vertx.executeBlocking(blockingPromise -> {
            try {
                String message = request.encode() + "\n";
                stdin.write(message);
                stdin.flush();
                blockingPromise.complete();
            } catch (IOException e) {
                blockingPromise.fail(e);
            }
        }, res -> {
            if (res.failed()) {
                pendingRequests.remove(id);
                promise.fail(res.cause());
            }
        });
        
        return promise.future();
    }
    
    /**
     * Send a JSON-RPC notification (no response expected)
     */
    private void sendNotification(String method, JsonObject params) {
        JsonObject notification = new JsonObject()
            .put("jsonrpc", JSONRPC_VERSION)
            .put("method", method)
            .put("params", params);
        
        vertx.executeBlocking(blockingPromise -> {
            try {
                String message = notification.encode() + "\n";
                stdin.write(message);
                stdin.flush();
                blockingPromise.complete();
            } catch (IOException e) {
                blockingPromise.fail(e);
            }
        }, res -> {
            if (res.failed()) {
                System.err.println("Failed to send notification: " + res.cause().getMessage());
            }
        });
    }
    
    /**
     * Create a JSON-RPC request
     */
    private JsonObject createRequest(String method, JsonObject params) {
        String id = String.valueOf(requestId.getAndIncrement());
        return new JsonObject()
            .put("jsonrpc", JSONRPC_VERSION)
            .put("id", id)
            .put("method", method)
            .put("params", params);
    }
    
    /**
     * Start reading from stdout and process JSON-RPC messages
     */
    private void startReadingStdout() {
        vertx.executeBlocking(blockingPromise -> {
            try {
                String line;
                while ((line = stdout.readLine()) != null) {
                    final String message = line;
                    vertx.runOnContext(v -> processMessage(message));
                }
            } catch (IOException e) {
                System.err.println("Error reading stdout: " + e.getMessage());
            }
        }, res -> {
            // Process terminated or error occurred
            if (res.failed()) {
                System.err.println("Stdout reader failed: " + res.cause().getMessage());
            }
        });
    }
    
    /**
     * Start reading from stderr for debugging
     */
    private void startReadingStderr() {
        vertx.executeBlocking(blockingPromise -> {
            try {
                String line;
                while ((line = stderr.readLine()) != null) {
                    System.err.println("MCP Server stderr: " + line);
                }
            } catch (IOException e) {
                System.err.println("Error reading stderr: " + e.getMessage());
            }
        }, res -> {
            // Process terminated or error occurred
        });
    }
    
    /**
     * Process a JSON-RPC message from stdout
     */
    private void processMessage(String message) {
        try {
            JsonObject json = new JsonObject(message);
            
            // Check if it's a response to a request
            if (json.containsKey("id")) {
                String id = json.getValue("id").toString();
                Promise<JsonObject> promise = pendingRequests.remove(id);
                if (promise != null) {
                    if (json.containsKey("error")) {
                        JsonObject error = json.getJsonObject("error");
                        promise.fail(new RuntimeException(
                            "JSON-RPC error: " + error.getString("message", "Unknown error")));
                    } else {
                        promise.complete(json);
                    }
                }
            }
            // Check if it's a notification
            else if (json.containsKey("method") && !json.containsKey("id")) {
                if (notificationHandler != null) {
                    notificationHandler.accept(json);
                }
            }
        } catch (Exception e) {
            System.err.println("Failed to process message: " + e.getMessage());
        }
    }
    
    /**
     * Shutdown the process and clean up resources
     */
    public Future<Void> shutdown() {
        Promise<Void> promise = Promise.promise();
        
        // Send shutdown request if initialized
        if (initialized) {
            JsonObject request = createRequest("shutdown", new JsonObject());
            sendRequest(request)
                .onComplete(ar -> {
                    // Regardless of shutdown response, terminate the process
                    terminateProcess();
                    promise.complete();
                });
        } else {
            terminateProcess();
            promise.complete();
        }
        
        return promise.future();
    }
    
    private void terminateProcess() {
        try {
            if (stdin != null) stdin.close();
            if (stdout != null) stdout.close();
            if (stderr != null) stderr.close();
            
            if (process != null && process.isAlive()) {
                process.destroy();
                // Give it time to shutdown gracefully
                vertx.setTimer(1000, id -> {
                    if (process.isAlive()) {
                        process.destroyForcibly();
                    }
                });
            }
        } catch (IOException e) {
            System.err.println("Error during shutdown: " + e.getMessage());
        }
    }
}
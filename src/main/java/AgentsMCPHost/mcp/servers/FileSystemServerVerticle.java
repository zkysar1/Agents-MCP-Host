package AgentsMCPHost.mcp.servers;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.eventbus.Message;

import java.util.UUID;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * MCP Server for file system operations.
 * Runs as a worker verticle on port 8084.
 * Implements MCP protocol with streamable HTTP transport.
 * Includes resources and roots workflows for file access control.
 */
public class FileSystemServerVerticle extends AbstractVerticle {
    
    private static FileSystemServerVerticle instance;
    private static final int PORT = 8084;
    private static final String MCP_PROTOCOL_VERSION = "2024-11-05";
    private HttpServer httpServer;
    
    // Session management
    private final Map<String, JsonObject> sessions = new ConcurrentHashMap<>();
    
    // File system roots (allowed directories)
    private final Set<String> allowedRoots = new HashSet<>();
    private final String SANDBOX_PATH = "/tmp/mcp-sandbox";
    
    // Resources (exposed files)
    private final Map<String, String> exposedResources = new ConcurrentHashMap<>();
    
    // Available tools
    private final JsonArray tools = new JsonArray()
        .add(new JsonObject()
            .put("name", "list")
            .put("description", "List directory contents")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("path", new JsonObject().put("type", "string"))
                    .put("recursive", new JsonObject().put("type", "boolean").put("default", false)))
                .put("required", new JsonArray().add("path"))))
        .add(new JsonObject()
            .put("name", "read")
            .put("description", "Read file content")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("path", new JsonObject().put("type", "string")))
                .put("required", new JsonArray().add("path"))))
        .add(new JsonObject()
            .put("name", "write")
            .put("description", "Write content to file")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("path", new JsonObject().put("type", "string"))
                    .put("content", new JsonObject().put("type", "string"))
                    .put("append", new JsonObject().put("type", "boolean").put("default", false)))
                .put("required", new JsonArray().add("path").add("content"))))
        .add(new JsonObject()
            .put("name", "delete")
            .put("description", "Delete file or directory")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("path", new JsonObject().put("type", "string")))
                .put("required", new JsonArray().add("path"))));
    
    @Override
    public void start(Promise<Void> startPromise) {
        instance = this;
        
        // Initialize sandbox directory
        initializeSandbox();
        
        // Set default allowed roots
        allowedRoots.add(SANDBOX_PATH);
        allowedRoots.add("/tmp");
        
        // Create router with static method pattern (following repo style)
        Router router = Router.router(vertx);
        setRouter(router);
        
        // Register event bus consumers for tool execution
        vertx.eventBus().consumer("mcp.filesystem.execute", this::executeToolAsync);
        
        // Register roots handler
        vertx.eventBus().consumer("mcp.filesystem.roots", this::handleRootsUpdate);
        
        // Register resources handler
        vertx.eventBus().consumer("mcp.filesystem.resources", this::handleResourcesRequest);
        
        // Create HTTP server options
        HttpServerOptions options = new HttpServerOptions()
            .setHost("localhost")  // Bind to localhost only (security requirement)
            .setPort(PORT)
            .setCompressionSupported(true);
        
        // Start HTTP server
        httpServer = vertx.createHttpServer(options);
        httpServer.requestHandler(router)
            .listen()
            .onSuccess(server -> {
                System.out.println("FileSystem MCP Server started on port " + PORT);
                vertx.eventBus().publish("log", "FileSystem MCP Server started,1,FileSystemServer,StartUp,MCP");
                
                // Notify host that server is ready
                vertx.eventBus().publish("mcp.server.ready", new JsonObject()
                    .put("server", "filesystem")
                    .put("port", PORT)
                    .put("tools", tools.size()));
                    
                startPromise.complete();
            })
            .onFailure(startPromise::fail);
    }
    
    /**
     * Initialize sandbox directory for safe file operations
     */
    private void initializeSandbox() {
        File sandbox = new File(SANDBOX_PATH);
        if (!sandbox.exists()) {
            sandbox.mkdirs();
            System.out.println("Created sandbox directory: " + SANDBOX_PATH);
        }
        
        // Create some sample files for testing
        File sampleFile = new File(SANDBOX_PATH + "/sample.txt");
        if (!sampleFile.exists()) {
            try {
                sampleFile.createNewFile();
                // Would write content in real implementation
            } catch (Exception e) {
                System.err.println("Failed to create sample file: " + e.getMessage());
            }
        }
    }
    
    /**
     * Static router configuration (following repo pattern)
     */
    public static void setRouter(Router router) {
        // Add body handler for JSON parsing
        router.route().handler(BodyHandler.create());
        
        // CORS headers for all routes
        router.route().handler(ctx -> {
            ctx.response()
                .putHeader("Access-Control-Allow-Origin", "http://localhost:8080")
                .putHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
                .putHeader("Access-Control-Allow-Headers", "Content-Type, MCP-Protocol-Version, Mcp-Session-Id");
            
            if (ctx.request().method().name().equals("OPTIONS")) {
                ctx.response().setStatusCode(204).end();
            } else {
                ctx.next();
            }
        });
        
        // MCP protocol endpoints
        router.post("/").handler(FileSystemServerVerticle::handleMcpRequest);
        router.get("/").handler(FileSystemServerVerticle::handleSseStream);
        
        // Resources endpoint
        router.get("/resources/:id").handler(FileSystemServerVerticle::handleResourceRequest);
    }
    
    /**
     * Handle MCP JSON-RPC requests
     */
    private static void handleMcpRequest(RoutingContext ctx) {
        try {
            // Validate headers
            String protocolVersion = ctx.request().getHeader("MCP-Protocol-Version");
            String sessionId = ctx.request().getHeader("Mcp-Session-Id");
            String origin = ctx.request().getHeader("Origin");
            
            // Validate origin (security requirement)
            if (!"http://localhost:8080".equals(origin)) {
                sendError(ctx, 403, "Invalid origin", null);
                return;
            }
            
            JsonObject request = ctx.body().asJsonObject();
            if (request == null) {
                sendError(ctx, 400, "Invalid JSON", null);
                return;
            }
            
            String method = request.getString("method");
            String id = request.getString("id");
            JsonObject params = request.getJsonObject("params", new JsonObject());
            
            // Get verticle instance to access instance methods
            FileSystemServerVerticle verticle = instance;
            if (verticle == null) {
                sendError(ctx, 500, "Server not initialized", id);
                return;
            }
            
            // Route based on method
            switch (method) {
                case "initialize":
                    verticle.handleInitialize(ctx, id, params, sessionId);
                    break;
                case "tools/list":
                    verticle.handleToolsList(ctx, id);
                    break;
                case "tools/call":
                    verticle.handleToolCall(ctx, id, params);
                    break;
                case "resources/list":
                    verticle.handleResourcesList(ctx, id);
                    break;
                case "resources/read":
                    verticle.handleResourceRead(ctx, id, params);
                    break;
                case "roots/list":
                    verticle.handleRootsList(ctx, id);
                    break;
                default:
                    sendError(ctx, 400, "Unknown method: " + method, id);
            }
            
        } catch (Exception e) {
            sendError(ctx, 500, "Internal error: " + e.getMessage(), null);
        }
    }
    
    /**
     * Handle SSE stream requests
     */
    private static void handleSseStream(RoutingContext ctx) {
        String accept = ctx.request().getHeader("Accept");
        
        if (!"text/event-stream".equals(accept)) {
            ctx.response()
                .setStatusCode(406)
                .end("SSE stream requires Accept: text/event-stream");
            return;
        }
        
        // Set up SSE response
        ctx.response()
            .putHeader("Content-Type", "text/event-stream")
            .putHeader("Cache-Control", "no-cache")
            .putHeader("Connection", "keep-alive")
            .setChunked(true);
        
        // Send initial ping
        ctx.response().write("event: ping\ndata: {\"type\":\"ping\"}\n\n");
        
        // Set up periodic ping to keep connection alive
        long timerId = ctx.vertx().setPeriodic(30000, id -> {
            ctx.response().write("event: ping\ndata: {\"type\":\"ping\"}\n\n");
        });
        
        // Clean up on close
        ctx.response().closeHandler(v -> {
            ctx.vertx().cancelTimer(timerId);
        });
    }
    
    /**
     * Handle resource request via HTTP
     */
    private static void handleResourceRequest(RoutingContext ctx) {
        String resourceId = ctx.pathParam("id");
        
        // In real implementation, would retrieve resource content
        ctx.response()
            .putHeader("Content-Type", "text/plain")
            .setStatusCode(200)
            .end("Resource content for: " + resourceId);
    }
    
    /**
     * Handle initialize request
     */
    private void handleInitialize(RoutingContext ctx, String id, JsonObject params, String sessionId) {
        // Store session with roots if provided
        JsonObject session = new JsonObject()
            .put("sessionId", sessionId)
            .put("protocolVersion", params.getString("protocolVersion"))
            .put("capabilities", params.getJsonObject("capabilities"));
        
        // Check for roots in params
        if (params.containsKey("roots")) {
            JsonArray roots = params.getJsonArray("roots");
            updateAllowedRoots(roots);
            session.put("roots", roots);
        }
        
        sessions.put(sessionId, session);
        
        // Send initialize response
        JsonObject response = new JsonObject()
            .put("jsonrpc", "2.0")
            .put("id", id)
            .put("result", new JsonObject()
                .put("protocolVersion", MCP_PROTOCOL_VERSION)
                .put("capabilities", new JsonObject()
                    .put("tools", new JsonObject().put("listChanged", true))
                    .put("resources", new JsonObject()
                        .put("subscribe", true)
                        .put("listChanged", true))
                    .put("roots", new JsonObject())
                    .put("prompts", new JsonObject()))
                .put("serverInfo", new JsonObject()
                    .put("name", "FileSystem Server")
                    .put("version", "1.0.0")));
        
        ctx.response()
            .setStatusCode(200)
            .putHeader("Content-Type", "application/json")
            .end(response.encode());
    }
    
    /**
     * Handle tools/list request
     */
    private void handleToolsList(RoutingContext ctx, String id) {
        JsonObject response = new JsonObject()
            .put("jsonrpc", "2.0")
            .put("id", id)
            .put("result", new JsonObject()
                .put("tools", tools));
        
        ctx.response()
            .setStatusCode(200)
            .putHeader("Content-Type", "application/json")
            .end(response.encode());
    }
    
    /**
     * Handle resources/list request
     */
    private void handleResourcesList(RoutingContext ctx, String id) {
        JsonArray resources = new JsonArray();
        
        exposedResources.forEach((resourceId, path) -> {
            resources.add(new JsonObject()
                .put("uri", "file://" + path)
                .put("name", new File(path).getName())
                .put("mimeType", "text/plain"));
        });
        
        JsonObject response = new JsonObject()
            .put("jsonrpc", "2.0")
            .put("id", id)
            .put("result", new JsonObject()
                .put("resources", resources));
        
        ctx.response()
            .setStatusCode(200)
            .putHeader("Content-Type", "application/json")
            .end(response.encode());
    }
    
    /**
     * Handle resources/read request
     */
    private void handleResourceRead(RoutingContext ctx, String id, JsonObject params) {
        String uri = params.getString("uri");
        
        // Mock resource content
        JsonObject response = new JsonObject()
            .put("jsonrpc", "2.0")
            .put("id", id)
            .put("result", new JsonObject()
                .put("contents", new JsonArray()
                    .add(new JsonObject()
                        .put("uri", uri)
                        .put("mimeType", "text/plain")
                        .put("text", "Sample resource content for " + uri))));
        
        ctx.response()
            .setStatusCode(200)
            .putHeader("Content-Type", "application/json")
            .end(response.encode());
    }
    
    /**
     * Handle roots/list request
     */
    private void handleRootsList(RoutingContext ctx, String id) {
        JsonArray roots = new JsonArray();
        allowedRoots.forEach(roots::add);
        
        JsonObject response = new JsonObject()
            .put("jsonrpc", "2.0")
            .put("id", id)
            .put("result", new JsonObject()
                .put("roots", roots));
        
        ctx.response()
            .setStatusCode(200)
            .putHeader("Content-Type", "application/json")
            .end(response.encode());
    }
    
    /**
     * Handle tools/call request
     */
    private void handleToolCall(RoutingContext ctx, String id, JsonObject params) {
        String toolName = params.getString("name");
        JsonObject arguments = params.getJsonObject("arguments", new JsonObject());
        
        // Use executeBlocking for tool execution (worker verticle pattern)
        vertx.executeBlocking(promise -> {
            try {
                Object result = executeTool(toolName, arguments);
                promise.complete(result);
            } catch (Exception e) {
                promise.fail(e);
            }
        }, res -> {
            if (res.succeeded()) {
                JsonObject response = new JsonObject()
                    .put("jsonrpc", "2.0")
                    .put("id", id)
                    .put("result", new JsonObject()
                        .put("content", new JsonArray()
                            .add(new JsonObject()
                                .put("type", "text")
                                .put("text", res.result().toString()))));
                
                ctx.response()
                    .setStatusCode(200)
                    .putHeader("Content-Type", "application/json")
                    .end(response.encode());
            } else {
                sendError(ctx, 500, "Tool execution failed: " + res.cause().getMessage(), id);
            }
        });
    }
    
    /**
     * Execute tool with given arguments (runs in worker thread)
     */
    private Object executeTool(String toolName, JsonObject arguments) {
        String path = arguments.getString("path", "/");
        
        // Validate path is within allowed roots
        if (!isPathAllowed(path)) {
            throw new SecurityException("Path not allowed: " + path);
        }
        
        switch (toolName) {
            case "list":
                boolean recursive = arguments.getBoolean("recursive", false);
                return listDirectory(path, recursive);
                
            case "read":
                return readFile(path);
                
            case "write":
                String content = arguments.getString("content", "");
                boolean append = arguments.getBoolean("append", false);
                return writeFile(path, content, append);
                
            case "delete":
                return deleteFile(path);
                
            default:
                throw new IllegalArgumentException("Unknown tool: " + toolName);
        }
    }
    
    /**
     * Check if path is within allowed roots
     */
    private boolean isPathAllowed(String path) {
        Path normalizedPath = Paths.get(path).normalize();
        
        for (String root : allowedRoots) {
            Path rootPath = Paths.get(root).normalize();
            if (normalizedPath.startsWith(rootPath)) {
                return true;
            }
        }
        
        return false;
    }
    
    /**
     * List directory contents (mock implementation)
     */
    private String listDirectory(String path, boolean recursive) {
        JsonArray files = new JsonArray()
            .add(new JsonObject()
                .put("name", "file1.txt")
                .put("type", "file")
                .put("size", 1024))
            .add(new JsonObject()
                .put("name", "file2.txt")
                .put("type", "file")
                .put("size", 2048))
            .add(new JsonObject()
                .put("name", "subdir")
                .put("type", "directory"));
        
        return new JsonObject()
            .put("path", path)
            .put("files", files)
            .put("count", files.size())
            .encode();
    }
    
    /**
     * Read file content (mock implementation)
     */
    private String readFile(String path) {
        // Add to exposed resources
        String resourceId = UUID.randomUUID().toString();
        exposedResources.put(resourceId, path);
        
        return new JsonObject()
            .put("path", path)
            .put("content", "Sample content from " + path)
            .put("resourceId", resourceId)
            .encode();
    }
    
    /**
     * Write file content (mock implementation)
     */
    private String writeFile(String path, String content, boolean append) {
        return new JsonObject()
            .put("success", true)
            .put("path", path)
            .put("bytesWritten", content.length())
            .put("append", append)
            .encode();
    }
    
    /**
     * Delete file (mock implementation)
     */
    private String deleteFile(String path) {
        return new JsonObject()
            .put("success", true)
            .put("path", path)
            .put("deleted", true)
            .encode();
    }
    
    /**
     * Execute tool via event bus (for internal calls)
     */
    private void executeToolAsync(Message<JsonObject> msg) {
        JsonObject request = msg.body();
        String toolName = request.getString("tool");
        JsonObject arguments = request.getJsonObject("arguments");
        
        vertx.executeBlocking(promise -> {
            try {
                Object result = executeTool(toolName, arguments);
                promise.complete(new JsonObject()
                    .put("success", true)
                    .put("result", result));
            } catch (Exception e) {
                promise.complete(new JsonObject()
                    .put("success", false)
                    .put("error", e.getMessage()));
            }
        }, res -> {
            msg.reply(res.result());
        });
    }
    
    /**
     * Handle roots update from client
     */
    private void handleRootsUpdate(Message<JsonObject> msg) {
        JsonObject request = msg.body();
        JsonArray roots = request.getJsonArray("roots", new JsonArray());
        
        updateAllowedRoots(roots);
        
        msg.reply(new JsonObject()
            .put("success", true)
            .put("rootsCount", allowedRoots.size()));
    }
    
    /**
     * Update allowed roots
     */
    private void updateAllowedRoots(JsonArray roots) {
        // Clear non-default roots
        allowedRoots.clear();
        allowedRoots.add(SANDBOX_PATH);  // Always keep sandbox
        
        // Add new roots
        for (int i = 0; i < roots.size(); i++) {
            String root = roots.getString(i);
            if (root != null && !root.isEmpty()) {
                allowedRoots.add(root);
            }
        }
        
        System.out.println("Updated allowed roots: " + allowedRoots);
    }
    
    /**
     * Handle resources request
     */
    private void handleResourcesRequest(Message<JsonObject> msg) {
        JsonArray resources = new JsonArray();
        
        exposedResources.forEach((id, path) -> {
            resources.add(new JsonObject()
                .put("id", id)
                .put("path", path));
        });
        
        msg.reply(new JsonObject()
            .put("resources", resources)
            .put("count", resources.size()));
    }
    
    /**
     * Send error response
     */
    private static void sendError(RoutingContext ctx, int statusCode, String message, String id) {
        JsonObject error = new JsonObject()
            .put("jsonrpc", "2.0")
            .put("error", new JsonObject()
                .put("code", statusCode)
                .put("message", message));
        
        if (id != null) {
            error.put("id", id);
        }
        
        ctx.response()
            .setStatusCode(statusCode)
            .putHeader("Content-Type", "application/json")
            .end(error.encode());
    }
    
    @Override
    public void stop(Promise<Void> stopPromise) {
        if (httpServer != null) {
            httpServer.close(stopPromise);
        } else {
            stopPromise.complete();
        }
    }
}
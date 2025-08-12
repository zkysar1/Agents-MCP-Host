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
import java.util.concurrent.ConcurrentHashMap;

/**
 * MCP Server for database operations.
 * Runs as a worker verticle on port 8083.
 * Implements MCP protocol with streamable HTTP transport.
 * Includes sampling workflow for large result sets.
 */
public class DatabaseServerVerticle extends AbstractVerticle {
    
    private static DatabaseServerVerticle instance;
    private static final int PORT = 8083;
    private static final String MCP_PROTOCOL_VERSION = "2024-11-05";
    private HttpServer httpServer;
    
    // Session management
    private final Map<String, JsonObject> sessions = new ConcurrentHashMap<>();
    
    // Available tools
    private final JsonArray tools = new JsonArray()
        .add(new JsonObject()
            .put("name", "query")
            .put("description", "Query database tables")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("table", new JsonObject().put("type", "string"))
                    .put("filter", new JsonObject().put("type", "object"))
                    .put("limit", new JsonObject().put("type", "integer").put("default", 100)))
                .put("required", new JsonArray().add("table"))))
        .add(new JsonObject()
            .put("name", "insert")
            .put("description", "Insert record into database")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("table", new JsonObject().put("type", "string"))
                    .put("data", new JsonObject().put("type", "object")))
                .put("required", new JsonArray().add("table").add("data"))))
        .add(new JsonObject()
            .put("name", "update")
            .put("description", "Update database records")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("table", new JsonObject().put("type", "string"))
                    .put("filter", new JsonObject().put("type", "object"))
                    .put("data", new JsonObject().put("type", "object")))
                .put("required", new JsonArray().add("table").add("data"))))
        .add(new JsonObject()
            .put("name", "delete")
            .put("description", "Delete database records")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("table", new JsonObject().put("type", "string"))
                    .put("filter", new JsonObject().put("type", "object")))
                .put("required", new JsonArray().add("table").add("filter"))));
    
    @Override
    public void start(Promise<Void> startPromise) {
        instance = this;
        
        // Create router with static method pattern (following repo style)
        Router router = Router.router(vertx);
        setRouter(router);
        
        // Register event bus consumers for tool execution
        vertx.eventBus().consumer("mcp.database.execute", this::executeToolAsync);
        
        // Register sampling request handler
        vertx.eventBus().consumer("mcp.database.sampling", this::handleSamplingRequest);
        
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
                System.out.println("Database MCP Server started on port " + PORT);
                vertx.eventBus().publish("log", "Database MCP Server started,1,DatabaseServer,StartUp,MCP");
                
                // Notify host that server is ready
                vertx.eventBus().publish("mcp.server.ready", new JsonObject()
                    .put("server", "database")
                    .put("port", PORT)
                    .put("tools", tools.size()));
                    
                startPromise.complete();
            })
            .onFailure(startPromise::fail);
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
        router.post("/").handler(DatabaseServerVerticle::handleMcpRequest);
        router.get("/").handler(DatabaseServerVerticle::handleSseStream);
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
            DatabaseServerVerticle verticle = instance;
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
     * Handle initialize request
     */
    private void handleInitialize(RoutingContext ctx, String id, JsonObject params, String sessionId) {
        // Store session
        sessions.put(sessionId, new JsonObject()
            .put("sessionId", sessionId)
            .put("protocolVersion", params.getString("protocolVersion"))
            .put("capabilities", params.getJsonObject("capabilities")));
        
        // Send initialize response
        JsonObject response = new JsonObject()
            .put("jsonrpc", "2.0")
            .put("id", id)
            .put("result", new JsonObject()
                .put("protocolVersion", MCP_PROTOCOL_VERSION)
                .put("capabilities", new JsonObject()
                    .put("tools", new JsonObject().put("listChanged", true))
                    .put("resources", new JsonObject())
                    .put("prompts", new JsonObject()))
                .put("serverInfo", new JsonObject()
                    .put("name", "Database Server")
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
                                .put("text", "Result: " + res.result()))));
                
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
        String table = arguments.getString("table", "users");
        JsonObject filter = arguments.getJsonObject("filter", new JsonObject());
        int limit = arguments.getInteger("limit", 100);
        
        switch (toolName) {
            case "query":
                // Mock database query
                JsonArray results = generateMockData(table, limit);
                
                // Trigger sampling workflow if results are large
                if (results.size() > 10) {
                    return new JsonObject()
                        .put("requiresSampling", true)
                        .put("resultCount", results.size())
                        .put("results", results)
                        .put("message", "Large result set - sampling recommended")
                        .encode();
                }
                
                return new JsonObject()
                    .put("table", table)
                    .put("resultCount", results.size())
                    .put("results", results)
                    .encode();
                
            case "insert":
                JsonObject data = arguments.getJsonObject("data", new JsonObject());
                return new JsonObject()
                    .put("success", true)
                    .put("table", table)
                    .put("insertedId", UUID.randomUUID().toString())
                    .put("data", data)
                    .encode();
                
            case "update":
                JsonObject updateData = arguments.getJsonObject("data", new JsonObject());
                return new JsonObject()
                    .put("success", true)
                    .put("table", table)
                    .put("updatedCount", (int)(Math.random() * 5) + 1)
                    .put("filter", filter)
                    .put("data", updateData)
                    .encode();
                
            case "delete":
                return new JsonObject()
                    .put("success", true)
                    .put("table", table)
                    .put("deletedCount", (int)(Math.random() * 3) + 1)
                    .put("filter", filter)
                    .encode();
                
            default:
                throw new IllegalArgumentException("Unknown tool: " + toolName);
        }
    }
    
    /**
     * Generate mock data for a table
     */
    private JsonArray generateMockData(String table, int limit) {
        JsonArray results = new JsonArray();
        
        switch (table.toLowerCase()) {
            case "users":
                for (int i = 0; i < Math.min(limit, 20); i++) {
                    results.add(new JsonObject()
                        .put("id", UUID.randomUUID().toString())
                        .put("name", "User " + (i + 1))
                        .put("email", "user" + (i + 1) + "@example.com")
                        .put("created", System.currentTimeMillis()));
                }
                break;
                
            case "products":
                for (int i = 0; i < Math.min(limit, 15); i++) {
                    results.add(new JsonObject()
                        .put("id", UUID.randomUUID().toString())
                        .put("name", "Product " + (i + 1))
                        .put("price", 10 + (Math.random() * 990))
                        .put("stock", (int)(Math.random() * 100)));
                }
                break;
                
            case "orders":
                for (int i = 0; i < Math.min(limit, 50); i++) {
                    results.add(new JsonObject()
                        .put("id", UUID.randomUUID().toString())
                        .put("userId", UUID.randomUUID().toString())
                        .put("total", 20 + (Math.random() * 480))
                        .put("status", i % 3 == 0 ? "completed" : "pending"));
                }
                break;
                
            default:
                // Generic table
                for (int i = 0; i < Math.min(limit, 10); i++) {
                    results.add(new JsonObject()
                        .put("id", i + 1)
                        .put("data", "Record " + (i + 1)));
                }
        }
        
        return results;
    }
    
    /**
     * Handle sampling request from client
     */
    private void handleSamplingRequest(Message<JsonObject> msg) {
        JsonObject request = msg.body();
        
        // Send SSE event requesting LLM summarization
        JsonObject samplingRequest = new JsonObject()
            .put("method", "sampling/create_message")
            .put("params", new JsonObject()
                .put("messages", new JsonArray()
                    .add(new JsonObject()
                        .put("role", "user")
                        .put("content", "Please summarize these database results: " + 
                             request.getJsonArray("results").size() + " records from " +
                             request.getString("table")))))
            .put("id", UUID.randomUUID().toString());
        
        // In real implementation, would send via SSE stream
        // For now, return a mock summary
        vertx.setTimer(100, id -> {
            msg.reply(new JsonObject()
                .put("summary", "Summary: Found " + request.getJsonArray("results").size() + 
                     " records in " + request.getString("table") + 
                     " table. Most records are recent with varied data patterns."));
        });
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
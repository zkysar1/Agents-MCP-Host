package AgentsMCPHost.mcp.servers.examples.weather;

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
 * MCP Server for weather operations.
 * Runs as a worker verticle on port 8082.
 * Implements MCP protocol with streamable HTTP transport.
 */
public class WeatherServer extends AbstractVerticle {
    
    private static WeatherServer instance;
    private static final int PORT = 8082;
    private static final String MCP_PROTOCOL_VERSION = "2024-11-05";
    private HttpServer httpServer;
    
    // Session management
    private final Map<String, JsonObject> sessions = new ConcurrentHashMap<>();
    
    // Available tools
    private final JsonArray tools = new JsonArray()
        .add(new JsonObject()
            .put("name", "weather")
            .put("description", "Get current weather for a location")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("latitude", new JsonObject().put("type", "number"))
                    .put("longitude", new JsonObject().put("type", "number")))
                .put("required", new JsonArray().add("latitude").add("longitude"))))
        .add(new JsonObject()
            .put("name", "forecast")
            .put("description", "Get weather forecast for a location")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("latitude", new JsonObject().put("type", "number"))
                    .put("longitude", new JsonObject().put("type", "number"))
                    .put("days", new JsonObject().put("type", "integer").put("default", 3)))
                .put("required", new JsonArray().add("latitude").add("longitude"))));
    
    @Override
    public void start(Promise<Void> startPromise) {
        instance = this;
        
        // Create router with static method pattern (following repo style)
        Router router = Router.router(vertx);
        setRouter(router);
        
        // Register event bus consumers for tool execution
        vertx.eventBus().consumer("mcp.weather.execute", this::executeToolAsync);
        
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
                System.out.println("Weather MCP Server started on port " + PORT);
                vertx.eventBus().publish("log", "Weather MCP Server started,1,WeatherServer,StartUp,MCP");
                
                // Notify host that server is ready
                vertx.eventBus().publish("mcp.server.ready", new JsonObject()
                    .put("server", "weather")
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
        router.post("/").handler(WeatherServer::handleMcpRequest);
        router.get("/").handler(WeatherServer::handleSseStream);
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
            WeatherServer verticle = instance;
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
                    .put("name", "Weather Server")
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
        double lat = arguments.getDouble("latitude", 37.7749);
        double lon = arguments.getDouble("longitude", -122.4194);
        
        switch (toolName) {
            case "weather":
                // Mock weather data (in real implementation, would call weather API)
                JsonObject weather = new JsonObject()
                    .put("location", new JsonObject()
                        .put("latitude", lat)
                        .put("longitude", lon)
                        .put("city", "San Francisco"))
                    .put("temperature", 68 + (int)(Math.random() * 15))
                    .put("feelsLike", 65 + (int)(Math.random() * 15))
                    .put("humidity", 60 + (int)(Math.random() * 30))
                    .put("conditions", "Partly Cloudy")
                    .put("windSpeed", 5 + (int)(Math.random() * 20))
                    .put("windDirection", "NW");
                return weather.encode();
                
            case "forecast":
                int days = arguments.getInteger("days", 3);
                JsonArray forecast = new JsonArray();
                for (int i = 0; i < days; i++) {
                    forecast.add(new JsonObject()
                        .put("day", i + 1)
                        .put("high", 70 + (int)(Math.random() * 20))
                        .put("low", 50 + (int)(Math.random() * 15))
                        .put("conditions", i % 2 == 0 ? "Sunny" : "Partly Cloudy")
                        .put("precipitation", (int)(Math.random() * 30) + "%"));
                }
                return new JsonObject()
                    .put("location", new JsonObject()
                        .put("latitude", lat)
                        .put("longitude", lon))
                    .put("forecast", forecast)
                    .encode();
                
            default:
                throw new IllegalArgumentException("Unknown tool: " + toolName);
        }
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
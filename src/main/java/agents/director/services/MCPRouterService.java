package agents.director.services;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.ErrorHandler;
import io.vertx.core.http.HttpMethod;


import java.util.Arrays;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import static agents.director.Driver.logLevel;

/**
 * Master router service for the MCP architecture.
 * Runs HTTP server on port 8080 and manages dynamic sub-router registration.
 */
public class MCPRouterService extends AbstractVerticle {
    
    
    private static final int HTTP_PORT = 8080;
    
    private Router mainRouter;
    private HttpServer httpServer;
    private static final Map<String, Router> pendingRouters = new ConcurrentHashMap<>();
    private static MCPRouterService instance;
    
    @Override
    public void start(Promise<Void> startPromise) {
        instance = this;
        
        // Create the main router
        mainRouter = Router.router(vertx);
        
        // Add global handlers
        addGlobalHandlers();
        
        // Add health check endpoint
        mainRouter.get("/health").handler(ctx -> {
            ctx.response()
                .putHeader("content-type", "application/json")
                .end(new JsonObject()
                    .put("status", "healthy")
                    .put("timestamp", System.currentTimeMillis())
                    .toString());
        });
        
        // Mount any routers that were registered before this service started
        mountRegisteredRouters();
        
        // Register event bus consumer for remount requests
        vertx.eventBus().consumer("mcp.router.remount", message -> {
            mountRegisteredRouters();
            message.reply(new JsonObject().put("status", "remounted"));
        });
        
        // Register event bus consumer for sub-router registration
        vertx.eventBus().<JsonObject>consumer("mcp.router.register", message -> {
            JsonObject request = message.body();
            String path = request.getString("path");
            String routerId = request.getString("routerId");
            
            if (path == null || routerId == null) {
                message.fail(400, "Missing path or routerId");
                return;
            }
            
            // The MCP server will publish its router on the event bus
            // We'll retrieve it and mount it
            vertx.eventBus().<Router>request(routerId + ".router", null)
                .onComplete(reply -> {
                    if (reply.succeeded()) {
                        Router subRouter = reply.result().body();
                        
                        // Mount the sub-router at the specified path
                        mainRouter.route(path + "/*").subRouter(subRouter);
                        
                        vertx.eventBus().publish("log", "Mounted sub-router at path: " + path + "" + ",2,MCPRouterService,Service,System");
                        message.reply(new JsonObject().put("status", "success"));
                    } else {
                        vertx.eventBus().publish("log", "Failed to get router for " + routerId + ": " + reply.cause() + "" + ",0,MCPRouterService,Service,System");
                        message.fail(500, "Failed to retrieve router");
                    }
                });
        });
        
        // Create HTTP server options
        HttpServerOptions options = new HttpServerOptions()
            .setPort(HTTP_PORT)
            .setCompressionSupported(true)
            .setHandle100ContinueAutomatically(true);
        
        // Create and start HTTP server
        httpServer = vertx.createHttpServer(options);
        
        httpServer
            .requestHandler(mainRouter)
            .listen(result -> {
                if (result.succeeded()) {
                    vertx.eventBus().publish("log", "MCPRouterService started on port " + HTTP_PORT + "" + ",2,MCPRouterService,Service,System");
                    
                    // Publish router ready event
                    vertx.eventBus().publish("mcp.router.ready", new JsonObject()
                        .put("port", HTTP_PORT)
                        .put("address", "localhost")
                        .put("timestamp", System.currentTimeMillis()));
                    
                    startPromise.complete();
                } else {
                    vertx.eventBus().publish("log", "Failed to start MCPRouterService" + ",0,MCPRouterService,Service,System");
                    startPromise.fail(result.cause());
                }
            });
    }
    
    private void addGlobalHandlers() {
        // CORS handler
        Set<String> allowedHeaders = new HashSet<>();
        allowedHeaders.add("content-type");
        allowedHeaders.add("authorization");
        
        Set<HttpMethod> allowedMethods = new HashSet<>();
        allowedMethods.add(HttpMethod.GET);
        allowedMethods.add(HttpMethod.POST);
        allowedMethods.add(HttpMethod.OPTIONS);
        
        mainRouter.route().handler(CorsHandler.create()
            .addOrigins(Arrays.asList("*"))
            .allowedHeaders(allowedHeaders)
            .allowedMethods(allowedMethods));
        
        // Body handler for JSON parsing
        mainRouter.route().handler(BodyHandler.create()
            .setBodyLimit(10 * 1024 * 1024)); // 10MB limit
        
        // Set default content-type for MCP paths
        mainRouter.route("/mcp/*").handler(ctx -> {
            ctx.response().putHeader("content-type", "application/json");
            ctx.next();
        });
        
        // Error handler
        mainRouter.route().failureHandler(ErrorHandler.create(vertx));
        
        // Global exception handler for MCP endpoints
        mainRouter.route("/mcp/*").failureHandler(ctx -> {
            Throwable failure = ctx.failure();
            int statusCode = ctx.statusCode();
            
            if (statusCode == -1) {
                statusCode = 500;
            }
            
            JsonObject error = new JsonObject()
                .put("jsonrpc", "2.0")
                .put("error", new JsonObject()
                    .put("code", statusCode)
                    .put("message", failure != null ? failure.getMessage() : "Unknown error"));
            
            ctx.response()
                .setStatusCode(statusCode)
                .putHeader("content-type", "application/json")
                .end(error.encode());
        });
    }
    
    /**
     * Static method to register a router directly
     * This is called by other verticles to register their routers
     */
    public static void registerRouter(String path, Router subRouter) {
        if (instance != null && instance.mainRouter != null) {
            // If the main router exists, mount immediately
            instance.mountRouter(path, subRouter);
        } else {
            // Otherwise, store for later mounting
            pendingRouters.put(path, subRouter);
            // Convert to log level 2 (detail) - remove print
            if (instance != null && instance.vertx != null && logLevel >= 2) {
                instance.vertx.eventBus().publish("log", "Router registered for path: " + path + " (pending mount),2,MCPRouterService,HTTP,Router");
            }
        }
    }
    
    /**
     * Mount a single router
     */
    private void mountRouter(String path, Router subRouter) {
        mainRouter.route(path + "/*").subRouter(subRouter);
        vertx.eventBus().publish("log", "Mounted router at path: " + path + "" + ",2,MCPRouterService,Service,System");
    }
    
    /**
     * Mount all pending routers to the main router
     * Called after the main router is created
     */
    private void mountRegisteredRouters() {
        for (Map.Entry<String, Router> entry : pendingRouters.entrySet()) {
            String path = entry.getKey();
            Router subRouter = entry.getValue();
            mountRouter(path, subRouter);
        }
        
        // Clear the map after mounting
        pendingRouters.clear();
    }
    
    @Override
    public void stop(Promise<Void> stopPromise) {
        instance = null;
        if (httpServer != null) {
            httpServer.close(result -> {
                if (result.succeeded()) {
                    vertx.eventBus().publish("log", "MCPRouterService stopped,2,MCPRouterService,Service,System");
                    stopPromise.complete();
                } else {
                    vertx.eventBus().publish("log", "Failed to stop MCPRouterService" + ",0,MCPRouterService,Service,System");
                    stopPromise.fail(result.cause());
                }
            });
        } else {
            stopPromise.complete();
        }
    }
}
package AgentsMCPHost.api;

import AgentsMCPHost.Driver;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import static AgentsMCPHost.Driver.logLevel;

/**
 * Main API verticle for host endpoints.
 * Provides HTTP server with CORS support.
 */
public class HostAPI extends AbstractVerticle {
  private static final int API_PORT = 8080;
  private HttpServer server;
  private Router mainRouter;
  private boolean systemReady = false;
  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    System.out.println("HostAPIVerticle starting (waiting for system ready)...");
    vertx.eventBus().publish("log", 
      "HostAPIVerticle initialized - waiting for system ready,1,HostAPIVerticle,StartUp,System");
    
    // Set up routes but don't start HTTP server yet
    setupRoutes();
    
    // Listen for system ready event
    vertx.eventBus().consumer("system.fully.ready", msg -> {
      if (!systemReady) {
        systemReady = true;
        System.out.println("[HostAPI] System ready signal received, starting HTTP server...");
        startHttpServer(startPromise);
      }
    });
    
    // Complete the deployment immediately (server will start later)
    startPromise.complete();
  }
  
  private void setupRoutes() {
    // Create the main router
    mainRouter = Router.router(vertx);
    
    // Configure CORS - must be first
    mainRouter.route().handler(CorsHandler.create()
      .allowedMethod(HttpMethod.OPTIONS)
      .allowedMethod(HttpMethod.GET)
      .allowedMethod(HttpMethod.POST)
      .allowedMethod(HttpMethod.PUT)
      .allowedHeader("Content-Type")
      .allowedHeader("X-API-KEY")
      .allowedHeader("origin")
      .addOrigin("*")
      .allowedHeader("Access-Control-Allow-Origin"));
    
    // Body handler for POST/PUT requests
    mainRouter.route().handler(BodyHandler.create());
    
    // Add root route
    mainRouter.route("/").handler(ctx -> {
      ctx.response()
        .putHeader("Content-Type", "text/html")
        .end("<h1>Welcome to ZAK-Agent Host API</h1><p>Health endpoint available at /health</p>");
    });
    
    // Mount individual endpoint verticles
    Health.setRouter(mainRouter);
    Status.setRouter(mainRouter);
    Conversation.setRouter(mainRouter);
    Test.setRouter(mainRouter);
    
    // Mount MCP status endpoints from McpHostManager
    AgentsMCPHost.mcp.core.McpHostManager.setRouter(mainRouter);
    
    // Add catch-all debug route
    mainRouter.route().handler(ctx -> {
      if (logLevel >= 3) {
        System.out.println("[DEBUG] No route matched for: " + ctx.request().path());
      }
      JsonObject response = new JsonObject()
        .put("error", "Resource not found")
        .put("path", ctx.request().path())
        .put("method", ctx.request().method().toString());
      ctx.response()
        .setStatusCode(404)
        .putHeader("Content-Type", "application/json")
        .end(response.encode());
    });
    
  }
  
  private void startHttpServer(Promise<Void> originalStartPromise) {
    // Configure HTTP server options
    HttpServerOptions options = new HttpServerOptions().setPort(API_PORT);
    
    System.out.println("Creating HTTP server on port " + API_PORT + "...");
    server = vertx.createHttpServer(options)
      .requestHandler(mainRouter)
      .listen(ar -> {
        if (ar.succeeded()) {
          System.out.println("*** Host API server successfully started on HTTP port " + API_PORT + " ***");
          System.out.println();
          System.out.println("=== Available API Endpoints ===");
          System.out.println();
          System.out.println("1. Health Check:");
          System.out.println("   GET http://localhost:" + API_PORT + "/health");
          System.out.println();
          System.out.println("2. Host Status:");
          System.out.println("   GET http://localhost:" + API_PORT + "/host/v1/status");
          System.out.println();
          System.out.println("3. Conversation API (OpenAI-compatible with auto MCP tool detection):");
          System.out.println("   POST http://localhost:" + API_PORT + "/host/v1/conversations");
          System.out.println("   ");
          System.out.println("   Examples:");
          System.out.println("   ");
          System.out.println("   # Simple message (uses LLM if API key is set):");
          System.out.println("     curl -X POST http://localhost:" + API_PORT + "/host/v1/conversations \\");
          System.out.println("       -H \"Content-Type: application/json\" \\");
          System.out.println("       -d '{\"messages\":[{\"role\":\"user\",\"content\":\"Hello\"}]}'");
          System.out.println("   ");
          System.out.println("   # Tool-triggering message (auto-detects and uses MCP tools):");
          System.out.println("     curl -X POST http://localhost:" + API_PORT + "/host/v1/conversations \\");
          System.out.println("       -H \"Content-Type: application/json\" \\");
          System.out.println("       -d '{\"messages\":[{\"role\":\"user\",\"content\":\"Calculate 10 plus 20\"}]}'");
          System.out.println();
          System.out.println("   Note: Messages with keywords like 'calculate', 'weather', 'database', or 'file'");
          System.out.println("         will automatically trigger MCP tools.");
          System.out.println();
          System.out.println("================================");
          
          vertx.eventBus().publish("log", 
            "Host API server started on HTTP port " + API_PORT + 
            " - Access at http://localhost:" + API_PORT + 
            ",0,HostAPIVerticle,StartUp,System");
          
          // Notify that HTTP server is now accepting requests
          vertx.eventBus().publish("http.server.ready", new JsonObject()
            .put("port", API_PORT)
            .put("timestamp", System.currentTimeMillis()));
        } else {
          System.err.println("!!! Host API server FAILED to start !!!");
          System.err.println("Error: " + ar.cause().getMessage());
          
          vertx.eventBus().publish("log", 
            "Host API server failed to start: " + ar.cause().getMessage() + 
            ",0,HostAPIVerticle,StartUp,System");
          
          // Note: We don't fail the original promise since verticle is already deployed
          vertx.eventBus().publish("http.server.failed", new JsonObject()
            .put("error", ar.cause().getMessage()));
        }
      });
  }
  
  @Override
  public void stop() throws Exception {
    if (server != null) {
      server.close();
      vertx.eventBus().publish("log", 
        "Host API server stopped,1,HostAPIVerticle,Shutdown,System");
    }
  }
}
package agents.director.apis;

import agents.director.services.OracleConnectionManager;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.core.Vertx;

/**
 * Verticle handling host status endpoint.
 * Provides information about the host configuration and status.
 */
public class Status extends AbstractVerticle {
  private static long startTime = System.currentTimeMillis();
  private static Vertx vertxInstance;
  
  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    vertxInstance = vertx;
    // Simply mark as complete - router setup happens via static method
    startPromise.complete();
  }
  
  /**
   * Configure the router with status endpoint
   * @param parentRouter The parent router to attach to
   */
  public static void setRouter(Router parentRouter) {
    // Host API status endpoint
    parentRouter.get("/host/v1/status").handler(ctx -> {
      JsonObject status = new JsonObject()
        .put("status", "running")
        .put("version", "1.0.0")
        .put("timestamp", System.currentTimeMillis())
        .put("uptime", System.currentTimeMillis() - startTime)
        .put("environment", System.getProperty("os.name", "Unknown"));
      
      // Add database connection status
      try {
        OracleConnectionManager oracleManager = OracleConnectionManager.getInstance();
        JsonObject dbStatus = oracleManager.getConnectionStatus();
        status.put("database", dbStatus);
      } catch (Exception e) {
        // Database might not be initialized
        status.put("database", new JsonObject()
          .put("healthy", false)
          .put("error", "Not initialized or error: " + e.getMessage()));
      }
      
      // Request MCP status from HostManager via event bus if vertx is available
      if (vertxInstance != null) {
        vertxInstance.eventBus().<JsonObject>request("mcp.host.status", new JsonObject())
          .onSuccess(reply -> {
            status.put("mcp", reply.body());
            ctx.response()
              .putHeader("content-type", "application/json")
              .setStatusCode(200)
              .end(status.encode());
          })
          .onFailure(err -> {
            // Return status without MCP info if request fails
            status.put("mcp", new JsonObject().put("error", "Could not retrieve MCP status"));
            ctx.response()
              .putHeader("content-type", "application/json")
              .setStatusCode(200)
              .end(status.encode());
          });
      } else {
        // Return without MCP status if vertx not available
        ctx.response()
          .putHeader("content-type", "application/json")
          .setStatusCode(200)
          .end(status.encode());
      }
    });
  }
}
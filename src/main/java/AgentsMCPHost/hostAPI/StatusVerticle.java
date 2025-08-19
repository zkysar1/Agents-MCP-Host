package AgentsMCPHost.hostAPI;

import AgentsMCPHost.Driver;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;

/**
 * Verticle handling host status endpoint.
 * Provides information about the host configuration and status.
 */
public class StatusVerticle extends AbstractVerticle {
  private static long startTime = System.currentTimeMillis();
  
  @Override
  public void start(Promise<Void> startPromise) throws Exception {
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
        .put("instanceId", Driver.instanceId)
        .put("awsRegion", Driver.awsRegion);
      
      ctx.response()
        .putHeader("content-type", "application/json")
        .setStatusCode(200)
        .end(status.encode());
    });
  }
}
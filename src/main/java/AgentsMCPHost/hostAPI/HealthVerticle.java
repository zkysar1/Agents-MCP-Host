package AgentsMCPHost.hostAPI;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;

/**
 * Verticle handling health check endpoint.
 * Provides system health information including memory usage, uptime, etc.
 */
public class HealthVerticle extends AbstractVerticle {
  private static long startTime = System.currentTimeMillis();
  
  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    // Simply mark as complete - router setup happens via static method
    startPromise.complete();
  }
  
  /**
   * Configure the router with health endpoint
   * @param parentRouter The parent router to attach to
   */
  public static void setRouter(Router parentRouter) {
    // Public health endpoint - no authentication required
    parentRouter.get("/health").handler(ctx -> {
      // Simple health check with system info
      Runtime runtime = Runtime.getRuntime();
      long maxMemory = runtime.maxMemory();
      long totalMemory = runtime.totalMemory();
      long freeMemory = runtime.freeMemory();
      long usedMemory = totalMemory - freeMemory;
      
      JsonObject health = new JsonObject()
        .put("status", "healthy")
        .put("timestamp", System.currentTimeMillis())
        .put("version", "1.0.0")
        .put("service", "ZAK-Agent Host API")
        .put("uptime", System.currentTimeMillis() - startTime)
        .put("memory", new JsonObject()
          .put("usedMB", usedMemory / 1048576)
          .put("freeMB", freeMemory / 1048576)
          .put("totalMB", totalMemory / 1048576)
          .put("maxMB", maxMemory / 1048576)
          .put("usagePercent", Math.round(usedMemory * 10000.0 / maxMemory) / 100.0))
        .put("threads", Thread.activeCount())
        .put("processors", runtime.availableProcessors());
      
      ctx.response()
        .putHeader("content-type", "application/json")
        .setStatusCode(200)
        .end(health.encode());
    });
  }
}
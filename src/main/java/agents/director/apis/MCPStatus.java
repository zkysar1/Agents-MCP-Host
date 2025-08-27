package agents.director.apis;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.web.Router;

/**
 * Verticle handling MCP (Model Context Protocol) status endpoints.
 * Provides information about MCP infrastructure including servers, clients, and tools.
 */
public class MCPStatus extends AbstractVerticle {
  private static Vertx vertxInstance;
  
  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    vertxInstance = vertx;
    // Simply mark as complete - router setup happens via static method
    startPromise.complete();
  }
  
  /**
   * Configure the router with MCP status endpoints
   * @param parentRouter The parent router to attach to
   */
  public static void setRouter(Router parentRouter) {
    // MCP system status endpoint
    parentRouter.get("/host/v1/mcp/status").handler(ctx -> {
      if (vertxInstance != null) {
        vertxInstance.eventBus().<JsonObject>request("mcp.status", new JsonObject())
          .onSuccess(reply -> {
            JsonObject status = reply.body();
            ctx.response()
              .putHeader("content-type", "application/json")
              .setStatusCode(200)
              .end(status.encode());
          })
          .onFailure(err -> {
            JsonObject errorResponse = new JsonObject()
              .put("error", "MCP system not available")
              .put("message", err.getMessage())
              .put("timestamp", System.currentTimeMillis());
            
            ctx.response()
              .putHeader("content-type", "application/json")
              .setStatusCode(503)
              .end(errorResponse.encode());
          });
      } else {
        ctx.response()
          .putHeader("content-type", "application/json")
          .setStatusCode(503)
          .end(new JsonObject()
            .put("error", "Service not initialized")
            .encode());
      }
    });
    
    // MCP tools listing endpoint
    parentRouter.get("/host/v1/mcp/tools").handler(ctx -> {
      if (vertxInstance != null) {
        vertxInstance.eventBus().<JsonObject>request("mcp.tools.list", new JsonObject())
          .onSuccess(reply -> {
            JsonObject tools = reply.body();
            ctx.response()
              .putHeader("content-type", "application/json")
              .setStatusCode(200)
              .end(tools.encode());
          })
          .onFailure(err -> {
            JsonObject errorResponse = new JsonObject()
              .put("error", "Unable to retrieve MCP tools")
              .put("message", err.getMessage())
              .put("timestamp", System.currentTimeMillis());
            
            ctx.response()
              .putHeader("content-type", "application/json")
              .setStatusCode(503)
              .end(errorResponse.encode());
          });
      } else {
        ctx.response()
          .putHeader("content-type", "application/json")
          .setStatusCode(503)
          .end(new JsonObject()
            .put("error", "Service not initialized")
            .encode());
      }
    });
    
    // MCP clients listing endpoint
    parentRouter.get("/host/v1/mcp/clients").handler(ctx -> {
      if (vertxInstance != null) {
        vertxInstance.eventBus().<JsonObject>request("mcp.clients.list", new JsonObject())
          .onSuccess(reply -> {
            JsonObject clients = reply.body();
            ctx.response()
              .putHeader("content-type", "application/json")
              .setStatusCode(200)
              .end(clients.encode());
          })
          .onFailure(err -> {
            JsonObject errorResponse = new JsonObject()
              .put("error", "Unable to retrieve MCP clients")
              .put("message", err.getMessage())
              .put("timestamp", System.currentTimeMillis());
            
            ctx.response()
              .putHeader("content-type", "application/json")
              .setStatusCode(503)
              .end(errorResponse.encode());
          });
      } else {
        ctx.response()
          .putHeader("content-type", "application/json")
          .setStatusCode(503)
          .end(new JsonObject()
            .put("error", "Service not initialized")
            .encode());
      }
    });
    
    // MCP servers listing endpoint
    parentRouter.get("/host/v1/mcp/servers").handler(ctx -> {
      if (vertxInstance != null) {
        vertxInstance.eventBus().<JsonObject>request("mcp.servers.list", new JsonObject())
          .onSuccess(reply -> {
            JsonObject servers = reply.body();
            ctx.response()
              .putHeader("content-type", "application/json")
              .setStatusCode(200)
              .end(servers.encode());
          })
          .onFailure(err -> {
            // Try alternative event bus address
            vertxInstance.eventBus().<JsonObject>request("mcp.host.status", new JsonObject())
              .onSuccess(statusReply -> {
                // Extract server info from status if available
                JsonObject status = statusReply.body();
                JsonObject serversInfo = new JsonObject()
                  .put("servers", status.getJsonArray("servers", new JsonArray()))
                  .put("timestamp", System.currentTimeMillis());
                
                ctx.response()
                  .putHeader("content-type", "application/json")
                  .setStatusCode(200)
                  .end(serversInfo.encode());
              })
              .onFailure(statusErr -> {
                JsonObject errorResponse = new JsonObject()
                  .put("error", "Unable to retrieve MCP servers")
                  .put("message", statusErr.getMessage())
                  .put("timestamp", System.currentTimeMillis());
                
                ctx.response()
                  .putHeader("content-type", "application/json")
                  .setStatusCode(503)
                  .end(errorResponse.encode());
              });
          });
      } else {
        ctx.response()
          .putHeader("content-type", "application/json")
          .setStatusCode(503)
          .end(new JsonObject()
            .put("error", "Service not initialized")
            .encode());
      }
    });
  }
}
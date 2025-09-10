package agents.director.mcp.base;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

import agents.director.services.MCPRouterService;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.List;
import java.util.ArrayList;

/**
 * Base class for all MCP servers.
 * Provides common MCP protocol implementation for tools/list and tools/call.
 */
public abstract class MCPServerBase extends AbstractVerticle {
    
    
    
    protected Router router;
    protected final Map<String, MCPTool> tools = new HashMap<>();
    protected final String serverName;
    protected final String serverPath;
    
    protected MCPServerBase(String serverName, String serverPath) {
        this.serverName = serverName;
        this.serverPath = serverPath;
    }
    
    @Override
    public void start(Promise<Void> startPromise) {
        // Create router
        router = Router.router(vertx);
        
        // Register MCP endpoints
        router.post("/tools/list").handler(this::handleToolsList);
        router.post("/tools/call").handler(this::handleToolCall);
        
        // Initialize server-specific tools
        initializeTools();
        
        // Register router with MCPRouterService using static method
        MCPRouterService.registerRouter(serverPath, router);
        vertx.eventBus().publish("log", "" + serverName + " registered router at path: " + serverPath + "" + ",2,MCPServerBase,MCP,System");
        
        // Notify that the server is ready
        onServerReady();
        startPromise.complete();
    }
    
    /**
     * Initialize the tools provided by this server.
     * Subclasses must implement this to register their tools.
     */
    protected abstract void initializeTools();
    
    /**
     * Called when the server is successfully registered and ready.
     * Subclasses can override for additional initialization.
     */
    protected void onServerReady() {
        // Default: no additional action
    }
    
    /**
     * Register a tool with this server
     */
    protected void registerTool(MCPTool tool) {
        tools.put(tool.getName(), tool);
        vertx.eventBus().publish("log", "" + serverName + " registered tool: " + tool.getName() + "" + ",3,MCPServerBase,MCP,System");
    }
    
    /**
     * Handle the tools/list request
     */
    private void handleToolsList(RoutingContext ctx) {
        try {
            MCPRequest request = MCPRequest.fromJson(ctx.body().asJsonObject());
            
            if (!request.isValid()) {
                sendError(ctx, request.getId(), MCPResponse.ErrorCodes.INVALID_REQUEST, "Invalid request format");
                return;
            }
            
            // Build tools array
            JsonArray toolsArray = new JsonArray();
            for (MCPTool tool : tools.values()) {
                toolsArray.add(tool.toJson());
            }
            
            JsonObject result = new JsonObject()
                .put("tools", toolsArray);
            
            sendSuccess(ctx, request.getId(), result);
            
        } catch (Exception e) {
            vertx.eventBus().publish("log", "Error handling tools/list" + ",0,MCPServerBase,MCP,System");
            sendError(ctx, null, MCPResponse.ErrorCodes.INTERNAL_ERROR, "Internal server error");
        }
    }
    
    /**
     * Handle the tools/call request
     */
    private void handleToolCall(RoutingContext ctx) {
        try {
            MCPRequest request = MCPRequest.fromJson(ctx.body().asJsonObject());
            
            if (!request.isValid()) {
                sendError(ctx, request.getId(), MCPResponse.ErrorCodes.INVALID_REQUEST, "Invalid request format");
                return;
            }
            
            JsonObject params = request.getParams();
            String toolName = params.getString("name");
            JsonObject arguments = params.getJsonObject("arguments", new JsonObject());
            
            if (toolName == null) {
                sendError(ctx, request.getId(), MCPResponse.ErrorCodes.INVALID_PARAMS, "Missing tool name");
                return;
            }
            
            MCPTool tool = tools.get(toolName);
            if (tool == null) {
                sendError(ctx, request.getId(), MCPResponse.ErrorCodes.METHOD_NOT_FOUND, 
                    "Tool not found: " + toolName);
                return;
            }
            
            // Execute the tool
            executeTool(ctx, request.getId(), toolName, arguments);
            
        } catch (Exception e) {
            vertx.eventBus().publish("log", "Error handling tools/call: " + e.getMessage() + 
                " - Stack: " + e.getClass().getName() + ",0,MCPServerBase,MCP,System");
            e.printStackTrace(); // Log full stack trace to console
            sendError(ctx, null, MCPResponse.ErrorCodes.INTERNAL_ERROR, "Internal server error: " + e.getMessage());
        }
    }
    
    /**
     * Execute a specific tool. Subclasses must implement this.
     */
    protected abstract void executeTool(RoutingContext ctx, String requestId, 
                                      String toolName, JsonObject arguments);
    
    /**
     * Send a success response
     */
    protected void sendSuccess(RoutingContext ctx, String requestId, JsonObject result) {
        MCPResponse response = MCPResponse.success(
            requestId != null ? requestId : UUID.randomUUID().toString(), 
            result
        );
        
        ctx.response()
            .putHeader("content-type", "application/json")
            .end(response.toJson().encode());
    }
    
    /**
     * Send an error response
     */
    protected void sendError(RoutingContext ctx, String requestId, int code, String message) {
        MCPResponse response = MCPResponse.error(
            requestId != null ? requestId : UUID.randomUUID().toString(),
            code,
            message
        );
        
        ctx.response()
            .putHeader("content-type", "application/json")
            .setStatusCode(400)
            .end(response.toJson().encode());
    }
    
    /**
     * Send an error response with additional data
     */
    protected void sendError(RoutingContext ctx, String requestId, int code, 
                           String message, JsonObject data) {
        MCPResponse response = MCPResponse.error(
            requestId != null ? requestId : UUID.randomUUID().toString(),
            code,
            message,
            data
        );
        
        ctx.response()
            .putHeader("content-type", "application/json")
            .setStatusCode(400)
            .end(response.toJson().encode());
    }
    
    /**
     * Execute blocking code (for DB or LLM operations)
     */
    protected <T> void executeBlocking(Promise<T> promise, 
                                     io.vertx.core.Handler<Promise<T>> blockingHandler,
                                     io.vertx.core.Handler<io.vertx.core.AsyncResult<T>> resultHandler) {
        vertx.executeBlocking(blockingHandler, false, resultHandler);
    }
    
    /**
     * Get tool definition by name
     */
    protected MCPTool getTool(String toolName) {
        return tools.get(toolName);
    }
    
    /**
     * Get all registered tools
     */
    protected List<MCPTool> getAllTools() {
        return new ArrayList<>(tools.values());
    }
}
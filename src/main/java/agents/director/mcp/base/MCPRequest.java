package agents.director.mcp.base;

import io.vertx.core.json.JsonObject;

/**
 * Represents a JSON-RPC request in MCP protocol format.
 */
public class MCPRequest {
    
    private final String jsonrpc = "2.0";
    private final String id;
    private final String method;
    private final JsonObject params;
    
    public MCPRequest(String id, String method, JsonObject params) {
        this.id = id;
        this.method = method;
        this.params = params;
    }
    
    public String getJsonrpc() {
        return jsonrpc;
    }
    
    public String getId() {
        return id;
    }
    
    public String getMethod() {
        return method;
    }
    
    public JsonObject getParams() {
        return params;
    }
    
    /**
     * Convert to JSON for transmission
     */
    public JsonObject toJson() {
        JsonObject json = new JsonObject()
            .put("jsonrpc", jsonrpc)
            .put("id", id)
            .put("method", method);
            
        if (params != null && !params.isEmpty()) {
            json.put("params", params);
        }
        
        return json;
    }
    
    /**
     * Create from incoming JSON
     */
    public static MCPRequest fromJson(JsonObject json) {
        return new MCPRequest(
            json.getString("id"),
            json.getString("method"),
            json.getJsonObject("params", new JsonObject())
        );
    }
    
    /**
     * Validate the request format
     */
    public boolean isValid() {
        return id != null && !id.isEmpty() && 
               method != null && !method.isEmpty() &&
               "2.0".equals(jsonrpc);
    }
    
    @Override
    public String toString() {
        return "MCPRequest{id='" + id + "', method='" + method + "', params=" + params + "}";
    }
}
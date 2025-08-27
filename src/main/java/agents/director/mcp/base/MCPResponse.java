package agents.director.mcp.base;

import io.vertx.core.json.JsonObject;

/**
 * Represents a JSON-RPC response in MCP protocol format.
 * Can be either a success response with result or an error response.
 */
public class MCPResponse {
    
    private final String jsonrpc = "2.0";
    private final String id;
    private final JsonObject result;
    private final JsonObject error;
    
    private MCPResponse(String id, JsonObject result, JsonObject error) {
        this.id = id;
        this.result = result;
        this.error = error;
    }
    
    /**
     * Create a success response
     */
    public static MCPResponse success(String id, JsonObject result) {
        return new MCPResponse(id, result, null);
    }
    
    /**
     * Create an error response
     */
    public static MCPResponse error(String id, int code, String message) {
        JsonObject error = new JsonObject()
            .put("code", code)
            .put("message", message);
        return new MCPResponse(id, null, error);
    }
    
    /**
     * Create an error response with additional data
     */
    public static MCPResponse error(String id, int code, String message, JsonObject data) {
        JsonObject error = new JsonObject()
            .put("code", code)
            .put("message", message);
        if (data != null) {
            error.put("data", data);
        }
        return new MCPResponse(id, null, error);
    }
    
    public String getJsonrpc() {
        return jsonrpc;
    }
    
    public String getId() {
        return id;
    }
    
    public JsonObject getResult() {
        return result;
    }
    
    public JsonObject getError() {
        return error;
    }
    
    public boolean isSuccess() {
        return result != null && error == null;
    }
    
    public boolean isError() {
        return error != null && result == null;
    }
    
    /**
     * Convert to JSON for transmission
     */
    public JsonObject toJson() {
        JsonObject json = new JsonObject()
            .put("jsonrpc", jsonrpc)
            .put("id", id);
            
        if (isSuccess()) {
            json.put("result", result);
        } else if (isError()) {
            json.put("error", error);
        }
        
        return json;
    }
    
    /**
     * Create from incoming JSON
     */
    public static MCPResponse fromJson(JsonObject json) {
        String id = json.getString("id");
        JsonObject result = json.getJsonObject("result");
        JsonObject error = json.getJsonObject("error");
        
        return new MCPResponse(id, result, error);
    }
    
    @Override
    public String toString() {
        if (isSuccess()) {
            return "MCPResponse{id='" + id + "', result=" + result + "}";
        } else {
            return "MCPResponse{id='" + id + "', error=" + error + "}";
        }
    }
    
    // Standard MCP error codes
    public static class ErrorCodes {
        public static final int PARSE_ERROR = -32700;
        public static final int INVALID_REQUEST = -32600;
        public static final int METHOD_NOT_FOUND = -32601;
        public static final int INVALID_PARAMS = -32602;
        public static final int INTERNAL_ERROR = -32603;
    }
}
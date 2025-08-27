package agents.director.mcp.base;

import io.vertx.core.json.JsonObject;

/**
 * Represents an MCP tool definition with name, description, and input schema.
 */
public class MCPTool {
    
    private final String name;
    private final String description;
    private final JsonObject inputSchema;
    
    public MCPTool(String name, String description, JsonObject inputSchema) {
        this.name = name;
        this.description = description;
        this.inputSchema = inputSchema;
    }
    
    public String getName() {
        return name;
    }
    
    public String getDescription() {
        return description;
    }
    
    public JsonObject getInputSchema() {
        return inputSchema;
    }
    
    /**
     * Convert to JSON format for MCP protocol
     */
    public JsonObject toJson() {
        return new JsonObject()
            .put("name", name)
            .put("description", description)
            .put("inputSchema", inputSchema);
    }
    
    /**
     * Create from JSON
     */
    public static MCPTool fromJson(JsonObject json) {
        return new MCPTool(
            json.getString("name"),
            json.getString("description"),
            json.getJsonObject("inputSchema")
        );
    }
    
    @Override
    public String toString() {
        return "MCPTool{name='" + name + "', description='" + description + "'}";
    }
}
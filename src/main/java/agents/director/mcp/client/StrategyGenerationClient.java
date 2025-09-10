package agents.director.mcp.client;

import agents.director.mcp.base.MCPClientBase;
import io.vertx.core.Vertx;

/**
 * MCP Client for Strategy Generation Server.
 * Provides tools for generating and optimizing execution strategies.
 * 
 * Available tools:
 * - strategy_generation__analyze_complexity: Analyze query complexity
 * - strategy_generation__create_strategy: Create execution strategies
 * - strategy_generation__optimize_strategy: Optimize existing strategies
 * 
 * This client maintains a 1:1 relationship with the Strategy Generation Server.
 */
public class StrategyGenerationClient extends MCPClientBase {
    
    private static final String SERVER_PATH = "/mcp/servers/strategy-gen";
    
    /**
     * Create a new Strategy Generation client
     * @param baseUrl The base URL (e.g., http://localhost:8080)
     */
    public StrategyGenerationClient(String baseUrl) {
        super("StrategyGeneration", baseUrl + SERVER_PATH);
    }
    
    @Override
    protected void onClientReady() {
        // Log available tools for verification
        if (tools.size() > 0) {
            vertx.eventBus().publish("log", 
                "StrategyGenerationClient ready with tools: " + tools.keySet() + 
                ",2,StrategyGenerationClient,MCP,System");
        }
    }
}
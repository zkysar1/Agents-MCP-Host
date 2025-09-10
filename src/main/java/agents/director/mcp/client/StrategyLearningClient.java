package agents.director.mcp.client;

import agents.director.mcp.base.MCPClientBase;
import io.vertx.core.Vertx;

/**
 * MCP Client for Strategy Learning Server.
 * Provides tools for learning from execution patterns and improving strategies.
 * 
 * Available tools:
 * - strategy_learning__record_execution: Record strategy execution results
 * - strategy_learning__analyze_patterns: Analyze execution patterns
 * - strategy_learning__suggest_improvements: Suggest strategy improvements
 * 
 * This client maintains a 1:1 relationship with the Strategy Learning Server.
 */
public class StrategyLearningClient extends MCPClientBase {
    
    private static final String SERVER_PATH = "/mcp/servers/strategy-learning";
    
    /**
     * Create a new Strategy Learning client
     * @param baseUrl The base URL (e.g., http://localhost:8080)
     */
    public StrategyLearningClient(String baseUrl) {
        super("StrategyLearning", baseUrl + SERVER_PATH);
    }
    
    @Override
    protected void onClientReady() {
        // Log available tools for verification
        if (tools.size() > 0) {
            vertx.eventBus().publish("log", 
                "StrategyLearningClient ready with tools: " + tools.keySet() + 
                ",2,StrategyLearningClient,MCP,System");
        }
    }
}
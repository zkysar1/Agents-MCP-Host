package agents.director.mcp.client;

import agents.director.mcp.base.MCPClientBase;
import io.vertx.core.Vertx;

/**
 * MCP Client for Strategy Orchestrator Server.
 * Provides tools for executing and managing multi-step strategies.
 * 
 * Available tools:
 * - strategy_orchestrator__execute_step: Execute individual strategy steps
 * - strategy_orchestrator__evaluate_progress: Evaluate strategy progress
 * - strategy_orchestrator__adapt_strategy: Adapt strategy based on feedback
 * 
 * This client maintains a 1:1 relationship with the Strategy Orchestrator Server.
 */
public class StrategyOrchestratorClient extends MCPClientBase {
    
    private static final String SERVER_PATH = "/mcp/servers/strategy-orchestrator";
    
    /**
     * Create a new Strategy Orchestrator client
     * @param baseUrl The base URL (e.g., http://localhost:8080)
     */
    public StrategyOrchestratorClient(String baseUrl) {
        super("StrategyOrchestrator", baseUrl + SERVER_PATH);
    }
    
    @Override
    protected void onClientReady() {
        // Log available tools for verification
        if (tools.size() > 0) {
            vertx.eventBus().publish("log", 
                "StrategyOrchestratorClient ready with tools: " + tools.keySet() + 
                ",2,StrategyOrchestratorClient,MCP,System");
        }
    }
}
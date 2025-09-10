package agents.director.mcp.client;

import agents.director.mcp.base.MCPClientBase;
import io.vertx.core.Vertx;

/**
 * MCP Client for Intent Analysis Server.
 * Provides tools for deep analysis of user intent and output preferences.
 * 
 * Available tools:
 * - intent_analysis__extract_intent: Extract primary and secondary intents
 * - intent_analysis__determine_output_format: Determine preferred output format
 * - intent_analysis__suggest_interaction_style: Suggest interaction patterns
 * 
 * This client maintains a 1:1 relationship with the Intent Analysis Server.
 */
public class IntentAnalysisClient extends MCPClientBase {
    
    private static final String SERVER_PATH = "/mcp/servers/intent-analysis";
    
    /**
     * Create a new Intent Analysis client
     * @param baseUrl The base URL (e.g., http://localhost:8080)
     */
    public IntentAnalysisClient(String baseUrl) {
        super("IntentAnalysis", baseUrl + SERVER_PATH);
    }
    
    @Override
    protected void onClientReady() {
        // Log available tools for verification
        if (tools.size() > 0) {
            vertx.eventBus().publish("log", 
                "IntentAnalysisClient ready with tools: " + tools.keySet() + 
                ",2,IntentAnalysisClient,MCP,System");
        }
    }
}
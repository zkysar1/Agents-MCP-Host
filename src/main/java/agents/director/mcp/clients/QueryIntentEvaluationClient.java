package agents.director.mcp.clients;

import agents.director.mcp.base.MCPClientBase;

/**
 * MCP Client for Query Intent Evaluation Server.
 * Provides tools for evaluating user query intent and selecting strategies.
 * 
 * Available tools:
 * - evaluate_query_intent: Evaluate the intent behind user queries
 * - select_tool_strategy: Select appropriate tools based on intent
 * - learn_from_success: Learn from successful query executions
 * 
 * This clients maintains a 1:1 relationship with the Query Intent Evaluation Server.
 */
public class QueryIntentEvaluationClient extends MCPClientBase {
    
    private static final String SERVER_PATH = "/mcp/servers/query-intent";
    
    /**
     * Create a new Query Intent Evaluation clients
     * @param baseUrl The base URL (e.g., http://localhost:8080)
     */
    public QueryIntentEvaluationClient(String baseUrl) {
        super("QueryIntentEvaluation", baseUrl + SERVER_PATH);
    }
    
    @Override
    protected void onClientReady() {
        // Log available tools for verification
        if (tools.size() > 0) {
            vertx.eventBus().publish("log", 
                "QueryIntentEvaluationClient ready with tools: " + tools.keySet() + 
                ",2,QueryIntentEvaluationClient,MCP,System");
        }
    }
}
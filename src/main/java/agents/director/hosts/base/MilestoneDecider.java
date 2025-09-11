package agents.director.hosts.base;

import agents.director.hosts.base.MilestoneType;
import agents.director.services.LlmAPIService;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import java.util.Arrays;

/**
 * Simple milestone decision maker that replaces the complex IntentEngine.
 * 
 * This class has ONE job: Determine which milestone (1-6) to stop at
 * based on the backstory, guidance, and query.
 * 
 * Much simpler than the old 10-level pipeline depth analysis!
 */
public class MilestoneDecider {
    
    private final LlmAPIService llmService;
    
    public MilestoneDecider() {
        this.llmService = LlmAPIService.getInstance();
    }
    
    /**
     * Decide which milestone to stop at (1-6)
     * 
     * @param backstory The agent's backstory/context
     * @param guidance The user's guidance/requirements
     * @param query The user's actual query
     * @return Future with the target milestone number (1-6)
     */
    public Future<Integer> decideTargetMilestone(String backstory, String guidance, String query) {
        Promise<Integer> promise = Promise.promise();
        
        // If LLM is not available, use rule-based decision
        if (!llmService.isInitialized()) {
            int milestone = determineByRules(query, backstory, guidance);
            promise.complete(milestone);
            return promise.future();
        }
        
        // Build simple, clear prompt for LLM
        String systemPrompt = "You are a milestone selector for a data query system. " +
            "Based on the user's query and context, determine which milestone to stop at.\n\n" +
            "The 6 milestones are sequential (cannot skip):\n" +
            MilestoneType.getMilestoneDescriptions() + "\n" +
            "Return ONLY a single number 1-6 indicating the target milestone.\n" +
            "Guidelines:\n" +
            "- If user just wants to understand intent: stop at 1\n" +
            "- If user wants to explore schema: stop at 2\n" +
            "- If user wants to see data structure: stop at 3\n" +
            "- If user only wants SQL generated: stop at 4\n" +
            "- If user wants raw data/results: stop at 5\n" +
            "- If user wants a natural language answer: stop at 6 (default)\n";
        
        String userPrompt = "Backstory: " + (backstory != null ? backstory : "General assistant") + "\n" +
            "Guidance: " + (guidance != null ? guidance : "Help the user") + "\n" +
            "User Query: " + query + "\n\n" +
            "Which milestone should we stop at? Return only the number (1-6).";
        
        // Call LLM
        java.util.List<String> messages = Arrays.asList(
            new JsonObject().put("role", "system").put("content", systemPrompt).encode(),
            new JsonObject().put("role", "user").put("content", userPrompt).encode()
        );
        
        // Get current Vert.x context
        Context vertxContext = Vertx.currentContext();
        
        llmService.chatCompletion(messages, 0.1, 50) // Low temperature, short response
            .whenComplete((result, error) -> {
                Runnable handler = () -> {
                    if (error != null) {
                        // Fallback to rules on error
                        int milestone = determineByRules(query, backstory, guidance);
                        promise.complete(milestone);
                        return;
                    }
                    
                    try {
                        String content = result.getJsonArray("choices")
                            .getJsonObject(0)
                            .getJsonObject("message")
                            .getString("content");
                        
                        // Parse the number from response
                        int milestone = parseMilestoneNumber(content);
                        
                        // Validate range
                        milestone = Math.max(1, Math.min(6, milestone));
                        
                        promise.complete(milestone);
                    } catch (Exception e) {
                        // Fallback to rules on parse error
                        int milestone = determineByRules(query, backstory, guidance);
                        promise.complete(milestone);
                    }
                };
                
                if (vertxContext != null) {
                    vertxContext.runOnContext(v -> handler.run());
                } else {
                    handler.run();
                }
            });
        
        return promise.future();
    }
    
    /**
     * Simple rule-based milestone determination
     */
    private int determineByRules(String query, String backstory, String guidance) {
        String lower = query.toLowerCase();
        
        // Check guidance for hints
        if (guidance != null) {
            String guidanceLower = guidance.toLowerCase();
            if (guidanceLower.contains("sql only") || guidanceLower.contains("just sql")) {
                return 4; // Stop at SQL generation
            }
            if (guidanceLower.contains("raw data") || guidanceLower.contains("just data")) {
                return 5; // Stop at execution
            }
            if (guidanceLower.contains("intent only")) {
                return 1; // Stop at intent
            }
        }
        
        // Check query for hints
        if (lower.contains("show sql") || lower.contains("generate sql") || 
            lower.contains("write sql") || lower.contains("create query")) {
            return 4; // User wants SQL
        }
        
        if (lower.contains("what tables") || lower.contains("show schema") || 
            lower.contains("database structure")) {
            return 2; // User wants schema info
        }
        
        if (lower.contains("columns") || lower.contains("fields") || 
            lower.contains("data types")) {
            return 3; // User wants column info
        }
        
        if (lower.contains("execute") || lower.contains("run query") || 
            lower.contains("get data")) {
            return 5; // User wants execution
        }
        
        // Default: Go all the way to natural language response
        return 6;
    }
    
    /**
     * Parse milestone number from LLM response
     */
    private int parseMilestoneNumber(String content) {
        if (content == null || content.isEmpty()) {
            return 6; // Default
        }
        
        // Try to find a number 1-6 in the response
        content = content.trim();
        
        // Check if it's just a number
        try {
            return Integer.parseInt(content);
        } catch (NumberFormatException e) {
            // Not a simple number
        }
        
        // Look for the first digit 1-6
        for (char c : content.toCharArray()) {
            if (c >= '1' && c <= '6') {
                return Character.getNumericValue(c);
            }
        }
        
        // Default to 6 if no clear number found
        return 6;
    }
    
    /**
     * Get a description of what will happen up to a given milestone
     */
    public static String getMilestoneDescription(int targetMilestone) {
        StringBuilder description = new StringBuilder();
        description.append("Will execute milestones 1 through ").append(targetMilestone).append(":\n");
        
        for (int i = 1; i <= targetMilestone; i++) {
            MilestoneType type = MilestoneType.fromNumber(i);
            if (type != null) {
                description.append(i).append(". ").append(type.getDescription()).append("\n");
            }
        }
        
        return description.toString();
    }
}
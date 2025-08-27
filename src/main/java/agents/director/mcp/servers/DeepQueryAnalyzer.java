package agents.director.mcp.servers;

import agents.director.services.LlmAPIService;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * DeepQueryAnalyzer - Deeply analyzes queries to extract ALL semantic concepts.
 * 
 * This tool goes beyond simple entity extraction to understand:
 * - Business intent and goals
 * - Implicit requirements and assumptions
 * - Temporal aspects (time ranges, recent/old data)
 * - Aggregation needs (sums, averages, counts)
 * - Comparison operations (greater than, between)
 * - Sorting and limiting requirements
 * - Related concepts that might need joining
 * 
 * Zero hardcoding - works with ANY dataset by focusing on universal patterns.
 */
public class DeepQueryAnalyzer {
    
    private final LlmAPIService llmService;
    
    public DeepQueryAnalyzer(LlmAPIService llmService) {
        this.llmService = llmService;
    }
    
    /**
     * Deeply analyze a natural language query
     */
    public Future<JsonObject> analyze(String query, JsonArray conversationHistory) {
        if (query == null || query.trim().isEmpty()) {
            return Future.succeededFuture(new JsonObject()
                .put("error", "No query provided"));
        }
        
        // Build comprehensive analysis prompt
        String prompt = buildDeepAnalysisPrompt(query, conversationHistory);
        
        JsonArray messages = new JsonArray()
            .add(new JsonObject()
                .put("role", "system")
                .put("content", getSystemPrompt()))
            .add(new JsonObject()
                .put("role", "user")
                .put("content", prompt));
        
        return llmService.chatCompletion(messages, null)
            .map(response -> {
                // Extract the response content
                JsonArray choices = response.getJsonArray("choices", new JsonArray());
                if (!choices.isEmpty()) {
                    JsonObject firstChoice = choices.getJsonObject(0);
                    JsonObject message = firstChoice.getJsonObject("message", new JsonObject());
                    String content = message.getString("content", "");
                    return parseAnalysisResponse(content);
                }
                return createBasicAnalysis(query);
            })
            .recover(err -> {
                // Fallback to basic analysis on error
                return Future.succeededFuture(createBasicAnalysis(query));
            });
    }
    
    /**
     * Get the system prompt for deep analysis
     */
    private String getSystemPrompt() {
        return """
            You are an expert query analyst using cognitive scaffolding to extract semantic concepts.
            
            Your task is to analyze queries through multiple cognitive lenses to extract ALL relevant concepts,
            including implicit and inferred entities. Think like a domain expert who understands both the
            explicit request and the unstated context.
            
            Use systematic reasoning to uncover hidden requirements and related concepts that a human
            expert would naturally consider. Look beyond surface-level nouns to understand the full
            semantic landscape of the query.
            
            Return your analysis as structured JSON with comprehensive extraction and reasoning chains.
            """;
    }
    
    /**
     * Build the analysis prompt
     */
    private String buildDeepAnalysisPrompt(String query, JsonArray history) {
        StringBuilder prompt = new StringBuilder();
        
        prompt.append("Analyze this query through multiple cognitive stages:\n\n");
        prompt.append("Query: \"").append(query).append("\"\n\n");
        
        if (history != null && !history.isEmpty()) {
            prompt.append("Conversation Context:\n");
            for (int i = Math.max(0, history.size() - 3); i < history.size(); i++) {
                JsonObject msg = history.getJsonObject(i);
                prompt.append("- ").append(msg.getString("role")).append(": ")
                      .append(msg.getString("content")).append("\n");
            }
            prompt.append("\n");
        }
        
        prompt.append("""
        COGNITIVE ANALYSIS PROCESS:
        
        Step 1 - Surface Analysis:
        What are the explicit nouns, verbs, and entities directly mentioned in the query?
        List each one you identify.
        
        Step 2 - Semantic Expansion:
        For each explicit concept, what implicit concepts are naturally associated?
        Example: "California" → implies cities, regions, zip codes, geographic boundaries
        Example: "orders" → implies customers, products, dates, amounts, statuses
        Example: "employee" → implies departments, salaries, hire dates, managers
        
        Step 3 - Domain Knowledge Inference:
        What background knowledge would a business analyst assume?
        What related data would typically be needed to answer this type of question?
        What are the likely relationships between the concepts?
        
        Step 4 - Query Intent Mapping:
        What is the real business question being asked?
        Is this: counting, summing, listing, comparing, trending, or exploring?
        What level of detail is expected in the answer?
        
        Step 5 - Hidden Requirements:
        What constraints or filters might be implied but not stated?
        What sorting or grouping would make the answer most useful?
        What time context might be relevant?
        
        IMPORTANT: Think through each step systematically. Extract concepts that a human expert
        would naturally consider, even if not explicitly mentioned.
        
        Return a comprehensive JSON analysis with this structure:
        {
            "intent": "the core business question",
            "entities": ["ALL nouns/objects, both explicit and inferred"],
            "attributes": ["properties that might be relevant"],
            "relationships": ["how entities connect to each other"],
            "filters": [{"concept": "what", "condition": "how", "value": "what value"}],
            "aggregations": [{"type": "sum/count/avg/max/min", "of": "what", "by": "grouping"}],
            "temporal": {"type": "recent/range/specific", "implicit": true/false, "details": "..."},
            "sorting": [{"by": "attribute", "order": "asc/desc", "reason": "why"}],
            "implicit_requirements": ["things not stated but likely needed"],
            "reasoning_chain": "explanation of your semantic expansion process"
        }
        """);
        
        return prompt.toString();
    }
    
    /**
     * Parse the LLM response into structured analysis
     */
    private JsonObject parseAnalysisResponse(String response) {
        try {
            // Try to parse as JSON
            JsonObject parsed = new JsonObject(response);
            
            // Ensure all expected fields exist
            if (!parsed.containsKey("intent")) parsed.put("intent", "query data");
            if (!parsed.containsKey("entities")) parsed.put("entities", new JsonArray());
            if (!parsed.containsKey("attributes")) parsed.put("attributes", new JsonArray());
            if (!parsed.containsKey("relationships")) parsed.put("relationships", new JsonArray());
            if (!parsed.containsKey("filters")) parsed.put("filters", new JsonArray());
            if (!parsed.containsKey("aggregations")) parsed.put("aggregations", new JsonArray());
            if (!parsed.containsKey("temporal")) parsed.put("temporal", new JsonObject());
            if (!parsed.containsKey("sorting")) parsed.put("sorting", new JsonArray());
            if (!parsed.containsKey("implicit_requirements")) {
                parsed.put("implicit_requirements", new JsonArray());
            }
            if (!parsed.containsKey("reasoning_chain")) {
                parsed.put("reasoning_chain", "No reasoning chain provided");
            }
            
            // Add analysis metadata
            parsed.put("analysis_type", "deep");
            parsed.put("confidence", calculateConfidence(parsed));
            
            return parsed;
            
        } catch (Exception e) {
            // If parsing fails, extract what we can
            return extractFallbackAnalysis(response);
        }
    }
    
    /**
     * Calculate confidence score based on completeness
     */
    private double calculateConfidence(JsonObject analysis) {
        int score = 0;
        int total = 10;
        
        if (!analysis.getString("intent", "").isEmpty()) score++;
        if (!analysis.getJsonArray("entities", new JsonArray()).isEmpty()) score++;
        if (!analysis.getJsonArray("attributes", new JsonArray()).isEmpty()) score++;
        if (!analysis.getJsonArray("relationships", new JsonArray()).isEmpty()) score++;
        if (!analysis.getJsonArray("filters", new JsonArray()).isEmpty()) score++;
        if (!analysis.getJsonArray("aggregations", new JsonArray()).isEmpty()) score++;
        if (!analysis.getJsonObject("temporal", new JsonObject()).isEmpty()) score++;
        if (!analysis.getJsonArray("sorting", new JsonArray()).isEmpty()) score++;
        if (analysis.containsKey("limit") && analysis.getJsonObject("limit") != null) score++;
        if (!analysis.getJsonArray("implicit_requirements", new JsonArray()).isEmpty()) score++;
        
        return (double) score / total;
    }
    
    /**
     * Extract basic analysis from text response
     */
    private JsonObject extractFallbackAnalysis(String response) {
        JsonObject analysis = new JsonObject();
        
        // Extract entities (words that look like nouns)
        JsonArray entities = new JsonArray();
        String[] words = response.split("\\s+");
        for (String word : words) {
            if (word.length() > 3 && Character.isUpperCase(word.charAt(0))) {
                entities.add(word.toLowerCase());
            }
        }
        
        analysis.put("intent", "query data")
                .put("entities", entities)
                .put("attributes", new JsonArray())
                .put("analysis_type", "fallback")
                .put("confidence", 0.3);
        
        return analysis;
    }
    
    /**
     * Create basic analysis without LLM
     */
    private JsonObject createBasicAnalysis(String query) {
        JsonObject analysis = new JsonObject();
        String lowerQuery = query.toLowerCase();
        
        // Detect intent
        String intent = "query";
        if (lowerQuery.contains("count") || lowerQuery.contains("how many")) {
            intent = "count";
        } else if (lowerQuery.contains("sum") || lowerQuery.contains("total")) {
            intent = "aggregate";
        } else if (lowerQuery.contains("average") || lowerQuery.contains("avg")) {
            intent = "average";
        } else if (lowerQuery.contains("list") || lowerQuery.contains("show")) {
            intent = "list";
        }
        
        // Extract potential entities (simple word extraction)
        JsonArray entities = new JsonArray();
        String[] commonWords = {"the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for",
                               "of", "with", "by", "from", "up", "about", "into", "through", "after",
                               "where", "what", "which", "when", "how", "why", "who", "show", "list",
                               "get", "find", "select", "give", "all", "every", "each"};
        
        String[] words = query.split("\\s+");
        for (String word : words) {
            String clean = word.replaceAll("[^a-zA-Z0-9]", "").toLowerCase();
            // Include any word that's not a common word and has at least 2 chars
            if (clean.length() >= 2 && !contains(commonWords, clean)) {
                // Special handling for common entity patterns
                if (clean.endsWith("s") && clean.length() > 3) {
                    // Likely a plural entity (employees, orders, etc.)
                    entities.add(clean);
                    // Also add singular form
                    entities.add(clean.substring(0, clean.length() - 1));
                } else if (clean.endsWith("ies") && clean.length() > 4) {
                    // Handle plurals like "companies" -> "company"
                    entities.add(clean);
                    entities.add(clean.substring(0, clean.length() - 3) + "y");
                } else {
                    entities.add(clean);
                }
            }
        }
        
        // If no entities found, extract any nouns from the query
        if (entities.isEmpty()) {
            // As a last resort, add the most significant non-common word
            for (String word : words) {
                String clean = word.replaceAll("[^a-zA-Z0-9]", "").toLowerCase();
                if (clean.length() >= 2 && !contains(commonWords, clean)) {
                    entities.add(clean);
                    break;
                }
            }
        }
        
        // Detect filters
        JsonArray filters = new JsonArray();
        if (lowerQuery.contains("where") || lowerQuery.contains("with") || lowerQuery.contains("having")) {
            filters.add(new JsonObject()
                .put("detected", true)
                .put("type", "condition"));
        }
        
        // Detect temporal
        JsonObject temporal = new JsonObject();
        if (lowerQuery.contains("today") || lowerQuery.contains("yesterday") || 
            lowerQuery.contains("recent") || lowerQuery.contains("last")) {
            temporal.put("type", "recent");
        }
        
        analysis.put("intent", intent)
                .put("entities", entities)
                .put("attributes", new JsonArray())
                .put("filters", filters)
                .put("temporal", temporal)
                .put("analysis_type", "basic")
                .put("confidence", 0.5);
        
        return analysis;
    }
    
    /**
     * Helper to check if array contains string
     */
    private boolean contains(String[] array, String value) {
        for (String item : array) {
            if (item.equals(value)) return true;
        }
        return false;
    }
}
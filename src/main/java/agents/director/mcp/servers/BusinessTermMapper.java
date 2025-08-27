package agents.director.mcp.servers;

import agents.director.services.LlmAPIService;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BusinessTermMapper - Maps business language to technical database terms.
 * 
 * This tool bridges the gap between how users describe things and how they're
 * stored in the database. It learns from:
 * - Column names and their variations
 * - Enumeration values and descriptions
 * - Common business synonyms
 * - Domain-specific terminology
 * 
 * Works with ANY dataset by analyzing the actual schema and data.
 */
public class BusinessTermMapper {
    
    private final LlmAPIService llmService;
    private final EnumerationMapper enumMapper;
    
    // Thread-safe cache for learned mappings
    private final Map<String, Set<String>> termMappings = new ConcurrentHashMap<>();
    
    public BusinessTermMapper(LlmAPIService llmService, EnumerationMapper enumMapper) {
        this.llmService = llmService;
        this.enumMapper = enumMapper;
    }
    
    /**
     * Map business terms to database terms
     */
    public Future<JsonObject> mapTerms(JsonArray businessTerms, JsonObject schemaContext) {
        if (businessTerms == null || businessTerms.isEmpty()) {
            return Future.succeededFuture(new JsonObject()
                .put("mappings", new JsonArray())
                .put("confidence", 0.0));
        }
        
        // Extract available database terms from schema context
        Set<String> databaseTerms = extractDatabaseTerms(schemaContext);
        
        // Get enumeration mappings
        Future<JsonObject> enumMappingsFuture = getEnumerationMappings();
        
        return enumMappingsFuture.compose(enumMappings -> {
            // Build mapping prompt
            String prompt = buildMappingPrompt(businessTerms, databaseTerms, enumMappings);
            
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
                        return parseMappingResponse(content, businessTerms);
                    }
                    return performRuleBasedMapping(businessTerms, databaseTerms, enumMappings);
                })
                .recover(err -> {
                    // Fallback to rule-based mapping
                    return Future.succeededFuture(performRuleBasedMapping(businessTerms, databaseTerms, enumMappings));
                });
        });
    }
    
    /**
     * Extract database terms from schema context
     */
    private Set<String> extractDatabaseTerms(JsonObject schemaContext) {
        Set<String> terms = new HashSet<>();
        
        // Extract table names
        JsonArray tables = schemaContext.getJsonArray("tables", new JsonArray());
        for (int i = 0; i < tables.size(); i++) {
            JsonObject table = tables.getJsonObject(i);
            String tableName = table.getString("tableName", table.getString("name", ""));
            if (!tableName.isEmpty()) {
                terms.add(tableName.toLowerCase());
                // Also add without underscores
                terms.add(tableName.toLowerCase().replace("_", ""));
            }
        }
        
        // Extract column names
        JsonArray columns = schemaContext.getJsonArray("columns", new JsonArray());
        for (int i = 0; i < columns.size(); i++) {
            JsonObject col = columns.getJsonObject(i);
            String colName = col.getString("columnName", col.getString("name", ""));
            if (!colName.isEmpty()) {
                terms.add(colName.toLowerCase());
                // Also add without underscores
                terms.add(colName.toLowerCase().replace("_", ""));
            }
        }
        
        // Extract from matched schema if present
        JsonObject matchResult = schemaContext.getJsonObject("match_result");
        if (matchResult != null) {
            JsonArray tableMatches = matchResult.getJsonArray("tableMatches", new JsonArray());
            for (int i = 0; i < tableMatches.size(); i++) {
                JsonObject match = tableMatches.getJsonObject(i);
                terms.add(match.getString("tableName", "").toLowerCase());
            }
            
            JsonArray columnMatches = matchResult.getJsonArray("columnMatches", new JsonArray());
            for (int i = 0; i < columnMatches.size(); i++) {
                JsonObject match = columnMatches.getJsonObject(i);
                terms.add(match.getString("columnName", "").toLowerCase());
            }
        }
        
        return terms;
    }
    
    /**
     * Get enumeration mappings from the system
     */
    private Future<JsonObject> getEnumerationMappings() {
        return enumMapper.detectEnumerationTables()
            .map(enumTables -> {
                JsonObject mappings = new JsonObject();
                
                // Limit enumeration tables to process
                int maxEnums = Math.min(enumTables.size(), 10);
                
                for (int i = 0; i < maxEnums; i++) {
                    JsonObject enumTable = enumTables.getJsonObject(i);
                    if (enumTable != null) {
                        String tableName = enumTable.getString("tableName");
                        if (tableName != null) {
                            mappings.put(tableName, enumTable);
                        }
                    }
                }
                
                return mappings;
            })
            .recover(err -> {
                System.err.println("[BusinessTermMapper] Failed to get enumerations: " + err.getMessage());
                return Future.succeededFuture(new JsonObject());
            });
    }
    
    /**
     * Get system prompt for mapping
     */
    private String getSystemPrompt() {
        return """
            You are an expert at mapping business language to technical database terms using inference chains.
            
            Your task is to build logical connections between how users describe concepts and how they're
            stored in databases. Think like a domain expert who understands both business terminology
            and database conventions.
            
            Use systematic reasoning to uncover non-obvious mappings through semantic relationships,
            common abbreviations, industry patterns, and contextual clues.
            
            Return your analysis as structured JSON with reasoning chains explaining each mapping.
            """;
    }
    
    /**
     * Build the mapping prompt
     */
    private String buildMappingPrompt(JsonArray businessTerms, Set<String> databaseTerms, 
                                    JsonObject enumMappings) {
        StringBuilder prompt = new StringBuilder();
        
        prompt.append("Map business terms to database terms using inference chains.\n\n");
        
        prompt.append("Business Terms to Map:\n");
        prompt.append("=====================\n");
        for (int i = 0; i < businessTerms.size(); i++) {
            prompt.append(i+1).append(". \"").append(businessTerms.getString(i)).append("\"\n");
        }
        
        prompt.append("\nAvailable Database Terms:\n");
        prompt.append("========================\n");
        List<String> sortedTerms = new ArrayList<>(databaseTerms);
        Collections.sort(sortedTerms);
        int count = 0;
        for (String term : sortedTerms) {
            prompt.append("- ").append(term).append("\n");
            if (++count >= 50) { // Limit to prevent prompt overflow
                prompt.append("... and ").append(sortedTerms.size() - 50).append(" more terms\n");
                break;
            }
        }
        
        if (!enumMappings.isEmpty()) {
            prompt.append("\nEnumeration/Lookup Tables:\n");
            prompt.append("=========================\n");
            for (String key : enumMappings.fieldNames()) {
                JsonObject enumInfo = enumMappings.getJsonObject(key);
                prompt.append("- ").append(key).append(" (")
                      .append(enumInfo.getInteger("rowCount", 0)).append(" values)\n");
            }
        }
        
        prompt.append("""        
        
        INFERENCE CHAIN MAPPING PROCESS:
        ==============================
        
        For EACH business term, follow this systematic process:
        
        Step 1 - Term Analysis:
        ---------------------
        - What does this term mean in general business context?
        - What data would typically represent this concept?
        - What are common synonyms or related terms?
        
        Step 2 - Pattern Recognition:
        ---------------------------
        - Check for abbreviations (e.g., "cust" → "customer", "amt" → "amount")
        - Look for compound terms (e.g., "order date" → "ORDER_DATE" or "ORDERDATE")
        - Consider domain conventions (e.g., "ID" suffix for identifiers)
        - Check for plural/singular variations
        
        Step 3 - Semantic Bridging:
        -------------------------
        - If no direct match, what related concepts might contain this data?
        - Example: "location" might map to "ADDRESS", "CITY", "STATE", or "REGION"
        - Example: "status" might map to "STATUS_CODE" with enumeration table
        - Example: "product" might be in "ITEMS", "INVENTORY", or "PRODUCTS" table
        
        Step 4 - Context-Aware Inference:
        -------------------------------
        - Consider the query context - what type of data is being requested?
        - If looking for geographic data, prioritize location-related columns
        - If looking for temporal data, prioritize date/time columns
        - If looking for quantities, prioritize numeric columns
        
        Step 5 - Confidence Assessment:
        -----------------------------
        - Rate your confidence based on:
          * Exact match = 1.0
          * Clear abbreviation or synonym = 0.8-0.9
          * Logical inference = 0.6-0.7
          * Educated guess = 0.4-0.5
        
        IMPORTANT: Build inference chains, not just direct mappings.
        
        Example inference chain:
        "California" → geographic location → state name → look for state columns → 
        found "STATE" in CUSTOMERS table → confidence 0.8
        
        Return comprehensive JSON with this structure:
        {
            "mappings": [{
                "business_term": "the original term",
                "database_terms": ["matched_term1", "matched_term2"],
                "confidence": 0.0-1.0,
                "mapping_type": "exact|abbreviation|synonym|inference",
                "reasoning_chain": "explanation of how you made this connection"
            }],
            "unmapped": ["terms with no viable matches"],
            "inference_insights": "overall patterns or domain knowledge discovered"
        }
        """);
        
        return prompt.toString();
    }
    
    /**
     * Parse the LLM mapping response
     */
    private JsonObject parseMappingResponse(String response, JsonArray originalTerms) {
        JsonObject parsed = null;
        
        try {
            // First try to extract JSON from the response (LLM might include extra text)
            String jsonStr = response.trim();
            
            // Find JSON boundaries if wrapped in text
            int startIdx = jsonStr.indexOf('{');
            int endIdx = jsonStr.lastIndexOf('}');
            
            if (startIdx >= 0 && endIdx > startIdx) {
                jsonStr = jsonStr.substring(startIdx, endIdx + 1);
            }
            
            parsed = new JsonObject(jsonStr);
            
            // Ensure required fields
            if (!parsed.containsKey("mappings")) {
                parsed.put("mappings", new JsonArray());
            }
            if (!parsed.containsKey("unmapped")) {
                parsed.put("unmapped", new JsonArray());
            }
            if (!parsed.containsKey("inference_insights")) {
                parsed.put("inference_insights", "No insights provided");
            }
            
            // Calculate overall confidence
            JsonArray mappings = parsed.getJsonArray("mappings");
            double totalConfidence = 0.0;
            int mappedCount = 0;
            
            for (int i = 0; i < mappings.size(); i++) {
                JsonObject mapping = mappings.getJsonObject(i);
                if (mapping.containsKey("confidence")) {
                    totalConfidence += mapping.getDouble("confidence");
                    mappedCount++;
                }
            }
            
            double overallConfidence = mappedCount > 0 ? 
                totalConfidence / mappedCount : 0.0;
            
            parsed.put("overall_confidence", overallConfidence);
            parsed.put("mapping_method", "llm");
            
            // Store successful mappings in cache
            updateMappingCache(mappings);
            
            return parsed;
            
        } catch (Exception e) {
            System.err.println("[BusinessTermMapper] Failed to parse LLM response: " + e.getMessage());
            System.err.println("[BusinessTermMapper] Raw response: " + 
                              (response.length() > 200 ? response.substring(0, 200) + "..." : response));
            
            // Return empty mapping on parse error
            return new JsonObject()
                .put("mappings", new JsonArray())
                .put("unmapped", originalTerms)
                .put("overall_confidence", 0.0)
                .put("mapping_method", "failed")
                .put("error", "Failed to parse LLM response: " + e.getMessage());
        }
    }
    
    /**
     * Perform rule-based mapping as fallback
     */
    private JsonObject performRuleBasedMapping(JsonArray businessTerms, Set<String> databaseTerms,
                                             JsonObject enumMappings) {
        JsonArray mappings = new JsonArray();
        JsonArray unmapped = new JsonArray();
        
        for (int i = 0; i < businessTerms.size(); i++) {
            // Type safety check
            Object termObj = businessTerms.getValue(i);
            if (!(termObj instanceof String)) {
                unmapped.add(termObj != null ? termObj.toString() : "null");
                continue;
            }
            
            String businessTerm = ((String) termObj).toLowerCase();
            JsonArray matches = new JsonArray();
            double bestScore = 0.0;
            
            // Try exact match first
            if (databaseTerms.contains(businessTerm)) {
                matches.add(businessTerm);
                bestScore = 1.0;
            } else {
                // Try fuzzy matching
                for (String dbTerm : databaseTerms) {
                    double score = calculateSimilarity(businessTerm, dbTerm);
                    if (score > 0.6) {
                        matches.add(dbTerm);
                        bestScore = Math.max(bestScore, score);
                    }
                }
            }
            
            if (!matches.isEmpty()) {
                mappings.add(new JsonObject()
                    .put("business_term", businessTerms.getString(i))
                    .put("database_terms", matches)
                    .put("confidence", bestScore)
                    .put("mapping_type", bestScore == 1.0 ? "exact" : "fuzzy"));
            } else {
                unmapped.add(businessTerms.getString(i));
            }
        }
        
        return new JsonObject()
            .put("mappings", mappings)
            .put("unmapped", unmapped)
            .put("overall_confidence", mappings.size() / (double) businessTerms.size())
            .put("mapping_method", "rule_based");
    }
    
    /**
     * Calculate similarity between two terms
     */
    private double calculateSimilarity(String term1, String term2) {
        // Normalize terms
        String norm1 = term1.toLowerCase().replace("_", "").replace("-", "");
        String norm2 = term2.toLowerCase().replace("_", "").replace("-", "");
        
        // Exact match after normalization
        if (norm1.equals(norm2)) {
            return 0.9;
        }
        
        // One contains the other
        if (norm1.contains(norm2) || norm2.contains(norm1)) {
            return 0.8;
        }
        
        // Check common abbreviations
        if (isAbbreviation(norm1, norm2) || isAbbreviation(norm2, norm1)) {
            return 0.7;
        }
        
        // Levenshtein distance
        int distance = levenshteinDistance(norm1, norm2);
        double maxLen = Math.max(norm1.length(), norm2.length());
        double similarity = 1.0 - (distance / maxLen);
        
        // Boost if they share words
        if (shareWords(term1, term2)) {
            similarity += 0.2;
        }
        
        return Math.min(1.0, similarity);
    }
    
    /**
     * Check if one term is abbreviation of another
     */
    private boolean isAbbreviation(String abbr, String full) {
        if (abbr.length() >= full.length()) {
            return false;
        }
        
        // Check if abbr matches first letters of words in full
        String[] words = full.split("(?=[A-Z])|_|-|\\s");
        StringBuilder firstLetters = new StringBuilder();
        for (String word : words) {
            if (!word.isEmpty()) {
                firstLetters.append(word.charAt(0));
            }
        }
        
        return abbr.equalsIgnoreCase(firstLetters.toString());
    }
    
    /**
     * Check if terms share words
     */
    private boolean shareWords(String term1, String term2) {
        Set<String> words1 = new HashSet<>(Arrays.asList(
            term1.toLowerCase().split("[_\\-\\s]+")));
        Set<String> words2 = new HashSet<>(Arrays.asList(
            term2.toLowerCase().split("[_\\-\\s]+")));
        
        words1.retainAll(words2);
        return !words1.isEmpty();
    }
    
    /**
     * Calculate Levenshtein distance
     */
    private int levenshteinDistance(String s1, String s2) {
        int[][] dp = new int[s1.length() + 1][s2.length() + 1];
        
        for (int i = 0; i <= s1.length(); i++) {
            dp[i][0] = i;
        }
        for (int j = 0; j <= s2.length(); j++) {
            dp[0][j] = j;
        }
        
        for (int i = 1; i <= s1.length(); i++) {
            for (int j = 1; j <= s2.length(); j++) {
                int cost = s1.charAt(i - 1) == s2.charAt(j - 1) ? 0 : 1;
                dp[i][j] = Math.min(Math.min(
                    dp[i - 1][j] + 1,      // deletion
                    dp[i][j - 1] + 1),     // insertion
                    dp[i - 1][j - 1] + cost // substitution
                );
            }
        }
        
        return dp[s1.length()][s2.length()];
    }
    
    /**
     * Update mapping cache with successful mappings
     */
    private void updateMappingCache(JsonArray mappings) {
        for (int i = 0; i < mappings.size(); i++) {
            JsonObject mapping = mappings.getJsonObject(i);
            String businessTerm = mapping.getString("business_term", "").toLowerCase();
            JsonArray dbTerms = mapping.getJsonArray("database_terms", new JsonArray());
            
            if (!businessTerm.isEmpty() && !dbTerms.isEmpty()) {
                termMappings.computeIfAbsent(businessTerm, k -> new HashSet<>());
                for (int j = 0; j < dbTerms.size(); j++) {
                    termMappings.get(businessTerm).add(dbTerms.getString(j));
                }
            }
        }
    }
    
    /**
     * Get cached mappings for a term
     */
    public Set<String> getCachedMappings(String businessTerm) {
        return termMappings.getOrDefault(businessTerm.toLowerCase(), Collections.emptySet());
    }
}
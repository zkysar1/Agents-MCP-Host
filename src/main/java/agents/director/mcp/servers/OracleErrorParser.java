package agents.director.mcp.servers;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * OracleErrorParser - Parses Oracle error messages and provides schema-aware feedback.
 * 
 * This parser analyzes Oracle error messages to extract meaningful information about
 * schema mismatches and provides specific correction suggestions. It also caches
 * successful query patterns for future use.
 * 
 * Key features:
 * - Parses Oracle error codes and extracts relevant details
 * - Maps errors to specific schema issues
 * - Suggests corrections based on available schema
 * - Caches successful mappings for pattern reuse
 */
public class OracleErrorParser {
    
    private final Map<String, String> errorPatterns = new HashMap<>();
    private final Map<String, List<String>> successfulMappings = new ConcurrentHashMap<>();
    
    // Common Oracle error patterns
    private static final Pattern TABLE_NAME_PATTERN = Pattern.compile(
        "\"([A-Z_]+)\"\\.\"([A-Z_]+)\"|([A-Z_]+)\\s+(?:table|view)",
        Pattern.CASE_INSENSITIVE
    );
    
    private static final Pattern COLUMN_NAME_PATTERN = Pattern.compile(
        "\"([A-Z_]+)\"\\.\"([A-Z_]+)\"|\"([A-Z_]+)\"",
        Pattern.CASE_INSENSITIVE
    );
    
    public OracleErrorParser() {
        // Initialize common Oracle error patterns
        errorPatterns.put("ORA-00942", "table_not_found");
        errorPatterns.put("ORA-00904", "column_not_found");
        errorPatterns.put("ORA-00918", "ambiguous_column");
        errorPatterns.put("ORA-00936", "missing_expression");
        errorPatterns.put("ORA-00933", "invalid_sql_command");
        errorPatterns.put("ORA-00923", "from_keyword_not_found");
        errorPatterns.put("ORA-01722", "invalid_number");
        errorPatterns.put("ORA-01747", "invalid_column_specification");
    }
    
    /**
     * Parse an Oracle error message and return structured feedback
     */
    public JsonObject parseError(String errorMessage, JsonObject schemaContext) {
        if (errorMessage == null || errorMessage.isEmpty()) {
            return new JsonObject()
                .put("error_type", "unknown")
                .put("message", "No error message provided");
        }
        
        String errorCode = extractErrorCode(errorMessage);
        String errorType = errorPatterns.getOrDefault(errorCode, "unknown");
        
        JsonObject feedback = new JsonObject()
            .put("error_code", errorCode)
            .put("error_type", errorType)
            .put("original_message", errorMessage);
        
        // Extract specific details based on error type
        switch (errorType) {
            case "table_not_found":
                handleTableNotFound(errorMessage, schemaContext, feedback);
                break;
                
            case "column_not_found":
                handleColumnNotFound(errorMessage, schemaContext, feedback);
                break;
                
            case "ambiguous_column":
                handleAmbiguousColumn(errorMessage, schemaContext, feedback);
                break;
                
            default:
                feedback.put("general_hint", "Check SQL syntax and ensure all references match the schema");
        }
        
        return feedback;
    }
    
    /**
     * Handle ORA-00942: table or view does not exist
     */
    private void handleTableNotFound(String errorMessage, JsonObject schemaContext, JsonObject feedback) {
        String tableName = extractTableName(errorMessage);
        if (tableName != null) {
            feedback.put("missing_table", tableName);
            
            // Find similar tables
            JsonArray suggestions = findSimilarTables(tableName, schemaContext);
            feedback.put("table_suggestions", suggestions);
            
            // Add specific correction hint
            if (!suggestions.isEmpty()) {
                feedback.put("correction_hint", 
                    "Table '" + tableName + "' does not exist. Did you mean: " + 
                    suggestions.getJsonObject(0).getString("table") + "?");
            } else {
                feedback.put("correction_hint", 
                    "Table '" + tableName + "' does not exist. Check available tables in the schema.");
            }
        }
    }
    
    /**
     * Handle ORA-00904: invalid identifier (column not found)
     */
    private void handleColumnNotFound(String errorMessage, JsonObject schemaContext, JsonObject feedback) {
        String columnRef = extractColumnRef(errorMessage);
        if (columnRef != null) {
            feedback.put("missing_column", columnRef);
            
            // Parse table.column format
            String tableName = null;
            String columnName = columnRef;
            
            if (columnRef.contains(".")) {
                String[] parts = columnRef.split("\\.");
                if (parts.length == 2) {
                    tableName = parts[0];
                    columnName = parts[1];
                }
            }
            
            // Find similar columns
            JsonArray suggestions = findSimilarColumns(columnName, tableName, schemaContext);
            feedback.put("column_suggestions", suggestions);
            
            // Add specific correction hint
            if (!suggestions.isEmpty()) {
                JsonObject firstSuggestion = suggestions.getJsonObject(0);
                String suggestedColumn = firstSuggestion.getString("column");
                String suggestedTable = firstSuggestion.getString("table", "");
                
                feedback.put("correction_hint", 
                    "Column '" + columnRef + "' not found. Did you mean: " + 
                    (suggestedTable.isEmpty() ? suggestedColumn : suggestedTable + "." + suggestedColumn) + "?");
            } else {
                feedback.put("correction_hint", 
                    "Column '" + columnRef + "' not found. Check column names in the schema.");
            }
        }
    }
    
    /**
     * Handle ORA-00918: column ambiguously defined
     */
    private void handleAmbiguousColumn(String errorMessage, JsonObject schemaContext, JsonObject feedback) {
        feedback.put("correction_hint", 
            "Column reference is ambiguous. Prefix column names with table aliases or table names.");
        feedback.put("example", "Use: TABLE_ALIAS.COLUMN_NAME or TABLE_NAME.COLUMN_NAME");
    }
    
    /**
     * Cache a successful query mapping
     */
    public void cacheSuccessfulMapping(String query, String sql, JsonObject schema) {
        if (query == null || sql == null || query.isEmpty() || sql.isEmpty()) {
            return;
        }
        
        // Generate cache key from query entities
        String key = generateCacheKey(query);
        
        // Store the successful SQL pattern
        successfulMappings.computeIfAbsent(key, k -> new ArrayList<>())
            .add(sql);
        
        // Limit cache size per key
        List<String> mappings = successfulMappings.get(key);
        if (mappings.size() > 5) {
            mappings.remove(0); // Remove oldest
        }
    }
    
    /**
     * Get cached successful patterns for a query
     */
    public List<String> getCachedPatterns(String query) {
        String key = generateCacheKey(query);
        return successfulMappings.getOrDefault(key, Collections.emptyList());
    }
    
    /**
     * Extract Oracle error code from message
     */
    private String extractErrorCode(String message) {
        Pattern p = Pattern.compile("ORA-\\d{5}");
        Matcher m = p.matcher(message);
        return m.find() ? m.group() : "UNKNOWN";
    }
    
    /**
     * Extract table name from error message
     */
    private String extractTableName(String errorMessage) {
        // Try to extract from patterns like "table or view does not exist"
        Matcher m = TABLE_NAME_PATTERN.matcher(errorMessage);
        if (m.find()) {
            // Return first non-null group
            for (int i = 1; i <= m.groupCount(); i++) {
                String group = m.group(i);
                if (group != null) {
                    return group.toUpperCase();
                }
            }
        }
        
        // Fallback: look for uppercase words that might be table names
        Pattern simplePattern = Pattern.compile("\\b([A-Z][A-Z_]+)\\b");
        Matcher simpleMatcher = simplePattern.matcher(errorMessage);
        if (simpleMatcher.find()) {
            return simpleMatcher.group(1);
        }
        
        return null;
    }
    
    /**
     * Extract column reference from error message
     */
    private String extractColumnRef(String errorMessage) {
        Matcher m = COLUMN_NAME_PATTERN.matcher(errorMessage);
        if (m.find()) {
            // Check for table.column format
            String table = m.group(1);
            String column = m.group(2);
            if (table != null && column != null) {
                return table + "." + column;
            }
            
            // Single column
            String singleColumn = m.group(3);
            if (singleColumn != null) {
                return singleColumn;
            }
        }
        
        // Fallback: look for pattern after "invalid identifier"
        int idx = errorMessage.toLowerCase().indexOf("invalid identifier");
        if (idx >= 0) {
            String afterError = errorMessage.substring(0, idx).trim();
            // Extract last quoted or uppercase word before "invalid identifier"
            Pattern p = Pattern.compile("\"([^\"]+)\"|\\b([A-Z][A-Z_]*)\\b");
            Matcher pm = p.matcher(afterError);
            String lastMatch = null;
            while (pm.find()) {
                lastMatch = pm.group(1) != null ? pm.group(1) : pm.group(2);
            }
            return lastMatch;
        }
        
        return null;
    }
    
    /**
     * Find similar table names in schema
     */
    private JsonArray findSimilarTables(String tableName, JsonObject schemaContext) {
        JsonArray suggestions = new JsonArray();
        
        if (schemaContext == null || tableName == null) {
            return suggestions;
        }
        
        // Get tables from schema context
        JsonArray tables = schemaContext.getJsonArray("tables", new JsonArray());
        
        // Also check tableMatches from schema matching
        JsonObject matchResult = schemaContext.getJsonObject("match_result");
        if (matchResult != null) {
            JsonArray tableMatches = matchResult.getJsonArray("tableMatches", new JsonArray());
            for (int i = 0; i < tableMatches.size(); i++) {
                JsonObject match = tableMatches.getJsonObject(i);
                String matchedTable = match.getString("tableName", "");
                if (!matchedTable.isEmpty()) {
                    tables.add(new JsonObject().put("tableName", matchedTable));
                }
            }
        }
        
        // Find similar names
        for (int i = 0; i < tables.size(); i++) {
            JsonObject table = tables.getJsonObject(i);
            String candidateName = table.getString("tableName", table.getString("name", ""));
            
            double similarity = calculateSimilarity(tableName, candidateName);
            if (similarity > 0.5) {
                suggestions.add(new JsonObject()
                    .put("table", candidateName)
                    .put("similarity", similarity));
            }
        }
        
        // Sort by similarity
        return sortBySimilarity(suggestions);
    }
    
    /**
     * Find similar column names in schema
     */
    private JsonArray findSimilarColumns(String columnName, String tableName, JsonObject schemaContext) {
        JsonArray suggestions = new JsonArray();
        
        if (schemaContext == null || columnName == null) {
            return suggestions;
        }
        
        // Get columns from schema context
        JsonArray columns = schemaContext.getJsonArray("columns", new JsonArray());
        
        // Also check columnMatches from schema matching
        JsonObject matchResult = schemaContext.getJsonObject("match_result");
        if (matchResult != null) {
            JsonArray columnMatches = matchResult.getJsonArray("columnMatches", new JsonArray());
            for (int i = 0; i < columnMatches.size(); i++) {
                JsonObject match = columnMatches.getJsonObject(i);
                columns.add(match);
            }
        }
        
        // Find similar names
        for (int i = 0; i < columns.size(); i++) {
            JsonObject col = columns.getJsonObject(i);
            String candidateColumn = col.getString("columnName", col.getString("name", ""));
            String candidateTable = col.getString("tableName", col.getString("table", ""));
            
            // If table specified, only consider columns from that table
            if (tableName != null && !tableName.isEmpty() && 
                !candidateTable.equalsIgnoreCase(tableName)) {
                continue;
            }
            
            double similarity = calculateSimilarity(columnName, candidateColumn);
            if (similarity > 0.5) {
                suggestions.add(new JsonObject()
                    .put("column", candidateColumn)
                    .put("table", candidateTable)
                    .put("similarity", similarity));
            }
        }
        
        return sortBySimilarity(suggestions);
    }
    
    /**
     * Calculate string similarity
     */
    private double calculateSimilarity(String s1, String s2) {
        if (s1 == null || s2 == null) {
            return 0.0;
        }
        
        s1 = s1.toUpperCase();
        s2 = s2.toUpperCase();
        
        if (s1.equals(s2)) {
            return 1.0;
        }
        
        // Remove underscores and compare
        String norm1 = s1.replace("_", "");
        String norm2 = s2.replace("_", "");
        
        if (norm1.equals(norm2)) {
            return 0.9;
        }
        
        // Check containment
        if (s1.contains(s2) || s2.contains(s1)) {
            return 0.7;
        }
        
        // Levenshtein distance
        int distance = levenshteinDistance(s1, s2);
        double maxLen = Math.max(s1.length(), s2.length());
        return 1.0 - (distance / maxLen);
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
     * Sort suggestions by similarity score
     */
    private JsonArray sortBySimilarity(JsonArray suggestions) {
        List<JsonObject> list = new ArrayList<>();
        for (int i = 0; i < suggestions.size(); i++) {
            list.add(suggestions.getJsonObject(i));
        }
        
        list.sort((a, b) -> Double.compare(
            b.getDouble("similarity", 0.0),
            a.getDouble("similarity", 0.0)
        ));
        
        JsonArray sorted = new JsonArray();
        for (JsonObject obj : list) {
            sorted.add(obj);
        }
        
        return sorted;
    }
    
    /**
     * Generate cache key from query
     */
    private String generateCacheKey(String query) {
        // Extract key terms from query
        String normalized = query.toLowerCase()
            .replaceAll("[^a-z0-9\\s]", "")
            .replaceAll("\\s+", " ")
            .trim();
        
        // Extract main entities/keywords
        String[] words = normalized.split(" ");
        Set<String> keywords = new HashSet<>();
        
        for (String word : words) {
            if (word.length() > 3 && !isCommonWord(word)) {
                keywords.add(word);
            }
        }
        
        // Create sorted key
        List<String> sortedKeywords = new ArrayList<>(keywords);
        Collections.sort(sortedKeywords);
        
        return String.join("_", sortedKeywords);
    }
    
    /**
     * Check if word is common (to exclude from cache key)
     */
    private boolean isCommonWord(String word) {
        Set<String> commonWords = new HashSet<>(Arrays.asList(
            "select", "from", "where", "order", "group", "having",
            "join", "left", "right", "inner", "outer", "with",
            "and", "or", "not", "in", "like", "between",
            "show", "list", "get", "find", "display", "return"
        ));
        
        return commonWords.contains(word);
    }
}
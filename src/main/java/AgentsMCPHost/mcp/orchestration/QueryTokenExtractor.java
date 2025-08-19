package AgentsMCPHost.mcp.orchestration;

import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

/**
 * QueryTokenExtractor - Extracts potential keywords from user queries.
 * Zero schema knowledge - works with any domain.
 * Pure function, no state, thread-safe.
 * 
 * Example:
 * "Show pending deliveries from California" →
 * tokens: ["show", "pending", "deliveries", "california"]
 * entities: ["pending", "deliveries", "california"]
 * actions: ["show"]
 */
public class QueryTokenExtractor {
    
    // Common English stop words to filter out
    private static final Set<String> STOP_WORDS = new HashSet<>(Arrays.asList(
        "a", "an", "and", "are", "as", "at", "be", "been", "by", "for", "from",
        "has", "had", "have", "he", "in", "is", "it", "its", "of", "on", "that",
        "the", "to", "was", "will", "with", "what", "where", "when", "which",
        "who", "why", "how", "all", "me", "my", "our", "their", "this", "these",
        "those", "there", "here", "i", "you", "we", "they", "them"
    ));
    
    // Common SQL/query action words
    private static final Set<String> ACTION_WORDS = new HashSet<>(Arrays.asList(
        "show", "list", "get", "find", "display", "select", "fetch", "retrieve",
        "search", "query", "give", "tell", "count", "sum", "average", "max",
        "min", "calculate", "compute", "return", "provide", "identify", "locate"
    ));
    
    // Common filter/condition words
    private static final Set<String> CONDITION_WORDS = new HashSet<>(Arrays.asList(
        "where", "having", "between", "like", "equals", "greater", "less", "than",
        "above", "below", "over", "under", "before", "after", "during", "since",
        "until", "within", "outside", "matching", "containing", "starting", "ending"
    ));
    
    // Pattern to extract quoted strings
    private static final Pattern QUOTED_PATTERN = Pattern.compile("'([^']+)'|\"([^\"]+)\"");
    
    // Pattern to extract numbers (including decimals and negatives)
    private static final Pattern NUMBER_PATTERN = Pattern.compile("-?\\d+(\\.\\d+)?");
    
    // Pattern to extract potential identifiers (alphanumeric with underscores)
    private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("\\b[a-zA-Z][a-zA-Z0-9_]*\\b");
    
    /**
     * Main extraction method - returns comprehensive token analysis
     */
    public static QueryTokens extract(String query) {
        if (query == null || query.trim().isEmpty()) {
            return new QueryTokens();
        }
        
        QueryTokens result = new QueryTokens();
        String normalizedQuery = query.toLowerCase().trim();
        
        // Extract quoted values first (they're explicit)
        result.quotedValues = extractQuotedValues(query);
        
        // Extract numeric values
        result.numericValues = extractNumericValues(normalizedQuery);
        
        // Extract all tokens
        result.allTokens = tokenize(normalizedQuery);
        
        // Classify tokens
        for (String token : result.allTokens) {
            classifyToken(token, result);
        }
        
        // Extract potential table/column references (multi-word)
        result.potentialCompounds = extractCompoundTerms(normalizedQuery);
        
        return result;
    }
    
    /**
     * Simple extraction for just potential entities
     */
    public static Set<String> extractPotentialEntities(String query) {
        QueryTokens tokens = extract(query);
        Set<String> entities = new HashSet<>();
        
        // Add all potential entities
        entities.addAll(tokens.potentialEntities);
        entities.addAll(tokens.quotedValues);
        entities.addAll(tokens.potentialCompounds);
        
        // Add tokens that aren't actions or conditions
        for (String token : tokens.unknownTokens) {
            if (!ACTION_WORDS.contains(token) && !CONDITION_WORDS.contains(token)) {
                entities.add(token);
            }
        }
        
        return entities;
    }
    
    /**
     * Extract quoted values from the query
     */
    private static Set<String> extractQuotedValues(String query) {
        Set<String> quoted = new HashSet<>();
        Matcher matcher = QUOTED_PATTERN.matcher(query);
        
        while (matcher.find()) {
            String value = matcher.group(1) != null ? matcher.group(1) : matcher.group(2);
            if (value != null && !value.isEmpty()) {
                quoted.add(value.toLowerCase());
            }
        }
        
        return quoted;
    }
    
    /**
     * Extract numeric values from the query
     */
    private static Set<String> extractNumericValues(String query) {
        Set<String> numbers = new HashSet<>();
        Matcher matcher = NUMBER_PATTERN.matcher(query);
        
        while (matcher.find()) {
            numbers.add(matcher.group());
        }
        
        return numbers;
    }
    
    /**
     * Tokenize the query into individual words
     */
    private static List<String> tokenize(String query) {
        // Remove quoted strings first to avoid tokenizing them
        String withoutQuotes = query.replaceAll(QUOTED_PATTERN.pattern(), " ");
        
        // Split on whitespace and punctuation (except underscore)
        return Arrays.stream(withoutQuotes.split("[\\s,;.!?()\\[\\]{}]+"))
            .map(String::toLowerCase)
            .filter(s -> !s.isEmpty())
            .collect(Collectors.toList());
    }
    
    /**
     * Classify a token into appropriate categories
     */
    private static void classifyToken(String token, QueryTokens result) {
        // Skip if it's a stop word
        if (STOP_WORDS.contains(token)) {
            return;
        }
        
        // Check if it's an action word
        if (ACTION_WORDS.contains(token)) {
            result.actionWords.add(token);
            return;
        }
        
        // Check if it's a condition word
        if (CONDITION_WORDS.contains(token)) {
            result.conditionWords.add(token);
            return;
        }
        
        // Check if it's numeric (already extracted)
        if (result.numericValues.contains(token)) {
            return;
        }
        
        // Check if it looks like a potential entity
        if (looksLikeEntity(token)) {
            result.potentialEntities.add(token);
        } else {
            result.unknownTokens.add(token);
        }
    }
    
    /**
     * Check if a token looks like it could be an entity name
     */
    private static boolean looksLikeEntity(String token) {
        // Entity names are typically:
        // - At least 2 characters
        // - Start with a letter
        // - Can contain letters, numbers, underscores
        // - Often plural or contain certain patterns
        
        if (token.length() < 2) {
            return false;
        }
        
        // Check if it matches identifier pattern
        if (IDENTIFIER_PATTERN.matcher(token).matches()) {
            // Additional heuristics for entity-like tokens
            return token.length() > 2 || // Longer tokens more likely entities
                   token.endsWith("s") || // Plural forms
                   token.contains("_") || // Database naming convention
                   Character.isUpperCase(token.charAt(0)); // Proper nouns
        }
        
        return false;
    }
    
    /**
     * Extract potential compound terms (multi-word entities)
     */
    private static Set<String> extractCompoundTerms(String query) {
        Set<String> compounds = new HashSet<>();
        
        // Look for patterns like "word word" that might be compound terms
        // This is a simple approach - could be enhanced with NLP
        String[] words = query.split("\\s+");
        
        for (int i = 0; i < words.length - 1; i++) {
            String word1 = words[i];
            String word2 = words[i + 1];
            
            // Skip if either is a stop word or action word
            if (STOP_WORDS.contains(word1) || STOP_WORDS.contains(word2) ||
                ACTION_WORDS.contains(word1) || CONDITION_WORDS.contains(word2)) {
                continue;
            }
            
            // Create compound
            String compound = word1 + "_" + word2;
            if (looksLikeEntity(word1) && looksLikeEntity(word2)) {
                compounds.add(compound);
            }
        }
        
        return compounds;
    }
    
    /**
     * Result class containing extracted tokens
     */
    public static class QueryTokens {
        public List<String> allTokens = new ArrayList<>();
        public Set<String> actionWords = new HashSet<>();
        public Set<String> conditionWords = new HashSet<>();
        public Set<String> potentialEntities = new HashSet<>();
        public Set<String> quotedValues = new HashSet<>();
        public Set<String> numericValues = new HashSet<>();
        public Set<String> unknownTokens = new HashSet<>();
        public Set<String> potentialCompounds = new HashSet<>();
        public String intent = "query";  // Default intent
        
        /**
         * Get a confidence score for this extraction
         */
        public double getConfidence() {
            double score = 0.0;
            
            // Higher confidence if we found clear entities
            if (!potentialEntities.isEmpty()) score += 0.3;
            
            // Higher confidence if we have quoted values (explicit)
            if (!quotedValues.isEmpty()) score += 0.3;
            
            // Higher confidence if we have clear actions
            if (!actionWords.isEmpty()) score += 0.2;
            
            // Higher confidence if we have numeric filters
            if (!numericValues.isEmpty()) score += 0.1;
            
            // Lower confidence if too many unknown tokens
            if (unknownTokens.size() > allTokens.size() / 2) score -= 0.2;
            
            return Math.max(0.0, Math.min(1.0, score));
        }
        
        /**
         * Add an entity to the collection
         */
        public void addEntity(String entity) {
            if (entity != null && !entity.trim().isEmpty()) {
                potentialEntities.add(entity.toLowerCase().trim());
            }
        }
        
        /**
         * Get all potential search terms (entities + quoted + compounds)
         */
        public Set<String> getAllSearchTerms() {
            Set<String> terms = new HashSet<>();
            terms.addAll(potentialEntities);
            terms.addAll(quotedValues);
            terms.addAll(potentialCompounds);
            terms.addAll(unknownTokens); // Include unknowns as potential terms
            return terms;
        }
        
        @Override
        public String toString() {
            return String.format(
                "QueryTokens[actions=%s, entities=%s, quoted=%s, numbers=%s, confidence=%.2f]",
                actionWords, potentialEntities, quotedValues, numericValues, getConfidence()
            );
        }
    }
}
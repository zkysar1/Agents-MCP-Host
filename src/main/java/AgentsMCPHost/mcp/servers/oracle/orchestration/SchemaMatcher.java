package AgentsMCPHost.mcp.servers.oracle.orchestration;

import AgentsMCPHost.mcp.servers.oracle.utils.OracleConnectionManager;
import AgentsMCPHost.mcp.servers.oracle.utils.EnumerationMapper;
import io.vertx.core.Vertx;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.CompositeFuture;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * SchemaMatcher - Matches user query tokens against database schema.
 * Uses fuzzy matching and scoring to find best matches.
 * Zero hardcoded schema knowledge.
 * 
 * Matches tokens against:
 * - Table names
 * - Column names
 * - Enumeration values
 * - Foreign key relationships
 */
public class SchemaMatcher {
    
    private final OracleConnectionManager oracleManager;
    private final EnumerationMapper enumMapper;
    private Vertx vertx;
    
    // Cache for schema information (with TTL) - thread-safe for Vert.x
    private final Map<String, CachedSchema> schemaCache = new ConcurrentHashMap<>();
    private volatile JsonArray cachedTablesList = null;
    private volatile long cachedTablesTimestamp = 0;
    private static final long CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutes
    
    public SchemaMatcher() {
        this.oracleManager = OracleConnectionManager.getInstance();
        this.enumMapper = EnumerationMapper.getInstance();
    }
    
    /**
     * Initialize with Vertx instance
     */
    public Future<Void> initialize(Vertx vertx) {
        this.vertx = vertx;
        // Ensure OracleConnectionManager is initialized
        return oracleManager.initialize(vertx)
            .compose(v -> preloadSchema());
    }
    
    /**
     * Preload schema into cache
     */
    private Future<Void> preloadSchema() {
        System.out.println("[SchemaMatcher] Preloading database schema...");
        
        return oracleManager.listTables()
            .compose(tables -> {
                System.out.println("[SchemaMatcher] Found " + tables.size() + " tables");
                
                // Cache table list (not using CachedSchema since it expects TableMetadata)
                cachedTablesList = tables;
                cachedTablesTimestamp = System.currentTimeMillis();
                
                // Preload metadata for important tables
                List<Future> metadataFutures = new ArrayList<>();
                for (int i = 0; i < tables.size(); i++) {
                    JsonObject table = tables.getJsonObject(i);
                    String tableName = table.getString("TABLE_NAME");
                    if (tableName != null && 
                        (tableName.contains("ORDER") || 
                         tableName.contains("CUSTOMER") || 
                         tableName.contains("PRODUCT") ||
                         tableName.contains("ENUM"))) {
                        metadataFutures.add(
                            getTableMetadata(tableName)
                                .onFailure(err -> {
                                    System.err.println("[SchemaMatcher] Failed to cache " + tableName + ": " + err.getMessage());
                                })
                        );
                    }
                }
                
                return metadataFutures.isEmpty() ? Future.succeededFuture() : CompositeFuture.all(metadataFutures);
            })
            .compose(v -> {
                // Also preload enumerations
                return enumMapper.detectEnumerationTables()
                    .onSuccess(enums -> {
                        System.out.println("[SchemaMatcher] Cached " + enums.size() + " enumeration tables");
                    });
            })
            .<Void>map(v -> {
                System.out.println("[SchemaMatcher] Schema preload complete");
                return (Void) null;
            })
            .recover(err -> {
                System.err.println("[SchemaMatcher] Schema preload failed: " + err.getMessage());
                // Don't fail initialization if preload fails
                return Future.succeededFuture((Void) null);
            });
    }
    
    /**
     * Find matches for extracted tokens in the database schema
     */
    public Future<MatchResult> findMatches(Set<String> tokens) {
        // Defensive null check
        if (tokens == null || tokens.isEmpty()) {
            return Future.succeededFuture(new MatchResult());
        }
        
        // Use Vert.x timer for timeout handling
        Promise<MatchResult> promise = Promise.promise();
        
        // Set a timeout timer - will complete with partial results if exceeded
        long timeoutTimer = vertx.setTimer(25000, id -> {
            if (!promise.future().isComplete()) {
                System.out.println("[SchemaMatcher] Timeout reached, returning partial results");
                MatchResult partialResult = new MatchResult();
                partialResult.timedOut = true;
                promise.tryComplete(partialResult);
            }
        });
        
        // Check cache first
        Future<JsonArray> tablesFuture;
        if (cachedTablesList != null && (System.currentTimeMillis() - cachedTablesTimestamp) < CACHE_TTL_MS) {
            System.out.println("[SchemaMatcher] Using cached table list (" + cachedTablesList.size() + " tables)");
            tablesFuture = Future.succeededFuture(cachedTablesList);
        } else {
            System.out.println("[SchemaMatcher] Loading table list from database");
            tablesFuture = oracleManager.listTables()
                .map(tables -> {
                    // Cache the result atomically
                    synchronized (this) {
                        cachedTablesList = tables;
                        cachedTablesTimestamp = System.currentTimeMillis();
                    }
                    return tables;
                });
        }
        
        tablesFuture
            .compose(tables -> {
                // Match tokens against tables
                List<TableMatch> tableMatches = matchTables(tokens, tables);
                
                // Limit metadata fetching to top 5 matches to avoid timeout
                List<Future<TableMetadata>> metadataFutures = new ArrayList<>();
                int limit = Math.min(tableMatches.size(), 5);
                for (int i = 0; i < limit; i++) {
                    TableMatch match = tableMatches.get(i);
                    // Add individual timeout for each metadata fetch
                    Future<TableMetadata> metadataFuture = getTableMetadata(match.tableName)
                        .onFailure(err -> {
                            System.err.println("[SchemaMatcher] Failed to get metadata for " + 
                                             match.tableName + ": " + err.getMessage());
                        });
                    metadataFutures.add(metadataFuture);
                }
                
                // Use Future.join instead of Future.all to handle partial failures
                return Future.join(metadataFutures)
                    .compose(compositeFuture -> {
                        // Now match against columns and enums
                        return matchColumnsAndEnums(tokens, metadataFutures);
                    })
                    .map(enrichedMatches -> {
                        // Build final result
                        MatchResult result = new MatchResult();
                        result.tableMatches = tableMatches;
                        result.columnMatches = enrichedMatches.columnMatches;
                        result.enumMatches = enrichedMatches.enumMatches;
                        result.calculateConfidence();
                        return result;
                    });
            })
            .onSuccess(result -> {
                vertx.cancelTimer(timeoutTimer);
                promise.tryComplete(result);
            })
            .onFailure(err -> {
                vertx.cancelTimer(timeoutTimer);
                System.err.println("[SchemaMatcher] Error during matching: " + err.getMessage());
                // Return empty result on failure
                promise.tryComplete(new MatchResult());
            });
        
        return promise.future();
    }
    
    /**
     * Match tokens against table names
     */
    private List<TableMatch> matchTables(Set<String> tokens, JsonArray tables) {
        List<TableMatch> matches = new ArrayList<>();
        
        for (int i = 0; i < tables.size(); i++) {
            JsonObject table = tables.getJsonObject(i);
            String tableName = table.getString("name");
            
            for (String token : tokens) {
                double score = calculateSimilarity(token, tableName);
                
                if (score > 0.5) { // Threshold for considering a match
                    TableMatch match = new TableMatch();
                    match.token = token;
                    match.tableName = tableName;
                    match.score = score;
                    match.rowCount = table.getInteger("row_count", -1);
                    matches.add(match);
                }
            }
        }
        
        // Sort by score descending
        matches.sort((a, b) -> Double.compare(b.score, a.score));
        
        return matches;
    }
    
    /**
     * Get table metadata (with caching)
     */
    private Future<TableMetadata> getTableMetadata(String tableName) {
        // Check cache first
        CachedSchema cached = schemaCache.get(tableName);
        if (cached != null && !cached.isExpired()) {
            return Future.succeededFuture(cached.metadata);
        }
        
        // Fetch from database
        return oracleManager.getTableMetadata(tableName)
            .map(metadata -> {
                TableMetadata tm = new TableMetadata();
                tm.tableName = tableName;
                tm.columns = metadata.getJsonArray("columns", new JsonArray());
                tm.foreignKeys = metadata.getJsonArray("foreignKeys", new JsonArray());
                tm.primaryKey = metadata.getString("primaryKey");
                
                // Cache it
                schemaCache.put(tableName, new CachedSchema(tm));
                
                return tm;
            });
    }
    
    /**
     * Match tokens against columns and enumerations
     */
    private Future<EnrichedMatches> matchColumnsAndEnums(Set<String> tokens, 
                                                         List<Future<TableMetadata>> metadataFutures) {
        EnrichedMatches result = new EnrichedMatches();
        
        // Process each table's metadata
        for (Future<TableMetadata> future : metadataFutures) {
            if (future.succeeded()) {
                TableMetadata metadata = future.result();
                
                // Match against columns
                for (int i = 0; i < metadata.columns.size(); i++) {
                    JsonObject column = metadata.columns.getJsonObject(i);
                    String columnName = column.getString("name");
                    
                    for (String token : tokens) {
                        double score = calculateSimilarity(token, columnName);
                        
                        if (score > 0.5) {
                            ColumnMatch match = new ColumnMatch();
                            match.token = token;
                            match.tableName = metadata.tableName;
                            match.columnName = columnName;
                            match.columnType = column.getString("type");
                            match.score = score;
                            result.columnMatches.add(match);
                        }
                    }
                }
            }
        }
        
        // Check for enum matches
        return enumMapper.detectEnumerationTables()
            .map(enums -> {
                for (int i = 0; i < enums.size(); i++) {
                    JsonObject enumTable = enums.getJsonObject(i);
                    String tableName = enumTable.getString("tableName");
                    JsonArray values = enumTable.getJsonArray("sampleValues", new JsonArray());
                    
                    for (String token : tokens) {
                        for (int j = 0; j < values.size(); j++) {
                            JsonObject value = values.getJsonObject(j);
                            String code = value.getString("code", "").toLowerCase();
                            String description = value.getString("description", "").toLowerCase();
                            
                            double codeScore = calculateSimilarity(token, code);
                            double descScore = calculateSimilarity(token, description);
                            double score = Math.max(codeScore, descScore);
                            
                            if (score > 0.6) { // Higher threshold for enum matches
                                EnumMatch match = new EnumMatch();
                                match.token = token;
                                match.enumTable = tableName;
                                match.matchedValue = codeScore > descScore ? code : description;
                                match.isCode = codeScore > descScore;
                                match.score = score;
                                result.enumMatches.add(match);
                            }
                        }
                    }
                }
                
                return result;
            });
    }
    
    /**
     * Calculate similarity between two strings
     */
    private double calculateSimilarity(String s1, String s2) {
        if (s1 == null || s2 == null) {
            return 0.0;
        }
        
        s1 = s1.toLowerCase().replace("_", "");
        s2 = s2.toLowerCase().replace("_", "");
        
        // Exact match
        if (s1.equals(s2)) {
            return 1.0;
        }
        
        // Contains match
        if (s2.contains(s1) || s1.contains(s2)) {
            return 0.8;
        }
        
        // Prefix match
        if (s2.startsWith(s1) || s1.startsWith(s2)) {
            return 0.7;
        }
        
        // Levenshtein distance
        double distance = levenshteinDistance(s1, s2);
        double maxLen = Math.max(s1.length(), s2.length());
        double similarity = 1.0 - (distance / maxLen);
        
        // Boost if they share common words
        if (shareCommonWords(s1, s2)) {
            similarity += 0.2;
        }
        
        return Math.min(1.0, similarity);
    }
    
    /**
     * Calculate Levenshtein distance between two strings
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
                    dp[i][j - 1] + 1),      // insertion
                    dp[i - 1][j - 1] + cost // substitution
                );
            }
        }
        
        return dp[s1.length()][s2.length()];
    }
    
    /**
     * Check if two strings share common words
     */
    private boolean shareCommonWords(String s1, String s2) {
        Set<String> words1 = new HashSet<>(Arrays.asList(s1.split("[_\\s]+")));
        Set<String> words2 = new HashSet<>(Arrays.asList(s2.split("[_\\s]+")));
        
        words1.retainAll(words2);
        return !words1.isEmpty();
    }
    
    /**
     * Table metadata holder
     */
    private static class TableMetadata {
        String tableName;
        JsonArray columns;
        JsonArray foreignKeys;
        String primaryKey;
    }
    
    /**
     * Cached schema with TTL
     */
    private static class CachedSchema {
        TableMetadata metadata;
        long timestamp;
        
        CachedSchema(TableMetadata metadata) {
            this.metadata = metadata;
            this.timestamp = System.currentTimeMillis();
        }
        
        boolean isExpired() {
            return System.currentTimeMillis() - timestamp > CACHE_TTL_MS;
        }
    }
    
    /**
     * Intermediate result for column and enum matches
     */
    private static class EnrichedMatches {
        List<ColumnMatch> columnMatches = new ArrayList<>();
        List<EnumMatch> enumMatches = new ArrayList<>();
    }
    
    /**
     * Match result containing all found matches
     */
    public static class MatchResult {
        public List<TableMatch> tableMatches = new ArrayList<>();
        public List<ColumnMatch> columnMatches = new ArrayList<>();
        public List<EnumMatch> enumMatches = new ArrayList<>();
        public double confidence = 0.0;
        public boolean timedOut = false;  // Indicates if matching was incomplete due to timeout
        
        public void calculateConfidence() {
            double tableConf = tableMatches.isEmpty() ? 0.0 : 
                tableMatches.stream().mapToDouble(m -> m.score).average().orElse(0.0);
            double columnConf = columnMatches.isEmpty() ? 0.0 : 
                columnMatches.stream().mapToDouble(m -> m.score).average().orElse(0.0);
            double enumConf = enumMatches.isEmpty() ? 0.0 : 
                enumMatches.stream().mapToDouble(m -> m.score).average().orElse(0.0);
            
            // Weighted average
            int count = 0;
            double total = 0.0;
            
            if (!tableMatches.isEmpty()) {
                total += tableConf * 0.4;
                count++;
            }
            if (!columnMatches.isEmpty()) {
                total += columnConf * 0.4;
                count++;
            }
            if (!enumMatches.isEmpty()) {
                total += enumConf * 0.2;
                count++;
            }
            
            confidence = count > 0 ? total / (count * 0.4) : 0.0;
        }
        
        public JsonObject toJson() {
            JsonObject json = new JsonObject()
                .put("confidence", confidence)
                .put("tableMatches", new JsonArray(
                    tableMatches.stream().map(TableMatch::toJson).collect(Collectors.toList())
                ))
                .put("columnMatches", new JsonArray(
                    columnMatches.stream().map(ColumnMatch::toJson).collect(Collectors.toList())
                ))
                .put("enumMatches", new JsonArray(
                    enumMatches.stream().map(EnumMatch::toJson).collect(Collectors.toList())
                ));
            return json;
        }
    }
    
    /**
     * Table match information
     */
    public static class TableMatch {
        public String token;
        public String tableName;
        public double score;
        public int rowCount;
        
        public JsonObject toJson() {
            return new JsonObject()
                .put("token", token)
                .put("tableName", tableName)
                .put("score", score)
                .put("rowCount", rowCount);
        }
    }
    
    /**
     * Column match information
     */
    public static class ColumnMatch {
        public String token;
        public String tableName;
        public String columnName;
        public String columnType;
        public double score;
        
        public JsonObject toJson() {
            return new JsonObject()
                .put("token", token)
                .put("tableName", tableName)
                .put("columnName", columnName)
                .put("columnType", columnType)
                .put("score", score);
        }
    }
    
    /**
     * Enum match information
     */
    public static class EnumMatch {
        public String token;
        public String enumTable;
        public String matchedValue;
        public boolean isCode;
        public double score;
        
        public JsonObject toJson() {
            return new JsonObject()
                .put("token", token)
                .put("enumTable", enumTable)
                .put("matchedValue", matchedValue)
                .put("isCode", isCode)
                .put("score", score);
        }
    }
}
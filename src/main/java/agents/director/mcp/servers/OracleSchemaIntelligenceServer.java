package agents.director.mcp.servers;

import agents.director.mcp.base.MCPServerBase;
import agents.director.mcp.base.MCPTool;
import agents.director.mcp.base.MCPResponse;
import agents.director.services.LlmAPIService;
import io.vertx.core.Promise;
import io.vertx.core.Future;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.web.RoutingContext;


import agents.director.services.OracleConnectionManager;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;

/**
 * MCP Server for intelligent schema matching and discovery.
 * Uses a two-step process: fuzzy matching followed by LLM verification.
 * Oracle-specific as it connects to Oracle database for schema information.
 * Deployed as a Worker Verticle due to blocking DB and LLM operations.
 */
public class OracleSchemaIntelligenceServer extends MCPServerBase {
    
    
    
    private OracleConnectionManager connectionManager;
    private LlmAPIService llmService;
    
    // Cache for schema metadata
    private Map<String, List<TableInfo>> schemaCache = new ConcurrentHashMap<>();
    private long cacheTimestamp = 0;
    private static final long CACHE_TTL = 300000; // 5 minutes
    
    // Inner classes for schema representation
    private static class TableInfo {
        String schema;
        String tableName;
        String comment;
        List<ColumnInfo> columns = new ArrayList<>();
        
        JsonObject toJson() {
            JsonObject json = new JsonObject()
                .put("schema", schema)
                .put("tableName", tableName);
            if (comment != null) json.put("comment", comment);
            
            JsonArray cols = new JsonArray();
            for (ColumnInfo col : columns) {
                cols.add(col.toJson());
            }
            json.put("columns", cols);
            return json;
        }
    }
    
    private static class ColumnInfo {
        String columnName;
        String dataType;
        int size;
        boolean nullable;
        String comment;
        
        JsonObject toJson() {
            JsonObject json = new JsonObject()
                .put("columnName", columnName)
                .put("dataType", dataType)
                .put("size", size)
                .put("nullable", nullable);
            if (comment != null) json.put("comment", comment);
            return json;
        }
    }
    
    public OracleSchemaIntelligenceServer() {
        super("OracleSchemaIntelligenceServer", "/mcp/servers/oracle-schema-intel");
    }
    
    @Override
    public void start(Promise<Void> startPromise) {
        // Get connection manager instance (already initialized in Driver)
        connectionManager = OracleConnectionManager.getInstance();
        
        // Check if connection manager is healthy
        if (!connectionManager.isConnectionHealthy()) {
            vertx.eventBus().publish("log", "Oracle Connection Manager not healthy - server will operate with limited functionality,1,OracleSchemaIntelligenceServer,MCP,System");
        } else {
            vertx.eventBus().publish("log", "OracleSchemaIntelligenceServer using connection pool,2,OracleSchemaIntelligenceServer,MCP,System");
        }
        
        // Initialize LLM service
        llmService = LlmAPIService.getInstance();
        
        // Continue with parent initialization regardless
        super.start(startPromise);
    }
    
    @Override
    protected void initializeTools() {
        // Register match_oracle_schema tool
        registerTool(new MCPTool(
            "match_oracle_schema",
            "Find Oracle tables/columns that best match the concepts in the query analysis.",
            new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("analysis", new JsonObject()
                        .put("type", "object")
                        .put("description", "Result from analyze_query or extracted query intent."))
                    .put("maxSuggestions", new JsonObject()
                        .put("type", "integer")
                        .put("description", "Maximum schema suggestions to return.")
                        .put("default", 5))
                    .put("confidenceThreshold", new JsonObject()
                        .put("type", "number")
                        .put("description", "Minimum confidence (0.0-1.0) for suggestions.")
                        .put("default", 0.5)))
                .put("required", new JsonArray().add("analysis"))
        ));
        
        // Register discover_column_semantics tool
        registerTool(new MCPTool(
            "discover_column_semantics",
            "Analyze given Oracle columns to determine semantic types or roles.",
            new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("table", new JsonObject()
                        .put("type", "string")
                        .put("description", "Table name containing the columns."))
                    .put("columns", new JsonObject()
                        .put("type", "array")
                        .put("items", new JsonObject().put("type", "string"))
                        .put("description", "List of column names to analyze."))
                    .put("sampleValues", new JsonObject()
                        .put("type", "integer")
                        .put("description", "Number of sample rows to analyze.")
                        .put("default", 50)))
                .put("required", new JsonArray().add("table").add("columns"))
        ));
        
        // Register infer_table_relationships tool
        registerTool(new MCPTool(
            "infer_table_relationships",
            "Infer relationships (FK/PK or logical) between multiple Oracle tables.",
            new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("tables", new JsonObject()
                        .put("type", "array")
                        .put("items", new JsonObject().put("type", "string"))
                        .put("description", "List of table names to check for relationships."))
                    .put("includeIndirect", new JsonObject()
                        .put("type", "boolean")
                        .put("description", "Include indirect relationships via join tables.")
                        .put("default", false)))
                .put("required", new JsonArray().add("tables"))
        ));
        
        // Register discover_sample_data tool
        registerTool(new MCPTool(
            "discover_sample_data",
            "Get sample data from tables to understand content and identify patterns.",
            new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("table_names", new JsonObject()
                        .put("type", "array")
                        .put("items", new JsonObject().put("type", "string"))
                        .put("description", "Tables to sample data from."))
                    .put("limit", new JsonObject()
                        .put("type", "integer")
                        .put("description", "Number of rows per table.")
                        .put("default", 5)))
                .put("required", new JsonArray().add("table_names"))
        ));
    }
    
    @Override
    protected void executeTool(RoutingContext ctx, String requestId, String toolName, JsonObject arguments) {
        switch (toolName) {
            case "match_oracle_schema":
                matchSchema(ctx, requestId, arguments);
                break;
            case "discover_column_semantics":
                discoverColumnSemantics(ctx, requestId, arguments);
                break;
            case "infer_table_relationships":
                inferTableRelationships(ctx, requestId, arguments);
                break;
            case "discover_sample_data":
                discoverSampleData(ctx, requestId, arguments);
                break;
            default:
                sendError(ctx, requestId, MCPResponse.ErrorCodes.METHOD_NOT_FOUND, 
                    "Unknown tool: " + toolName);
        }
    }
    
    /**
     * CRITICAL: Two-step matching process
     * Step 1: Fuzzy matching on schema elements
     * Step 2: LLM verification of fuzzy matches
     */
    private void matchSchema(RoutingContext ctx, String requestId, JsonObject arguments) {
        JsonObject analysis = arguments.getJsonObject("analysis");
        int maxSuggestions = arguments.getInteger("maxSuggestions", 5);
        double confidenceThreshold = arguments.getDouble("confidenceThreshold", 0.5);
        
        if (analysis == null) {
            sendError(ctx, requestId, MCPResponse.ErrorCodes.INVALID_PARAMS, "Analysis object is required");
            return;
        }
        
        executeBlocking(Promise.<JsonObject>promise(), promise -> {
            try {
                // Load schema if not cached
                loadSchemaIfNeeded();
                
                // Extract search terms from analysis
                List<String> searchTerms = extractSearchTerms(analysis);
                
                // Log the analysis object for debugging
                vertx.eventBus().publish("log", "Analysis object: " + analysis.encodePrettily() + ",3,OracleSchemaIntelligenceServer,MCP,System");
                
                // STEP 1: Fuzzy matching
                vertx.eventBus().publish("log", "Step 1: Performing fuzzy matching for terms: " + searchTerms + "" + ",2,OracleSchemaIntelligenceServer,MCP,System");
                List<SchemaMatch> fuzzyMatches = performFuzzyMatching(searchTerms);
                
                // STEP 2: LLM verification (if LLM is available)
                List<SchemaMatch> verifiedMatches;
                if (llmService.isInitialized() && !fuzzyMatches.isEmpty()) {
                    vertx.eventBus().publish("log", "Step 2: Verifying " + fuzzyMatches.size() + " fuzzy matches with LLM" + ",2,OracleSchemaIntelligenceServer,MCP,System");
                    verifiedMatches = verifyMatchesWithLLM(fuzzyMatches, analysis, searchTerms);
                } else {
                    verifiedMatches = fuzzyMatches;
                }
                
                // Filter by confidence and limit results
                List<SchemaMatch> finalMatches = verifiedMatches.stream()
                    .filter(m -> m.confidence >= confidenceThreshold)
                    .sorted((a, b) -> Double.compare(b.confidence, a.confidence))
                    .limit(maxSuggestions)
                    .collect(Collectors.toList());
                
                // FALLBACK: If LLM rejected all matches but we have fuzzy matches, use them with reduced confidence
                if (finalMatches.isEmpty() && !fuzzyMatches.isEmpty()) {
                    vertx.eventBus().publish("log", "LLM rejected all matches, using top fuzzy matches as fallback,2,OracleSchemaIntelligenceServer,MCP,System");
                    finalMatches = fuzzyMatches.stream()
                        .peek(match -> {
                            match.confidence *= 0.7; // Reduce confidence
                            match.reason += " (Fuzzy match fallback - LLM verification inconclusive)";
                        })
                        .sorted((a, b) -> Double.compare(b.confidence, a.confidence))
                        .limit(Math.min(3, maxSuggestions)) // Take top 3 fuzzy matches
                        .collect(Collectors.toList());
                }
                
                // Build response
                JsonObject result = new JsonObject();
                JsonArray matches = new JsonArray();
                
                for (SchemaMatch match : finalMatches) {
                    JsonObject matchJson = new JsonObject()
                        .put("table", match.table.toJson())
                        .put("relevantColumns", match.relevantColumns)
                        .put("confidence", match.confidence)
                        .put("matchReason", match.reason);
                    matches.add(matchJson);
                }
                
                result.put("matches", matches);
                result.put("searchTerms", new JsonArray(searchTerms));
                result.put("totalCandidates", fuzzyMatches.size());
                result.put("afterVerification", finalMatches.size());
                
                promise.complete(result);
                
            } catch (Exception e) {
                vertx.eventBus().publish("log", "Schema matching failed: " + e.getMessage() + ",0,OracleSchemaIntelligenceServer,MCP,System");
                promise.fail(e);
            }
        }, res -> {
            if (res.succeeded()) {
                sendSuccess(ctx, requestId, res.result());
            } else {
                sendError(ctx, requestId, MCPResponse.ErrorCodes.INTERNAL_ERROR, 
                    "Schema matching failed: " + res.cause().getMessage());
            }
        });
    }
    
    private void discoverColumnSemantics(RoutingContext ctx, String requestId, JsonObject arguments) {
        String tableName = arguments.getString("table");
        JsonArray columns = arguments.getJsonArray("columns");
        int sampleValues = arguments.getInteger("sampleValues", 50);
        
        if (tableName == null || columns == null || columns.isEmpty()) {
            sendError(ctx, requestId, MCPResponse.ErrorCodes.INVALID_PARAMS, 
                "Table name and columns are required");
            return;
        }
        
        executeBlocking(Promise.<JsonObject>promise(), promise -> {
            try {
                JsonObject result = new JsonObject();
                JsonArray semantics = new JsonArray();
                
                for (int i = 0; i < columns.size(); i++) {
                    String columnName = columns.getString(i);
                    JsonObject columnSemantics = analyzeColumnSemantics(
                        tableName, columnName, sampleValues);
                    semantics.add(columnSemantics);
                }
                
                result.put("table", tableName);
                result.put("columnSemantics", semantics);
                
                promise.complete(result);
                
            } catch (Exception e) {
                vertx.eventBus().publish("log", "Column semantics discovery failed" + ",0,OracleSchemaIntelligenceServer,MCP,System");
                promise.fail(e);
            }
        }, res -> {
            if (res.succeeded()) {
                sendSuccess(ctx, requestId, res.result());
            } else {
                sendError(ctx, requestId, MCPResponse.ErrorCodes.INTERNAL_ERROR, 
                    "Column semantics discovery failed: " + res.cause().getMessage());
            }
        });
    }
    
    private void inferTableRelationships(RoutingContext ctx, String requestId, JsonObject arguments) {
        JsonArray tables = arguments.getJsonArray("tables");
        boolean includeIndirect = arguments.getBoolean("includeIndirect", false);
        
        if (tables == null || tables.isEmpty()) {
            sendError(ctx, requestId, MCPResponse.ErrorCodes.INVALID_PARAMS, "Tables list is required");
            return;
        }
        
        executeBlocking(Promise.<JsonObject>promise(), promise -> {
            try {
                JsonObject result = new JsonObject();
                JsonArray relationships = new JsonArray();
                
                // Get foreign key relationships
                for (int i = 0; i < tables.size(); i++) {
                    String table1 = tables.getString(i);
                    for (int j = i + 1; j < tables.size(); j++) {
                        String table2 = tables.getString(j);
                        
                        List<JsonObject> rels = findRelationships(table1, table2, includeIndirect);
                        relationships.addAll(new JsonArray(rels));
                    }
                }
                
                result.put("relationships", relationships);
                result.put("tables", tables);
                
                promise.complete(result);
                
            } catch (Exception e) {
                vertx.eventBus().publish("log", "Table relationship inference failed" + ",0,OracleSchemaIntelligenceServer,MCP,System");
                promise.fail(e);
            }
        }, res -> {
            if (res.succeeded()) {
                sendSuccess(ctx, requestId, res.result());
            } else {
                sendError(ctx, requestId, MCPResponse.ErrorCodes.INTERNAL_ERROR, 
                    "Relationship inference failed: " + res.cause().getMessage());
            }
        });
    }
    
    // Helper methods for schema matching
    
    private void loadSchemaIfNeeded() throws Exception {
        long now = System.currentTimeMillis();
        
        // Check if cache is still valid
        if (!schemaCache.isEmpty() && (now - cacheTimestamp < CACHE_TTL)) {
            return;
        }
        
        vertx.eventBus().publish("log", "Loading Oracle schema metadata" + ",2,OracleSchemaIntelligenceServer,MCP,System");
        
        // Check connection health first
        if (!connectionManager.isConnectionHealthy()) {
            throw new RuntimeException("Oracle connection is not healthy");
        }
        
        try {
            Map<String, List<TableInfo>> newCache = connectionManager.executeWithConnection(conn -> {
                try {
                    Map<String, List<TableInfo>> tempCache = new HashMap<>();
                    DatabaseMetaData metaData = conn.getMetaData();
                    String currentSchema = conn.getSchema();
                    
                    // Get all tables
                    ResultSet tablesRs = metaData.getTables(null, currentSchema, "%", new String[]{"TABLE"});
                    
                    while (tablesRs.next()) {
                        TableInfo table = new TableInfo();
                        table.schema = tablesRs.getString("TABLE_SCHEM");
                        table.tableName = tablesRs.getString("TABLE_NAME");
                        table.comment = tablesRs.getString("REMARKS");
                        
                        // Get columns for this table
                        ResultSet columnsRs = metaData.getColumns(null, table.schema, table.tableName, "%");
                        while (columnsRs.next()) {
                            ColumnInfo column = new ColumnInfo();
                            column.columnName = columnsRs.getString("COLUMN_NAME");
                            column.dataType = columnsRs.getString("TYPE_NAME");
                            column.size = columnsRs.getInt("COLUMN_SIZE");
                            column.nullable = "YES".equals(columnsRs.getString("IS_NULLABLE"));
                            column.comment = columnsRs.getString("REMARKS");
                            table.columns.add(column);
                        }
                        columnsRs.close();
                        
                        // Add to cache
                        tempCache.computeIfAbsent(table.schema, k -> new ArrayList<>()).add(table);
                    }
                    tablesRs.close();
                    
                    return tempCache;
                } catch (SQLException e) {
                    throw new RuntimeException("Failed to load schema metadata: " + e.getMessage(), e);
                }
            }).toCompletionStage().toCompletableFuture().get();
            
            schemaCache.clear();
            schemaCache.putAll(newCache);
            cacheTimestamp = System.currentTimeMillis();
            vertx.eventBus().publish("log", "Loaded " + schemaCache.size() + " schemas with tables" + ",2,OracleSchemaIntelligenceServer,MCP,System");
        } catch (Exception e) {
            vertx.eventBus().publish("log", "Failed to load schema: " + e.getMessage() + ",0,OracleSchemaIntelligenceServer,MCP,System");
            throw e;
        }
    }
    
    private List<String> extractSearchTerms(JsonObject analysis) {
        Set<String> terms = new HashSet<>();
        
        // Extract from entities
        JsonArray entities = analysis.getJsonArray("entities");
        if (entities != null) {
            for (int i = 0; i < entities.size(); i++) {
                terms.add(entities.getString(i).toLowerCase());
            }
        }
        
        // Extract from original query tokens
        String originalQuery = analysis.getString("originalQuery", "");
        String[] words = originalQuery.toLowerCase().split("\\s+");
        for (String word : words) {
            if (word.length() > 2 && !isCommonWord(word)) {
                // Add the original term
                terms.add(word);
                // Also add mapped equivalent if different
                String mapped = mapTermToDbEquivalent(word);
                if (!mapped.equals(word)) {
                    terms.add(mapped);
                }
            }
        }
        
        return new ArrayList<>(terms);
    }
    
    private List<SchemaMatch> performFuzzyMatching(List<String> searchTerms) {
        List<SchemaMatch> matches = new ArrayList<>();
        
        if (schemaCache.isEmpty()) {
            vertx.eventBus().publish("log", "Warning: Schema cache is empty during fuzzy matching,1,OracleSchemaIntelligenceServer,MCP,System");
            return matches;
        }
        
        vertx.eventBus().publish("log", "Performing fuzzy matching on " + schemaCache.size() + " schemas,3,OracleSchemaIntelligenceServer,MCP,System");
        
        for (List<TableInfo> tables : schemaCache.values()) {
            for (TableInfo table : tables) {
                double tableScore = 0;
                List<String> matchedColumns = new ArrayList<>();
                String matchReason = "";
                
                // Check table name match
                for (String term : searchTerms) {
                    double similarity = calculateSimilarity(table.tableName.toLowerCase(), term);
                    if (similarity > 0.6) {
                        tableScore = Math.max(tableScore, similarity);
                        matchReason = "Table name matches '" + term + "'";
                    }
                    
                    // Log potential matches for debugging
                    if (term.contains("order") && table.tableName.toLowerCase().contains("order")) {
                        vertx.eventBus().publish("log", "Found potential order table: " + table.tableName + " (similarity: " + similarity + "),3,OracleSchemaIntelligenceServer,MCP,System");
                    }
                }
                
                // Check column matches
                for (ColumnInfo column : table.columns) {
                    for (String term : searchTerms) {
                        double similarity = calculateSimilarity(column.columnName.toLowerCase(), term);
                        
                        // Special handling for common variations
                        if (term.equals("state") && column.columnName.toLowerCase().contains("province")) {
                            similarity = 0.9; // High similarity for state->province mapping
                        } else if (term.equals("zip") && column.columnName.toLowerCase().contains("postal")) {
                            similarity = 0.9; // High similarity for zip->postal mapping
                        }
                        
                        if (similarity > 0.6) {
                            matchedColumns.add(column.columnName);
                            tableScore = Math.max(tableScore, similarity * 0.8); // Slightly lower weight for column match
                            if (matchReason.isEmpty()) {
                                matchReason = "Column '" + column.columnName + "' matches '" + term + "'";
                                if (term.equals("state") && column.columnName.toLowerCase().contains("province")) {
                                    matchReason += " (state maps to province)";
                                }
                            }
                        }
                    }
                }
                
                if (tableScore > 0) {
                    SchemaMatch match = new SchemaMatch();
                    match.table = table;
                    match.relevantColumns = new JsonArray(matchedColumns);
                    match.confidence = tableScore;
                    match.reason = matchReason;
                    matches.add(match);
                }
            }
        }
        
        return matches;
    }
    
    private List<SchemaMatch> verifyMatchesWithLLM(List<SchemaMatch> fuzzyMatches, 
                                                   JsonObject analysis, 
                                                   List<String> searchTerms) {
        try {
            // Prepare LLM prompt
            String systemPrompt = """
                You are a database schema expert. Your task is to verify and re-rank schema matches.
                Given a user's query analysis and a list of potential table matches from fuzzy matching,
                determine which tables are actually relevant and assign confidence scores (0.0-1.0).
                
                Consider:
                1. Semantic relevance to the user's intent
                2. Column names that would contain the requested data
                3. Common database naming patterns
                4. Likely join relationships
                5. IMPORTANT: Tables may still be relevant even if they don't contain all filter columns
                   (e.g., ORDERS table is relevant for "orders in California" even if location is in CUSTOMERS)
                
                Be INCLUSIVE rather than exclusive - if a table contains the main entity (orders, customers, etc.)
                it should have high confidence even if filters require joins to other tables.
                
                Respond with a JSON array of matches with adjusted confidence scores and reasons.
                """;
            
            JsonObject promptData = new JsonObject()
                .put("userIntent", analysis.getString("intent", ""))
                .put("queryType", analysis.getString("queryType", ""))
                .put("searchTerms", new JsonArray(searchTerms));
            
            JsonArray matchesForLLM = new JsonArray();
            for (SchemaMatch match : fuzzyMatches) {
                matchesForLLM.add(new JsonObject()
                    .put("tableName", match.table.tableName)
                    .put("columns", new JsonArray(
                        match.table.columns.stream()
                            .map(c -> c.columnName)
                            .collect(Collectors.toList())
                    ))
                    .put("fuzzyScore", match.confidence)
                    .put("fuzzyReason", match.reason));
            }
            promptData.put("fuzzyMatches", matchesForLLM);
            
            List<JsonObject> messages = Arrays.asList(
                new JsonObject().put("role", "system").put("content", systemPrompt),
                new JsonObject().put("role", "user").put("content", promptData.encode())
            );
            
            // Call LLM
            JsonObject llmResponse = llmService.chatCompletion(
                messages.stream().map(JsonObject::encode).collect(Collectors.toList()),
                0.0, // temperature
                1000 // max tokens
            ).join();
            
            String content = llmResponse.getJsonArray("choices")
                .getJsonObject(0)
                .getJsonObject("message")
                .getString("content");
            
            // Parse LLM response and update matches
            JsonArray verifiedMatches = parseJsonArray(content);
            
            // Update confidence scores based on LLM feedback
            Map<String, Double> llmScores = new HashMap<>();
            for (int i = 0; i < verifiedMatches.size(); i++) {
                JsonObject verified = verifiedMatches.getJsonObject(i);
                String tableName = verified.getString("tableName");
                Double confidence = verified.getDouble("confidence", 0.0);
                llmScores.put(tableName.toLowerCase(), confidence);
            }
            
            // Update original matches with LLM scores
            for (SchemaMatch match : fuzzyMatches) {
                Double llmScore = llmScores.get(match.table.tableName.toLowerCase());
                if (llmScore != null) {
                    match.confidence = llmScore;
                    match.reason += " (LLM verified)";
                } else {
                    match.confidence *= 0.5; // Reduce confidence if LLM didn't verify
                }
            }
            
        } catch (Exception e) {
            vertx.eventBus().publish("log", "LLM verification failed, using fuzzy matches only" + ",1,OracleSchemaIntelligenceServer,MCP,System");
        }
        
        return fuzzyMatches;
    }
    
    // Fuzzy string matching using Levenshtein distance
    private double calculateSimilarity(String s1, String s2) {
        int maxLen = Math.max(s1.length(), s2.length());
        if (maxLen == 0) return 1.0;
        
        int distance = levenshteinDistance(s1, s2);
        return 1.0 - (double) distance / maxLen;
    }
    
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
                dp[i][j] = Math.min(
                    Math.min(dp[i - 1][j] + 1, dp[i][j - 1] + 1),
                    dp[i - 1][j - 1] + cost
                );
            }
        }
        
        return dp[s1.length()][s2.length()];
    }
    
    private boolean isCommonWord(String word) {
        Set<String> commonWords = Set.of("the", "a", "an", "and", "or", "but", "in", "on", "at", 
                                        "to", "for", "of", "with", "by", "from", "is", "are", 
                                        "was", "were", "been", "have", "has", "had");
        return commonWords.contains(word);
    }
    
    /**
     * Maps common user terms to their database equivalents
     */
    private String mapTermToDbEquivalent(String term) {
        Map<String, String> termMappings = new HashMap<>();
        termMappings.put("state", "province");
        termMappings.put("zip", "postal");
        termMappings.put("zipcode", "postal_code");
        termMappings.put("phone", "tel");
        termMappings.put("cell", "mobile");
        
        String lowerTerm = term.toLowerCase();
        return termMappings.getOrDefault(lowerTerm, lowerTerm);
    }
    
    private JsonObject analyzeColumnSemantics(String tableName, String columnName, int sampleSize) 
            throws Exception {
        try {
            return connectionManager.executeWithConnection(conn -> {
                try {
                    JsonObject semantics = new JsonObject()
                        .put("column", columnName)
                        .put("table", tableName);
                    
                    // Get column metadata
                    DatabaseMetaData metaData = conn.getMetaData();
                    ResultSet rs = metaData.getColumns(null, conn.getSchema(), tableName, columnName);
                    
                    if (rs.next()) {
                        semantics.put("dataType", rs.getString("TYPE_NAME"));
                        semantics.put("size", rs.getInt("COLUMN_SIZE"));
                    }
                    rs.close();
                    
                    // Sample data analysis
                    String sql = String.format(
                        "SELECT DISTINCT %s FROM %s WHERE %s IS NOT NULL AND ROWNUM <= %d",
                        columnName, tableName, columnName, sampleSize
                    );
                    
                    Statement stmt = conn.createStatement();
                    ResultSet dataRs = stmt.executeQuery(sql);
                    
                    List<String> samples = new ArrayList<>();
                    while (dataRs.next() && samples.size() < 10) {
                        samples.add(dataRs.getString(1));
                    }
                    dataRs.close();
                    stmt.close();
                    
                    // Detect patterns
                    JsonArray patterns = new JsonArray();
                    if (samples.stream().allMatch(s -> s.matches("\\d+"))) {
                        patterns.add("numeric_id");
                    }
                    if (samples.stream().allMatch(s -> s.matches("[A-Z0-9]+"))) {
                        patterns.add("code");
                    }
                    if (samples.stream().allMatch(s -> s.contains("@"))) {
                        patterns.add("email");
                    }
                    if (samples.stream().allMatch(s -> s.matches("\\d{4}-\\d{2}-\\d{2}"))) {
                        patterns.add("date");
                    }
                    
                    semantics.put("detectedPatterns", patterns);
                    semantics.put("sampleValues", new JsonArray(samples.subList(0, Math.min(5, samples.size()))));
                    
                    return semantics;
                } catch (SQLException e) {
                    throw new RuntimeException("Failed to analyze column semantics", e);
                }
            }).toCompletionStage().toCompletableFuture().get();
        } catch (Exception e) {
            throw new Exception("Failed to analyze column semantics: " + e.getMessage(), e);
        }
    }
    
    private List<JsonObject> findRelationships(String table1, String table2, boolean includeIndirect) 
            throws Exception {
        try {
            return connectionManager.executeWithConnection(conn -> {
                try {
                    List<JsonObject> relationships = new ArrayList<>();
                    
                    // Check foreign keys
                    DatabaseMetaData metaData = conn.getMetaData();
                    
                    // Check FK from table1 to table2
                    ResultSet fks = metaData.getExportedKeys(null, conn.getSchema(), table2);
                    while (fks.next()) {
                        if (table1.equalsIgnoreCase(fks.getString("FKTABLE_NAME"))) {
                            JsonObject rel = new JsonObject()
                                .put("type", "foreign_key")
                                .put("fromTable", table1)
                                .put("fromColumn", fks.getString("FKCOLUMN_NAME"))
                                .put("toTable", table2)
                                .put("toColumn", fks.getString("PKCOLUMN_NAME"));
                            relationships.add(rel);
                        }
                    }
                    fks.close();
                    
                    // Check FK from table2 to table1
                    fks = metaData.getExportedKeys(null, conn.getSchema(), table1);
                    while (fks.next()) {
                        if (table2.equalsIgnoreCase(fks.getString("FKTABLE_NAME"))) {
                            JsonObject rel = new JsonObject()
                                .put("type", "foreign_key")
                                .put("fromTable", table2)
                                .put("fromColumn", fks.getString("FKCOLUMN_NAME"))
                                .put("toTable", table1)
                                .put("toColumn", fks.getString("PKCOLUMN_NAME"));
                            relationships.add(rel);
                        }
                    }
                    fks.close();
                    
                    return relationships;
                } catch (SQLException e) {
                    throw new RuntimeException("Failed to find relationships", e);
                }
            }).toCompletionStage().toCompletableFuture().get();
        } catch (Exception e) {
            throw new Exception("Failed to find relationships: " + e.getMessage(), e);
        }
    }
    
    private JsonArray parseJsonArray(String content) {
        try {
            int start = content.indexOf("[");
            int end = content.lastIndexOf("]");
            if (start >= 0 && end > start) {
                return new JsonArray(content.substring(start, end + 1));
            }
        } catch (Exception e) {
            vertx.eventBus().publish("log", "Failed to parse JSON array from content" + ",1,OracleSchemaIntelligenceServer,MCP,System");
        }
        return new JsonArray();
    }
    
    /**
     * Discover sample data from specified tables
     */
    private void discoverSampleData(RoutingContext ctx, String requestId, JsonObject arguments) {
        JsonArray tableNames = arguments.getJsonArray("table_names");
        int limit = arguments.getInteger("limit", 5);
        
        if (tableNames == null || tableNames.isEmpty()) {
            sendError(ctx, requestId, MCPResponse.ErrorCodes.INVALID_PARAMS, "table_names array is required");
            return;
        }
        
        JsonObject result = new JsonObject();
        JsonObject samples = new JsonObject();
        
        // Process each table
        List<Future<Void>> futures = new ArrayList<>();
        
        for (int i = 0; i < tableNames.size(); i++) {
            String tableName = tableNames.getString(i);
            
            // Validate table name to prevent SQL injection
            if (!isValidTableName(tableName)) {
                vertx.eventBus().publish("log", "Invalid table name: " + tableName + ",1,OracleSchemaIntelligenceServer,MCP,System");
                samples.put(tableName, new JsonObject()
                    .put("error", "Invalid table name format"));
                continue;
            }
            
            // Use connection manager to query table
            String query = "SELECT * FROM " + tableName + " WHERE ROWNUM <= " + limit;
            Future<Void> tableFuture = connectionManager.executeQuery(query)
                .onSuccess(rows -> {
                    // Extract column info from first row
                    JsonArray columns = new JsonArray();
                    if (!rows.isEmpty()) {
                        JsonObject firstRow = rows.getJsonObject(0);
                        for (String columnName : firstRow.fieldNames()) {
                            columns.add(new JsonObject()
                                .put("name", columnName)
                                .put("type", "UNKNOWN")); // Connection manager doesn't provide type info
                        }
                    }
                    
                    samples.put(tableName, new JsonObject()
                        .put("columns", columns)
                        .put("rows", rows)
                        .put("rowCount", rows.size()));
                })
                .onFailure(err -> {
                    vertx.eventBus().publish("log", "Failed to get sample data for table " + tableName + ": " + err.getMessage() + ",1,OracleSchemaIntelligenceServer,MCP,System");
                    samples.put(tableName, new JsonObject()
                        .put("error", err.getMessage()));
                })
                .mapEmpty(); // Convert to Future<Void>
            
            futures.add(tableFuture);
        }
        
        // Wait for all queries to complete
        CompletableFuture.allOf(futures.stream()
            .map(f -> f.toCompletionStage().toCompletableFuture())
            .toArray(CompletableFuture[]::new))
            .whenComplete((v, error) -> {
                result.put("samples", samples);
                result.put("tablesProcessed", samples.fieldNames().size());
                
                if (error == null) {
                    sendSuccess(ctx, requestId, result);
                } else {
                    sendError(ctx, requestId, MCPResponse.ErrorCodes.INTERNAL_ERROR, 
                        "Sample data discovery failed: " + error.getMessage());
                }
            });
    }
    
    /**
     * Validate table name to prevent SQL injection
     */
    private boolean isValidTableName(String tableName) {
        // Allow schema.table format
        if (tableName == null || tableName.trim().isEmpty()) {
            return false;
        }
        
        // Oracle naming rules: letters, numbers, underscore, $, #
        // Can have schema prefix with dot
        String pattern = "^[A-Za-z_$#][A-Za-z0-9_$#]*(\\.[A-Za-z_$#][A-Za-z0-9_$#]*)?$";
        return tableName.matches(pattern);
    }
    
    
    @Override
    public void stop(Promise<Void> stopPromise) {
        // Connection manager handles its own lifecycle
        // Just call parent stop
        try { 
            super.stop(stopPromise); 
        } catch (Exception e) { 
            stopPromise.fail(e); 
        }
    }
    
    // Inner class for schema matches
    private static class SchemaMatch {
        TableInfo table;
        JsonArray relevantColumns;
        double confidence;
        String reason;
    }
    
    /**
     * Get deployment options for this server (Worker Verticle)
     */
    public static DeploymentOptions getDeploymentOptions() {
        return new DeploymentOptions()
            .setWorker(true)
            .setWorkerPoolSize(4);
    }
}
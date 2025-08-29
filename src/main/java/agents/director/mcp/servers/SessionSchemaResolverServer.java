package agents.director.mcp.servers;

import agents.director.mcp.base.MCPServerBase;
import agents.director.mcp.base.MCPTool;
import agents.director.mcp.base.MCPResponse;
import agents.director.services.OracleConnectionManager;
import io.vertx.core.Promise;
import io.vertx.core.Future;
import io.vertx.core.CompositeFuture;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.web.RoutingContext;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * MCP Server for session-scoped Oracle schema resolution.
 * Provides lightning-fast schema discovery and resolution within a single session.
 * All learned intelligence is scoped to the session and discarded when it ends.
 */
public class SessionSchemaResolverServer extends MCPServerBase {
    
    private OracleConnectionManager connectionManager;
    
    // Session-scoped data structures
    private final Map<String, SessionIntelligence> sessionData = new ConcurrentHashMap<>();
    
    // Common Oracle schema patterns
    private static final Map<String, String> TABLE_PATTERNS = Map.of(
        "^(GL_|XLA_)", "GL",           // General Ledger
        "^(AP_|APX_)", "AP",           // Accounts Payable  
        "^(AR_|ARX_)", "AR",           // Accounts Receivable
        "^(HR_|PER_|PAY_)", "HR",      // Human Resources
        "^(MTL_|INV_)", "INV",         // Inventory
        "^(PO_|POX_)", "PO",           // Purchase Orders
        "^(OE_|ONT_)", "ONT",          // Order Management
        "^(FA_|FAX_)", "FA",           // Fixed Assets
        "^(WIP_)", "WIP",              // Work in Process
        "^(BOM_)", "BOM"               // Bills of Material
    );
    
    // Session intelligence container
    private static class SessionIntelligence {
        final String sessionId;
        final Map<String, String> schemaCache = new ConcurrentHashMap<>();
        final Map<String, Float> schemaAffinities = new ConcurrentHashMap<>();
        final Set<String> availableSchemas = ConcurrentHashMap.newKeySet();
        final Map<String, Integer> schemaUsageCount = new ConcurrentHashMap<>();
        String dominantSchema = null;
        long createdAt = System.currentTimeMillis();
        boolean discoveryComplete = false;
        
        SessionIntelligence(String sessionId) {
            this.sessionId = sessionId;
        }
        
        void recordSuccess(String tableName, String schema) {
            schemaCache.put(tableName.toUpperCase(), schema);
            schemaAffinities.merge(schema, 0.2f, (old, inc) -> Math.min(old + inc, 1.0f));
            schemaUsageCount.merge(schema, 1, Integer::sum);
            
            // Update dominant schema if this one is used frequently
            if (schemaUsageCount.get(schema) >= 3) {
                dominantSchema = schema;
            }
        }
        
        String getMostLikelySchema() {
            if (dominantSchema != null) {
                return dominantSchema;
            }
            
            return schemaAffinities.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse(null);
        }
    }
    
    public SessionSchemaResolverServer() {
        super("SessionSchemaResolverServer", "/mcp/servers/session-schema-resolver");
    }
    
    @Override
    protected void onServerReady() {
        connectionManager = OracleConnectionManager.getInstance();
        
        // Listen for session lifecycle events
        vertx.eventBus().<JsonObject>consumer("session.schema.resolver.init", msg -> {
            String sessionId = msg.body().getString("sessionId");
            initializeSession(sessionId);
        });
        
        vertx.eventBus().<JsonObject>consumer("session.schema.resolver.cleanup", msg -> {
            String sessionId = msg.body().getString("sessionId");
            cleanupSession(sessionId);
        });
    }
    
    @Override
    protected void initializeTools() {
        // Main resolution tool
        registerTool(new MCPTool(
            "resolve_table_schema",
            "Resolve the correct schema prefix for a table name within the current session",
            new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("tableName", new JsonObject()
                        .put("type", "string")
                        .put("description", "The table name to resolve"))
                    .put("sessionId", new JsonObject()
                        .put("type", "string")
                        .put("description", "The current session ID"))
                    .put("queryContext", new JsonObject()
                        .put("type", "object")
                        .put("description", "Optional context about the query")
                        .put("properties", new JsonObject()
                            .put("previousTables", new JsonObject()
                                .put("type", "array")
                                .put("items", new JsonObject().put("type", "string"))
                                .put("description", "Tables referenced earlier in session"))
                            .put("queryType", new JsonObject()
                                .put("type", "string")
                                .put("description", "Type of query being executed")))))
                .put("required", new JsonArray().add("tableName").add("sessionId"))
        ));
        
        // Discovery tool
        registerTool(new MCPTool(
            "discover_available_schemas",
            "Discover all accessible schemas for the session (run once at session start)",
            new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("sessionId", new JsonObject()
                        .put("type", "string")
                        .put("description", "The current session ID")))
                .put("required", new JsonArray().add("sessionId"))
        ));
        
        // Learning tool
        registerTool(new MCPTool(
            "learn_from_success",
            "Record a successful schema resolution to improve future guesses in this session",
            new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("tableName", new JsonObject()
                        .put("type", "string")
                        .put("description", "The table that was successfully accessed"))
                    .put("schema", new JsonObject()
                        .put("type", "string")
                        .put("description", "The schema where it was found"))
                    .put("sessionId", new JsonObject()
                        .put("type", "string")
                        .put("description", "The current session ID")))
                .put("required", new JsonArray().add("tableName").add("schema").add("sessionId"))
        ));
        
        // Debug tool
        registerTool(new MCPTool(
            "get_session_patterns",
            "Get learned patterns and statistics for the current session",
            new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("sessionId", new JsonObject()
                        .put("type", "string")
                        .put("description", "The current session ID")))
                .put("required", new JsonArray().add("sessionId"))
        ));
    }
    
    @Override
    protected void executeTool(RoutingContext ctx, String requestId, String toolName, JsonObject arguments) {
        switch (toolName) {
            case "resolve_table_schema":
                resolveTableSchema(ctx, requestId, arguments);
                break;
            case "discover_available_schemas":
                discoverSchemas(ctx, requestId, arguments);
                break;
            case "learn_from_success":
                learnFromSuccess(ctx, requestId, arguments);
                break;
            case "get_session_patterns":
                getSessionPatterns(ctx, requestId, arguments);
                break;
            default:
                sendError(ctx, requestId, MCPResponse.ErrorCodes.METHOD_NOT_FOUND, 
                    "Unknown tool: " + toolName);
        }
    }
    
    private void resolveTableSchema(RoutingContext ctx, String requestId, JsonObject arguments) {
        String tableName = arguments.getString("tableName");
        String sessionId = arguments.getString("sessionId");
        JsonObject queryContext = arguments.getJsonObject("queryContext", new JsonObject());
        
        // Execute as blocking operation
        executeBlocking(Promise.<JsonObject>promise(), promise -> {
            try {
                SessionIntelligence session = getOrCreateSession(sessionId);
                
                // 1. Check cache first (FAST)
                String cached = session.schemaCache.get(tableName.toUpperCase());
                if (cached != null) {
                    vertx.eventBus().publish("log", "Schema cache hit for " + tableName + ": " + cached + ",3,SessionSchemaResolver,Discovery,Cache");
                    promise.complete(new JsonObject()
                        .put("schema", cached)
                        .put("confidence", 1.0)
                        .put("method", "cache"));
                    return;
                }
                
                // 2. Try pattern-based guess (FAST)
                String patternGuess = guessSchemaFromPattern(tableName);
                if (patternGuess != null && quickVerifyTable(patternGuess, tableName)) {
                    session.recordSuccess(tableName, patternGuess);
                    promise.complete(new JsonObject()
                        .put("schema", patternGuess)
                        .put("confidence", 0.9)
                        .put("method", "pattern"));
                    return;
                }
                
                // 3. Use session affinity (FAST)
                if (session.dominantSchema != null && quickVerifyTable(session.dominantSchema, tableName)) {
                    session.recordSuccess(tableName, session.dominantSchema);
                    promise.complete(new JsonObject()
                        .put("schema", session.dominantSchema)
                        .put("confidence", 0.85)
                        .put("method", "session_affinity"));
                    return;
                }
                
                // 4. Parallel probe likely schemas (AGGRESSIVE)
                String found = parallelSchemaProbe(tableName, session);
                if (found != null) {
                    session.recordSuccess(tableName, found);
                    promise.complete(new JsonObject()
                        .put("schema", found)
                        .put("confidence", 0.7)
                        .put("method", "parallel_probe"));
                    return;
                }
                
                // 5. Query v$sql for this specific table (LAST RESORT)
                String discovered = mineQueryLogs(tableName);
                if (discovered != null) {
                    session.recordSuccess(tableName, discovered);
                    promise.complete(new JsonObject()
                        .put("schema", discovered)
                        .put("confidence", 0.6)
                        .put("method", "query_log_mining"));
                    return;
                }
                
                // Not found - return null (let Oracle handle it)
                promise.complete(new JsonObject()
                    .put("schema", null)
                    .put("confidence", 0.0)
                    .put("method", "not_found"));
                    
            } catch (Exception e) {
                promise.fail(e);
            }
        }, res -> {
            if (res.succeeded()) {
                sendSuccess(ctx, requestId, res.result());
            } else {
                sendError(ctx, requestId, MCPResponse.ErrorCodes.INTERNAL_ERROR,
                    "Schema resolution failed: " + res.cause().getMessage());
            }
        });
    }
    
    private void discoverSchemas(RoutingContext ctx, String requestId, JsonObject arguments) {
        String sessionId = arguments.getString("sessionId");
        
        executeBlocking(Promise.<JsonObject>promise(), promise -> {
            try {
                SessionIntelligence session = getOrCreateSession(sessionId);
                
                // Skip if already discovered
                if (session.discoveryComplete) {
                    promise.complete(new JsonObject()
                        .put("schemas", new JsonArray(new ArrayList<>(session.availableSchemas)))
                        .put("cached", true));
                    return;
                }
                
                // Aggressive discovery query
                String sql = """
                    WITH schema_stats AS (
                        SELECT 
                            owner,
                            COUNT(DISTINCT table_name) as table_count,
                            MAX(last_analyzed) as last_activity
                        FROM all_tables
                        WHERE owner NOT IN ('SYS', 'SYSTEM', 'XDB', 'APEX_040200', 'FLOWS_FILES')
                        AND owner NOT LIKE 'APEX_%'
                        GROUP BY owner
                    ),
                    recent_usage AS (
                        SELECT DISTINCT parsing_schema_name as schema_name
                        FROM v$sql
                        WHERE last_active_time > SYSDATE - 1
                        AND parsing_schema_name NOT IN ('SYS', 'SYSTEM')
                        AND ROWNUM <= 20
                    )
                    SELECT 
                        s.owner as schema_name,
                        s.table_count,
                        CASE WHEN r.schema_name IS NOT NULL THEN 'Y' ELSE 'N' END as recently_used
                    FROM schema_stats s
                    LEFT JOIN recent_usage r ON s.owner = r.schema_name
                    WHERE s.table_count > 0
                    ORDER BY recently_used DESC, s.table_count DESC
                """;
                
                connectionManager.executeWithConnection(conn -> {
                    try (PreparedStatement ps = conn.prepareStatement(sql);
                         ResultSet rs = ps.executeQuery()) {
                    
                    JsonArray schemas = new JsonArray();
                    while (rs.next()) {
                        String schema = rs.getString("schema_name");
                        session.availableSchemas.add(schema);
                        
                        JsonObject schemaInfo = new JsonObject()
                            .put("name", schema)
                            .put("tableCount", rs.getInt("table_count"))
                            .put("recentlyUsed", "Y".equals(rs.getString("recently_used")));
                        schemas.add(schemaInfo);
                        
                        // Boost affinity for recently used schemas
                        if ("Y".equals(rs.getString("recently_used"))) {
                            session.schemaAffinities.put(schema, 0.3f);
                        }
                    }
                    
                        session.discoveryComplete = true;
                        
                        promise.complete(new JsonObject()
                            .put("schemas", schemas)
                            .put("cached", false));
                        return null;
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }).onComplete(ar -> {
                    if (ar.failed() && ar.cause() instanceof RuntimeException) {
                        Throwable cause = ar.cause().getCause();
                        if (cause instanceof SQLException) {
                            promise.fail(cause);
                        } else {
                            promise.fail(ar.cause());
                        }
                    }
                });
                
            } catch (Exception e) {
                promise.fail(e);
            }
        }, res -> {
            if (res.succeeded()) {
                sendSuccess(ctx, requestId, res.result());
            } else {
                sendError(ctx, requestId, MCPResponse.ErrorCodes.INTERNAL_ERROR,
                    "Schema discovery failed: " + res.cause().getMessage());
            }
        });
    }
    
    private void learnFromSuccess(RoutingContext ctx, String requestId, JsonObject arguments) {
        String tableName = arguments.getString("tableName");
        String schema = arguments.getString("schema");
        String sessionId = arguments.getString("sessionId");
        
        SessionIntelligence session = getOrCreateSession(sessionId);
        session.recordSuccess(tableName, schema);
        
        // Also learn patterns
        String prefix = extractTablePrefix(tableName);
        if (prefix != null && prefix.length() >= 2) {
            session.schemaCache.put(prefix + "*", schema);
        }
        
        sendSuccess(ctx, requestId, new JsonObject()
            .put("success", true)
            .put("dominantSchema", session.dominantSchema));
    }
    
    private void getSessionPatterns(RoutingContext ctx, String requestId, JsonObject arguments) {
        String sessionId = arguments.getString("sessionId");
        SessionIntelligence session = sessionData.get(sessionId);
        
        if (session == null) {
            sendSuccess(ctx, requestId, new JsonObject()
                .put("exists", false));
            return;
        }
        
        JsonObject patterns = new JsonObject()
            .put("sessionId", sessionId)
            .put("cacheSize", session.schemaCache.size())
            .put("dominantSchema", session.dominantSchema)
            .put("schemaAffinities", convertMapToJsonObject(session.schemaAffinities))
            .put("schemaUsageCounts", convertMapToJsonObject(session.schemaUsageCount))
            .put("availableSchemas", new JsonArray(new ArrayList<>(session.availableSchemas)))
            .put("ageMinutes", (System.currentTimeMillis() - session.createdAt) / 60000);
            
        sendSuccess(ctx, requestId, patterns);
    }
    
    // Helper methods
    
    private SessionIntelligence getOrCreateSession(String sessionId) {
        return sessionData.computeIfAbsent(sessionId, SessionIntelligence::new);
    }
    
    private void initializeSession(String sessionId) {
        SessionIntelligence session = new SessionIntelligence(sessionId);
        sessionData.put(sessionId, session);
        vertx.eventBus().publish("log", "Initialized schema resolver for session: " + sessionId + ",2,SessionSchemaResolver,Session,Init");
    }
    
    private void cleanupSession(String sessionId) {
        SessionIntelligence removed = sessionData.remove(sessionId);
        if (removed != null) {
            vertx.eventBus().publish("log", "Cleaned up schema resolver for session: " + sessionId + 
                " (resolved " + removed.schemaCache.size() + " tables),2,SessionSchemaResolver,Session,Cleanup");
        }
    }
    
    private String guessSchemaFromPattern(String tableName) {
        String upperTable = tableName.toUpperCase();
        
        for (Map.Entry<String, String> pattern : TABLE_PATTERNS.entrySet()) {
            if (upperTable.matches(pattern.getKey() + ".*")) {
                return pattern.getValue();
            }
        }
        
        return null;
    }
    
    private boolean quickVerifyTable(String schema, String tableName) {
        String sql = "SELECT 1 FROM all_tables WHERE owner = ? AND table_name = ? AND ROWNUM = 1";
        
        return connectionManager.executeWithConnection(conn -> {
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, schema.toUpperCase());
                ps.setString(2, tableName.toUpperCase());
                
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next();
                }
            } catch (Exception e) {
                return false;
            }
        }).result();
    }
    
    private String parallelSchemaProbe(String tableName, SessionIntelligence session) {
        // Get top schemas to probe
        List<String> candidates = new ArrayList<>();
        
        // Add high-affinity schemas first
        session.schemaAffinities.entrySet().stream()
            .filter(e -> e.getValue() > 0.3f)
            .sorted(Map.Entry.<String, Float>comparingByValue().reversed())
            .limit(5)
            .forEach(e -> candidates.add(e.getKey()));
        
        // Add some available schemas if not enough
        if (candidates.size() < 5 && !session.availableSchemas.isEmpty()) {
            session.availableSchemas.stream()
                .filter(s -> !candidates.contains(s))
                .limit(5 - candidates.size())
                .forEach(candidates::add);
        }
        
        if (candidates.isEmpty()) {
            return null;
        }
        
        // Build single query to check all candidates
        String placeholders = candidates.stream()
            .map(s -> "?")
            .collect(Collectors.joining(","));
            
        String sql = String.format(
            "SELECT owner FROM all_tables WHERE table_name = ? AND owner IN (%s) AND ROWNUM = 1",
            placeholders
        );
        
        return connectionManager.executeWithConnection(conn -> {
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, tableName.toUpperCase());
                for (int i = 0; i < candidates.size(); i++) {
                    ps.setString(i + 2, candidates.get(i));
                }
                
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return rs.getString("owner");
                    }
                }
            } catch (Exception e) {
                vertx.eventBus().publish("log", "Parallel probe failed: " + e.getMessage() + ",1,SessionSchemaResolver,Discovery,Error");
            }
            return null;
        }).result();
    }
    
    private String mineQueryLogs(String tableName) {
        String sql = """
            SELECT parsing_schema_name, COUNT(*) as usage_count
            FROM v$sql
            WHERE UPPER(sql_text) LIKE '%' || ? || '%'
            AND parsing_schema_name IS NOT NULL
            AND parsing_schema_name NOT IN ('SYS', 'SYSTEM')
            AND last_active_time > SYSDATE - 7
            GROUP BY parsing_schema_name
            ORDER BY usage_count DESC
            FETCH FIRST 5 ROWS ONLY
        """;
        
        return connectionManager.executeWithConnection(conn -> {
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, tableName.toUpperCase());
                
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String schema = rs.getString("parsing_schema_name");
                        // Verify this schema actually has the table
                        if (quickVerifyTable(schema, tableName)) {
                            return schema;
                        }
                    }
                }
            } catch (Exception e) {
                vertx.eventBus().publish("log", "Query log mining failed: " + e.getMessage() + ",1,SessionSchemaResolver,Discovery,Error");
            }
            return null;
        }).result();
    }
    
    private String extractTablePrefix(String tableName) {
        Pattern pattern = Pattern.compile("^([A-Z]+_)");
        Matcher matcher = pattern.matcher(tableName.toUpperCase());
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }
    
    /**
     * Helper method to convert Maps to JsonObject
     */
    private <K, V> JsonObject convertMapToJsonObject(Map<K, V> map) {
        JsonObject json = new JsonObject();
        map.forEach((key, value) -> {
            json.put(key.toString(), value);
        });
        return json;
    }
    
    @Override
    public void stop(Promise<Void> stopPromise) {
        // Clean up all sessions
        sessionData.clear();
        try {
            super.stop(stopPromise);
        } catch (Exception e) {
            stopPromise.fail(e);
        }
    }
}
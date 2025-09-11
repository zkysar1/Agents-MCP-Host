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
        // Request-scoped cache to avoid repeated lookups in same request
        final Map<String, Boolean> requestScopedVerifyCache = new ConcurrentHashMap<>();
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
        
        // NEW: Direct resolution requests from OracleQueryExecutionServer
        vertx.eventBus().<JsonObject>consumer("session.schema.resolver.resolve", msg -> {
            JsonObject request = msg.body();
            String tableName = request.getString("tableName");
            String sessionId = request.getString("sessionId");
            
            // Resolve schema asynchronously
            resolveSchemaAsync(tableName, sessionId)
                .onSuccess(schema -> {
                    msg.reply(new JsonObject().put("schema", schema));
                })
                .onFailure(err -> {
                    vertx.eventBus().publish("log", 
                        "Schema resolution failed: " + err.getMessage() + 
                        ",1,SessionSchemaResolverServer,Resolution,Error");
                    // Fail open - reply with null to use DEFAULT_SCHEMA
                    msg.reply(new JsonObject().put("schema", null));
                });
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
        
        // Validate inputs
        if (tableName == null || tableName.trim().isEmpty()) {
            sendError(ctx, requestId, MCPResponse.ErrorCodes.INVALID_PARAMS, "tableName is required");
            return;
        }
        if (sessionId == null || sessionId.trim().isEmpty()) {
            sendError(ctx, requestId, MCPResponse.ErrorCodes.INVALID_PARAMS, "sessionId is required");
            return;
        }
        
        // Execute as blocking operation - but now with async operations inside
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
                
                // 2. Try pattern-based guess (FAST) - now async
                String patternGuess = guessSchemaFromPattern(tableName);
                Future<JsonObject> resolutionFuture = Future.succeededFuture();
                
                if (patternGuess != null) {
                    resolutionFuture = quickVerifyTableAsync(patternGuess, tableName, session)
                        .map(exists -> {
                            if (exists) {
                                session.recordSuccess(tableName, patternGuess);
                                return new JsonObject()
                                    .put("schema", patternGuess)
                                    .put("confidence", 0.9)
                                    .put("method", "pattern")
                                    .put("found", true);
                            }
                            return new JsonObject().put("found", false);
                        });
                } else {
                    resolutionFuture = Future.succeededFuture(new JsonObject().put("found", false));
                }
                
                // Chain the remaining operations
                resolutionFuture
                    .compose(result -> {
                        if (result.getBoolean("found", false)) {
                            return Future.succeededFuture(result);
                        }
                        
                        // 3. Use session affinity (FAST) - now async
                        if (session.dominantSchema != null) {
                            return quickVerifyTableAsync(session.dominantSchema, tableName, session)
                                .map(exists -> {
                                    if (exists) {
                                        session.recordSuccess(tableName, session.dominantSchema);
                                        return new JsonObject()
                                            .put("schema", session.dominantSchema)
                                            .put("confidence", 0.85)
                                            .put("method", "session_affinity")
                                            .put("found", true);
                                    }
                                    return new JsonObject().put("found", false);
                                });
                        }
                        return Future.succeededFuture(new JsonObject().put("found", false));
                    })
                    .compose(result -> {
                        if (result.getBoolean("found", false)) {
                            return Future.succeededFuture(result);
                        }
                        
                        // 4. Parallel probe likely schemas (AGGRESSIVE) - now async
                        return parallelSchemaProbeAsync(tableName, session)
                            .map(found -> {
                                if (found != null) {
                                    session.recordSuccess(tableName, found);
                                    return new JsonObject()
                                        .put("schema", found)
                                        .put("confidence", 0.7)
                                        .put("method", "parallel_probe")
                                        .put("found", true);
                                }
                                return new JsonObject().put("found", false);
                            });
                    })
                    .compose(result -> {
                        if (result.getBoolean("found", false)) {
                            return Future.succeededFuture(result);
                        }
                        
                        // 5. Query v$sql for this specific table (LAST RESORT) - now async
                        return mineQueryLogsAsync(tableName)
                            .map(discovered -> {
                                if (discovered != null) {
                                    session.recordSuccess(tableName, discovered);
                                    return new JsonObject()
                                        .put("schema", discovered)
                                        .put("confidence", 0.6)
                                        .put("method", "query_log_mining")
                                        .put("found", true);
                                }
                                // Not found - return null (let Oracle handle it)
                                return new JsonObject()
                                    .put("schema", null)
                                    .put("confidence", 0.0)
                                    .put("method", "not_found")
                                    .put("found", false);
                            });
                    })
                    .onSuccess(result -> {
                        // Remove the internal "found" flag before completing
                        result.remove("found");
                        promise.complete(result);
                    })
                    .onFailure(promise::fail);
                    
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
                        
                        return new JsonObject()
                            .put("schemas", schemas)
                            .put("cached", false);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }).onComplete(ar -> {
                    if (ar.succeeded()) {
                        promise.complete(ar.result());
                    } else if (ar.cause() instanceof RuntimeException) {
                        Throwable cause = ar.cause().getCause();
                        if (cause instanceof SQLException) {
                            promise.fail(cause);
                        } else {
                            promise.fail(ar.cause());
                        }
                    } else {
                        promise.fail(ar.cause());
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
        
        // Validate inputs
        if (tableName == null || schema == null || sessionId == null) {
            sendError(ctx, requestId, MCPResponse.ErrorCodes.INVALID_PARAMS, "tableName, schema, and sessionId are required");
            return;
        }
        
        // Validate schema name is a valid Oracle identifier
        if (!isValidOracleIdentifier(schema)) {
            sendError(ctx, requestId, MCPResponse.ErrorCodes.INVALID_PARAMS, "Invalid schema name: " + schema);
            return;
        }
        
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
    
    private Future<Boolean> quickVerifyTableAsync(String schema, String tableName) {
        return quickVerifyTableAsync(schema, tableName, null);
    }
    
    private Future<Boolean> quickVerifyTableAsync(String schema, String tableName, SessionIntelligence session) {
        // Check request-scoped cache first if session provided
        String cacheKey = schema.toUpperCase() + "." + tableName.toUpperCase();
        if (session != null && session.requestScopedVerifyCache.containsKey(cacheKey)) {
            return Future.succeededFuture(session.requestScopedVerifyCache.get(cacheKey));
        }
        
        String sql = "SELECT 1 FROM all_tables WHERE owner = ? AND table_name = ? AND ROWNUM = 1";
        
        return connectionManager.executeWithConnection(conn -> {
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, schema.toUpperCase());
                ps.setString(2, tableName.toUpperCase());
                
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next();
                }
            } catch (Exception e) {
                vertx.eventBus().publish("log", "Error verifying table " + schema + "." + tableName + ": " + e.getMessage() + ",2,SessionSchemaResolver,Discovery,Error");
                return false;
            }
        }).map(exists -> {
            // Cache result if session provided
            if (session != null && exists != null) {
                session.requestScopedVerifyCache.put(cacheKey, exists);
                // Clean cache periodically to prevent memory growth
                if (session.requestScopedVerifyCache.size() > 100) {
                    session.requestScopedVerifyCache.clear();
                }
            }
            return exists;
        }).recover(err -> {
            vertx.eventBus().publish("log", "Failed to verify table " + schema + "." + tableName + ": " + err.getMessage() + ",1,SessionSchemaResolver,Discovery,Error");
            return Future.succeededFuture(false);
        });
    }
    
    private Future<String> parallelSchemaProbeAsync(String tableName, SessionIntelligence session) {
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
            return Future.succeededFuture(null);
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
        }).recover(err -> {
            vertx.eventBus().publish("log", "Parallel schema probe failed: " + err.getMessage() + ",1,SessionSchemaResolver,Discovery,Error");
            return Future.succeededFuture(null);
        });
    }
    
    private Future<String> mineQueryLogsAsync(String tableName) {
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
            List<String> schemas = new ArrayList<>();
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, tableName.toUpperCase());
                
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        schemas.add(rs.getString("parsing_schema_name"));
                    }
                }
            } catch (Exception e) {
                vertx.eventBus().publish("log", "Query log mining failed: " + e.getMessage() + ",1,SessionSchemaResolver,Discovery,Error");
            }
            return schemas;
        }).compose(schemas -> {
            // Now verify each schema asynchronously
            if (schemas.isEmpty()) {
                return Future.succeededFuture(null);
            }
            
            // Check schemas one by one until we find a match
            return verifySchemaSequentially(schemas, tableName, 0);
        }).recover(err -> {
            vertx.eventBus().publish("log", "Query log mining failed: " + err.getMessage() + ",1,SessionSchemaResolver,Discovery,Error");
            return Future.succeededFuture(null);
        });
    }
    
    private Future<String> verifySchemaSequentially(List<String> schemas, String tableName, int index) {
        if (index >= schemas.size()) {
            return Future.succeededFuture(null);
        }
        
        String schema = schemas.get(index);
        return quickVerifyTableAsync(schema, tableName)
            .compose(exists -> {
                if (exists) {
                    return Future.succeededFuture(schema);
                } else {
                    // Try next schema
                    return verifySchemaSequentially(schemas, tableName, index + 1);
                }
            });
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
     * Async version of schema resolution for event bus calls
     */
    private Future<String> resolveSchemaAsync(String tableName, String sessionId) {
        // Check feature flag for demo mode
        if (!OracleConnectionManager.useSchemaExplorerTools()) {
            // Demo mode: Always return DEFAULT_SCHEMA immediately
            vertx.eventBus().publish("log", 
                "Schema exploration disabled (demo mode), using DEFAULT_SCHEMA for: " + tableName + 
                ",2,SessionSchemaResolverServer,Resolution,DemoMode");
            return Future.succeededFuture(OracleConnectionManager.getDefaultSchema());
        }
        
        // Validate input
        if (!isValidOracleIdentifier(tableName)) {
            return Future.failedFuture("Invalid table name: " + tableName);
        }
        
        SessionIntelligence session = getOrCreateSession(sessionId);
        
        // 1. Check cache first (FASTEST)
        String cachedSchema = session.schemaCache.get(tableName.toUpperCase());
        if (cachedSchema != null) {
            vertx.eventBus().publish("log", "Schema cache hit for " + tableName + ": " + cachedSchema + 
                ",3,SessionSchemaResolverServer,Resolution,Cache");
            return Future.succeededFuture(cachedSchema);
        }
        
        // 2. Try pattern-based guess
        String patternGuess = guessSchemaFromPattern(tableName);
        if (patternGuess != null) {
            return quickVerifyTableAsync(patternGuess, tableName, session)
                .map(exists -> {
                    if (exists) {
                        session.recordSuccess(tableName, patternGuess);
                        return patternGuess;
                    }
                    return null;
                });
        }
        
        // 3. Use session affinity if available
        if (session.dominantSchema != null) {
            return quickVerifyTableAsync(session.dominantSchema, tableName, session)
                .map(exists -> {
                    if (exists) {
                        session.recordSuccess(tableName, session.dominantSchema);
                        return session.dominantSchema;
                    }
                    return null;
                });
        }
        
        // 4. Try DEFAULT_SCHEMA as last resort
        String defaultSchema = OracleConnectionManager.getDefaultSchema();
        return quickVerifyTableAsync(defaultSchema, tableName, session)
            .map(exists -> {
                if (exists) {
                    session.recordSuccess(tableName, defaultSchema);
                    return defaultSchema;
                }
                // Return DEFAULT_SCHEMA anyway (fail open)
                return defaultSchema;
            });
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
    
    /**
     * Validate Oracle identifier (schema, table, column names)
     */
    private boolean isValidOracleIdentifier(String identifier) {
        if (identifier == null || identifier.isEmpty() || identifier.length() > 30) {
            return false;
        }
        // Oracle identifiers must start with a letter and contain only letters, numbers, and underscores
        return identifier.matches("^[A-Za-z][A-Za-z0-9_]*$");
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
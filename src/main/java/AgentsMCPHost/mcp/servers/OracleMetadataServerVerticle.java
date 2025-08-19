package AgentsMCPHost.mcp.servers;

import AgentsMCPHost.mcp.utils.OracleConnectionManager;
import AgentsMCPHost.mcp.utils.EnumerationMapper;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.eventbus.Message;

import java.util.UUID;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Oracle Metadata MCP Server - The Navigator
 * Provides database structure understanding and relationship mapping.
 * Runs on port 8081.
 * 
 * Tools:
 * - list_tables: Returns all user tables with row counts
 * - describe_table: Column details, types, constraints
 * - get_relationships: Foreign keys and references
 * - detect_enumerations: Auto-identify enum tables
 * - get_table_statistics: Row counts, null percentages, cardinality
 */
public class OracleMetadataServerVerticle extends AbstractVerticle {
    
    private static final int PORT = 8081;
    private static final String MCP_PROTOCOL_VERSION = "2024-11-05";
    private static OracleMetadataServerVerticle instance;
    private HttpServer httpServer;
    private OracleConnectionManager oracleManager;
    private EnumerationMapper enumMapper;
    
    // Session management
    private final Map<String, JsonObject> sessions = new ConcurrentHashMap<>();
    
    // Available tools
    private final JsonArray tools = new JsonArray()
        .add(new JsonObject()
            .put("name", "list_tables")
            .put("description", "List all Oracle database tables with row counts")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("include_system", new JsonObject()
                        .put("type", "boolean")
                        .put("default", false))
                    .put("include_counts", new JsonObject()
                        .put("type", "boolean")
                        .put("default", true)))
                .put("required", new JsonArray())))
        
        .add(new JsonObject()
            .put("name", "describe_table")
            .put("description", "Get detailed metadata about a table including columns, types, and constraints")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("table_name", new JsonObject()
                        .put("type", "string")))
                .put("required", new JsonArray().add("table_name"))))
        
        .add(new JsonObject()
            .put("name", "get_relationships")
            .put("description", "Get foreign key relationships for a table or between tables")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("table_name", new JsonObject()
                        .put("type", "string"))
                    .put("related_table", new JsonObject()
                        .put("type", "string")))
                .put("required", new JsonArray())))
        
        .add(new JsonObject()
            .put("name", "detect_enumerations")
            .put("description", "Automatically identify enumeration/lookup tables in the database")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject())
                .put("required", new JsonArray())))
        
        .add(new JsonObject()
            .put("name", "get_table_statistics")
            .put("description", "Get detailed statistics about a table's data")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("table_name", new JsonObject()
                        .put("type", "string"))
                    .put("sample_size", new JsonObject()
                        .put("type", "integer")
                        .put("default", 100)))
                .put("required", new JsonArray().add("table_name"))))
        
        // Advanced metadata discovery tools
        .add(new JsonObject()
            .put("name", "search_tables")
            .put("description", "Search for tables by name pattern")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("pattern", new JsonObject()
                        .put("type", "string")
                        .put("description", "SQL LIKE pattern (e.g., '%ORDER%')")))
                .put("required", new JsonArray().add("pattern"))))
        
        .add(new JsonObject()
            .put("name", "search_columns")
            .put("description", "Search for columns across all tables by name or type")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("column_pattern", new JsonObject()
                        .put("type", "string"))
                    .put("data_type", new JsonObject()
                        .put("type", "string"))
                    .put("table_pattern", new JsonObject()
                        .put("type", "string")))
                .put("required", new JsonArray())))
        
        .add(new JsonObject()
            .put("name", "get_foreign_keys")
            .put("description", "Get all foreign key relationships for a table")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("table_name", new JsonObject()
                        .put("type", "string")))
                .put("required", new JsonArray().add("table_name"))))
        
        .add(new JsonObject()
            .put("name", "get_indexes")
            .put("description", "Get index information for a table")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("table_name", new JsonObject()
                        .put("type", "string")))
                .put("required", new JsonArray().add("table_name"))))
        
        .add(new JsonObject()
            .put("name", "get_constraints")
            .put("description", "Get all constraints (PK, FK, CHECK, UNIQUE) for a table")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("table_name", new JsonObject()
                        .put("type", "string")))
                .put("required", new JsonArray().add("table_name"))))
        
        .add(new JsonObject()
            .put("name", "get_table_dependencies")
            .put("description", "Get tables that depend on or are depended upon by a given table")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("table_name", new JsonObject()
                        .put("type", "string")))
                .put("required", new JsonArray().add("table_name"))))
        
        .add(new JsonObject()
            .put("name", "analyze_join_paths")
            .put("description", "Find possible join paths between two tables")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("table1", new JsonObject()
                        .put("type", "string"))
                    .put("table2", new JsonObject()
                        .put("type", "string")))
                .put("required", new JsonArray().add("table1").add("table2"))))
        
        .add(new JsonObject()
            .put("name", "get_column_statistics")
            .put("description", "Get detailed statistics for a specific column")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("table_name", new JsonObject()
                        .put("type", "string"))
                    .put("column_name", new JsonObject()
                        .put("type", "string")))
                .put("required", new JsonArray().add("table_name").add("column_name"))));
    
    @Override
    public void start(Promise<Void> startPromise) {
        instance = this;
        
        // Initialize managers
        oracleManager = OracleConnectionManager.getInstance();
        enumMapper = EnumerationMapper.getInstance();
        enumMapper.initialize(vertx);
        
        // Initialize Oracle connection first
        oracleManager.initialize(vertx)
            .onSuccess(v -> {
                System.out.println("Oracle Metadata Server: Database connection initialized");
                
                // Create router
                Router router = Router.router(vertx);
                setRouter(router);
                
                // Register event bus consumers
                vertx.eventBus().consumer("mcp.oracle.metadata.execute", this::executeToolEventBus);
                
                // Create HTTP server
                HttpServerOptions options = new HttpServerOptions()
                    .setHost("localhost")
                    .setPort(PORT)
                    .setCompressionSupported(true);
                
                httpServer = vertx.createHttpServer(options);
                httpServer.requestHandler(router)
                    .listen()
                    .onSuccess(server -> {
                        System.out.println("Oracle Metadata MCP Server started on port " + PORT);
                        vertx.eventBus().publish("log", "Oracle Metadata Server started,1,OracleMetadata,StartUp,MCP");
                        
                        vertx.eventBus().publish("mcp.server.ready", new JsonObject()
                            .put("server", "oracle_metadata")
                            .put("port", PORT)
                            .put("tools", tools.size()));
                            
                        startPromise.complete();
                    })
                    .onFailure(startPromise::fail);
            })
            .onFailure(err -> {
                System.err.println("Oracle Metadata Server: Failed to initialize database: " + err.getMessage());
                startPromise.fail(err);
            });
    }
    
    /**
     * Static router configuration
     */
    public static void setRouter(Router router) {
        router.route().handler(BodyHandler.create());
        
        // CORS headers
        router.route().handler(ctx -> {
            ctx.response()
                .putHeader("Access-Control-Allow-Origin", "*")
                .putHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
                .putHeader("Access-Control-Allow-Headers", "Content-Type, MCP-Protocol-Version, Mcp-Session-Id");
            
            if (ctx.request().method().name().equals("OPTIONS")) {
                ctx.response().setStatusCode(204).end();
            } else {
                ctx.next();
            }
        });
        
        // MCP protocol endpoints
        router.post("/").handler(OracleMetadataServerVerticle::handleMcpRequest);
        router.get("/").handler(OracleMetadataServerVerticle::handleSseStream);
    }
    
    /**
     * Handle MCP JSON-RPC requests
     */
    private static void handleMcpRequest(RoutingContext ctx) {
        try {
            JsonObject request = ctx.body().asJsonObject();
            if (request == null) {
                sendError(ctx, 400, "Invalid JSON", null);
                return;
            }
            
            String method = request.getString("method");
            String id = request.getString("id");
            JsonObject params = request.getJsonObject("params", new JsonObject());
            
            OracleMetadataServerVerticle verticle = instance;
            if (verticle == null) {
                sendError(ctx, 500, "Server not initialized", id);
                return;
            }
            
            switch (method) {
                case "initialize":
                    verticle.handleInitialize(ctx, id, params);
                    break;
                case "tools/list":
                    verticle.handleToolsList(ctx, id);
                    break;
                case "tools/call":
                    verticle.handleToolCall(ctx, id, params);
                    break;
                default:
                    sendError(ctx, 400, "Unknown method: " + method, id);
            }
            
        } catch (Exception e) {
            sendError(ctx, 500, "Internal error: " + e.getMessage(), null);
        }
    }
    
    /**
     * Handle SSE stream requests
     */
    private static void handleSseStream(RoutingContext ctx) {
        String accept = ctx.request().getHeader("Accept");
        
        if (!"text/event-stream".equals(accept)) {
            ctx.response()
                .setStatusCode(406)
                .end("SSE stream requires Accept: text/event-stream");
            return;
        }
        
        ctx.response()
            .putHeader("Content-Type", "text/event-stream")
            .putHeader("Cache-Control", "no-cache")
            .putHeader("Connection", "keep-alive")
            .setChunked(true);
        
        ctx.response().write("event: ping\ndata: {\"type\":\"ping\"}\n\n");
        
        long timerId = ctx.vertx().setPeriodic(30000, id -> {
            ctx.response().write("event: ping\ndata: {\"type\":\"ping\"}\n\n");
        });
        
        ctx.response().closeHandler(v -> {
            ctx.vertx().cancelTimer(timerId);
        });
    }
    
    /**
     * Handle initialize request
     */
    private void handleInitialize(RoutingContext ctx, String id, JsonObject params) {
        String sessionId = UUID.randomUUID().toString();
        sessions.put(sessionId, new JsonObject()
            .put("sessionId", sessionId)
            .put("protocolVersion", params.getString("protocolVersion"))
            .put("capabilities", params.getJsonObject("capabilities")));
        
        JsonObject response = new JsonObject()
            .put("jsonrpc", "2.0")
            .put("id", id)
            .put("result", new JsonObject()
                .put("protocolVersion", MCP_PROTOCOL_VERSION)
                .put("capabilities", new JsonObject()
                    .put("tools", new JsonObject().put("listChanged", true)))
                .put("serverInfo", new JsonObject()
                    .put("name", "Oracle Metadata Server")
                    .put("version", "1.0.0")));
        
        ctx.response()
            .setStatusCode(200)
            .putHeader("Content-Type", "application/json")
            .end(response.encode());
    }
    
    /**
     * Handle tools/list request
     */
    private void handleToolsList(RoutingContext ctx, String id) {
        JsonObject response = new JsonObject()
            .put("jsonrpc", "2.0")
            .put("id", id)
            .put("result", new JsonObject()
                .put("tools", tools));
        
        ctx.response()
            .setStatusCode(200)
            .putHeader("Content-Type", "application/json")
            .end(response.encode());
    }
    
    /**
     * Handle tools/call request
     */
    private void handleToolCall(RoutingContext ctx, String id, JsonObject params) {
        String toolName = params.getString("name");
        JsonObject arguments = params.getJsonObject("arguments", new JsonObject());
        
        // Execute tool asynchronously without blocking
        executeToolAsync(toolName, arguments)
            .onSuccess(result -> {
                JsonObject response = new JsonObject()
                    .put("jsonrpc", "2.0")
                    .put("id", id)
                    .put("result", new JsonObject()
                        .put("content", new JsonArray()
                            .add(new JsonObject()
                                .put("type", "text")
                                .put("text", result))));
                
                ctx.response()
                    .setStatusCode(200)
                    .putHeader("Content-Type", "application/json")
                    .end(response.encode());
            })
            .onFailure(err -> {
                sendError(ctx, 500, "Tool execution failed: " + err.getMessage(), id);
            });
    }
    
    /**
     * Execute tool asynchronously with given arguments
     */
    private Future<String> executeToolAsync(String toolName, JsonObject arguments) {
        switch (toolName) {
            case "list_tables":
                return listTablesAsync(arguments);
            case "describe_table":
                return describeTableAsync(arguments);
            case "get_relationships":
                return getRelationshipsAsync(arguments);
            case "detect_enumerations":
                return detectEnumerationsAsync(arguments);
            case "get_table_statistics":
                return getTableStatisticsAsync(arguments);
            // New advanced metadata tools
            case "search_tables":
                return searchTablesAsync(arguments);
            case "search_columns":
                return searchColumnsAsync(arguments);
            case "get_foreign_keys":
                return getForeignKeysAsync(arguments);
            case "get_indexes":
                return getIndexesAsync(arguments);
            case "get_constraints":
                return getConstraintsAsync(arguments);
            case "get_table_dependencies":
                return getTableDependenciesAsync(arguments);
            case "analyze_join_paths":
                return analyzeJoinPathsAsync(arguments);
            case "get_column_statistics":
                return getColumnStatisticsAsync(arguments);
            default:
                return Future.failedFuture(new IllegalArgumentException("Unknown tool: " + toolName));
        }
    }
    
    /**
     * List all tables with optional row counts (non-blocking)
     */
    private Future<String> listTablesAsync(JsonObject arguments) {
        boolean includeSystem = arguments.getBoolean("include_system", false);
        boolean includeCounts = arguments.getBoolean("include_counts", true);
        
        return oracleManager.listTables()
            .compose(tables -> {
                JsonArray resultTables = new JsonArray();
                List<Future<JsonObject>> tableInfoFutures = new ArrayList<>();
                
                for (int i = 0; i < tables.size(); i++) {
                    JsonObject table = tables.getJsonObject(i);
                    String tableName = table.getString("name");
                    
                    // Filter system tables if needed
                    if (!includeSystem && (tableName.startsWith("SYS_") || tableName.startsWith("APEX_"))) {
                        continue;
                    }
                    
                    JsonObject tableInfo = new JsonObject()
                        .put("table_name", tableName)
                        .put("type", table.getString("type"));
                    
                    if (includeCounts) {
                        // Get row count asynchronously
                        String countQuery = "SELECT COUNT(*) as cnt FROM " + tableName;
                        Future<JsonObject> tableWithCount = oracleManager.executeQuery(countQuery)
                            .map(countResult -> {
                                int rowCount = countResult.getJsonObject(0).getInteger("CNT", 0);
                                return tableInfo.copy().put("row_count", rowCount);
                            })
                            .recover(err -> {
                                return Future.succeededFuture(
                                    tableInfo.copy()
                                        .put("row_count", -1)
                                        .put("count_error", err.getMessage())
                                );
                            });
                        tableInfoFutures.add(tableWithCount);
                    } else {
                        tableInfoFutures.add(Future.succeededFuture(tableInfo));
                    }
                }
                
                // Wait for all counts to complete
                return Future.all(tableInfoFutures)
                    .map(v -> {
                        for (Future<JsonObject> f : tableInfoFutures) {
                            resultTables.add(f.result());
                        }
                        
                        JsonObject result = new JsonObject()
                            .put("tables", resultTables)
                            .put("total_tables", resultTables.size());
                        
                        return result.encodePrettily();
                    });
            })
            .recover(err -> {
                return Future.succeededFuture(
                    new JsonObject()
                        .put("error", "Failed to list tables: " + err.getMessage())
                        .encodePrettily()
                );
            });
    }
    
    /**
     * Describe a table's structure (non-blocking)
     */
    private Future<String> describeTableAsync(JsonObject arguments) {
        String tableName = arguments.getString("table_name");
        if (tableName == null) {
            return Future.succeededFuture(
                new JsonObject()
                    .put("error", "table_name is required")
                    .encodePrettily()
            );
        }
        
        // Get table metadata and enum detection in parallel
        Future<JsonObject> metadataFuture = oracleManager.getTableMetadata(tableName.toUpperCase());
        Future<JsonArray> enumsFuture = enumMapper.detectEnumerationTables();
        
        return Future.all(metadataFuture, enumsFuture)
            .map(compositeFuture -> {
                JsonObject metadata = metadataFuture.result();
                JsonArray enums = enumsFuture.result();
                
                // Check if this is an enumeration table
                for (int i = 0; i < enums.size(); i++) {
                    JsonObject enumTable = enums.getJsonObject(i);
                    if (enumTable != null && tableName.equalsIgnoreCase(enumTable.getString("tableName"))) {
                        metadata.put("is_enumeration", true);
                        metadata.put("enum_details", enumTable);
                        break;
                    }
                }
                
                return metadata.encodePrettily();
            })
            .recover(err -> {
                return Future.succeededFuture(
                    new JsonObject()
                        .put("error", "Failed to describe table: " + err.getMessage())
                        .put("table_name", tableName)
                        .encodePrettily()
                );
            });
    }
    
    /**
     * Get foreign key relationships (non-blocking)
     */
    private Future<String> getRelationshipsAsync(JsonObject arguments) {
        String tableName = arguments.getString("table_name");
        String relatedTable = arguments.getString("related_table");
        JsonArray relationships = new JsonArray();
        
        Future<JsonArray> relationshipsFuture;
        
        if (tableName != null) {
            // Get relationships for specific table
            relationshipsFuture = oracleManager.getTableMetadata(tableName.toUpperCase())
                .map(metadata -> {
                    JsonArray foreignKeys = metadata.getJsonArray("foreignKeys", new JsonArray());
                    JsonArray tableRelationships = new JsonArray();
                    
                    for (int i = 0; i < foreignKeys.size(); i++) {
                        JsonObject fk = foreignKeys.getJsonObject(i);
                        
                        // Filter by related table if specified
                        if (relatedTable == null || 
                            relatedTable.equalsIgnoreCase(fk.getString("referencedTable"))) {
                            tableRelationships.add(new JsonObject()
                                .put("from_table", tableName.toUpperCase())
                                .put("from_column", fk.getString("column"))
                                .put("to_table", fk.getString("referencedTable"))
                                .put("to_column", fk.getString("referencedColumn"))
                                .put("relationship_type", "foreign_key"));
                        }
                    }
                    return tableRelationships;
                });
        } else {
            // Get all relationships in database
            relationshipsFuture = oracleManager.listTables()
                .compose(tables -> {
                    List<Future<JsonArray>> metadataFutures = new ArrayList<>();
                    
                    for (int i = 0; i < tables.size(); i++) {
                        String table = tables.getJsonObject(i).getString("name");
                        
                        Future<JsonArray> tableRelsFuture = oracleManager.getTableMetadata(table)
                            .map(metadata -> {
                                JsonArray tableRels = new JsonArray();
                                JsonArray foreignKeys = metadata.getJsonArray("foreignKeys", new JsonArray());
                                
                                for (int j = 0; j < foreignKeys.size(); j++) {
                                    JsonObject fk = foreignKeys.getJsonObject(j);
                                    tableRels.add(new JsonObject()
                                        .put("from_table", table)
                                        .put("from_column", fk.getString("column"))
                                        .put("to_table", fk.getString("referencedTable"))
                                        .put("to_column", fk.getString("referencedColumn"))
                                        .put("relationship_type", "foreign_key"));
                                }
                                return tableRels;
                            });
                        metadataFutures.add(tableRelsFuture);
                    }
                    
                    return Future.all(metadataFutures)
                        .map(v -> {
                            JsonArray allRels = new JsonArray();
                            for (Future<JsonArray> f : metadataFutures) {
                                allRels.addAll(f.result());
                            }
                            return allRels;
                        });
                });
        }
        
        return relationshipsFuture
            .compose(rels -> {
                // Suggest JOIN paths if two tables specified and no direct relationships
                if (tableName != null && relatedTable != null && rels.isEmpty()) {
                    return findJoinPathsAsync(tableName, relatedTable)
                        .map(joinPaths -> {
                            JsonObject result = new JsonObject()
                                .put("relationships", rels)
                                .put("total_relationships", rels.size());
                            
                            if (!joinPaths.isEmpty()) {
                                result.put("suggested_join_paths", joinPaths);
                            }
                            return result.encodePrettily();
                        });
                } else {
                    JsonObject result = new JsonObject()
                        .put("relationships", rels)
                        .put("total_relationships", rels.size());
                    return Future.succeededFuture(result.encodePrettily());
                }
            })
            .recover(err -> {
                return Future.succeededFuture(
                    new JsonObject()
                        .put("error", "Failed to get relationships: " + err.getMessage())
                        .encodePrettily()
                );
            });
    }
    
    /**
     * Detect enumeration tables (non-blocking)
     */
    private Future<String> detectEnumerationsAsync(JsonObject arguments) {
        return enumMapper.detectEnumerationTables()
            .map(enumerations -> {
                JsonObject result = new JsonObject()
                    .put("enumerations", enumerations)
                    .put("total_found", enumerations.size());
                
                return result.encodePrettily();
            })
            .recover(err -> {
                return Future.succeededFuture(
                    new JsonObject()
                        .put("error", "Failed to detect enumerations: " + err.getMessage())
                        .encodePrettily()
                );
            });
    }
    
    /**
     * Get table statistics (non-blocking)
     */
    private Future<String> getTableStatisticsAsync(JsonObject arguments) {
        String tableName = arguments.getString("table_name");
        int sampleSize = arguments.getInteger("sample_size", 100);
        
        if (tableName == null) {
            return Future.succeededFuture(
                new JsonObject()
                    .put("error", "table_name is required")
                    .encodePrettily()
            );
        }
        
        // Get row count and metadata in parallel
        String countQuery = "SELECT COUNT(*) as cnt FROM " + tableName;
        Future<JsonArray> countFuture = oracleManager.executeQuery(countQuery);
        Future<JsonObject> metadataFuture = oracleManager.getTableMetadata(tableName.toUpperCase());
        
        return Future.all(countFuture, metadataFuture)
            .compose(compositeFuture -> {
                int totalRows = countFuture.result().getJsonObject(0).getInteger("CNT", 0);
                JsonObject metadata = metadataFuture.result();
                JsonArray columns = metadata.getJsonArray("columns");
                
                // Create futures for all column statistics
                List<Future<JsonObject>> columnStatFutures = new ArrayList<>();
                
                for (int i = 0; i < columns.size(); i++) {
                    JsonObject column = columns.getJsonObject(i);
                    String columnName = column.getString("name");
                    String columnType = column.getString("type");
                    
                    // Create base column stat object
                    JsonObject baseColStat = new JsonObject()
                        .put("column_name", columnName)
                        .put("data_type", columnType)
                        .put("nullable", column.getBoolean("nullable"));
                    
                    // Gather all statistics for this column
                    Future<JsonObject> colStatFuture = getColumnStatisticsAsync(
                        tableName, columnName, columnType, totalRows, baseColStat
                    );
                    
                    columnStatFutures.add(colStatFuture);
                }
                
                // Wait for all column statistics
                return Future.all(columnStatFutures)
                    .map(v -> {
                        JsonArray columnStats = new JsonArray();
                        for (Future<JsonObject> f : columnStatFutures) {
                            columnStats.add(f.result());
                        }
                        
                        JsonObject stats = new JsonObject()
                            .put("total_rows", totalRows)
                            .put("column_statistics", columnStats)
                            .put("table_name", tableName.toUpperCase());
                        
                        return stats.encodePrettily();
                    });
            })
            .recover(err -> {
                return Future.succeededFuture(
                    new JsonObject()
                        .put("error", "Failed to get statistics: " + err.getMessage())
                        .put("table_name", tableName)
                        .encodePrettily()
                );
            });
    }
    
    /**
     * Get statistics for a single column (helper method)
     */
    private Future<JsonObject> getColumnStatisticsAsync(String tableName, String columnName, 
                                                        String columnType, int totalRows, 
                                                        JsonObject baseColStat) {
        // Create queries for this column
        String nullQuery = String.format(
            "SELECT COUNT(*) as cnt FROM %s WHERE %s IS NULL",
            tableName, columnName
        );
        String distinctQuery = String.format(
            "SELECT COUNT(DISTINCT %s) as cnt FROM %s",
            columnName, tableName
        );
        
        // Execute null and distinct queries in parallel
        Future<JsonArray> nullFuture = oracleManager.executeQuery(nullQuery);
        Future<JsonArray> distinctFuture = oracleManager.executeQuery(distinctQuery);
        
        List<Future<?>> futures = new ArrayList<>();
        futures.add(nullFuture);
        futures.add(distinctFuture);
        
        // Add numeric stats query if applicable
        final Future<JsonArray> numStatsFuture;
        if (columnType.contains("NUMBER") || columnType.contains("INT") || 
            columnType.contains("DECIMAL") || columnType.contains("FLOAT")) {
            String statsQuery = String.format(
                "SELECT MIN(%s) as min_val, MAX(%s) as max_val, AVG(%s) as avg_val FROM %s",
                columnName, columnName, columnName, tableName
            );
            numStatsFuture = oracleManager.executeQuery(statsQuery);
            futures.add(numStatsFuture);
        } else {
            numStatsFuture = null;
        }
        
        return Future.all(futures)
            .map(v -> {
                JsonObject colStat = baseColStat.copy();
                
                // Add null statistics
                int nullCount = nullFuture.result().getJsonObject(0).getInteger("CNT", 0);
                colStat.put("null_count", nullCount);
                colStat.put("null_percentage", totalRows > 0 ? (nullCount * 100.0 / totalRows) : 0);
                
                // Add distinct statistics
                int distinctCount = distinctFuture.result().getJsonObject(0).getInteger("CNT", 0);
                colStat.put("distinct_values", distinctCount);
                colStat.put("cardinality", totalRows > 0 ? (distinctCount * 1.0 / totalRows) : 0);
                
                // Add numeric statistics if available
                if (numStatsFuture != null && numStatsFuture.succeeded() && 
                    numStatsFuture.result() != null && !numStatsFuture.result().isEmpty()) {
                    JsonObject numStats = numStatsFuture.result().getJsonObject(0);
                    if (numStats != null) {
                        colStat.put("min_value", numStats.getValue("MIN_VAL"));
                        colStat.put("max_value", numStats.getValue("MAX_VAL"));
                        colStat.put("avg_value", numStats.getValue("AVG_VAL"));
                    }
                }
                
                return colStat;
            })
            .recover(err -> {
                // Return base stats with error note
                return Future.succeededFuture(
                    baseColStat.copy()
                        .put("stats_error", err.getMessage())
                );
            });
    }
    
    /**
     * Find join paths between two tables (non-blocking)
     */
    private Future<JsonArray> findJoinPathsAsync(String table1, String table2) {
        // Get metadata for both tables in parallel
        Future<JsonObject> meta1Future = oracleManager.getTableMetadata(table1.toUpperCase());
        Future<JsonObject> meta2Future = oracleManager.getTableMetadata(table2.toUpperCase());
        
        return Future.all(meta1Future, meta2Future)
            .map(compositeFuture -> {
                JsonArray paths = new JsonArray();
                
                JsonObject meta1 = meta1Future.result();
                JsonObject meta2 = meta2Future.result();
                
                JsonArray fks1 = meta1.getJsonArray("foreignKeys", new JsonArray());
                JsonArray fks2 = meta2.getJsonArray("foreignKeys", new JsonArray());
                
                // Check for common referenced tables
                for (int i = 0; i < fks1.size(); i++) {
                    JsonObject fk1 = fks1.getJsonObject(i);
                    if (fk1 == null) continue;
                    String ref1 = fk1.getString("referencedTable");
                    if (ref1 == null) continue;
                    
                    for (int j = 0; j < fks2.size(); j++) {
                        JsonObject fk2 = fks2.getJsonObject(j);
                        if (fk2 == null) continue;
                        String ref2 = fk2.getString("referencedTable");
                        if (ref2 != null && ref1.equals(ref2)) {
                            paths.add(new JsonObject()
                                .put("path", table1 + " -> " + ref1 + " <- " + table2)
                                .put("intermediate_table", ref1));
                        }
                    }
                }
                
                return paths;
            })
            .recover(err -> {
                // Return empty array on error
                return Future.succeededFuture(new JsonArray());
            });
    }
    
    /**
     * Search for tables by name pattern
     */
    private Future<String> searchTablesAsync(JsonObject arguments) {
        String pattern = arguments.getString("pattern", "%");
        
        String query = "SELECT table_name, num_rows, last_analyzed " +
                      "FROM user_tables " +
                      "WHERE UPPER(table_name) LIKE UPPER(?) " +
                      "ORDER BY table_name";
        
        return oracleManager.executeQuery(query, pattern)
            .map(tables -> {
                JsonObject result = new JsonObject()
                    .put("pattern", pattern)
                    .put("matches", tables)
                    .put("count", tables.size());
                return result.encodePrettily();
            })
            .recover(err -> {
                return Future.succeededFuture(new JsonObject()
                    .put("error", "Table search failed: " + err.getMessage())
                    .encodePrettily());
            });
    }
    
    /**
     * Search for columns across all tables
     */
    private Future<String> searchColumnsAsync(JsonObject arguments) {
        String columnPattern = arguments.getString("column_pattern", "%");
        String dataType = arguments.getString("data_type");
        String tablePattern = arguments.getString("table_pattern", "%");
        
        String query = "SELECT c.table_name, c.column_name, c.data_type, " +
                      "c.data_length, c.nullable, c.column_id " +
                      "FROM user_tab_columns c " +
                      "WHERE UPPER(c.column_name) LIKE UPPER(?) " +
                      "AND UPPER(c.table_name) LIKE UPPER(?) ";
        
        List<Object> params = new ArrayList<>();
        params.add(columnPattern);
        params.add(tablePattern);
        
        if (dataType != null && !dataType.isEmpty()) {
            query += "AND UPPER(c.data_type) = UPPER(?) ";
            params.add(dataType);
        }
        
        query += "ORDER BY c.table_name, c.column_id";
        
        return oracleManager.executeQuery(query, params.toArray())
            .map(columns -> {
                // Group by table for better organization
                JsonObject grouped = new JsonObject();
                for (int i = 0; i < columns.size(); i++) {
                    JsonObject col = columns.getJsonObject(i);
                    String tableName = col.getString("TABLE_NAME");
                    
                    if (!grouped.containsKey(tableName)) {
                        grouped.put(tableName, new JsonArray());
                    }
                    grouped.getJsonArray(tableName).add(col);
                }
                
                JsonObject result = new JsonObject()
                    .put("column_pattern", columnPattern)
                    .put("table_pattern", tablePattern)
                    .put("data_type", dataType)
                    .put("matches_by_table", grouped)
                    .put("total_matches", columns.size());
                
                return result.encodePrettily();
            })
            .recover(err -> {
                return Future.succeededFuture(new JsonObject()
                    .put("error", "Column search failed: " + err.getMessage())
                    .encodePrettily());
            });
    }
    
    /**
     * Get all foreign key relationships for a table
     */
    private Future<String> getForeignKeysAsync(JsonObject arguments) {
        String tableName = arguments.getString("table_name");
        
        if (tableName == null || tableName.isEmpty()) {
            return Future.succeededFuture(new JsonObject()
                .put("error", "Table name is required")
                .encodePrettily());
        }
        
        // Query for both outgoing and incoming foreign keys
        String query = "SELECT " +
                      "  'OUTGOING' as direction, " +
                      "  a.constraint_name, " +
                      "  a.table_name as from_table, " +
                      "  c.column_name as from_column, " +
                      "  b.table_name as to_table, " +
                      "  d.column_name as to_column, " +
                      "  a.delete_rule " +
                      "FROM user_constraints a " +
                      "JOIN user_constraints b ON a.r_constraint_name = b.constraint_name " +
                      "JOIN user_cons_columns c ON a.constraint_name = c.constraint_name " +
                      "JOIN user_cons_columns d ON b.constraint_name = d.constraint_name " +
                      "WHERE a.constraint_type = 'R' " +
                      "AND UPPER(a.table_name) = UPPER(?) " +
                      "UNION ALL " +
                      "SELECT " +
                      "  'INCOMING' as direction, " +
                      "  a.constraint_name, " +
                      "  a.table_name as from_table, " +
                      "  c.column_name as from_column, " +
                      "  b.table_name as to_table, " +
                      "  d.column_name as to_column, " +
                      "  a.delete_rule " +
                      "FROM user_constraints a " +
                      "JOIN user_constraints b ON a.r_constraint_name = b.constraint_name " +
                      "JOIN user_cons_columns c ON a.constraint_name = c.constraint_name " +
                      "JOIN user_cons_columns d ON b.constraint_name = d.constraint_name " +
                      "WHERE a.constraint_type = 'R' " +
                      "AND UPPER(b.table_name) = UPPER(?)";
        
        return oracleManager.executeQuery(query, tableName, tableName)
            .map(fks -> {
                // Separate outgoing and incoming
                JsonArray outgoing = new JsonArray();
                JsonArray incoming = new JsonArray();
                
                for (int i = 0; i < fks.size(); i++) {
                    JsonObject fk = fks.getJsonObject(i);
                    if ("OUTGOING".equals(fk.getString("DIRECTION"))) {
                        outgoing.add(fk);
                    } else {
                        incoming.add(fk);
                    }
                }
                
                JsonObject result = new JsonObject()
                    .put("table_name", tableName)
                    .put("outgoing_foreign_keys", outgoing)
                    .put("incoming_foreign_keys", incoming)
                    .put("total_relationships", fks.size());
                
                return result.encodePrettily();
            })
            .recover(err -> {
                return Future.succeededFuture(new JsonObject()
                    .put("error", "Failed to get foreign keys: " + err.getMessage())
                    .encodePrettily());
            });
    }
    
    /**
     * Get index information for a table
     */
    private Future<String> getIndexesAsync(JsonObject arguments) {
        String tableName = arguments.getString("table_name");
        
        if (tableName == null || tableName.isEmpty()) {
            return Future.succeededFuture(new JsonObject()
                .put("error", "Table name is required")
                .encodePrettily());
        }
        
        String query = "SELECT i.index_name, i.index_type, i.uniqueness, " +
                      "ic.column_name, ic.column_position, " +
                      "i.status, i.num_rows, i.distinct_keys " +
                      "FROM user_indexes i " +
                      "JOIN user_ind_columns ic ON i.index_name = ic.index_name " +
                      "WHERE UPPER(i.table_name) = UPPER(?) " +
                      "ORDER BY i.index_name, ic.column_position";
        
        return oracleManager.executeQuery(query, tableName)
            .map(indexCols -> {
                // Group columns by index
                JsonObject grouped = new JsonObject();
                
                for (int i = 0; i < indexCols.size(); i++) {
                    JsonObject col = indexCols.getJsonObject(i);
                    String indexName = col.getString("INDEX_NAME");
                    
                    if (!grouped.containsKey(indexName)) {
                        grouped.put(indexName, new JsonObject()
                            .put("index_name", indexName)
                            .put("index_type", col.getString("INDEX_TYPE"))
                            .put("uniqueness", col.getString("UNIQUENESS"))
                            .put("status", col.getString("STATUS"))
                            .put("num_rows", col.getValue("NUM_ROWS"))
                            .put("distinct_keys", col.getValue("DISTINCT_KEYS"))
                            .put("columns", new JsonArray()));
                    }
                    
                    grouped.getJsonObject(indexName)
                        .getJsonArray("columns")
                        .add(new JsonObject()
                            .put("column_name", col.getString("COLUMN_NAME"))
                            .put("position", col.getInteger("COLUMN_POSITION")));
                }
                
                JsonObject result = new JsonObject()
                    .put("table_name", tableName)
                    .put("indexes", grouped)
                    .put("index_count", grouped.size());
                
                return result.encodePrettily();
            })
            .recover(err -> {
                return Future.succeededFuture(new JsonObject()
                    .put("error", "Failed to get indexes: " + err.getMessage())
                    .encodePrettily());
            });
    }
    
    /**
     * Get all constraints for a table
     */
    private Future<String> getConstraintsAsync(JsonObject arguments) {
        String tableName = arguments.getString("table_name");
        
        if (tableName == null || tableName.isEmpty()) {
            return Future.succeededFuture(new JsonObject()
                .put("error", "Table name is required")
                .encodePrettily());
        }
        
        String query = "SELECT c.constraint_name, c.constraint_type, " +
                      "c.search_condition, c.delete_rule, c.status, " +
                      "cc.column_name, cc.position " +
                      "FROM user_constraints c " +
                      "LEFT JOIN user_cons_columns cc ON c.constraint_name = cc.constraint_name " +
                      "WHERE UPPER(c.table_name) = UPPER(?) " +
                      "ORDER BY c.constraint_type, c.constraint_name, cc.position";
        
        return oracleManager.executeQuery(query, tableName)
            .map(constraints -> {
                // Group by constraint type
                JsonObject byType = new JsonObject()
                    .put("PRIMARY_KEY", new JsonArray())
                    .put("FOREIGN_KEY", new JsonArray())
                    .put("UNIQUE", new JsonArray())
                    .put("CHECK", new JsonArray())
                    .put("NOT_NULL", new JsonArray());
                
                JsonObject currentConstraint = null;
                String lastConstraintName = null;
                
                for (int i = 0; i < constraints.size(); i++) {
                    JsonObject row = constraints.getJsonObject(i);
                    String constraintName = row.getString("CONSTRAINT_NAME");
                    String constraintType = row.getString("CONSTRAINT_TYPE");
                    
                    if (!constraintName.equals(lastConstraintName)) {
                        // New constraint
                        currentConstraint = new JsonObject()
                            .put("name", constraintName)
                            .put("type", getConstraintTypeName(constraintType))
                            .put("status", row.getString("STATUS"))
                            .put("columns", new JsonArray());
                        
                        if (row.getString("SEARCH_CONDITION") != null) {
                            currentConstraint.put("condition", row.getString("SEARCH_CONDITION"));
                        }
                        if (row.getString("DELETE_RULE") != null) {
                            currentConstraint.put("delete_rule", row.getString("DELETE_RULE"));
                        }
                        
                        String typeKey = getConstraintTypeKey(constraintType);
                        byType.getJsonArray(typeKey).add(currentConstraint);
                        lastConstraintName = constraintName;
                    }
                    
                    if (row.getString("COLUMN_NAME") != null && currentConstraint != null) {
                        JsonArray columns = currentConstraint.getJsonArray("columns");
                        if (columns != null) {
                            columns.add(row.getString("COLUMN_NAME"));
                        }
                    }
                }
                
                JsonObject result = new JsonObject()
                    .put("table_name", tableName)
                    .put("constraints", byType)
                    .put("total_constraints", constraints.size());
                
                return result.encodePrettily();
            })
            .recover(err -> {
                return Future.succeededFuture(new JsonObject()
                    .put("error", "Failed to get constraints: " + err.getMessage())
                    .encodePrettily());
            });
    }
    
    /**
     * Get table dependencies (tables that depend on this table or that this table depends on)
     */
    private Future<String> getTableDependenciesAsync(JsonObject arguments) {
        String tableName = arguments.getString("table_name");
        
        if (tableName == null || tableName.isEmpty()) {
            return Future.succeededFuture(new JsonObject()
                .put("error", "Table name is required")
                .encodePrettily());
        }
        
        // Find tables this table depends on (via foreign keys)
        String dependsOnQuery = "SELECT DISTINCT b.table_name as referenced_table " +
                               "FROM user_constraints a " +
                               "JOIN user_constraints b ON a.r_constraint_name = b.constraint_name " +
                               "WHERE a.constraint_type = 'R' " +
                               "AND UPPER(a.table_name) = UPPER(?)";
        
        // Find tables that depend on this table
        String dependedByQuery = "SELECT DISTINCT a.table_name as dependent_table " +
                                "FROM user_constraints a " +
                                "JOIN user_constraints b ON a.r_constraint_name = b.constraint_name " +
                                "WHERE a.constraint_type = 'R' " +
                                "AND UPPER(b.table_name) = UPPER(?)";
        
        Future<JsonArray> dependsOnFuture = oracleManager.executeQuery(dependsOnQuery, tableName);
        Future<JsonArray> dependedByFuture = oracleManager.executeQuery(dependedByQuery, tableName);
        
        return Future.all(dependsOnFuture, dependedByFuture)
            .map(results -> {
                JsonArray dependsOn = (JsonArray) results.list().get(0);
                JsonArray dependedBy = (JsonArray) results.list().get(1);
                
                JsonObject result = new JsonObject()
                    .put("table_name", tableName)
                    .put("depends_on", dependsOn)
                    .put("depended_by", dependedBy)
                    .put("is_standalone", dependsOn.isEmpty() && dependedBy.isEmpty());
                
                return result.encodePrettily();
            })
            .recover(err -> {
                return Future.succeededFuture(new JsonObject()
                    .put("error", "Failed to get dependencies: " + err.getMessage())
                    .encodePrettily());
            });
    }
    
    /**
     * Analyze possible join paths between two tables
     */
    private Future<String> analyzeJoinPathsAsync(JsonObject arguments) {
        String table1 = arguments.getString("table1");
        String table2 = arguments.getString("table2");
        
        if (table1 == null || table2 == null) {
            return Future.succeededFuture(new JsonObject()
                .put("error", "Both table1 and table2 are required")
                .encodePrettily());
        }
        
        // First check for direct relationship
        String directQuery = "SELECT 'DIRECT' as path_type, " +
                           "a.constraint_name, a.table_name as from_table, " +
                           "b.table_name as to_table " +
                           "FROM user_constraints a " +
                           "JOIN user_constraints b ON a.r_constraint_name = b.constraint_name " +
                           "WHERE a.constraint_type = 'R' " +
                           "AND ((UPPER(a.table_name) = UPPER(?) AND UPPER(b.table_name) = UPPER(?)) " +
                           "OR (UPPER(a.table_name) = UPPER(?) AND UPPER(b.table_name) = UPPER(?)))";
        
        return oracleManager.executeQuery(directQuery, table1, table2, table2, table1)
            .compose(directPaths -> {
                if (!directPaths.isEmpty()) {
                    // Direct relationship found
                    JsonObject result = new JsonObject()
                        .put("table1", table1)
                        .put("table2", table2)
                        .put("path_type", "DIRECT")
                        .put("paths", directPaths);
                    return Future.succeededFuture(result.encodePrettily());
                }
                
                // Look for one-hop paths through intermediate tables
                String oneHopQuery = "WITH t1_refs AS ( " +
                    "  SELECT DISTINCT b.table_name as ref_table " +
                    "  FROM user_constraints a " +
                    "  JOIN user_constraints b ON a.r_constraint_name = b.constraint_name " +
                    "  WHERE a.constraint_type = 'R' AND UPPER(a.table_name) = UPPER(?) " +
                    "), " +
                    "t2_refs AS ( " +
                    "  SELECT DISTINCT b.table_name as ref_table " +
                    "  FROM user_constraints a " +
                    "  JOIN user_constraints b ON a.r_constraint_name = b.constraint_name " +
                    "  WHERE a.constraint_type = 'R' AND UPPER(a.table_name) = UPPER(?) " +
                    ") " +
                    "SELECT ref_table as intermediate_table FROM t1_refs " +
                    "INTERSECT " +
                    "SELECT ref_table FROM t2_refs";
                
                return oracleManager.executeQuery(oneHopQuery, table1, table2)
                    .map(paths -> {
                        JsonObject result = new JsonObject()
                            .put("table1", table1)
                            .put("table2", table2);
                        
                        if (!paths.isEmpty()) {
                            result.put("path_type", "ONE_HOP")
                                  .put("intermediate_tables", paths);
                        } else {
                            result.put("path_type", "NO_PATH")
                                  .put("message", "No direct join path found between tables");
                        }
                        
                        return result.encodePrettily();
                    });
            })
            .recover(err -> {
                return Future.succeededFuture(new JsonObject()
                    .put("error", "Failed to analyze join paths: " + err.getMessage())
                    .encodePrettily());
            });
    }
    
    /**
     * Get detailed statistics for a specific column
     */
    private Future<String> getColumnStatisticsAsync(JsonObject arguments) {
        String tableName = arguments.getString("table_name");
        String columnName = arguments.getString("column_name");
        
        if (tableName == null || columnName == null) {
            return Future.succeededFuture(new JsonObject()
                .put("error", "Both table_name and column_name are required")
                .encodePrettily());
        }
        
        // Validate identifiers to prevent SQL injection
        try {
            String safeTable = safeTableName(tableName);
            String safeColumn = quoteIdentifier(columnName);
            
            // Get column metadata (safe - uses parameters)
            String metaQuery = "SELECT data_type, data_length, data_precision, " +
                              "data_scale, nullable, data_default " +
                              "FROM user_tab_columns " +
                              "WHERE UPPER(table_name) = UPPER(?) " +
                              "AND UPPER(column_name) = UPPER(?)";
            
            // Get statistics for the column (now safe with quoted identifiers)
            String statsQuery = "SELECT " +
                              "COUNT(*) as total_rows, " +
                              "COUNT(DISTINCT " + safeColumn + ") as distinct_values, " +
                              "COUNT(" + safeColumn + ") as non_null_count, " +
                              "COUNT(*) - COUNT(" + safeColumn + ") as null_count " +
                              "FROM " + safeTable;
            
            // Get sample values (now safe)
            String sampleQuery = "SELECT DISTINCT " + safeColumn + " as sample_value " +
                               "FROM " + safeTable + " " +
                               "WHERE " + safeColumn + " IS NOT NULL " +
                               "AND ROWNUM <= 10";
            
            Future<JsonArray> metaFuture = oracleManager.executeQuery(metaQuery, tableName, columnName);
            Future<JsonArray> statsFuture = oracleManager.executeQuery(statsQuery);
            Future<JsonArray> sampleFuture = oracleManager.executeQuery(sampleQuery);
            
            return Future.all(metaFuture, statsFuture, sampleFuture)
            .map(results -> {
                JsonArray meta = (JsonArray) results.list().get(0);
                JsonArray stats = (JsonArray) results.list().get(1);
                JsonArray samples = (JsonArray) results.list().get(2);
                
                if (meta.isEmpty()) {
                    return new JsonObject()
                        .put("error", "Column not found: " + tableName + "." + columnName)
                        .encodePrettily();
                }
                
                JsonObject metaObj = meta.getJsonObject(0);
                JsonObject statsObj = stats.isEmpty() ? new JsonObject() : stats.getJsonObject(0);
                
                // Calculate cardinality ratio
                Integer distinctValues = statsObj.getInteger("DISTINCT_VALUES", 0);
                Integer totalRows = statsObj.getInteger("TOTAL_ROWS", 0);
                double cardinalityRatio = totalRows > 0 ? 
                    (double) distinctValues / totalRows : 0;
                
                JsonObject result = new JsonObject()
                    .put("table_name", tableName)
                    .put("column_name", columnName)
                    .put("metadata", metaObj)
                    .put("statistics", statsObj)
                    .put("cardinality_ratio", cardinalityRatio)
                    .put("is_unique", cardinalityRatio == 1.0)
                    .put("is_enumeration", distinctValues > 0 && distinctValues <= 50)
                    .put("sample_values", samples);
                
                return result.encodePrettily();
            })
            .recover(err -> {
                return Future.succeededFuture(new JsonObject()
                    .put("error", "Failed to get column statistics: " + err.getMessage())
                    .encodePrettily());
            });
            
        } catch (IllegalArgumentException e) {
            // Invalid identifier provided
            return Future.succeededFuture(new JsonObject()
                .put("error", "Invalid table or column name: " + e.getMessage())
                .encodePrettily());
        }
    }
    
    /**
     * Helper: Get constraint type display name
     */
    private String getConstraintTypeName(String type) {
        switch (type) {
            case "P": return "PRIMARY KEY";
            case "R": return "FOREIGN KEY";
            case "U": return "UNIQUE";
            case "C": return "CHECK";
            default: return type;
        }
    }
    
    /**
     * Helper: Get constraint type key for grouping
     */
    private String getConstraintTypeKey(String type) {
        switch (type) {
            case "P": return "PRIMARY_KEY";
            case "R": return "FOREIGN_KEY";
            case "U": return "UNIQUE";
            case "C": return "CHECK";
            default: return "CHECK"; // Includes NOT NULL
        }
    }
    
    /**
     * Validate Oracle identifier (table/column name)
     * Prevents SQL injection by ensuring valid identifier format
     */
    private boolean isValidIdentifier(String identifier) {
        if (identifier == null || identifier.isEmpty()) {
            return false;
        }
        // Oracle identifiers: start with letter, contain letters/numbers/underscore/$/#
        // Max 128 chars in 12c+, but we'll be more restrictive for safety
        return identifier.matches("^[A-Za-z][A-Za-z0-9_$#]{0,127}$");
    }
    
    /**
     * Quote and validate an Oracle identifier
     * Returns quoted identifier safe for SQL concatenation
     */
    private String quoteIdentifier(String identifier) {
        if (!isValidIdentifier(identifier)) {
            throw new IllegalArgumentException("Invalid Oracle identifier: " + identifier);
        }
        // Use uppercase and quote to handle reserved words
        return "\"" + identifier.toUpperCase() + "\"";
    }
    
    /**
     * Safely build table reference for SQL
     */
    private String safeTableName(String tableName) {
        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("Table name cannot be null or empty");
        }
        // Handle schema.table format
        if (tableName.contains(".")) {
            String[] parts = tableName.split("\\.", 2);
            if (parts.length == 2) {
                return quoteIdentifier(parts[0]) + "." + quoteIdentifier(parts[1]);
            }
        }
        return quoteIdentifier(tableName);
    }
    
    /**
     * Execute tool via event bus (non-blocking)
     */
    private void executeToolEventBus(Message<JsonObject> msg) {
        JsonObject request = msg.body();
        String toolName = request.getString("tool");
        JsonObject arguments = request.getJsonObject("arguments");
        
        // Execute tool asynchronously
        executeToolAsync(toolName, arguments)
            .onSuccess(result -> {
                msg.reply(new JsonObject()
                    .put("success", true)
                    .put("result", result));
            })
            .onFailure(err -> {
                msg.reply(new JsonObject()
                    .put("success", false)
                    .put("error", err.getMessage()));
            });
    }
    
    /**
     * Send error response
     */
    private static void sendError(RoutingContext ctx, int statusCode, String message, String id) {
        JsonObject error = new JsonObject()
            .put("jsonrpc", "2.0")
            .put("error", new JsonObject()
                .put("code", statusCode)
                .put("message", message));
        
        if (id != null) {
            error.put("id", id);
        }
        
        ctx.response()
            .setStatusCode(statusCode)
            .putHeader("Content-Type", "application/json")
            .end(error.encode());
    }
    
    @Override
    public void stop(Promise<Void> stopPromise) {
        if (httpServer != null) {
            httpServer.close(stopPromise);
        } else {
            stopPromise.complete();
        }
    }
}
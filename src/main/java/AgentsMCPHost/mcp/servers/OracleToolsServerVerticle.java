package AgentsMCPHost.mcp.servers;

import AgentsMCPHost.mcp.orchestration.SchemaMatcher;
import AgentsMCPHost.mcp.orchestration.QueryTokenExtractor;
import AgentsMCPHost.mcp.utils.OracleConnectionManager;
import AgentsMCPHost.mcp.utils.EnumerationMapper;
import AgentsMCPHost.services.LlmAPIService;
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

import java.util.*;

/**
 * Oracle Tools MCP Server - Exposes ALL Oracle capabilities as individual, composable tools.
 * 
 * This server makes every Oracle capability available as an atomic tool that can be
 * used independently or composed by any orchestration layer. This eliminates the
 * monolithic Oracle Agent and enables maximum reusability.
 * 
 * Tools are organized into categories:
 * - Analysis: Query understanding and schema matching
 * - Generation: SQL creation and optimization
 * - Execution: Running queries and getting results
 * - Formatting: Converting results to user-friendly formats
 */
public class OracleToolsServerVerticle extends AbstractVerticle {
    
    private static final int PORT = 8086;
    private static final String MCP_PROTOCOL_VERSION = "2024-11-05";
    private HttpServer httpServer;
    
    // Services used by tools
    private OracleConnectionManager oracleManager;
    private SchemaMatcher schemaMatcher;
    private EnumerationMapper enumMapper;
    private LlmAPIService llmService;
    
    // Session management for stateful operations
    private final Map<String, JsonObject> sessions = new HashMap<>();
    
    // All available tools with detailed schemas
    private final JsonArray tools = new JsonArray()
        // ============ ANALYSIS TOOLS ============
        .add(new JsonObject()
            .put("name", "analyze_query")
            .put("description", "Analyze a natural language query to extract intent, entities, and requirements")
            .put("category", "analysis")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("query", new JsonObject()
                        .put("type", "string")
                        .put("description", "Natural language query to analyze"))
                    .put("context", new JsonObject()
                        .put("type", "array")
                        .put("description", "Optional conversation history")))
                .put("required", new JsonArray().add("query"))))
        
        .add(new JsonObject()
            .put("name", "match_schema")
            .put("description", "Match query tokens against database schema to find relevant tables and columns")
            .put("category", "analysis")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("tokens", new JsonObject()
                        .put("type", "array")
                        .put("items", new JsonObject().put("type", "string"))
                        .put("description", "Tokens to match against schema"))
                    .put("limit", new JsonObject()
                        .put("type", "integer")
                        .put("default", 5)
                        .put("description", "Maximum number of matches to return")))
                .put("required", new JsonArray().add("tokens"))))
        
        .add(new JsonObject()
            .put("name", "discover_enums")
            .put("description", "Discover enumeration tables and their values for business term mapping")
            .put("category", "analysis")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("refresh", new JsonObject()
                        .put("type", "boolean")
                        .put("default", false)
                        .put("description", "Force refresh of enum cache")))
                .put("required", new JsonArray())))
        
        .add(new JsonObject()
            .put("name", "discover_sample_data")
            .put("description", "Get sample data from tables to understand content")
            .put("category", "analysis")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("table_names", new JsonObject()
                        .put("type", "array")
                        .put("items", new JsonObject().put("type", "string"))
                        .put("description", "Tables to sample"))
                    .put("limit", new JsonObject()
                        .put("type", "integer")
                        .put("default", 5)
                        .put("description", "Rows per table")))
                .put("required", new JsonArray().add("table_names"))))
        
        // ============ GENERATION TOOLS ============
        .add(new JsonObject()
            .put("name", "generate_sql")
            .put("description", "Generate SQL from natural language using schema information and LLM")
            .put("category", "generation")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("query", new JsonObject()
                        .put("type", "string")
                        .put("description", "Natural language query"))
                    .put("schema_context", new JsonObject()
                        .put("type", "object")
                        .put("description", "Schema matching results"))
                    .put("discovered_data", new JsonObject()
                        .put("type", "object")
                        .put("description", "Sample data context")))
                .put("required", new JsonArray().add("query"))))
        
        .add(new JsonObject()
            .put("name", "optimize_sql")
            .put("description", "Optimize SQL query for better performance")
            .put("category", "generation")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("sql", new JsonObject()
                        .put("type", "string")
                        .put("description", "SQL query to optimize"))
                    .put("analyze", new JsonObject()
                        .put("type", "boolean")
                        .put("default", true)
                        .put("description", "Include execution plan analysis")))
                .put("required", new JsonArray().add("sql"))))
        
        .add(new JsonObject()
            .put("name", "validate_sql")
            .put("description", "Validate SQL syntax without executing")
            .put("category", "validation")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("sql", new JsonObject()
                        .put("type", "string")))
                .put("required", new JsonArray().add("sql"))))
        
        // ============ EXECUTION TOOLS ============
        .add(new JsonObject()
            .put("name", "execute_query")
            .put("description", "Execute a SQL SELECT query")
            .put("category", "execution")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("sql", new JsonObject()
                        .put("type", "string"))
                    .put("limit", new JsonObject()
                        .put("type", "integer")
                        .put("default", 100)))
                .put("required", new JsonArray().add("sql"))))
        
        .add(new JsonObject()
            .put("name", "explain_plan")
            .put("description", "Get execution plan for a query")
            .put("category", "execution")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("sql", new JsonObject()
                        .put("type", "string")))
                .put("required", new JsonArray().add("sql"))))
        
        // ============ FORMATTING TOOLS ============
        .add(new JsonObject()
            .put("name", "format_results")
            .put("description", "Convert query results to natural language response")
            .put("category", "formatting")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("original_query", new JsonObject()
                        .put("type", "string"))
                    .put("sql_executed", new JsonObject()
                        .put("type", "string"))
                    .put("results", new JsonObject()
                        .put("type", "array"))
                    .put("error", new JsonObject()
                        .put("type", "string")))
                .put("required", new JsonArray().add("original_query"))))
        
        .add(new JsonObject()
            .put("name", "summarize_data")
            .put("description", "Create statistical summary of data")
            .put("category", "formatting")
            .put("inputSchema", new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("data", new JsonObject()
                        .put("type", "array"))
                    .put("columns", new JsonObject()
                        .put("type", "array")))
                .put("required", new JsonArray().add("data"))));
    
    @Override
    public void start(Promise<Void> startPromise) {
        // Initialize services
        oracleManager = OracleConnectionManager.getInstance();
        oracleManager.initialize(vertx);
        
        schemaMatcher = new SchemaMatcher();
        schemaMatcher.initialize(vertx);
        enumMapper = EnumerationMapper.getInstance();
        llmService = LlmAPIService.getInstance();
        llmService.setupService(vertx);
        
        // Create router
        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());
        
        // MCP protocol endpoints
        router.post("/").handler(this::handleInitialize);
        router.post("/tools/list").handler(this::handleListTools);
        router.post("/tools/call").handler(this::handleCallTool);
        
        // Health check
        router.get("/health").handler(ctx -> {
            ctx.response()
                .putHeader("Content-Type", "application/json")
                .end(new JsonObject()
                    .put("status", "healthy")
                    .put("server", "oracle-tools")
                    .put("tools", tools.size())
                    .encode());
        });
        
        // Start server
        HttpServerOptions options = new HttpServerOptions()
            .setPort(PORT)
            .setHost("0.0.0.0");
        
        httpServer = vertx.createHttpServer(options);
        httpServer.requestHandler(router);
        
        httpServer.listen(ar -> {
            if (ar.succeeded()) {
                System.out.println("Oracle Tools MCP Server started on port " + PORT);
                System.out.println("Exposing " + tools.size() + " Oracle tools for composition");
                
                // Register event bus handlers for each tool
                registerToolHandlers();
                
                // Publish tool discovery to MCP infrastructure
                publishToolDiscovery();
                
                // Notify MCP system
                vertx.eventBus().publish("mcp.server.ready", new JsonObject()
                    .put("server", "oracle-tools")
                    .put("port", PORT)
                    .put("tools", tools.size()));
                
                startPromise.complete();
            } else {
                System.err.println("Failed to start Oracle Tools Server: " + ar.cause().getMessage());
                startPromise.fail(ar.cause());
            }
        });
    }
    
    /**
     * Handle MCP initialize request
     */
    private void handleInitialize(RoutingContext ctx) {
        JsonObject response = new JsonObject()
            .put("protocolVersion", MCP_PROTOCOL_VERSION)
            .put("serverInfo", new JsonObject()
                .put("name", "oracle-tools-server")
                .put("version", "1.0.0"))
            .put("capabilities", new JsonObject()
                .put("tools", true)
                .put("resources", false));
        
        ctx.response()
            .putHeader("Content-Type", "application/json")
            .end(response.encode());
    }
    
    /**
     * Handle list tools request
     */
    private void handleListTools(RoutingContext ctx) {
        ctx.response()
            .putHeader("Content-Type", "application/json")
            .end(new JsonObject().put("tools", tools).encode());
    }
    
    /**
     * Handle tool call request - Route to appropriate handler
     */
    private void handleCallTool(RoutingContext ctx) {
        JsonObject request = ctx.body().asJsonObject();
        String toolName = request.getString("name");
        JsonObject arguments = request.getJsonObject("arguments", new JsonObject());
        
        System.out.println("[OracleTools] Executing tool: " + toolName);
        
        Future<JsonObject> resultFuture;
        
        // Route to appropriate tool handler
        switch (toolName) {
            // Analysis tools
            case "analyze_query":
                resultFuture = analyzeQuery(arguments);
                break;
            case "match_schema":
                resultFuture = matchSchema(arguments);
                break;
            case "discover_enums":
                resultFuture = discoverEnums(arguments);
                break;
            case "discover_sample_data":
                resultFuture = discoverSampleData(arguments);
                break;
                
            // Generation tools
            case "generate_sql":
                resultFuture = generateSql(arguments);
                break;
            case "optimize_sql":
                resultFuture = optimizeSql(arguments);
                break;
            case "validate_sql":
                resultFuture = validateSql(arguments);
                break;
                
            // Execution tools
            case "execute_query":
                resultFuture = executeQuery(arguments);
                break;
            case "explain_plan":
                resultFuture = explainPlan(arguments);
                break;
                
            // Formatting tools
            case "format_results":
                resultFuture = formatResults(arguments);
                break;
            case "summarize_data":
                resultFuture = summarizeData(arguments);
                break;
                
            default:
                resultFuture = Future.failedFuture("Unknown tool: " + toolName);
        }
        
        resultFuture
            .onSuccess(result -> {
                ctx.response()
                    .putHeader("Content-Type", "application/json")
                    .end(new JsonObject()
                        .put("content", new JsonArray().add(new JsonObject()
                            .put("type", "text")
                            .put("text", result.encode())))
                        .encode());
            })
            .onFailure(err -> {
                ctx.response()
                    .setStatusCode(500)
                    .putHeader("Content-Type", "application/json")
                    .end(new JsonObject()
                        .put("error", err.getMessage())
                        .encode());
            });
    }
    
    // ============ TOOL IMPLEMENTATIONS ============
    
    /**
     * Analyze a natural language query using LLM
     */
    private Future<JsonObject> analyzeQuery(JsonObject arguments) {
        String query = arguments.getString("query");
        
        // Null check for required parameter
        if (query == null || query.trim().isEmpty()) {
            String error = "Missing required argument: query";
            vertx.eventBus().publish("log",
                "analyzeQuery missing query argument,0,OracleTools,Error,Validation");
            System.err.println("[OracleTools] analyzeQuery: " + error);
            return Future.failedFuture(error);
        }
        
        JsonArray context = arguments.getJsonArray("context", new JsonArray());
        
        String prompt = buildAnalysisPrompt(query, context);
        
        JsonArray messages = new JsonArray()
            .add(new JsonObject()
                .put("role", "system")
                .put("content", "You are a database query analyst. Extract intent, entities, and requirements."))
            .add(new JsonObject()
                .put("role", "user")
                .put("content", prompt));
        
        return llmService.chatCompletion(messages)
            .map(response -> {
                String content = extractLlmContent(response);
                try {
                    return new JsonObject(content);
                } catch (Exception e) {
                    // Fallback to pattern-based analysis
                    return performPatternAnalysis(query);
                }
            })
            .recover(err -> {
                // Fallback to pattern-based analysis
                return Future.succeededFuture(performPatternAnalysis(query));
            });
    }
    
    /**
     * Match tokens against database schema
     */
    private Future<JsonObject> matchSchema(JsonObject arguments) {
        JsonArray tokensArray = arguments.getJsonArray("tokens");
        
        // Null check for required parameter
        if (tokensArray == null || tokensArray.isEmpty()) {
            // Try to get entities from arguments as fallback
            JsonArray entities = arguments.getJsonArray("entities");
            if (entities == null || entities.isEmpty()) {
                String error = "Missing required argument: tokens or entities";
                vertx.eventBus().publish("log",
                    "matchSchema missing tokens/entities,0,OracleTools,Error,Validation");
                System.err.println("[OracleTools] matchSchema: " + error);
                return Future.failedFuture(error);
            }
            tokensArray = entities;
        }
        
        Set<String> tokens = new HashSet<>();
        for (int i = 0; i < tokensArray.size(); i++) {
            tokens.add(tokensArray.getString(i));
        }
        
        return schemaMatcher.findMatches(tokens)
            .map(matches -> {
                JsonObject result = new JsonObject();
                
                JsonArray tables = new JsonArray();
                for (SchemaMatcher.TableMatch tm : matches.tableMatches) {
                    tables.add(new JsonObject()
                        .put("table", tm.tableName)
                        .put("score", tm.score)
                        .put("matched_token", tm.token));
                }
                
                JsonArray columns = new JsonArray();
                for (SchemaMatcher.ColumnMatch cm : matches.columnMatches) {
                    columns.add(new JsonObject()
                        .put("table", cm.tableName)
                        .put("column", cm.columnName)
                        .put("type", cm.columnType)
                        .put("score", cm.score));
                }
                
                result.put("tables", tables)
                      .put("columns", columns)
                      .put("confidence", matches.confidence)
                      .put("timed_out", matches.timedOut);
                
                return result;
            });
    }
    
    /**
     * Discover enumeration tables
     */
    private Future<JsonObject> discoverEnums(JsonObject arguments) {
        boolean refresh = arguments.getBoolean("refresh", false);
        
        return enumMapper.detectEnumerationTables()
            .map(enums -> new JsonObject()
                .put("enumeration_tables", enums)
                .put("count", enums.size()));
    }
    
    /**
     * Get sample data from tables
     */
    private Future<JsonObject> discoverSampleData(JsonObject arguments) {
        JsonArray tableNames = arguments.getJsonArray("table_names");
        
        // Null check and try alternatives
        if (tableNames == null || tableNames.isEmpty()) {
            // Try to get tables from schema_matches
            JsonObject schemaMatches = arguments.getJsonObject("schema_matches");
            if (schemaMatches != null && schemaMatches.containsKey("tables")) {
                JsonArray tables = schemaMatches.getJsonArray("tables");
                if (tables != null && !tables.isEmpty()) {
                    tableNames = new JsonArray();
                    for (int i = 0; i < Math.min(3, tables.size()); i++) {
                        JsonObject table = tables.getJsonObject(i);
                        // Look for "table" field (from schema_matches) or "name" field
                        if (table != null) {
                            String tableName = table.getString("table");
                            if (tableName == null) {
                                tableName = table.getString("name");
                            }
                            if (tableName != null) {
                                tableNames.add(tableName);
                                System.out.println("[OracleTools] Extracted table for sample data: " + tableName);
                            }
                        }
                    }
                }
            }
            
            if (tableNames == null || tableNames.isEmpty()) {
                String error = "Missing required argument: table_names";
                vertx.eventBus().publish("log",
                    "discoverSampleData missing table_names,0,OracleTools,Error,Validation");
                System.err.println("[OracleTools] discoverSampleData: " + error);
                return Future.failedFuture(error);
            }
        }
        
        int limit = arguments.getInteger("limit", 5);
        
        // Log what tables we're about to query
        System.out.println("[OracleTools] discoverSampleData for " + tableNames.size() + " tables");
        for (int i = 0; i < tableNames.size(); i++) {
            System.out.println("[OracleTools]   Table " + (i + 1) + ": " + tableNames.getString(i));
        }
        
        // Create a final copy for use in lambda
        final JsonArray finalTableNames = tableNames;
        List<Future<JsonObject>> futures = new ArrayList<>();
        
        for (int i = 0; i < finalTableNames.size(); i++) {
            String tableName = finalTableNames.getString(i);
            // Get sample data directly from Oracle
            String sampleQuery = "SELECT * FROM " + tableName + " WHERE ROWNUM <= " + limit;
            futures.add(
                oracleManager.executeQuery(sampleQuery)
                    .map(rows -> {
                        // executeQuery returns JsonArray directly
                        JsonObject tableData = new JsonObject();
                        tableData.put("rows", rows);
                        tableData.put("rowCount", rows.size());
                        return tableData;
                    })
                    .recover(err -> {
                        // Return empty result on error
                        return Future.succeededFuture(new JsonObject()
                            .put("error", err.getMessage())
                            .put("rows", new JsonArray())
                            .put("rowCount", 0));
                    })
            );
        }
        
        return Future.all(futures)
            .map(compositeFuture -> {
                JsonObject result = new JsonObject();
                for (int i = 0; i < futures.size(); i++) {
                    if (futures.get(i).succeeded()) {
                        result.put(finalTableNames.getString(i), futures.get(i).result());
                    }
                }
                return result;
            });
    }
    
    /**
     * Generate SQL from natural language
     */
    private Future<JsonObject> generateSql(JsonObject arguments) {
        String query = arguments.getString("query");
        
        // Try both schema_context and schema_matches (orchestration passes schema_matches)
        JsonObject schemaContext = arguments.getJsonObject("schema_context");
        if (schemaContext == null || schemaContext.isEmpty()) {
            schemaContext = arguments.getJsonObject("schema_matches", new JsonObject());
            if (!schemaContext.isEmpty()) {
                System.out.println("[OracleTools] Using schema_matches for SQL generation");
            }
        }
        
        // Try both discovered_data and sample_data (orchestration passes sample_data)
        JsonObject discoveredData = arguments.getJsonObject("discovered_data");
        if (discoveredData == null || discoveredData.isEmpty()) {
            discoveredData = arguments.getJsonObject("sample_data", new JsonObject());
            if (!discoveredData.isEmpty()) {
                System.out.println("[OracleTools] Using sample_data for SQL generation");
            }
        }
        
        // Log what we're working with
        System.out.println("[OracleTools] generateSql inputs:");
        System.out.println("[OracleTools]   Query: " + query);
        System.out.println("[OracleTools]   Schema context size: " + schemaContext.size());
        if (schemaContext.containsKey("tables")) {
            JsonArray tables = schemaContext.getJsonArray("tables");
            System.out.println("[OracleTools]   Tables found: " + tables.size());
            for (int i = 0; i < Math.min(3, tables.size()); i++) {
                JsonObject table = tables.getJsonObject(i);
                System.out.println("[OracleTools]     - " + table.getString("table", "unknown"));
            }
        }
        if (schemaContext.containsKey("columns")) {
            JsonArray columns = schemaContext.getJsonArray("columns");
            System.out.println("[OracleTools]   Columns found: " + columns.size());
        }
        
        String prompt = buildSqlGenerationPrompt(query, schemaContext, discoveredData);
        
        JsonArray messages = new JsonArray()
            .add(new JsonObject()
                .put("role", "system")
                .put("content", "You are an Oracle SQL expert. Generate precise SQL queries."))
            .add(new JsonObject()
                .put("role", "user")
                .put("content", prompt));
        
        return llmService.chatCompletion(messages)
            .map(response -> {
                String rawSql = extractLlmContent(response);
                String cleanSql = stripMarkdownFromSql(rawSql);
                
                System.out.println("[OracleTools] Generated SQL (clean): " + cleanSql);
                
                return new JsonObject()
                    .put("sql", cleanSql)
                    .put("confidence", 0.8);
            });
    }
    
    /**
     * Optimize SQL query
     */
    private Future<JsonObject> optimizeSql(JsonObject arguments) {
        // Try to get SQL from various places
        String sql = arguments.getString("sql");
        if (sql == null) {
            // Try from generated_sql object
            JsonObject generatedSql = arguments.getJsonObject("generated_sql");
            if (generatedSql != null) {
                sql = generatedSql.getString("sql");
            }
        }
        
        // Clean markdown if present
        if (sql != null) {
            sql = stripMarkdownFromSql(sql);
        }
        
        // Create final variable for lambda
        final String finalSql = sql;
        
        boolean analyze = arguments.getBoolean("analyze", true);
        
        if (!analyze) {
            return Future.succeededFuture(new JsonObject()
                .put("optimized_sql", finalSql)
                .put("changes", "No optimization requested"));
        }
        
        // Get execution plan
        return oracleManager.executeQuery("EXPLAIN PLAN FOR " + finalSql)
            .compose(v -> oracleManager.executeQuery(
                "SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY())"))
            .map(planRows -> {
                String plan = planRows.encodePrettily();
                
                // Use LLM to suggest optimizations
                String prompt = "Given this SQL and execution plan, suggest optimizations:\n\n" +
                              "SQL: " + finalSql + "\n\n" +
                              "Plan: " + plan;
                
                return new JsonObject()
                    .put("optimized_sql", finalSql)
                    .put("execution_plan", plan)
                    .put("suggestions", "Consider adding indexes on join columns");
            })
            .recover(err -> {
                return Future.succeededFuture(new JsonObject()
                    .put("optimized_sql", finalSql)
                    .put("error", "Could not optimize: " + err.getMessage()));
            });
    }
    
    /**
     * Validate SQL syntax
     */
    private Future<JsonObject> validateSql(JsonObject arguments) {
        String sql = arguments.getString("sql");
        
        // Try to parse/prepare the statement without executing
        return oracleManager.executeQuery("SELECT 1 FROM DUAL WHERE 1=0")
            .map(v -> new JsonObject()
                .put("valid", true)
                .put("sql", sql))
            .recover(err -> Future.succeededFuture(new JsonObject()
                .put("valid", false)
                .put("error", err.getMessage())));
    }
    
    /**
     * Execute SQL query
     */
    private Future<JsonObject> executeQuery(JsonObject arguments) {
        String sql = arguments.getString("sql");
        
        // Null check and try alternatives
        if (sql == null || sql.trim().isEmpty()) {
            // Try from generated_sql object
            JsonObject generatedSql = arguments.getJsonObject("generated_sql");
            if (generatedSql != null) {
                sql = generatedSql.getString("sql");
            }
            
            if (sql == null || sql.trim().isEmpty()) {
                // Try from optimized_sql object
                JsonObject optimizedSql = arguments.getJsonObject("optimized_sql");
                if (optimizedSql != null) {
                    sql = optimizedSql.getString("optimized_sql");
                    if (sql == null || sql.trim().isEmpty()) {
                        sql = optimizedSql.getString("sql");
                    }
                }
            }
            
            if (sql == null || sql.trim().isEmpty()) {
                String error = "Missing required argument: sql";
                vertx.eventBus().publish("log",
                    "executeQuery missing sql argument,0,OracleTools,Error,Validation");
                System.err.println("[OracleTools] executeQuery: " + error);
                return Future.failedFuture(error);
            }
        }
        
        // Clean markdown if present
        sql = stripMarkdownFromSql(sql);
        System.out.println("[OracleTools] Executing SQL: " + sql.substring(0, Math.min(100, sql.length())));
        
        
        int limit = arguments.getInteger("limit", 100);
        
        // Add limit if not present
        final String finalSql;
        if (!sql.toUpperCase().contains("FETCH") && !sql.toUpperCase().contains("ROWNUM")) {
            finalSql = sql + " FETCH FIRST " + limit + " ROWS ONLY";
        } else {
            finalSql = sql;
        }
        
        return oracleManager.executeQuery(finalSql)
            .map(results -> new JsonObject()
                .put("results", results)
                .put("row_count", results.size())
                .put("sql_executed", finalSql))
            .recover(error -> {
                String errorMsg = error.getMessage();
                System.out.println("[OracleTools] Query execution failed: " + errorMsg);
                
                // If it's ORA-00933 and the SQL has a semicolon, try removing it
                if (errorMsg != null && errorMsg.contains("ORA-00933") && finalSql.contains(";")) {
                    System.out.println("[OracleTools] Detected ORA-00933 with semicolon, retrying without semicolon");
                    String cleanedSql = finalSql.replace(";", "").trim();
                    return oracleManager.executeQuery(cleanedSql)
                        .map(results -> new JsonObject()
                            .put("results", results)
                            .put("row_count", results.size())
                            .put("sql_executed", cleanedSql)
                            .put("note", "Removed semicolon after ORA-00933 error"));
                }
                // Otherwise, propagate the original error
                return Future.failedFuture(error);
            });
    }
    
    /**
     * Get execution plan
     */
    private Future<JsonObject> explainPlan(JsonObject arguments) {
        String sql = arguments.getString("sql");
        
        return oracleManager.executeQuery("EXPLAIN PLAN FOR " + sql)
            .compose(v -> oracleManager.executeQuery(
                "SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY())"))
            .map(planRows -> new JsonObject()
                .put("plan", planRows)
                .put("sql", sql));
    }
    
    /**
     * Format results to natural language
     */
    private Future<JsonObject> formatResults(JsonObject arguments) {
        String originalQuery = arguments.getString("original_query", arguments.getString("query", ""));
        String sqlExecuted = arguments.getString("sql_executed", arguments.getString("generated_sql", ""));
        
        // Handle results - could be JsonArray, String (error), or missing
        JsonArray results = null;
        String error = arguments.getString("error");
        
        // Check if results is a String (error message)
        Object resultsObj = arguments.getValue("results");
        if (resultsObj instanceof String) {
            // Results is an error string
            error = (String) resultsObj;
            System.out.println("[OracleTools] formatResults: results is error string: " + error);
        } else if (resultsObj instanceof JsonArray) {
            results = (JsonArray) resultsObj;
        } else if (resultsObj == null) {
            results = new JsonArray();
            System.out.println("[OracleTools] formatResults: no results provided");
        }
        
        if (error != null) {
            return Future.succeededFuture(new JsonObject()
                .put("formatted", "I encountered an error: " + error)
                .put("success", false));
        }
        
        // Ensure we have results
        if (results == null) {
            results = new JsonArray();
        }
        
        String prompt = buildFormattingPrompt(originalQuery, sqlExecuted, results);
        
        JsonArray messages = new JsonArray()
            .add(new JsonObject()
                .put("role", "system")
                .put("content", "Convert database results to natural language."))
            .add(new JsonObject()
                .put("role", "user")
                .put("content", prompt));
        
        return llmService.chatCompletion(messages)
            .map(response -> new JsonObject()
                .put("formatted", extractLlmContent(response))
                .put("success", true));
    }
    
    /**
     * Summarize data statistically
     */
    private Future<JsonObject> summarizeData(JsonObject arguments) {
        JsonArray data = arguments.getJsonArray("data");
        
        if (data.isEmpty()) {
            return Future.succeededFuture(new JsonObject()
                .put("summary", "No data to summarize"));
        }
        
        // Calculate basic statistics
        JsonObject summary = new JsonObject()
            .put("row_count", data.size())
            .put("columns", data.getJsonObject(0).fieldNames());
        
        return Future.succeededFuture(summary);
    }
    
    // ============ HELPER METHODS ============
    
    private String buildAnalysisPrompt(String query, JsonArray context) {
        return "Analyze this query and extract:\n" +
               "1. intent (count, list, search, aggregate)\n" +
               "2. entities (tables/business objects mentioned)\n" +
               "3. filters (conditions)\n" +
               "4. required_capabilities\n\n" +
               "Query: " + query + "\n\n" +
               "Return as JSON.";
    }
    
    private String buildSqlGenerationPrompt(String query, JsonObject schema, JsonObject data) {
        StringBuilder prompt = new StringBuilder();
        prompt.append("Generate Oracle SQL for: ").append(query).append("\n\n");
        
        // Add explicit schema information
        if (schema != null && !schema.isEmpty()) {
            prompt.append("IMPORTANT: Use ONLY the following discovered schema information:\n\n");
            
            if (schema.containsKey("tables")) {
                prompt.append("Available Tables:\n");
                JsonArray tables = schema.getJsonArray("tables");
                for (int i = 0; i < tables.size(); i++) {
                    JsonObject table = tables.getJsonObject(i);
                    prompt.append("  - ").append(table.getString("table")).append("\n");
                }
                prompt.append("\n");
            }
            
            if (schema.containsKey("columns")) {
                prompt.append("Available Columns:\n");
                JsonArray columns = schema.getJsonArray("columns");
                for (int i = 0; i < Math.min(20, columns.size()); i++) {
                    JsonObject col = columns.getJsonObject(i);
                    prompt.append("  - ").append(col.getString("table")).append(".")
                          .append(col.getString("column")).append(" (")
                          .append(col.getString("type")).append(")\n");
                }
                prompt.append("\n");
            }
            
            prompt.append("Full schema details:\n").append(schema.encodePrettily()).append("\n\n");
        }
        
        // Add sample data if available
        if (data != null && !data.isEmpty()) {
            prompt.append("Sample data from tables:\n").append(data.encodePrettily()).append("\n\n");
        }
        
        prompt.append("Rules:\n");
        prompt.append("1. Use ONLY the tables and columns shown above\n");
        prompt.append("2. Table names are case-sensitive in Oracle (use uppercase as shown)\n");
        prompt.append("3. For 'California', check if it's in a city, state, or country column\n");
        prompt.append("4. For 'pending', check status-related columns or enumeration tables\n");
        prompt.append("5. DO NOT add semicolons at the end of the SQL\n");
        prompt.append("6. If you need to join with enumeration tables, use the proper foreign key relationships\n");
        prompt.append("\nReturn only the SQL query, no explanation or markdown.");
        
        return prompt.toString();
    }
    
    private String buildFormattingPrompt(String query, String sql, JsonArray results) {
        return "User asked: " + query + "\n\n" +
               "Results: " + results.encodePrettily() + "\n\n" +
               "Provide a natural language response.";
    }
    
    private JsonObject performPatternAnalysis(String query) {
        // Fallback pattern-based analysis
        String lower = query.toLowerCase();
        
        JsonObject analysis = new JsonObject();
        
        // Detect intent
        if (lower.contains("how many") || lower.contains("count")) {
            analysis.put("intent", "count");
        } else if (lower.contains("list") || lower.contains("show")) {
            analysis.put("intent", "list");
        } else {
            analysis.put("intent", "search");
        }
        
        // Extract entities
        JsonArray entities = new JsonArray();
        if (lower.contains("orders")) entities.add("orders");
        if (lower.contains("customers")) entities.add("customers");
        if (lower.contains("products")) entities.add("products");
        analysis.put("entities", entities);
        
        return analysis;
    }
    
    private String extractLlmContent(JsonObject response) {
        JsonArray choices = response.getJsonArray("choices", new JsonArray());
        if (!choices.isEmpty()) {
            return choices.getJsonObject(0)
                .getJsonObject("message")
                .getString("content", "");
        }
        return "";
    }
    
    /**
     * Strip markdown code blocks from SQL
     */
    private String stripMarkdownFromSql(String sql) {
        if (sql == null || sql.isEmpty()) {
            return sql;
        }
        
        // Remove ```sql and ``` markers
        String cleaned = sql;
        
        // Check for markdown code blocks
        if (cleaned.contains("```")) {
            // Extract content between ```sql and ```
            int startIdx = cleaned.indexOf("```sql");
            if (startIdx >= 0) {
                startIdx += 6; // Skip past ```sql
                int endIdx = cleaned.indexOf("```", startIdx);
                if (endIdx > startIdx) {
                    cleaned = cleaned.substring(startIdx, endIdx).trim();
                }
            } else {
                // Try just ``` markers
                startIdx = cleaned.indexOf("```");
                if (startIdx >= 0) {
                    startIdx += 3; // Skip past ```
                    int endIdx = cleaned.indexOf("```", startIdx);
                    if (endIdx > startIdx) {
                        cleaned = cleaned.substring(startIdx, endIdx).trim();
                    }
                }
            }
        }
        
        // Remove trailing semicolons (Oracle JDBC doesn't want them)
        cleaned = cleaned.trim();
        while (cleaned.endsWith(";")) {
            cleaned = cleaned.substring(0, cleaned.length() - 1).trim();
        }
        
        // Log the cleaning
        if (!cleaned.equals(sql)) {
            System.out.println("[OracleTools] Cleaned SQL:");
            System.out.println("[OracleTools]   Original: " + sql.substring(0, Math.min(200, sql.length())));
            System.out.println("[OracleTools]   Cleaned: " + cleaned.substring(0, Math.min(200, cleaned.length())));
            if (sql.contains(";") && !cleaned.contains(";")) {
                System.out.println("[OracleTools]   Removed trailing semicolon(s)");
            }
        }
        
        return cleaned;
    }
    
    /**
     * Register event bus handlers for tools
     */
    private void registerToolHandlers() {
        System.out.println("[OracleTools] Registering event bus handlers for " + tools.size() + " tools");
        
        for (int i = 0; i < tools.size(); i++) {
            JsonObject tool = tools.getJsonObject(i);
            String toolName = tool.getString("name");
            String fullToolName = "oracle-tools__" + toolName;
            
            // Register handler for direct tool calls
            vertx.eventBus().consumer("tool." + fullToolName, msg -> {
                JsonObject request = (JsonObject) msg.body();
                System.out.println("[OracleTools] Handling tool call: " + fullToolName);
                
                // Log through event bus
                vertx.eventBus().publish("log",
                    "OracleTools executing " + toolName + ",3,OracleTools,Execution,Tool");
                
                JsonObject arguments = request.getJsonObject("arguments", new JsonObject());
                
                // Route to appropriate handler
                handleToolExecution(toolName, arguments)
                    .onSuccess(result -> {
                        // Ensure MCP format
                        JsonObject mcpResult = wrapInMcpFormat(result);
                        
                        // Log success
                        vertx.eventBus().publish("log",
                            "Tool " + toolName + " completed,2,OracleTools,Success,Tool");
                        
                        msg.reply(mcpResult);
                    })
                    .onFailure(err -> {
                        // Log error
                        vertx.eventBus().publish("log",
                            "Tool " + toolName + " failed: " + err.getMessage() + ",0,OracleTools,Error,Tool");
                        
                        msg.fail(500, err.getMessage());
                    });
            });
            
            System.out.println("[OracleTools] Registered handler for: " + fullToolName);
        }
    }
    
    /**
     * Publish tool discovery event
     */
    private void publishToolDiscovery() {
        // Transform tools to match expected format
        JsonArray discoveredTools = new JsonArray();
        
        for (int i = 0; i < tools.size(); i++) {
            JsonObject tool = tools.getJsonObject(i);
            discoveredTools.add(new JsonObject()
                .put("name", tool.getString("name"))
                .put("description", tool.getString("description"))
                .put("category", tool.getString("category", "oracle")));
        }
        
        JsonObject discovery = new JsonObject()
            .put("server", "oracle-tools")
            .put("client", "oracle-tools")  // Act as both server and client
            .put("tools", discoveredTools);
        
        System.out.println("[OracleTools] Publishing tool discovery: " + discoveredTools.size() + " tools");
        
        // Publish to event bus for MCP infrastructure
        vertx.eventBus().publish("mcp.tools.discovered", discovery);
        
        // Also publish aggregated tools event
        vertx.eventBus().publish("mcp.tools.aggregated", new JsonObject()
            .put("totalTools", tools.size())
            .put("server", "oracle-tools"));
    }
    
    /**
     * Wrap result in MCP format
     */
    private JsonObject wrapInMcpFormat(JsonObject result) {
        // If already in MCP format, return as-is
        if (result.containsKey("content") && result.getValue("content") instanceof JsonArray) {
            return result;
        }
        
        // Wrap in standard MCP format
        return new JsonObject()
            .put("content", new JsonArray()
                .add(new JsonObject()
                    .put("type", "text")
                    .put("text", result.encodePrettily())))
            .put("isError", false);
    }
    
    /**
     * Route tool execution to appropriate method
     */
    private Future<JsonObject> handleToolExecution(String toolName, JsonObject arguments) {
        System.out.println("[OracleTools] Executing tool: " + toolName);
        
        switch (toolName) {
            case "analyze_query":
                return analyzeQuery(arguments);
            case "match_schema":
                return matchSchema(arguments);
            case "discover_enums":
                return discoverEnums(arguments);
            case "discover_sample_data":
                return discoverSampleData(arguments);
            case "generate_sql":
                return generateSql(arguments);
            case "optimize_sql":
                return optimizeSql(arguments);
            case "validate_sql":
                return validateSql(arguments);
            case "execute_query":
                return executeQuery(arguments);
            case "explain_plan":
                return explainPlan(arguments);
            case "format_results":
                return formatResults(arguments);
            case "summarize_data":
                return summarizeData(arguments);
            default:
                return Future.failedFuture("Unknown tool: " + toolName);
        }
    }
    
    @Override
    public void stop(Promise<Void> stopPromise) {
        if (httpServer != null) {
            httpServer.close(ar -> {
                System.out.println("Oracle Tools Server stopped");
                stopPromise.complete();
            });
        } else {
            stopPromise.complete();
        }
    }
}
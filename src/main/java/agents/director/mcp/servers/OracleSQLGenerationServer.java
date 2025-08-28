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
import java.util.concurrent.CompletableFuture;

/**
 * MCP Server for Oracle SQL generation and optimization.
 * Generates SQL queries from natural language intent and schema mappings.
 * Can also optimize queries using EXPLAIN PLAN.
 * Deployed as a Worker Verticle due to blocking DB and LLM operations.
 */
public class OracleSQLGenerationServer extends MCPServerBase {
    
    
    
    private OracleConnectionManager connectionManager;
    private LlmAPIService llmService;
    
    // SQL generation templates and patterns
    private static final Map<String, String> QUERY_TEMPLATES = Map.of(
        "count", "SELECT COUNT(*) AS total_count FROM {table} {where}",
        "sum", "SELECT SUM({column}) AS total_sum FROM {table} {where}",
        "average", "SELECT AVG({column}) AS average_value FROM {table} {where}",
        "list", "SELECT {columns} FROM {table} {where} {orderby}",
        "distinct", "SELECT DISTINCT {columns} FROM {table} {where}",
        "groupby", "SELECT {columns}, {aggregation} FROM {table} {where} GROUP BY {groupby} {having}"
    );
    
    public OracleSQLGenerationServer() {
        super("OracleSQLGenerationServer", "/mcp/servers/oracle-sql-gen");
    }
    
    @Override
    public void start(Promise<Void> startPromise) {
        // Get connection manager instance (already initialized in Driver)
        connectionManager = OracleConnectionManager.getInstance();
        
        // Check if connection manager is healthy
        if (!connectionManager.isConnectionHealthy()) {
            vertx.eventBus().publish("log", "Oracle Connection Manager not healthy - server will operate with limited functionality,1,OracleSQLGenerationServer,MCP,System");
        } else {
            vertx.eventBus().publish("log", "OracleSQLGenerationServer using connection pool,2,OracleSQLGenerationServer,MCP,System");
        }
        
        // Initialize LLM service
        llmService = LlmAPIService.getInstance();
        if (!llmService.isInitialized()) {
            vertx.eventBus().publish("log", "LLM service not initialized - SQL generation will use templates only,1,OracleSQLGenerationServer,MCP,System");
        }
        
        // Continue with parent initialization regardless
        super.start(startPromise);
    }
    
    @Override
    protected void initializeTools() {
        // Register generate_oracle_sql tool
        registerTool(new MCPTool(
            "generate_oracle_sql",
            "Generate an Oracle SQL query based on a natural language request and schema context.",
            new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("analysis", new JsonObject()
                        .put("type", "object")
                        .put("description", "Output from analyze_query (user intent and entities)."))
                    .put("schemaMatches", new JsonObject()
                        .put("type", "object")
                        .put("description", "Output from match_oracle_schema (matched tables/columns)."))
                    .put("includeEnums", new JsonObject()
                        .put("type", "boolean")
                        .put("description", "If true, translate enum codes to descriptions in results if possible.")
                        .put("default", true))
                    .put("maxComplexity", new JsonObject()
                        .put("type", "string")
                        .put("enum", new JsonArray().add("simple").add("medium").add("complex"))
                        .put("description", "If set, limit the complexity of generated SQL (e.g., number of JOINs).")
                        .put("default", "complex")))
                .put("required", new JsonArray().add("analysis").add("schemaMatches"))
        ));
        
        // Register optimize_oracle_sql tool
        registerTool(new MCPTool(
            "optimize_oracle_sql",
            "Optimize an Oracle SQL query for performance using EXPLAIN PLAN and heuristics.",
            new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("sql", new JsonObject()
                        .put("type", "string")
                        .put("description", "The SQL query to analyze and optimize."))
                    .put("dialectVersion", new JsonObject()
                        .put("type", "string")
                        .put("description", "Oracle SQL dialect/version if relevant.")
                        .put("default", "Oracle12c"))
                    .put("applyHints", new JsonObject()
                        .put("type", "boolean")
                        .put("description", "Whether to apply optimizer hints to the SQL.")
                        .put("default", false)))
                .put("required", new JsonArray().add("sql"))
        ));
    }
    
    @Override
    protected void executeTool(RoutingContext ctx, String requestId, String toolName, JsonObject arguments) {
        switch (toolName) {
            case "generate_oracle_sql":
                generateSQL(ctx, requestId, arguments);
                break;
            case "optimize_oracle_sql":
                optimizeSQL(ctx, requestId, arguments);
                break;
            default:
                sendError(ctx, requestId, MCPResponse.ErrorCodes.METHOD_NOT_FOUND, 
                    "Unknown tool: " + toolName);
        }
    }
    
    private void generateSQL(RoutingContext ctx, String requestId, JsonObject arguments) {
        JsonObject analysis = arguments.getJsonObject("analysis");
        JsonObject schemaMatches = arguments.getJsonObject("schemaMatches");
        boolean includeEnums = arguments.getBoolean("includeEnums", true);
        String maxComplexity = arguments.getString("maxComplexity", "complex");
        
        if (analysis == null || schemaMatches == null) {
            sendError(ctx, requestId, MCPResponse.ErrorCodes.INVALID_PARAMS, 
                "Both analysis and schemaMatches are required");
            return;
        }
        
        executeBlocking(Promise.<JsonObject>promise(), promise -> {
            try {
                JsonObject result = new JsonObject();
                String sql;
                
                if (llmService.isInitialized()) {
                    // Use LLM for SQL generation
                    sql = generateSQLWithLLM(analysis, schemaMatches, includeEnums, maxComplexity);
                } else {
                    // Fall back to template-based generation
                    sql = generateSQLWithTemplates(analysis, schemaMatches);
                }
                
                result.put("sql", sql);
                result.put("dialect", "Oracle");
                result.put("includesEnums", includeEnums);
                result.put("complexity", analyzeComplexity(sql));
                
                // Add metadata about the generation
                JsonObject metadata = new JsonObject()
                    .put("method", llmService.isInitialized() ? "llm" : "template")
                    .put("confidence", llmService.isInitialized() ? 0.85 : 0.65)
                    .put("tables", extractTablesFromSQL(sql))
                    .put("hasJoins", sql.toUpperCase().contains("JOIN"))
                    .put("hasAggregation", hasAggregation(sql));
                
                result.put("metadata", metadata);
                promise.complete(result);
                
            } catch (Exception e) {
                vertx.eventBus().publish("log", "SQL generation failed: " + e.getMessage() + ",0,OracleSQLGenerationServer,MCP,System");
                promise.fail(e);
            }
        }, res -> {
            if (res.succeeded()) {
                sendSuccess(ctx, requestId, res.result());
            } else {
                sendError(ctx, requestId, MCPResponse.ErrorCodes.INTERNAL_ERROR, 
                    "SQL generation failed: " + res.cause().getMessage());
            }
        });
    }
    
    private void optimizeSQL(RoutingContext ctx, String requestId, JsonObject arguments) {
        String sql = arguments.getString("sql");
        String dialectVersion = arguments.getString("dialectVersion", "Oracle12c");
        boolean applyHints = arguments.getBoolean("applyHints", false);
        
        if (sql == null || sql.trim().isEmpty()) {
            sendError(ctx, requestId, MCPResponse.ErrorCodes.INVALID_PARAMS, "SQL query is required");
            return;
        }
        
        executeBlocking(Promise.<JsonObject>promise(), promise -> {
            try {
                JsonObject result = new JsonObject();
                
                // Get EXPLAIN PLAN
                JsonObject explainPlan = getExplainPlan(sql);
                result.put("originalSQL", sql);
                result.put("explainPlan", explainPlan);
                
                // Analyze the plan and generate optimization suggestions
                JsonArray suggestions = analyzeExplainPlan(explainPlan, sql);
                result.put("suggestions", suggestions);
                
                // Generate optimized SQL if requested
                if (applyHints || !suggestions.isEmpty()) {
                    String optimizedSQL = applyOptimizations(sql, suggestions, applyHints);
                    result.put("optimizedSQL", optimizedSQL);
                    
                    // Get EXPLAIN PLAN for optimized query
                    JsonObject optimizedPlan = getExplainPlan(optimizedSQL);
                    result.put("optimizedExplainPlan", optimizedPlan);
                    
                    // Compare costs
                    double originalCost = explainPlan.getDouble("totalCost", Double.MAX_VALUE);
                    double optimizedCost = optimizedPlan.getDouble("totalCost", Double.MAX_VALUE);
                    result.put("costReduction", originalCost - optimizedCost);
                    result.put("improvementPercent", 
                        originalCost > 0 ? ((originalCost - optimizedCost) / originalCost) * 100 : 0);
                }
                
                promise.complete(result);
                
            } catch (Exception e) {
                vertx.eventBus().publish("log", "SQL optimization failed" + ",0,OracleSQLGenerationServer,MCP,System");
                promise.fail(e);
            }
        }, res -> {
            if (res.succeeded()) {
                sendSuccess(ctx, requestId, res.result());
            } else {
                sendError(ctx, requestId, MCPResponse.ErrorCodes.INTERNAL_ERROR, 
                    "SQL optimization failed: " + res.cause().getMessage());
            }
        });
    }
    
    private String generateSQLWithLLM(JsonObject analysis, JsonObject schemaMatches, 
                                     boolean includeEnums, String maxComplexity) throws Exception {
        String systemPrompt = """
            You are an Oracle SQL expert. Generate precise, efficient Oracle SQL queries based on:
            1. The user's analyzed intent
            2. The matched schema elements
            3. Oracle SQL best practices
            
            Rules:
            - Use proper Oracle syntax (e.g., ROWNUM for limiting rows)
            - Include appropriate JOINs when multiple tables are needed
            - Add WHERE clauses for any filters mentioned
            - Use appropriate aggregate functions when needed
            - Consider enum translations if includeEnums is true
            - Respect the maxComplexity setting
            - Use table aliases for clarity
            - Include ORDER BY when relevant
            
            Respond with ONLY the SQL query, no explanations.
            """;
        
        JsonObject promptData = new JsonObject();
        promptData.put("userIntent", analysis.getString("intent", ""));
        promptData.put("queryType", analysis.getString("queryType", ""));
        promptData.put("entities", analysis.getJsonArray("entities", new JsonArray()));
        promptData.put("timeframe", analysis.getString("timeframe"));
        promptData.put("aggregations", analysis.getJsonArray("aggregations", new JsonArray()));
        
        // Extract matched tables and columns
        JsonArray matches = schemaMatches.getJsonArray("matches", new JsonArray());
        JsonArray tablesInfo = new JsonArray();
        for (int i = 0; i < matches.size(); i++) {
            JsonObject match = matches.getJsonObject(i);
            JsonObject table = match.getJsonObject("table");
            tablesInfo.add(new JsonObject()
                .put("tableName", table.getString("tableName"))
                .put("relevantColumns", match.getJsonArray("relevantColumns", new JsonArray()))
                .put("confidence", match.getDouble("confidence")));
        }
        promptData.put("matchedTables", tablesInfo);
        promptData.put("includeEnums", includeEnums);
        promptData.put("maxComplexity", maxComplexity);
        
        // Add complexity constraints
        if ("simple".equals(maxComplexity)) {
            promptData.put("constraints", "No JOINs, single table only");
        } else if ("medium".equals(maxComplexity)) {
            promptData.put("constraints", "Maximum 2 JOINs");
        }
        
        List<JsonObject> messages = Arrays.asList(
            new JsonObject().put("role", "system").put("content", systemPrompt),
            new JsonObject().put("role", "user").put("content", promptData.encodePrettily())
        );
        
        JsonObject llmResponse = llmService.chatCompletion(
            messages.stream().map(JsonObject::encode).collect(Collectors.toList()),
            0.0, // temperature 0 for consistent SQL
            500  // max tokens
        ).join();
        
        String sql = llmResponse.getJsonArray("choices")
            .getJsonObject(0)
            .getJsonObject("message")
            .getString("content")
            .trim();
        
        // Clean up the SQL
        sql = sql.replaceAll("```sql", "").replaceAll("```", "").trim();
        
        return sql;
    }
    
    private String generateSQLWithTemplates(JsonObject analysis, JsonObject schemaMatches) {
        String queryType = analysis.getString("queryType", "retrieval");
        JsonArray aggregations = analysis.getJsonArray("aggregations", new JsonArray());
        JsonArray matches = schemaMatches.getJsonArray("matches", new JsonArray());
        
        if (matches.isEmpty()) {
            throw new RuntimeException("No schema matches found to generate SQL");
        }
        
        // Get the best match
        JsonObject bestMatch = matches.getJsonObject(0);
        JsonObject table = bestMatch.getJsonObject("table");
        String tableName = table.getString("tableName");
        JsonArray columns = table.getJsonArray("columns");
        JsonArray relevantColumns = bestMatch.getJsonArray("relevantColumns", new JsonArray());
        
        String sql;
        
        if (queryType.equals("aggregation") && !aggregations.isEmpty()) {
            String aggType = aggregations.getString(0).toLowerCase();
            String template = QUERY_TEMPLATES.getOrDefault(aggType, QUERY_TEMPLATES.get("count"));
            
            String columnToAggregate = relevantColumns.isEmpty() ? "*" : relevantColumns.getString(0);
            sql = template.replace("{table}", tableName)
                         .replace("{column}", columnToAggregate)
                         .replace("{where}", "");
        } else {
            // Default to list query
            String columnsStr = relevantColumns.isEmpty() ? "*" : 
                relevantColumns.stream()
                    .map(Object::toString)
                    .collect(Collectors.joining(", "));
            
            sql = QUERY_TEMPLATES.get("list")
                .replace("{columns}", columnsStr)
                .replace("{table}", tableName)
                .replace("{where}", "")
                .replace("{orderby}", "");
        }
        
        // Add timeframe WHERE clause if present
        String timeframe = analysis.getString("timeframe");
        if (timeframe != null && !timeframe.isEmpty()) {
            sql = addTimeframeCondition(sql, timeframe);
        }
        
        return sql.trim();
    }
    
    private JsonObject getExplainPlan(String sql) throws Exception {
        try {
            return connectionManager.executeWithConnection(conn -> {
                try {
                    JsonObject plan = new JsonObject();
                    Statement stmt = conn.createStatement();
                    
                    try {
                        // Generate unique statement ID
                        String stmtId = "STMT_" + System.currentTimeMillis();
                        
                        // Run EXPLAIN PLAN
                        stmt.execute("EXPLAIN PLAN SET STATEMENT_ID = '" + stmtId + "' FOR " + sql);
                        
                        // Query the plan table
                        String planQuery = """
                            SELECT LEVEL, LPAD(' ', 2 * (LEVEL - 1)) || operation || ' ' || 
                                   NVL(options, '') AS operation,
                                   object_name, cost, cardinality, bytes, cpu_cost, io_cost
                            FROM plan_table
                            WHERE statement_id = ?
                            START WITH id = 0
                            CONNECT BY PRIOR id = parent_id AND statement_id = ?
                            ORDER BY id
                            """;
                        
                        PreparedStatement ps = conn.prepareStatement(planQuery);
                        ps.setString(1, stmtId);
                        ps.setString(2, stmtId);
                        
                        ResultSet rs = ps.executeQuery();
                        JsonArray operations = new JsonArray();
                        double totalCost = 0;
                        
                        while (rs.next()) {
                            JsonObject op = new JsonObject()
                                .put("level", rs.getInt("LEVEL"))
                                .put("operation", rs.getString("operation"))
                                .put("objectName", rs.getString("object_name"))
                                .put("cost", rs.getDouble("cost"))
                                .put("cardinality", rs.getLong("cardinality"))
                                .put("bytes", rs.getLong("bytes"))
                                .put("cpuCost", rs.getLong("cpu_cost"))
                                .put("ioCost", rs.getLong("io_cost"));
                            operations.add(op);
                            
                            if (rs.getInt("LEVEL") == 1) {
                                totalCost = rs.getDouble("cost");
                            }
                        }
                        
                        rs.close();
                        ps.close();
                        
                        // Clean up plan table
                        stmt.execute("DELETE FROM plan_table WHERE statement_id = '" + stmtId + "'");
                        
                        plan.put("operations", operations);
                        plan.put("totalCost", totalCost);
                        
                    } finally {
                        stmt.close();
                    }
                    
                    return plan;
                } catch (SQLException e) {
                    throw new RuntimeException("Failed to get execution plan: " + e.getMessage(), e);
                }
            }).toCompletionStage().toCompletableFuture().get();
        } catch (Exception e) {
            throw new Exception("Failed to get execution plan: " + e.getMessage(), e);
        }
    }
    
    private JsonArray analyzeExplainPlan(JsonObject explainPlan, String sql) {
        JsonArray suggestions = new JsonArray();
        JsonArray operations = explainPlan.getJsonArray("operations", new JsonArray());
        
        for (int i = 0; i < operations.size(); i++) {
            JsonObject op = operations.getJsonObject(i);
            String operation = op.getString("operation", "").toUpperCase();
            
            // Check for full table scans
            if (operation.contains("TABLE ACCESS FULL")) {
                String tableName = op.getString("objectName");
                suggestions.add(new JsonObject()
                    .put("type", "index")
                    .put("severity", "high")
                    .put("message", "Full table scan detected on " + tableName)
                    .put("suggestion", "Consider adding an index on frequently queried columns"));
            }
            
            // Check for expensive sorts
            if (operation.contains("SORT") && op.getDouble("cost", 0.0) > 1000) {
                suggestions.add(new JsonObject()
                    .put("type", "sort")
                    .put("severity", "medium")
                    .put("message", "Expensive sort operation detected")
                    .put("suggestion", "Consider adding an index on ORDER BY columns"));
            }
            
            // Check for nested loops on large datasets
            if (operation.contains("NESTED LOOPS") && op.getLong("cardinality", 0L) > 10000) {
                suggestions.add(new JsonObject()
                    .put("type", "join")
                    .put("severity", "high")
                    .put("message", "Nested loops on large dataset")
                    .put("suggestion", "Consider using HASH JOIN hint for large datasets"));
            }
        }
        
        // Check for missing WHERE clause
        if (!sql.toUpperCase().contains("WHERE") && sql.toUpperCase().contains("FROM")) {
            suggestions.add(new JsonObject()
                .put("type", "filter")
                .put("severity", "medium")
                .put("message", "No WHERE clause detected")
                .put("suggestion", "Add WHERE clause to filter results and improve performance"));
        }
        
        return suggestions;
    }
    
    private String applyOptimizations(String sql, JsonArray suggestions, boolean applyHints) {
        String optimizedSQL = sql;
        
        if (applyHints) {
            // Apply Oracle optimizer hints based on suggestions
            for (int i = 0; i < suggestions.size(); i++) {
                JsonObject suggestion = suggestions.getJsonObject(i);
                String type = suggestion.getString("type");
                
                if ("join".equals(type) && suggestion.getString("suggestion", "").contains("HASH JOIN")) {
                    // Add USE_HASH hint
                    optimizedSQL = addHint(optimizedSQL, "USE_HASH");
                } else if ("index".equals(type)) {
                    // Add INDEX hint (would need actual index name in real implementation)
                    optimizedSQL = addHint(optimizedSQL, "INDEX");
                }
            }
        }
        
        // Apply other optimizations
        // Add ROWNUM limit if not present and SELECT without aggregation
        if (!optimizedSQL.toUpperCase().contains("ROWNUM") && 
            !optimizedSQL.toUpperCase().contains("COUNT") &&
            !optimizedSQL.toUpperCase().contains("SUM") &&
            !optimizedSQL.toUpperCase().contains("AVG")) {
            optimizedSQL = addRowNumLimit(optimizedSQL, 1000);
        }
        
        return optimizedSQL;
    }
    
    private String addHint(String sql, String hint) {
        // Add Oracle optimizer hint after SELECT
        return sql.replaceFirst("SELECT", "SELECT /*+ " + hint + " */");
    }
    
    private String addRowNumLimit(String sql, int limit) {
        if (sql.toUpperCase().contains("WHERE")) {
            return sql.replaceFirst("WHERE", "WHERE ROWNUM <= " + limit + " AND");
        } else {
            return sql + " WHERE ROWNUM <= " + limit;
        }
    }
    
    private String addTimeframeCondition(String sql, String timeframe) {
        String condition = "";
        
        switch (timeframe.toLowerCase()) {
            case "today":
                condition = "TRUNC(date_column) = TRUNC(SYSDATE)";
                break;
            case "yesterday":
                condition = "TRUNC(date_column) = TRUNC(SYSDATE - 1)";
                break;
            case "last week":
                condition = "date_column >= TRUNC(SYSDATE) - 7";
                break;
            case "last month":
                condition = "date_column >= ADD_MONTHS(TRUNC(SYSDATE), -1)";
                break;
            case "this month":
                condition = "TRUNC(date_column, 'MM') = TRUNC(SYSDATE, 'MM')";
                break;
            case "last year":
                condition = "TRUNC(date_column, 'YYYY') = TRUNC(SYSDATE, 'YYYY') - 1";
                break;
            case "this year":
                condition = "TRUNC(date_column, 'YYYY') = TRUNC(SYSDATE, 'YYYY')";
                break;
        }
        
        if (!condition.isEmpty()) {
            if (sql.toUpperCase().contains("WHERE")) {
                sql += " AND " + condition;
            } else {
                sql += " WHERE " + condition;
            }
        }
        
        return sql;
    }
    
    private String analyzeComplexity(String sql) {
        String upperSQL = sql.toUpperCase();
        int joinCount = countOccurrences(upperSQL, "JOIN");
        int subqueryCount = countOccurrences(upperSQL, "SELECT") - 1; // Minus the main SELECT
        
        if (joinCount == 0 && subqueryCount == 0) {
            return "simple";
        } else if (joinCount <= 2 && subqueryCount <= 1) {
            return "medium";
        } else {
            return "complex";
        }
    }
    
    private int countOccurrences(String text, String search) {
        int count = 0;
        int index = 0;
        while ((index = text.indexOf(search, index)) != -1) {
            count++;
            index += search.length();
        }
        return count;
    }
    
    private JsonArray extractTablesFromSQL(String sql) {
        JsonArray tables = new JsonArray();
        String upperSQL = sql.toUpperCase();
        
        // Simple extraction - would need proper SQL parser for production
        String[] parts = upperSQL.split("\\s+FROM\\s+");
        if (parts.length > 1) {
            String fromClause = parts[1].split("\\s+(WHERE|GROUP|ORDER|HAVING)\\s+")[0];
            String[] tableRefs = fromClause.split("\\s*(,|JOIN|LEFT|RIGHT|INNER|OUTER)\\s+");
            
            for (String tableRef : tableRefs) {
                String table = tableRef.trim().split("\\s+")[0];
                if (!table.isEmpty() && !table.equals("ON")) {
                    tables.add(table);
                }
            }
        }
        
        return tables;
    }
    
    private boolean hasAggregation(String sql) {
        String upperSQL = sql.toUpperCase();
        return upperSQL.contains("COUNT(") || upperSQL.contains("SUM(") || 
               upperSQL.contains("AVG(") || upperSQL.contains("MAX(") || 
               upperSQL.contains("MIN(") || upperSQL.contains("GROUP BY");
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
    
    /**
     * Get deployment options for this server (Worker Verticle)
     */
    public static DeploymentOptions getDeploymentOptions() {
        return new DeploymentOptions()
            .setWorker(true)
            .setWorkerPoolSize(3);
    }
}
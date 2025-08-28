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
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.concurrent.CompletableFuture;

/**
 * MCP Server for Oracle SQL validation and error analysis.
 * Validates SQL queries against the database schema and provides error explanations.
 * Deployed as a Worker Verticle due to blocking DB operations.
 */
public class OracleSQLValidationServer extends MCPServerBase {
    
    
    
    private OracleConnectionManager connectionManager;
    private LlmAPIService llmService;
    
    // Common Oracle error codes and their meanings
    private static final Map<String, ErrorInfo> ORACLE_ERRORS = new HashMap<>();
    
    static {
        // Initialize common Oracle errors
        ORACLE_ERRORS.put("ORA-00942", new ErrorInfo(
            "Table or view does not exist",
            "The specified table or view was not found in the database schema.",
            Arrays.asList(
                "Verify the table name is spelled correctly",
                "Check if you have access permissions to the table",
                "Ensure you're connected to the correct schema",
                "Use fully qualified name (schema.table)"
            )
        ));
        
        ORACLE_ERRORS.put("ORA-00904", new ErrorInfo(
            "Invalid identifier",
            "A column name or other identifier is not valid or does not exist.",
            Arrays.asList(
                "Check column names for typos",
                "Verify the column exists in the table",
                "Ensure proper case sensitivity (Oracle is case-sensitive for quoted identifiers)",
                "Remove any special characters from identifiers"
            )
        ));
        
        ORACLE_ERRORS.put("ORA-00936", new ErrorInfo(
            "Missing expression",
            "The SQL statement has a syntax error - an expression is expected but not found.",
            Arrays.asList(
                "Check for missing column names in SELECT clause",
                "Verify all operators have operands",
                "Ensure proper comma placement in lists",
                "Check for incomplete WHERE or HAVING clauses"
            )
        ));
        
        ORACLE_ERRORS.put("ORA-00933", new ErrorInfo(
            "SQL command not properly ended",
            "The SQL statement contains syntax that is not valid.",
            Arrays.asList(
                "Remove any trailing semicolons in the query",
                "Check for missing keywords (FROM, WHERE, etc.)",
                "Verify parentheses are balanced",
                "Ensure proper clause order (SELECT, FROM, WHERE, GROUP BY, HAVING, ORDER BY)"
            )
        ));
        
        ORACLE_ERRORS.put("ORA-01722", new ErrorInfo(
            "Invalid number",
            "An attempt was made to convert a character string to a number, but the string cannot be converted.",
            Arrays.asList(
                "Check data types in WHERE conditions",
                "Ensure numeric comparisons use numeric values",
                "Use TO_NUMBER() for explicit conversions",
                "Verify date formats when comparing dates"
            )
        ));
        
        ORACLE_ERRORS.put("ORA-00918", new ErrorInfo(
            "Column ambiguously defined",
            "A column name appears in multiple tables in the FROM clause without proper qualification.",
            Arrays.asList(
                "Use table aliases to qualify column names",
                "Prefix ambiguous columns with table names",
                "Check JOIN conditions for proper aliases"
            )
        ));
    }
    
    // Inner class for error information
    private static class ErrorInfo {
        final String summary;
        final String description;
        final List<String> solutions;
        
        ErrorInfo(String summary, String description, List<String> solutions) {
            this.summary = summary;
            this.description = description;
            this.solutions = solutions;
        }
    }
    
    public OracleSQLValidationServer() {
        super("OracleSQLValidationServer", "/mcp/servers/oracle-sql-val");
    }
    
    @Override
    public void start(Promise<Void> startPromise) {
        // Get connection manager instance (already initialized in Driver)
        connectionManager = OracleConnectionManager.getInstance();
        
        // Check if connection manager is healthy
        if (!connectionManager.isConnectionHealthy()) {
            vertx.eventBus().publish("log", "Oracle Connection Manager not healthy - server will operate with limited functionality,1,OracleSQLValidationServer,MCP,System");
        } else {
            vertx.eventBus().publish("log", "OracleSQLValidationServer using connection pool,2,OracleSQLValidationServer,MCP,System");
        }
        
        // Initialize LLM service
        llmService = LlmAPIService.getInstance();
        
        // Continue with parent initialization regardless
        super.start(startPromise);
    }
    
    @Override
    protected void initializeTools() {
        // Register validate_oracle_sql tool
        registerTool(new MCPTool(
            "validate_oracle_sql",
            "Validate an Oracle SQL query against the database schema and permissions.",
            new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("sql", new JsonObject()
                        .put("type", "string")
                        .put("description", "The SQL query to validate."))
                    .put("checkPermissions", new JsonObject()
                        .put("type", "boolean")
                        .put("description", "Check if the current user has permissions for the query.")
                        .put("default", false))
                    .put("suggestFixes", new JsonObject()
                        .put("type", "boolean")
                        .put("description", "If invalid, attempt to suggest fixes for errors.")
                        .put("default", true))
                    .put("fullSchema", new JsonObject()
                        .put("type", "object")
                        .put("description", "Full database schema for better error suggestions (optional)")))
                .put("required", new JsonArray().add("sql"))
        ));
        
        // Register explain_oracle_error tool
        registerTool(new MCPTool(
            "explain_oracle_error",
            "Provide a detailed explanation and possible solution for an Oracle error code or message.",
            new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("errorCode", new JsonObject()
                        .put("type", "string")
                        .put("description", "Oracle error code (e.g., ORA-00942)."))
                    .put("errorMessage", new JsonObject()
                        .put("type", "string")
                        .put("description", "Full Oracle error message text.")
                        .put("default", "")))
                .put("required", new JsonArray().add("errorCode"))
        ));
        
        // Register explain_plan tool
        registerTool(new MCPTool(
            "explain_plan",
            "Get execution plan for an Oracle SQL query to understand performance characteristics.",
            new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("sql", new JsonObject()
                        .put("type", "string")
                        .put("description", "The SQL query to analyze.")))
                .put("required", new JsonArray().add("sql"))
        ));
    }
    
    @Override
    protected void executeTool(RoutingContext ctx, String requestId, String toolName, JsonObject arguments) {
        switch (toolName) {
            case "validate_oracle_sql":
                validateSQL(ctx, requestId, arguments);
                break;
            case "explain_oracle_error":
                explainError(ctx, requestId, arguments);
                break;
            case "explain_plan":
                explainPlan(ctx, requestId, arguments);
                break;
            default:
                sendError(ctx, requestId, MCPResponse.ErrorCodes.METHOD_NOT_FOUND, 
                    "Unknown tool: " + toolName);
        }
    }
    
    private void validateSQL(RoutingContext ctx, String requestId, JsonObject arguments) {
        String sql = arguments.getString("sql");
        boolean checkPermissions = arguments.getBoolean("checkPermissions", false);
        boolean suggestFixes = arguments.getBoolean("suggestFixes", true);
        JsonObject fullSchema = arguments.getJsonObject("fullSchema"); // NEW: Extract full schema
        
        if (sql == null || sql.trim().isEmpty()) {
            sendError(ctx, requestId, MCPResponse.ErrorCodes.INVALID_PARAMS, "SQL query is required");
            return;
        }
        
        executeBlocking(Promise.<JsonObject>promise(), promise -> {
            try {
                JsonObject result = new JsonObject();
                result.put("sql", sql);
                result.put("valid", false); // Default to invalid
                
                // Perform multiple validation checks
                JsonArray validationSteps = new JsonArray();
                
                // 1. Basic syntax validation
                JsonObject syntaxCheck = performSyntaxCheck(sql);
                validationSteps.add(syntaxCheck);
                
                if (!syntaxCheck.getBoolean("passed", false)) {
                    result.put("validationSteps", validationSteps);
                    result.put("errors", syntaxCheck.getJsonArray("errors", new JsonArray()));
                    
                    if (suggestFixes && syntaxCheck.containsKey("suggestedFix")) {
                        result.put("suggestedSQL", syntaxCheck.getString("suggestedFix"));
                    }
                    
                    promise.complete(result);
                    return;
                }
                
                // 2. Schema validation (parse and check tables/columns exist)
                JsonObject schemaCheck = performSchemaValidation(sql);
                validationSteps.add(schemaCheck);
                
                if (!schemaCheck.getBoolean("passed", false)) {
                    result.put("validationSteps", validationSteps);
                    result.put("errors", schemaCheck.getJsonArray("errors", new JsonArray()));
                    
                    if (suggestFixes) {
                        String suggestedSQL = suggestSchemaFixes(sql, schemaCheck, fullSchema);
                        if (suggestedSQL != null) {
                            result.put("suggestedSQL", suggestedSQL);
                        }
                    }
                    
                    promise.complete(result);
                    return;
                }
                
                // 3. Execute with EXPLAIN PLAN to catch runtime errors
                JsonObject executionCheck = performExecutionValidation(sql);
                validationSteps.add(executionCheck);
                
                if (!executionCheck.getBoolean("passed", false)) {
                    result.put("validationSteps", validationSteps);
                    result.put("errors", executionCheck.getJsonArray("errors", new JsonArray()));
                    
                    if (suggestFixes && llmService.isInitialized()) {
                        String suggestedSQL = suggestExecutionFixes(sql, executionCheck);
                        if (suggestedSQL != null) {
                            result.put("suggestedSQL", suggestedSQL);
                        }
                    }
                    
                    promise.complete(result);
                    return;
                }
                
                // 4. Permission check if requested
                if (checkPermissions) {
                    JsonObject permissionCheck = performPermissionCheck(sql);
                    validationSteps.add(permissionCheck);
                    
                    if (!permissionCheck.getBoolean("passed", false)) {
                        result.put("validationSteps", validationSteps);
                        result.put("errors", permissionCheck.getJsonArray("errors", new JsonArray()));
                        result.put("permissionIssues", true);
                        promise.complete(result);
                        return;
                    }
                }
                
                // All checks passed
                result.put("valid", true);
                result.put("validationSteps", validationSteps);
                result.put("message", "SQL query is valid and can be executed");
                
                // Add execution plan preview
                result.put("executionPlan", executionCheck.getJsonObject("plan", new JsonObject()));
                
                promise.complete(result);
                
            } catch (Exception e) {
                vertx.eventBus().publish("log", "SQL validation failed" + ",0,OracleSQLValidationServer,MCP,System");
                promise.fail(e);
            }
        }, res -> {
            if (res.succeeded()) {
                sendSuccess(ctx, requestId, res.result());
            } else {
                sendError(ctx, requestId, MCPResponse.ErrorCodes.INTERNAL_ERROR, 
                    "SQL validation failed: " + res.cause().getMessage());
            }
        });
    }
    
    private void explainError(RoutingContext ctx, String requestId, JsonObject arguments) {
        String errorCode = arguments.getString("errorCode");
        String errorMessage = arguments.getString("errorMessage", "");
        
        if (errorCode == null || errorCode.trim().isEmpty()) {
            sendError(ctx, requestId, MCPResponse.ErrorCodes.INVALID_PARAMS, "Error code is required");
            return;
        }
        
        JsonObject result = new JsonObject();
        result.put("errorCode", errorCode.toUpperCase());
        result.put("errorMessage", errorMessage);
        
        // Check if we have a pre-defined explanation
        ErrorInfo errorInfo = ORACLE_ERRORS.get(errorCode.toUpperCase());
        
        if (errorInfo != null) {
            result.put("summary", errorInfo.summary);
            result.put("description", errorInfo.description);
            result.put("solutions", new JsonArray(errorInfo.solutions));
            result.put("source", "knowledge_base");
        } else if (llmService.isInitialized()) {
            // Use LLM to explain unknown errors
            executeBlocking(Promise.<JsonObject>promise(), promise -> {
                try {
                    JsonObject explanation = explainErrorWithLLM(errorCode, errorMessage);
                    result.mergeIn(explanation);
                    result.put("source", "llm_analysis");
                    promise.complete(result);
                } catch (Exception e) {
                    vertx.eventBus().publish("log", "LLM error explanation failed" + ",0,OracleSQLValidationServer,MCP,System");
                    // Fall back to generic explanation
                    result.put("summary", "Unknown Oracle error");
                    result.put("description", "This error code is not in our knowledge base.");
                    result.put("solutions", new JsonArray().add("Consult Oracle documentation"));
                    result.put("source", "fallback");
                    promise.complete(result);
                }
            }, res -> {
                sendSuccess(ctx, requestId, res.result());
            });
            return;
        } else {
            // Generic fallback
            result.put("summary", "Oracle error: " + errorCode);
            result.put("description", "Error details not available in knowledge base.");
            result.put("solutions", new JsonArray()
                .add("Check Oracle documentation for " + errorCode)
                .add("Verify the SQL syntax and schema objects")
                .add("Review error message for specific details"));
            result.put("source", "fallback");
        }
        
        sendSuccess(ctx, requestId, result);
    }
    
    private JsonObject performSyntaxCheck(String sql) {
        JsonObject check = new JsonObject()
            .put("step", "syntax_validation")
            .put("passed", true);
        
        JsonArray errors = new JsonArray();
        
        // Basic syntax checks
        String upperSQL = sql.toUpperCase().trim();
        
        // Check for common syntax errors
        if (upperSQL.endsWith(";")) {
            errors.add(new JsonObject()
                .put("type", "syntax")
                .put("message", "SQL should not end with semicolon")
                .put("position", sql.length() - 1));
            check.put("suggestedFix", sql.substring(0, sql.length() - 1).trim());
        }
        
        // Check for balanced parentheses
        int openParens = 0;
        for (char c : sql.toCharArray()) {
            if (c == '(') openParens++;
            else if (c == ')') openParens--;
            if (openParens < 0) {
                errors.add(new JsonObject()
                    .put("type", "syntax")
                    .put("message", "Unbalanced parentheses - too many closing parentheses"));
                break;
            }
        }
        if (openParens > 0) {
            errors.add(new JsonObject()
                .put("type", "syntax")
                .put("message", "Unbalanced parentheses - missing closing parentheses"));
        }
        
        // Check for required keywords
        if (upperSQL.contains("SELECT") && !upperSQL.contains("FROM")) {
            errors.add(new JsonObject()
                .put("type", "syntax")
                .put("message", "SELECT statement missing FROM clause"));
        }
        
        // Check clause order
        String[] clauses = {"SELECT", "FROM", "WHERE", "GROUP BY", "HAVING", "ORDER BY"};
        int lastPos = -1;
        for (String clause : clauses) {
            int pos = upperSQL.indexOf(clause);
            if (pos != -1 && pos < lastPos) {
                errors.add(new JsonObject()
                    .put("type", "syntax")
                    .put("message", "Incorrect clause order - " + clause + " appears in wrong position"));
                break;
            }
            if (pos != -1) lastPos = pos;
        }
        
        if (!errors.isEmpty()) {
            check.put("passed", false);
            check.put("errors", errors);
        }
        
        return check;
    }
    
    private JsonObject performSchemaValidation(String sql) {
        JsonObject check = new JsonObject()
            .put("step", "schema_validation");
        
        JsonArray errors = new JsonArray();
        
        try {
            // Use connection manager to execute EXPLAIN PLAN
            JsonObject validationResult = connectionManager.executeWithConnection(conn -> {
                try {
                    Statement stmt = conn.createStatement();
                    
                    // Try to create an explain plan
                    String stmtId = "VAL_" + System.currentTimeMillis();
                    String explainSQL = "EXPLAIN PLAN SET STATEMENT_ID = '" + stmtId + "' FOR " + sql;
                    
                    try {
                        stmt.execute(explainSQL);
                        // If successful, clean up
                        stmt.execute("DELETE FROM plan_table WHERE statement_id = '" + stmtId + "'");
                        return new JsonObject().put("success", true);
                    } catch (SQLException e) {
                        // Parse the error to identify schema issues
                        String errorCode = "ORA-" + String.format("%05d", Math.abs(e.getErrorCode()));
                        String errorMsg = e.getMessage();
                        
                        JsonObject error = new JsonObject()
                            .put("type", "schema")
                            .put("code", errorCode)
                            .put("message", errorMsg);
                        
                        // Extract specific object that caused error
                        if (errorMsg.contains("table or view does not exist")) {
                            String tableName = extractObjectName(errorMsg);
                            if (tableName != null) {
                                error.put("object", tableName);
                                error.put("objectType", "table");
                            }
                        } else if (errorMsg.contains("invalid identifier")) {
                            String columnName = extractObjectName(errorMsg);
                            if (columnName != null) {
                                error.put("object", columnName);
                                error.put("objectType", "column");
                                
                                // Add suggestions for common terminology differences
                                if (columnName.toLowerCase().contains("state")) {
                                    error.put("suggestion", "Try using 'PROVINCE' instead of 'STATE'");
                                } else if (columnName.toLowerCase().contains("zip")) {
                                    error.put("suggestion", "Try using 'POSTAL_CODE' instead of 'ZIP' or 'ZIPCODE'");
                                }
                            }
                        }
                        
                        return new JsonObject().put("success", false).put("error", error);
                    } finally {
                        stmt.close();
                    }
                } catch (SQLException e) {
                    return new JsonObject().put("success", false)
                        .put("error", new JsonObject()
                            .put("type", "connection")
                            .put("message", "Database operation error: " + e.getMessage()));
                }
            }).toCompletionStage().toCompletableFuture().get();
            
            if (validationResult.getBoolean("success", false)) {
                check.put("passed", true);
            } else {
                errors.add(validationResult.getJsonObject("error"));
                check.put("passed", false);
            }
            
        } catch (Exception e) {
            vertx.eventBus().publish("log", "Schema validation error: " + e.getMessage() + ",0,OracleSQLValidationServer,MCP,System");
            errors.add(new JsonObject()
                .put("type", "connection")
                .put("message", "Database connection error during validation"));
            check.put("passed", false);
        }
        
        if (!errors.isEmpty()) {
            check.put("errors", errors);
        }
        
        return check;
    }
    
    private JsonObject performExecutionValidation(String sql) {
        JsonObject check = new JsonObject()
            .put("step", "execution_validation");
        
        try {
            // Use connection manager to get execution plan
            JsonObject planResult = connectionManager.executeWithConnection(conn -> {
                try {
                    Statement stmt = conn.createStatement();
                    String stmtId = "EXEC_" + System.currentTimeMillis();
                    
                    stmt.execute("EXPLAIN PLAN SET STATEMENT_ID = '" + stmtId + "' FOR " + sql);
                    
                    // Retrieve plan details
                    String planQuery = """
                        SELECT operation, options, object_name, cost
                        FROM plan_table
                        WHERE statement_id = ?
                        ORDER BY id
                        """;
                    
                    PreparedStatement ps = conn.prepareStatement(planQuery);
                    ps.setString(1, stmtId);
                    ResultSet rs = ps.executeQuery();
                    
                    JsonArray operations = new JsonArray();
                    double totalCost = 0;
                    
                    while (rs.next()) {
                        JsonObject op = new JsonObject()
                            .put("operation", rs.getString("operation"))
                            .put("options", rs.getString("options"))
                            .put("object", rs.getString("object_name"))
                            .put("cost", rs.getDouble("cost"));
                        operations.add(op);
                        totalCost += rs.getDouble("cost");
                    }
                    
                    rs.close();
                    ps.close();
                    
                    // Clean up
                    stmt.execute("DELETE FROM plan_table WHERE statement_id = '" + stmtId + "'");
                    stmt.close();
                    
                    return new JsonObject()
                        .put("success", true)
                        .put("operations", operations)
                        .put("estimatedCost", totalCost);
                    
                } catch (SQLException e) {
                    return new JsonObject()
                        .put("success", false)
                        .put("errorCode", "ORA-" + String.format("%05d", Math.abs(e.getErrorCode())))
                        .put("errorMessage", e.getMessage());
                }
            }).toCompletionStage().toCompletableFuture().get();
            
            if (planResult.getBoolean("success", false)) {
                check.put("passed", true);
                check.put("plan", new JsonObject()
                    .put("operations", planResult.getJsonArray("operations"))
                    .put("estimatedCost", planResult.getDouble("estimatedCost")));
            } else {
                check.put("passed", false);
                check.put("errors", new JsonArray().add(new JsonObject()
                    .put("type", "execution")
                    .put("code", planResult.getString("errorCode"))
                    .put("message", planResult.getString("errorMessage"))));
            }
            
        } catch (Exception e) {
            check.put("passed", false);
            check.put("errors", new JsonArray().add(new JsonObject()
                .put("type", "execution")
                .put("message", "Execution validation failed: " + e.getMessage())));
        }
        
        return check;
    }
    
    private JsonObject performPermissionCheck(String sql) {
        JsonObject check = new JsonObject()
            .put("step", "permission_check")
            .put("passed", true);
        
        JsonArray errors = new JsonArray();
        
        try {
            // Extract tables from SQL
            Set<String> tables = extractTablesFromSQL(sql);
            
            for (String table : tables) {
                // Use connection manager to check permissions
                JsonObject permResult = connectionManager.executeWithConnection(conn -> {
                    try {
                        // Check if user has SELECT privilege on table
                        String privQuery = """
                            SELECT privilege
                            FROM user_tab_privs
                            WHERE table_name = UPPER(?)
                            AND privilege IN ('SELECT', 'INSERT', 'UPDATE', 'DELETE')
                            """;
                        
                        PreparedStatement ps = conn.prepareStatement(privQuery);
                        ps.setString(1, table);
                        ResultSet rs = ps.executeQuery();
                        
                        boolean hasPrivilege = false;
                        while (rs.next()) {
                            hasPrivilege = true;
                            break;
                        }
                        rs.close();
                        ps.close();
                        
                        if (!hasPrivilege) {
                            // Check if it's user's own table
                            String ownerQuery = "SELECT 1 FROM user_tables WHERE table_name = UPPER(?)";
                            ps = conn.prepareStatement(ownerQuery);
                            ps.setString(1, table);
                            rs = ps.executeQuery();
                            hasPrivilege = rs.next();
                            rs.close();
                            ps.close();
                        }
                        
                        return new JsonObject()
                            .put("table", table)
                            .put("hasPrivilege", hasPrivilege);
                            
                    } catch (SQLException e) {
                        return new JsonObject()
                            .put("table", table)
                            .put("error", e.getMessage());
                    }
                }).toCompletionStage().toCompletableFuture().get();
                
                if (permResult.containsKey("error")) {
                    throw new RuntimeException("Permission check failed for table " + table + ": " + permResult.getString("error"));
                }
                
                if (!permResult.getBoolean("hasPrivilege", false)) {
                    errors.add(new JsonObject()
                        .put("type", "permission")
                        .put("object", table)
                        .put("message", "Insufficient privileges on table " + table));
                }
            }
            
        } catch (Exception e) {
            vertx.eventBus().publish("log", "Permission check error: " + e.getMessage() + ",0,OracleSQLValidationServer,MCP,System");
            errors.add(new JsonObject()
                .put("type", "permission")
                .put("message", "Unable to verify permissions"));
        }
        
        if (!errors.isEmpty()) {
            check.put("passed", false);
            check.put("errors", errors);
        }
        
        return check;
    }
    
    private String suggestSchemaFixes(String sql, JsonObject schemaCheck, JsonObject fullSchema) {
        JsonArray errors = schemaCheck.getJsonArray("errors", new JsonArray());
        String suggestedSQL = sql;
        
        for (int i = 0; i < errors.size(); i++) {
            JsonObject error = errors.getJsonObject(i);
            String objectType = error.getString("objectType");
            String object = error.getString("object");
            
            if ("table".equals(objectType) && object != null) {
                // Try to find similar table names
                try {
                    String similarTable = findSimilarTable(object);
                    if (similarTable != null) {
                        suggestedSQL = suggestedSQL.replaceAll("\\b" + Pattern.quote(object) + "\\b", similarTable);
                    }
                } catch (SQLException e) {
                    vertx.eventBus().publish("log", "Error finding similar tables" + ",0,OracleSQLValidationServer,MCP,System");
                }
            } else if ("column".equals(objectType) && object != null) {
                // Try to find similar column names
                String suggestion = error.getString("suggestion", null);
                
                // First try to use fullSchema if available
                if (fullSchema != null && fullSchema.containsKey("tables")) {
                    Set<String> queryTables = extractTablesFromSQL(sql);
                    JsonArray schemaTables = fullSchema.getJsonArray("tables");
                    
                    String bestMatch = null;
                    double bestScore = 0.0;
                    
                    for (int j = 0; j < schemaTables.size(); j++) {
                        JsonObject table = schemaTables.getJsonObject(j);
                        String tableName = table.getString("name");
                        
                        // Check if this table is used in the query
                        if (queryTables.stream().anyMatch(t -> t.equalsIgnoreCase(tableName))) {
                            JsonArray columns = table.getJsonArray("columns");
                            
                            for (int k = 0; k < columns.size(); k++) {
                                JsonObject column = columns.getJsonObject(k);
                                String columnName = column.getString("name");
                                
                                // Calculate similarity
                                double similarity = calculateStringSimilarity(object.toLowerCase(), columnName.toLowerCase());
                                
                                // Also check for common mappings
                                if ((object.toLowerCase().contains("state") || object.toLowerCase().equals("province")) 
                                    && (columnName.equalsIgnoreCase("CITY") || columnName.equalsIgnoreCase("COUNTRY_ID"))) {
                                    // For California query, CITY is more appropriate than missing PROVINCE
                                    similarity = 0.8;
                                }
                                
                                if (similarity > bestScore && similarity > 0.5) {
                                    bestScore = similarity;
                                    bestMatch = columnName;
                                }
                            }
                        }
                    }
                    
                    if (bestMatch != null) {
                        suggestedSQL = suggestedSQL.replaceAll("\\b" + Pattern.quote(object) + "\\b", bestMatch);
                        error.put("appliedFix", "Changed '" + object + "' to '" + bestMatch + "' (found in schema)");
                    }
                } else if (suggestion != null) {
                    // Fall back to hardcoded suggestions
                    if (object.toLowerCase().contains("state")) {
                        suggestedSQL = suggestedSQL.replaceAll("\\b" + Pattern.quote(object) + "\\b", "PROVINCE");
                        error.put("appliedFix", "Changed '" + object + "' to 'PROVINCE' (Canadian terminology)");
                    } else if (object.toLowerCase().contains("zip")) {
                        suggestedSQL = suggestedSQL.replaceAll("\\b" + Pattern.quote(object) + "\\b", "POSTAL_CODE");
                        error.put("appliedFix", "Changed '" + object + "' to 'POSTAL_CODE' (Canadian terminology)");
                    } else if (object.toLowerCase().contains("ssn")) {
                        suggestedSQL = suggestedSQL.replaceAll("\\b" + Pattern.quote(object) + "\\b", "SIN");
                        error.put("appliedFix", "Changed '" + object + "' to 'SIN' (Social Insurance Number)");
                    }
                } else {
                    // Try to find similar column names from the tables in the query
                    try {
                        Set<String> tables = extractTablesFromSQL(sql);
                        for (String table : tables) {
                            String similarColumn = findSimilarColumn(table, object);
                            if (similarColumn != null) {
                                suggestedSQL = suggestedSQL.replaceAll("\\b" + Pattern.quote(object) + "\\b", similarColumn);
                                break;
                            }
                        }
                    } catch (Exception e) {
                        vertx.eventBus().publish("log", "Error finding similar columns: " + e.getMessage() + ",0,OracleSQLValidationServer,MCP,System");
                    }
                }
            }
        }
        
        return suggestedSQL.equals(sql) ? null : suggestedSQL;
    }
    
    private String suggestExecutionFixes(String sql, JsonObject executionCheck) {
        if (!llmService.isInitialized()) {
            return null;
        }
        
        try {
            JsonArray errors = executionCheck.getJsonArray("errors", new JsonArray());
            
            String systemPrompt = """
                You are an Oracle SQL expert. Fix the following SQL query based on the error.
                Return ONLY the corrected SQL, no explanations.
                """;
            
            JsonObject promptData = new JsonObject()
                .put("sql", sql)
                .put("errors", errors);
            
            List<JsonObject> messages = Arrays.asList(
                new JsonObject().put("role", "system").put("content", systemPrompt),
                new JsonObject().put("role", "user").put("content", promptData.encodePrettily())
            );
            
            // Convert messages to List<String> format expected by LLM service
            List<String> messageStrings = new ArrayList<>();
            for (JsonObject msg : messages) {
                messageStrings.add(msg.encode());
            }
            
            JsonObject response = llmService.chatCompletion(
                messageStrings,
                0.0,
                500
            ).join();
            
            return response.getJsonArray("choices")
                .getJsonObject(0)
                .getJsonObject("message")
                .getString("content")
                .trim();
                
        } catch (Exception e) {
            vertx.eventBus().publish("log", "Failed to suggest fixes with LLM" + ",0,OracleSQLValidationServer,MCP,System");
            return null;
        }
    }
    
    private JsonObject explainErrorWithLLM(String errorCode, String errorMessage) throws Exception {
        String systemPrompt = """
            You are an Oracle database expert. Explain the following Oracle error and provide solutions.
            Format your response as JSON with fields: summary, description, solutions (array).
            """;
        
        JsonObject promptData = new JsonObject()
            .put("errorCode", errorCode)
            .put("errorMessage", errorMessage);
        
        List<JsonObject> messages = Arrays.asList(
            new JsonObject().put("role", "system").put("content", systemPrompt),
            new JsonObject().put("role", "user").put("content", promptData.encode())
        );
        
        // Convert messages to List<String> format expected by LLM service
        List<String> messageStrings = new ArrayList<>();
        for (JsonObject msg : messages) {
            messageStrings.add(msg.encode());
        }
        
        JsonObject response = llmService.chatCompletion(
            messageStrings,
            0.0,
            500
        ).join();
        
        String content = response.getJsonArray("choices")
            .getJsonObject(0)
            .getJsonObject("message")
            .getString("content");
        
        // Parse JSON from response
        try {
            return new JsonObject(content);
        } catch (Exception e) {
            // Fallback if not valid JSON
            JsonObject fallback = new JsonObject();
            fallback.put("summary", "Oracle error " + errorCode);
            fallback.put("description", content);
            fallback.put("solutions", new JsonArray().add("Review the error details above"));
            return fallback;
        }
    }
    
    private String findSimilarTable(String tableName) throws SQLException {
        try {
            // Use connection manager to find similar tables
            return connectionManager.executeWithConnection(conn -> {
                try {
                    String query = "SELECT table_name FROM user_tables";
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(query);
                    
                    String bestMatch = null;
                    double bestScore = 0.0;
                    
                    while (rs.next()) {
                        String candidate = rs.getString("table_name");
                        double similarity = calculateSimilarity(tableName.toUpperCase(), candidate);
                        if (similarity > 0.7 && similarity > bestScore) {
                            bestScore = similarity;
                            bestMatch = candidate;
                        }
                    }
                    
                    rs.close();
                    stmt.close();
                    
                    return bestMatch;
                } catch (SQLException e) {
                    throw new RuntimeException("Failed to find similar tables", e);
                }
            }).toCompletionStage().toCompletableFuture().get();
        } catch (Exception e) {
            throw new SQLException("Failed to find similar tables: " + e.getMessage(), e);
        }
    }
    
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
    
    private String extractObjectName(String errorMessage) {
        // Try to extract object name from error message
        Pattern pattern = Pattern.compile("\"([^\"]+)\"");
        Matcher matcher = pattern.matcher(errorMessage);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }
    
    private Set<String> extractTablesFromSQL(String sql) {
        Set<String> tables = new HashSet<>();
        String upperSQL = sql.toUpperCase();
        
        // Simple extraction - would need proper parser for production
        String[] parts = upperSQL.split("\\s+FROM\\s+");
        if (parts.length > 1) {
            String fromClause = parts[1].split("\\s+(WHERE|GROUP|ORDER|HAVING)\\s+")[0];
            String[] tableRefs = fromClause.split("\\s*(,|JOIN|LEFT|RIGHT|INNER|OUTER)\\s+");
            
            for (String tableRef : tableRefs) {
                String table = tableRef.trim().split("\\s+")[0];
                if (!table.isEmpty() && !table.equals("ON")) {
                    tables.add(table.replace("\"", ""));
                }
            }
        }
        
        return tables;
    }
    
    /**
     * Get execution plan for a SQL query
     */
    private void explainPlan(RoutingContext ctx, String requestId, JsonObject arguments) {
        String sql = arguments.getString("sql");
        
        if (sql == null || sql.trim().isEmpty()) {
            sendError(ctx, requestId, MCPResponse.ErrorCodes.INVALID_PARAMS, "SQL query is required");
            return;
        }
        
        // Use connection manager to execute the explain plan
        connectionManager.executeWithConnection(conn -> {
            try {
                JsonObject result = new JsonObject();
                
                // Generate unique statement ID
                String stmtId = "PLAN_" + System.currentTimeMillis() + "_" + Thread.currentThread().getId();
                
                // Use PreparedStatement to safely set statement ID
                String explainPlanSql = "EXPLAIN PLAN SET STATEMENT_ID = ? FOR " + sql;
                PreparedStatement explainStmt = conn.prepareStatement(explainPlanSql);
                explainStmt.setString(1, stmtId);
                
                // Execute EXPLAIN PLAN
                try {
                    explainStmt.execute();
                } catch (SQLException e) {
                    // If the SQL is invalid, this will throw
                    throw new SQLException("Failed to create execution plan: " + e.getMessage(), e);
                } finally {
                    explainStmt.close();
                }
                
                // Retrieve the plan
                String planQuery = """
                    SELECT 
                        id,
                        parent_id,
                        operation,
                        options,
                        object_name,
                        object_alias,
                        cardinality,
                        bytes,
                        cost,
                        cpu_cost,
                        io_cost,
                        temp_space,
                        access_predicates,
                        filter_predicates
                    FROM plan_table
                    WHERE statement_id = ?
                    ORDER BY id
                    """;
                
                PreparedStatement ps = conn.prepareStatement(planQuery);
                ps.setString(1, stmtId);
                ResultSet rs = ps.executeQuery();
                
                JsonArray planSteps = new JsonArray();
                double totalCost = 0;
                long totalBytes = 0;
                
                while (rs.next()) {
                    JsonObject step = new JsonObject();
                    step.put("id", rs.getInt("id"));
                    
                    Integer parentId = rs.getObject("parent_id") != null ? rs.getInt("parent_id") : null;
                    if (parentId != null) {
                        step.put("parentId", parentId);
                    }
                    
                    step.put("operation", rs.getString("operation"));
                    
                    String options = rs.getString("options");
                    if (options != null) {
                        step.put("options", options);
                    }
                    
                    String objectName = rs.getString("object_name");
                    if (objectName != null) {
                        step.put("object", objectName);
                    }
                    
                    // Handle potentially NULL values
                    if (rs.getObject("cardinality") != null) {
                        step.put("cardinality", rs.getLong("cardinality"));
                    }
                    if (rs.getObject("bytes") != null) {
                        step.put("bytes", rs.getLong("bytes"));
                    }
                    if (rs.getObject("cost") != null) {
                        step.put("cost", rs.getDouble("cost"));
                    }
                    
                    // Optional fields
                    if (rs.getObject("cpu_cost") != null) {
                        step.put("cpuCost", rs.getDouble("cpu_cost"));
                    }
                    if (rs.getObject("io_cost") != null) {
                        step.put("ioCost", rs.getDouble("io_cost"));
                    }
                    if (rs.getObject("temp_space") != null) {
                        step.put("tempSpace", rs.getLong("temp_space"));
                    }
                    
                    String accessPredicates = rs.getString("access_predicates");
                    if (accessPredicates != null) {
                        step.put("accessPredicates", accessPredicates);
                    }
                    
                    String filterPredicates = rs.getString("filter_predicates");
                    if (filterPredicates != null) {
                        step.put("filterPredicates", filterPredicates);
                    }
                    
                    planSteps.add(step);
                    
                    // Safely accumulate totals
                    if (rs.getObject("cost") != null) {
                        totalCost += rs.getDouble("cost");
                    }
                    if (rs.getObject("bytes") != null) {
                        totalBytes += rs.getLong("bytes");
                    }
                }
                
                rs.close();
                ps.close();
                
                // Clean up plan table using PreparedStatement
                PreparedStatement deleteStmt = conn.prepareStatement(
                    "DELETE FROM plan_table WHERE statement_id = ?");
                deleteStmt.setString(1, stmtId);
                deleteStmt.execute();
                deleteStmt.close();
                
                // Build result
                result.put("planSteps", planSteps);
                result.put("summary", new JsonObject()
                    .put("totalCost", totalCost)
                    .put("totalBytes", totalBytes)
                    .put("stepCount", planSteps.size()));
                
                // Add performance insights
                JsonArray insights = new JsonArray();
                
                // Check for expensive operations
                for (int i = 0; i < planSteps.size(); i++) {
                    JsonObject step = planSteps.getJsonObject(i);
                    String operation = step.getString("operation");
                    
                    if ("FULL".equals(step.getString("options")) && "TABLE ACCESS".equals(operation)) {
                        insights.add("Full table scan detected on " + step.getString("object", "unknown table") + 
                                    " - consider adding an index");
                    }
                    
                    if (operation != null && operation.contains("SORT")) {
                        insights.add("Sort operation detected - may impact performance for large datasets");
                    }
                    
                    if (operation != null && operation.contains("HASH JOIN")) {
                        insights.add("Hash join detected - ensure sufficient memory allocation");
                    }
                }
                
                if (totalCost > 1000) {
                    insights.add("High total cost detected (" + totalCost + ") - query optimization recommended");
                }
                
                result.put("insights", insights);
                return result;
                
            } catch (SQLException e) {
                vertx.eventBus().publish("log", "Failed to generate execution plan: " + e.getMessage() + ",0,OracleSQLValidationServer,MCP,System");
                throw new RuntimeException("Execution plan generation failed: " + e.getMessage(), e);
            }
        }).onComplete(ar -> {
            if (ar.succeeded()) {
                sendSuccess(ctx, requestId, ar.result());
            } else {
                sendError(ctx, requestId, MCPResponse.ErrorCodes.INTERNAL_ERROR, 
                    ar.cause().getMessage());
            }
        });
    }
    
    
    private String findSimilarColumn(String tableName, String targetColumn) {
        try {
            return connectionManager.executeWithConnection(conn -> {
                try {
                    DatabaseMetaData metaData = conn.getMetaData();
                    ResultSet rs = metaData.getColumns(null, conn.getSchema(), tableName.toUpperCase(), "%");
                    
                    String bestMatch = null;
                    double bestSimilarity = 0.0;
                    
                    while (rs.next()) {
                        String columnName = rs.getString("COLUMN_NAME");
                        double similarity = calculateStringSimilarity(targetColumn.toUpperCase(), columnName);
                        
                        // Special handling for common variations
                        if (targetColumn.toLowerCase().contains("state") && columnName.toLowerCase().contains("province")) {
                            similarity = 0.9;
                        } else if (targetColumn.toLowerCase().contains("zip") && columnName.toLowerCase().contains("postal")) {
                            similarity = 0.9;
                        }
                        
                        if (similarity > bestSimilarity && similarity > 0.6) {
                            bestSimilarity = similarity;
                            bestMatch = columnName;
                        }
                    }
                    
                    rs.close();
                    return bestMatch;
                    
                } catch (SQLException e) {
                    vertx.eventBus().publish("log", "Error finding similar column: " + e.getMessage() + ",0,OracleSQLValidationServer,MCP,System");
                    return null;
                }
            }).toCompletionStage().toCompletableFuture().get();
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Calculate string similarity using Levenshtein distance
     */
    private double calculateStringSimilarity(String s1, String s2) {
        if (s1 == null || s2 == null) {
            return 0.0;
        }
        
        if (s1.equalsIgnoreCase(s2)) {
            return 1.0;
        }
        
        // Calculate Levenshtein distance
        int distance = levenshteinDistance(s1.toLowerCase(), s2.toLowerCase());
        int maxLength = Math.max(s1.length(), s2.length());
        
        if (maxLength == 0) {
            return 1.0;
        }
        
        return 1.0 - ((double) distance / maxLength);
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
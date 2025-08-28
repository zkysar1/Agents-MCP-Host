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


import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * MCP Server for business term mapping and enumeration translation.
 * This server is database-agnostic in logic but configured for Oracle use.
 * It relies heavily on LLM for context-aware mapping and should be called
 * late in the pipeline with context from other tools.
 * NOT deployed as a Worker Verticle since operations are mostly in-memory or LLM-based.
 */
public class BusinessMappingServer extends MCPServerBase {
    
    
    
    // Oracle connection for enum lookups
    private static final String ORACLE_URL = System.getenv("ORACLE_URL") != null ? 
        System.getenv("ORACLE_URL") : "jdbc:oracle:thin:@localhost:1521:XE";
    private static final String ORACLE_USER = System.getenv("ORACLE_USER") != null ? 
        System.getenv("ORACLE_USER") : "system";
    private static final String ORACLE_PASSWORD = System.getenv("ORACLE_PASSWORD") != null ? 
        System.getenv("ORACLE_PASSWORD") : "oracle";
    
    private Connection dbConnection;
    private LlmAPIService llmService;
    
    // Cache for enum mappings
    private final Map<String, Map<String, EnumMapping>> enumCache = new ConcurrentHashMap<>();
    private long enumCacheTimestamp = 0;
    private static final long ENUM_CACHE_TTL = 600000; // 10 minutes
    
    // Common business term patterns (for fallback when LLM unavailable)
    private static final Map<String, List<String>> COMMON_TERM_PATTERNS = new HashMap<>();
    
    static {
        COMMON_TERM_PATTERNS.put("customer", Arrays.asList("customer", "client", "account", "user", "member"));
        COMMON_TERM_PATTERNS.put("order", Arrays.asList("order", "purchase", "transaction", "sale"));
        COMMON_TERM_PATTERNS.put("product", Arrays.asList("product", "item", "sku", "merchandise", "goods"));
        COMMON_TERM_PATTERNS.put("employee", Arrays.asList("employee", "staff", "worker", "personnel", "emp"));
        COMMON_TERM_PATTERNS.put("date", Arrays.asList("date", "datetime", "timestamp", "created", "modified"));
        COMMON_TERM_PATTERNS.put("status", Arrays.asList("status", "state", "flag", "active", "enabled"));
        COMMON_TERM_PATTERNS.put("amount", Arrays.asList("amount", "total", "sum", "price", "cost", "value"));
        COMMON_TERM_PATTERNS.put("name", Arrays.asList("name", "title", "description", "label"));
        COMMON_TERM_PATTERNS.put("location", Arrays.asList("state", "province", "region", "territory", "prov"));
        COMMON_TERM_PATTERNS.put("postal", Arrays.asList("zip", "zipcode", "postal", "postal_code", "postcode"));
        // Add specific US->Canadian mappings
        COMMON_TERM_PATTERNS.put("province", Arrays.asList("state", "province", "prov"));  
        COMMON_TERM_PATTERNS.put("postal_code", Arrays.asList("zip", "zipcode", "postal_code"));
        COMMON_TERM_PATTERNS.put("phone", Arrays.asList("phone", "telephone", "tel", "mobile", "cell"));
        COMMON_TERM_PATTERNS.put("address", Arrays.asList("address", "addr", "street", "location"));
    }
    
    // Inner class for enum mappings
    private static class EnumMapping {
        final String code;
        final String description;
        final String table;
        final String column;
        
        EnumMapping(String code, String description, String table, String column) {
            this.code = code;
            this.description = description;
            this.table = table;
            this.column = column;
        }
    }
    
    public BusinessMappingServer() {
        super("BusinessMappingServer", "/mcp/servers/business-map");
    }
    
    @Override
    public void start(Promise<Void> startPromise) {
        // Initialize database connection for enum lookups
        initializeDatabase().onComplete(ar -> {
            if (ar.succeeded()) {
                llmService = LlmAPIService.getInstance();
                if (!llmService.isInitialized()) {
                    vertx.eventBus().publish("log", "LLM service not initialized - business mapping will use patterns only,1,BusinessMappingServer,MCP,System");
                }
                // Load initial enum cache
                loadEnumCache();
                super.start(startPromise);
            } else {
                vertx.eventBus().publish("log", "Failed to initialize database connection" + ",0,BusinessMappingServer,MCP,System");
                // Continue anyway - business mapping can work without DB
                llmService = LlmAPIService.getInstance();
                super.start(startPromise);
            }
        });
    }
    
    @Override
    protected void initializeTools() {
        // Register map_business_terms tool
        registerTool(new MCPTool(
            "map_business_terms",
            "Map business/domain terms to database tables or columns.",
            new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("terms", new JsonObject()
                        .put("type", "array")
                        .put("items", new JsonObject().put("type", "string"))
                        .put("description", "List of business terms to map (e.g., 'employee name')."))
                    .put("context", new JsonObject()
                        .put("type", "object")
                        .put("description", "Context from previous tools (schema info, query analysis, etc.)"))
                    .put("tableContext", new JsonObject()
                        .put("type", "array")
                        .put("items", new JsonObject().put("type", "string"))
                        .put("description", "Optional list of tables to limit the search.")))
                .put("required", new JsonArray().add("terms"))
        ));
        
        // Register translate_enum tool
        registerTool(new MCPTool(
            "translate_enum",
            "Translate enum codes to descriptions or vice versa for a given table column.",
            new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("table", new JsonObject()
                        .put("type", "string")
                        .put("description", "Table name that contains the enum."))
                    .put("column", new JsonObject()
                        .put("type", "string")
                        .put("description", "Column name that contains the enum code or description."))
                    .put("values", new JsonObject()
                        .put("type", "array")
                        .put("items", new JsonObject().put("type", "string"))
                        .put("description", "List of values to translate."))
                    .put("direction", new JsonObject()
                        .put("type", "string")
                        .put("enum", new JsonArray()
                            .add("code_to_description")
                            .add("description_to_code")
                            .add("auto"))
                        .put("description", "Translation direction. 'auto' will infer from input format.")
                        .put("default", "auto")))
                .put("required", new JsonArray().add("table").add("column").add("values"))
        ));
    }
    
    @Override
    protected void executeTool(RoutingContext ctx, String requestId, String toolName, JsonObject arguments) {
        switch (toolName) {
            case "map_business_terms":
                mapBusinessTerms(ctx, requestId, arguments);
                break;
            case "translate_enum":
                translateEnum(ctx, requestId, arguments);
                break;
            default:
                sendError(ctx, requestId, MCPResponse.ErrorCodes.METHOD_NOT_FOUND, 
                    "Unknown tool: " + toolName);
        }
    }
    
    /**
     * Map business terms to database objects.
     * This is context-dependent and should be called late in the pipeline.
     */
    private void mapBusinessTerms(RoutingContext ctx, String requestId, JsonObject arguments) {
        JsonArray terms = arguments.getJsonArray("terms");
        JsonObject context = arguments.getJsonObject("context", new JsonObject());
        JsonArray tableContext = arguments.getJsonArray("tableContext", new JsonArray());
        
        if (terms == null || terms.isEmpty()) {
            sendError(ctx, requestId, MCPResponse.ErrorCodes.INVALID_PARAMS, "Terms array is required");
            return;
        }
        
        JsonObject result = new JsonObject();
        JsonArray mappings = new JsonArray();
        
        // If LLM is available and we have context, use LLM for intelligent mapping
        if (llmService.isInitialized() && !context.isEmpty()) {
            executeBlocking(Promise.<JsonObject>promise(), promise -> {
                try {
                    JsonArray llmMappings = mapTermsWithLLM(terms, context, tableContext);
                    promise.complete(new JsonObject().put("mappings", llmMappings));
                } catch (Exception e) {
                    vertx.eventBus().publish("log", "LLM mapping failed" + ",0,BusinessMappingServer,MCP,System");
                    // Fall back to pattern matching
                    JsonArray patternMappings = mapTermsWithPatterns(terms, tableContext);
                    promise.complete(new JsonObject().put("mappings", patternMappings));
                }
            }, res -> {
                result.put("mappings", res.result());
                result.put("method", res.succeeded() ? "llm_context_aware" : "pattern_matching");
                result.put("confidence", res.succeeded() ? 0.85 : 0.6);
                sendSuccess(ctx, requestId, result);
            });
        } else {
            // Use pattern matching as fallback
            mappings = mapTermsWithPatterns(terms, tableContext);
            result.put("mappings", mappings);
            result.put("method", "pattern_matching");
            result.put("confidence", 0.6);
            result.put("warning", "LLM unavailable or no context provided - using pattern matching");
            sendSuccess(ctx, requestId, result);
        }
    }
    
    private void translateEnum(RoutingContext ctx, String requestId, JsonObject arguments) {
        String table = arguments.getString("table");
        String column = arguments.getString("column");
        JsonArray values = arguments.getJsonArray("values");
        String direction = arguments.getString("direction", "auto");
        
        if (table == null || column == null || values == null || values.isEmpty()) {
            sendError(ctx, requestId, MCPResponse.ErrorCodes.INVALID_PARAMS, 
                "Table, column, and values are required");
            return;
        }
        
        executeBlocking(Promise.<JsonObject>promise(), promise -> {
            try {
                // Ensure enum cache is loaded
                loadEnumCacheIfNeeded();
                
                JsonObject result = new JsonObject();
                JsonArray translations = new JsonArray();
                
                // Get enum mappings for this table/column
                String cacheKey = table.toUpperCase() + "." + column.toUpperCase();
                Map<String, EnumMapping> columnEnums = enumCache.get(cacheKey);
                
                if (columnEnums == null || columnEnums.isEmpty()) {
                    // Try to load enums for this specific column
                    columnEnums = loadEnumsForColumn(table, column);
                    if (!columnEnums.isEmpty()) {
                        enumCache.put(cacheKey, columnEnums);
                    }
                }
                
                // Determine direction if auto
                String finalDirection = direction;
                if ("auto".equals(finalDirection)) {
                    finalDirection = inferTranslationDirection(values, columnEnums);
                }
                
                // Perform translations
                for (int i = 0; i < values.size(); i++) {
                    String value = values.getString(i);
                    JsonObject translation = new JsonObject()
                        .put("original", value);
                    
                    if (columnEnums != null && !columnEnums.isEmpty()) {
                        if ("code_to_description".equals(finalDirection)) {
                            EnumMapping mapping = columnEnums.get(value.toUpperCase());
                            if (mapping != null) {
                                translation.put("translated", mapping.description);
                                translation.put("found", true);
                            } else {
                                translation.put("translated", value);
                                translation.put("found", false);
                            }
                        } else {
                            // description_to_code
                            boolean found = false;
                            for (EnumMapping mapping : columnEnums.values()) {
                                if (mapping.description.equalsIgnoreCase(value)) {
                                    translation.put("translated", mapping.code);
                                    translation.put("found", true);
                                    found = true;
                                    break;
                                }
                            }
                            if (!found) {
                                translation.put("translated", value);
                                translation.put("found", false);
                            }
                        }
                    } else {
                        translation.put("translated", value);
                        translation.put("found", false);
                        translation.put("error", "No enum mappings found");
                    }
                    
                    translations.add(translation);
                }
                
                result.put("table", table);
                result.put("column", column);
                result.put("direction", finalDirection);
                result.put("translations", translations);
                result.put("totalMappings", columnEnums != null ? columnEnums.size() : 0);
                
                promise.complete(result);
                
            } catch (Exception e) {
                vertx.eventBus().publish("log", "Enum translation failed" + ",0,BusinessMappingServer,MCP,System");
                promise.fail(e);
            }
        }, res -> {
            if (res.succeeded()) {
                sendSuccess(ctx, requestId, res.result());
            } else {
                sendError(ctx, requestId, MCPResponse.ErrorCodes.INTERNAL_ERROR, 
                    "Enum translation failed: " + res.cause().getMessage());
            }
        });
    }
    
    private JsonArray mapTermsWithLLM(JsonArray terms, JsonObject context, JsonArray tableContext) 
            throws Exception {
        
        String systemPrompt = """
            You are a database schema expert. Map business terms to database columns based on:
            1. The ACTUAL database schema provided (fullDatabaseSchema)
            2. The provided schema information and context
            3. Common database naming conventions
            4. The semantic meaning of terms
            5. Regional terminology variations
            
            CRITICAL: When fullDatabaseSchema is provided, use ONLY columns that exist in that schema!
            
            Consider:
            - Business terms may map to multiple columns
            - Use the actual schema first, then schema matches and query analysis context
            - Prefer exact matches, then semantic matches
            - Consider compound terms (e.g., "employee name" -> EMPLOYEE.FULL_NAME)
            - Be aware of regional variations:
              * "state" might need to map to "CITY" if "PROVINCE" doesn't exist
              * "California" is a US state, so might be in CITY column if no STATE/PROVINCE exists
              * "zip code" maps to "POSTAL_CODE"
              * "SSN" maps to "SIN" (Social Insurance Number)
            - Common column name patterns:
              * Location fields: STATE, PROVINCE, REGION, TERRITORY, CITY
              * Postal codes: ZIP, POSTAL_CODE, POSTCODE
              * Phone numbers: PHONE, TEL, MOBILE, CELL
              * Status fields: STATUS, STATUS_ID, STATUS_CODE
            
            For location queries like "California":
            - Check if STATE/PROVINCE columns exist
            - If not, consider CITY column (many databases store US states in CITY field)
            - Consider COUNTRY_ID for country-level filtering
            - ALWAYS suggest checking BOTH customer location AND shipping location:
              * Customer location: CUSTOMERS.CITY, CUSTOMERS.COUNTRY_ID
              * Shipping location: ORDERS.SHIPPING_CITY, ORDERS.SHIPPING_COUNTRY
            
            Respond with a JSON array of mappings.
            Each mapping should have:
            {
              "term": "original business term",
              "mappings": [
                {
                  "table": "TABLE_NAME",
                  "column": "COLUMN_NAME",
                  "confidence": 0.0-1.0,
                  "reason": "why this mapping makes sense"
                }
              ]
            }
            """;
        
        JsonObject promptData = new JsonObject();
        promptData.put("businessTerms", terms);
        promptData.put("context", context);
        
        if (!tableContext.isEmpty()) {
            promptData.put("limitToTables", tableContext);
        }
        
        // Add any schema information from context
        if (context.containsKey("schemaMatches")) {
            promptData.put("availableSchema", context.getJsonObject("schemaMatches"));
        }
        
        // Add full schema information if available
        if (context.containsKey("fullSchema")) {
            JsonObject fullSchema = context.getJsonObject("fullSchema");
            if (fullSchema != null && fullSchema.containsKey("tables")) {
                promptData.put("fullDatabaseSchema", fullSchema.getJsonArray("tables"));
            }
        }
        
        List<JsonObject> messages = Arrays.asList(
            new JsonObject().put("role", "system").put("content", systemPrompt),
            new JsonObject().put("role", "user").put("content", promptData.encodePrettily())
        );
        
        JsonObject response = llmService.chatCompletion(
            messages.stream()
                .map(JsonObject::encode)
                .collect(Collectors.toList()),
            0.0, // temperature 0 for consistent mapping
            1000
        ).join();
        
        String content = response.getJsonArray("choices")
            .getJsonObject(0)
            .getJsonObject("message")
            .getString("content");
        
        // Parse JSON array from response
        return parseJsonArrayFromContent(content);
    }
    
    private JsonArray mapTermsWithPatterns(JsonArray terms, JsonArray tableContext) {
        JsonArray mappings = new JsonArray();
        
        for (int i = 0; i < terms.size(); i++) {
            String term = terms.getString(i).toLowerCase();
            JsonObject mapping = new JsonObject()
                .put("term", terms.getString(i));
            
            JsonArray termMappings = new JsonArray();
            
            // Check each pattern category
            for (Map.Entry<String, List<String>> entry : COMMON_TERM_PATTERNS.entrySet()) {
                String category = entry.getKey();
                List<String> patterns = entry.getValue();
                
                // Check if term matches any pattern
                for (String pattern : patterns) {
                    if (term.contains(pattern) || pattern.contains(term)) {
                        // Create more specific mapping suggestions based on pattern
                        String columnPattern = pattern.toUpperCase();
                        
                        // Special handling for known variations
                        if ("location".equals(category) || "province".equals(category)) {
                            if (term.contains("state") || term.equals("california") || term.contains("california")) {
                                // For US states like California, suggest both CITY and shipping fields
                                termMappings.add(new JsonObject()
                                    .put("table", "CUSTOMERS")
                                    .put("column", "CITY")
                                    .put("confidence", 0.9)
                                    .put("reason", "US state name often stored in CITY field"));
                                termMappings.add(new JsonObject()
                                    .put("table", "ORDERS")
                                    .put("column", "SHIPPING_CITY")
                                    .put("confidence", 0.9)
                                    .put("reason", "Check shipping location for orders"));
                                // Still suggest PROVINCE in case it exists
                                termMappings.add(new JsonObject()
                                    .put("table", "*")
                                    .put("column", "PROVINCE")
                                    .put("confidence", 0.7)
                                    .put("reason", "Geographic location mapping (US state -> Canadian province)"));
                                continue; // Skip the generic pattern addition
                            } else if (term.contains("province")) {
                                columnPattern = "PROVINCE";
                            }
                        } else if ("postal".equals(category) || "postal_code".equals(category)) {
                            columnPattern = "POSTAL_CODE";
                            termMappings.add(new JsonObject()
                                .put("table", "*")
                                .put("column", "POSTAL_CODE")
                                .put("confidence", 0.85)
                                .put("reason", "Postal code mapping (zip -> postal_code)"));
                        }
                        
                        JsonObject suggestion = new JsonObject()
                            .put("table", "*")
                            .put("column", columnPattern)
                            .put("confidence", 0.7)
                            .put("reason", "Pattern match for " + category + " (term: " + term + ")");
                        
                        // If we have table context, be more specific
                        if (!tableContext.isEmpty()) {
                            suggestion.put("table", tableContext.getString(0));
                        }
                        
                        termMappings.add(suggestion);
                        break; // One match per category
                    }
                }
            }
            
            // If no patterns matched, create a generic suggestion
            if (termMappings.isEmpty()) {
                termMappings.add(new JsonObject()
                    .put("table", "UNKNOWN")
                    .put("column", term.toUpperCase())
                    .put("confidence", 0.3)
                    .put("reason", "No pattern match - direct name mapping"));
            }
            
            mapping.put("mappings", termMappings);
            mappings.add(mapping);
        }
        
        return mappings;
    }
    
    private void loadEnumCacheIfNeeded() {
        if (System.currentTimeMillis() - enumCacheTimestamp > ENUM_CACHE_TTL || enumCache.isEmpty()) {
            loadEnumCache();
        }
    }
    
    private void loadEnumCache() {
        if (dbConnection == null) {
            vertx.eventBus().publish("log", "Database connection not available for enum cache loading,1,BusinessMappingServer,MCP,System");
            return;
        }
        
        try {
            // This is a simplified approach - in production, you'd have a dedicated
            // enum reference table or configuration
            vertx.eventBus().publish("log", "Loading enum cache from database,2,BusinessMappingServer,MCP,System");
            
            // Example: Look for tables with common enum patterns
            String query = """
                SELECT DISTINCT 
                    t.table_name,
                    c.column_name,
                    'CODE' as enum_type
                FROM user_tables t
                JOIN user_tab_columns c ON t.table_name = c.table_name
                WHERE c.column_name LIKE '%STATUS%' 
                   OR c.column_name LIKE '%TYPE%'
                   OR c.column_name LIKE '%FLAG%'
                   OR c.column_name LIKE '%CODE%'
                """;
            
            Statement stmt = dbConnection.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            
            while (rs.next()) {
                String table = rs.getString("table_name");
                String column = rs.getString("column_name");
                
                // Try to load enum values for this column
                Map<String, EnumMapping> columnEnums = loadEnumsForColumn(table, column);
                if (!columnEnums.isEmpty()) {
                    String cacheKey = table.toUpperCase() + "." + column.toUpperCase();
                    enumCache.put(cacheKey, columnEnums);
                }
            }
            
            rs.close();
            stmt.close();
            
            enumCacheTimestamp = System.currentTimeMillis();
            vertx.eventBus().publish("log", "Enum cache loaded with " + enumCache.size() + " column mappings" + ",2,BusinessMappingServer,MCP,System");
            
        } catch (SQLException e) {
            vertx.eventBus().publish("log", "Failed to load enum cache" + ",0,BusinessMappingServer,MCP,System");
        }
    }
    
    private Map<String, EnumMapping> loadEnumsForColumn(String table, String column) {
        Map<String, EnumMapping> enums = new HashMap<>();
        
        try {
            // Try to find distinct values - only practical for small cardinality
            String query = String.format(
                "SELECT DISTINCT %s as code FROM %s WHERE %s IS NOT NULL AND ROWNUM <= 100",
                column, table, column
            );
            
            Statement stmt = dbConnection.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            
            while (rs.next()) {
                String code = rs.getString("code");
                if (code != null && code.length() <= 10) { // Likely an enum if short
                    // Generate description based on code (in production, would look up from reference table)
                    String description = generateEnumDescription(code);
                    enums.put(code.toUpperCase(), new EnumMapping(code, description, table, column));
                }
            }
            
            rs.close();
            stmt.close();
            
        } catch (SQLException e) {
            vertx.eventBus().publish("log", "Could not load enums for " + table + "." + column + ": " + e.getMessage() + "" + ",3,BusinessMappingServer,MCP,System");
        }
        
        return enums;
    }
    
    private String generateEnumDescription(String code) {
        // Simple heuristic-based description generation
        // In production, this would come from a reference table
        
        Map<String, String> commonCodes = Map.of(
            "Y", "Yes",
            "N", "No",
            "A", "Active",
            "I", "Inactive",
            "P", "Pending",
            "C", "Completed",
            "F", "Failed",
            "S", "Success",
            "D", "Deleted",
            "E", "Enabled"
        );
        
        String description = commonCodes.get(code.toUpperCase());
        if (description != null) {
            return description;
        }
        
        // Try numeric codes
        switch (code) {
            case "0": return "False/Inactive";
            case "1": return "True/Active";
            case "2": return "Pending";
            case "3": return "Processing";
            default: return "Code: " + code;
        }
    }
    
    private String inferTranslationDirection(JsonArray values, Map<String, EnumMapping> columnEnums) {
        if (values.isEmpty() || columnEnums == null || columnEnums.isEmpty()) {
            return "code_to_description";
        }
        
        // Check if values look like codes or descriptions
        String firstValue = values.getString(0);
        
        // If it matches a known code, assume code_to_description
        if (columnEnums.containsKey(firstValue.toUpperCase())) {
            return "code_to_description";
        }
        
        // If it matches a known description, assume description_to_code
        for (EnumMapping mapping : columnEnums.values()) {
            if (mapping.description.equalsIgnoreCase(firstValue)) {
                return "description_to_code";
            }
        }
        
        // Default based on length and format
        if (firstValue.length() <= 3 && firstValue.matches("[A-Z0-9]+")) {
            return "code_to_description";
        } else {
            return "description_to_code";
        }
    }
    
    private JsonArray parseJsonArrayFromContent(String content) {
        try {
            // Try to find JSON array in the content
            int startIdx = content.indexOf("[");
            int endIdx = content.lastIndexOf("]");
            
            if (startIdx != -1 && endIdx != -1 && endIdx > startIdx) {
                String jsonStr = content.substring(startIdx, endIdx + 1);
                return new JsonArray(jsonStr);
            }
        } catch (Exception e) {
            vertx.eventBus().publish("log", "Failed to extract JSON array from LLM response" + ",1,BusinessMappingServer,MCP,System");
        }
        
        // Return empty array if parsing fails
        return new JsonArray();
    }
    
    private Future<Void> initializeDatabase() {
        Promise<Void> promise = Promise.<Void>promise();
        
        vertx.executeBlocking(blockingPromise -> {
            try {
                Class.forName("oracle.jdbc.driver.OracleDriver");
                dbConnection = DriverManager.getConnection(ORACLE_URL, ORACLE_USER, ORACLE_PASSWORD);
                dbConnection.setAutoCommit(true);
                
                vertx.eventBus().publish("log", "Oracle database connection established for business mapping,2,BusinessMappingServer,MCP,System");
                blockingPromise.complete();
                
            } catch (Exception e) {
                vertx.eventBus().publish("log", "Failed to connect to Oracle database" + ",0,BusinessMappingServer,MCP,System");
                blockingPromise.fail(e);
            }
        }, false, res -> {
            if (res.succeeded()) {
                promise.complete();
            } else {
                promise.fail(res.cause());
            }
        });
        
        return promise.future();
    }
    
    @Override
    public void stop(Promise<Void> stopPromise) {
        if (dbConnection != null) {
            vertx.executeBlocking(promise -> {
                try {
                    dbConnection.close();
                    vertx.eventBus().publish("log", "Database connection closed,2,BusinessMappingServer,MCP,System");
                    promise.complete();
                } catch (SQLException e) {
                    vertx.eventBus().publish("log", "Failed to close database connection" + ",0,BusinessMappingServer,MCP,System");
                    promise.fail(e);
                }
            }, res -> {
                try {
                    super.stop(stopPromise);
                } catch (Exception e) {
                    stopPromise.fail(e);
                }
            });
        } else {
            try {
                super.stop(stopPromise);
            } catch (Exception e) {
                stopPromise.fail(e);
            }
        }
    }
    
    /**
     * Get deployment options for this server (Regular Verticle, not Worker)
     */
    public static DeploymentOptions getDeploymentOptions() {
        return new DeploymentOptions()
            .setWorker(false); // Not a worker verticle
    }
}
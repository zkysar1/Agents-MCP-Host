package agents.director.mcp.servers;

import agents.director.mcp.base.MCPServerBase;
import agents.director.mcp.base.MCPTool;
import agents.director.mcp.base.MCPResponse;
import agents.director.services.LlmAPIService;
import agents.director.services.OracleConnectionManager;
import io.vertx.core.Promise;
import io.vertx.core.Future;
import io.vertx.core.CompositeFuture;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.web.RoutingContext;


import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * MCP Server for business term mapping and enumeration translation.
 * This server is database-agnostic in logic but configured for Oracle use.
 * It relies heavily on LLM for context-aware mapping and should be called
 * late in the pipeline with context from other tools.
 * NOT deployed as a Worker Verticle since operations are mostly in-memory or LLM-based.
 */
public class BusinessMappingServer extends MCPServerBase {
    
    
    
    private OracleConnectionManager connectionManager;
    private LlmAPIService llmService;
    
    // Cache for enum mappings
    private final Map<String, Map<String, EnumMapping>> enumCache = new ConcurrentHashMap<>();
    private long enumCacheTimestamp = 0;
    private static final long ENUM_CACHE_TTL = 600000; // 10 minutes
    
    // Common business term patterns (for fallback when LLM unavailable)
    private static final Map<String, List<String>> COMMON_TERM_PATTERNS = new HashMap<>();
    
    static {
        COMMON_TERM_PATTERNS.put("customer", Arrays.asList("customer", "clients", "account", "user", "member"));
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
        final String column;  // The ID/CODE column
        final String descriptionColumn;  // The description column (e.g., STATUS_DESCRIPTION)
        
        // Constructor for enum tables with both ID and description columns
        EnumMapping(String code, String description, String table, String idColumn, String descriptionColumn) {
            this.code = code;
            this.description = description;
            this.table = table;
            this.column = idColumn;
            this.descriptionColumn = descriptionColumn;
        }
        
        // Legacy constructor for backward compatibility (assumes column is ID column)
        EnumMapping(String code, String description, String table, String column) {
            this(code, description, table, column, null);
        }
    }
    
    public BusinessMappingServer() {
        super("BusinessMappingServer", "/mcp/servers/business-map");
    }
    
    @Override
    public void start(Promise<Void> startPromise) {
        // Initialize services
        connectionManager = OracleConnectionManager.getInstance();
        llmService = LlmAPIService.getInstance();
        
        // Initialize connection manager if not already done
        connectionManager.initialize(vertx).onComplete(ar -> {
            if (ar.succeeded()) {
                if (!llmService.isInitialized()) {
                    vertx.eventBus().publish("log", "LLM service not initialized - business mapping will use patterns only,1,BusinessMappingServer,MCP,System");
                }
                // Don't load cache at startup - it will be loaded on demand per session
                vertx.eventBus().publish("log", "BusinessMappingServer started with database connection (cache will load on demand),2,BusinessMappingServer,MCP,System");
            } else {
                vertx.eventBus().publish("log", "BusinessMappingServer started but database initialization failed: " + ar.cause().getMessage() + ",1,BusinessMappingServer,MCP,System");
            }
            super.start(startPromise);
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
        
        // Register load_enum_cache tool
        registerTool(new MCPTool(
            "load_enum_cache",
            "Load or refresh the enum cache for the current session. Should be called before enum operations.",
            new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("force_refresh", new JsonObject()
                        .put("type", "boolean")
                        .put("description", "Force a cache refresh even if cache is still valid")
                        .put("default", false)))
                .put("required", new JsonArray())
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
        
        // Register get_enum_metadata tool
        registerTool(new MCPTool(
            "get_enum_metadata",
            "Get metadata about enum tables and columns, including the actual column names for ID and description fields.",
            new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("table", new JsonObject()
                        .put("type", "string")
                        .put("description", "Optional: Specific enum table name to get metadata for"))
                    .put("column", new JsonObject()
                        .put("type", "string")
                        .put("description", "Optional: Specific column name to check for enum mappings")))
                .put("required", new JsonArray())
        ));
    }
    
    @Override
    protected void executeTool(RoutingContext ctx, String requestId, String toolName, JsonObject arguments) {
        switch (toolName) {
            case "map_business_terms":
                mapBusinessTerms(ctx, requestId, arguments);
                break;
            case "load_enum_cache":
                loadEnumCacheTool(ctx, requestId, arguments);
                break;
            case "translate_enum":
                translateEnum(ctx, requestId, arguments);
                break;
            case "get_enum_metadata":
                getEnumMetadata(ctx, requestId, arguments);
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
        
        // Ensure enum cache is loaded and wait for it
        Future<Void> cacheLoadFuture = loadEnumCacheIfNeeded();
        
        // Process after cache is potentially refreshed
        cacheLoadFuture.compose(v -> {
            // Get enum mappings for this table/column
            String cacheKey = table.toUpperCase() + "." + column.toUpperCase();
            Map<String, EnumMapping> columnEnums = enumCache.get(cacheKey);
            
            // Create a future that will either use cached enums or load them
            Future<Map<String, EnumMapping>> enumsFuture;
            if (columnEnums == null || columnEnums.isEmpty()) {
                // Try to load enums for this specific column
                enumsFuture = loadEnumsForColumn(table, column)
                    .map(loadedEnums -> {
                        if (!loadedEnums.isEmpty()) {
                            synchronized(enumCache) {
                                enumCache.put(cacheKey, loadedEnums);
                            }
                        }
                        return loadedEnums;
                    });
            } else {
                enumsFuture = Future.succeededFuture(columnEnums);
            }
            
            // Process the enum translations asynchronously
            return enumsFuture
            .map(enums -> {
                JsonObject result = new JsonObject();
                JsonArray translations = new JsonArray();
                
                // Determine direction if auto
                String finalDirection = direction;
                if ("auto".equals(finalDirection)) {
                    finalDirection = inferTranslationDirection(values, enums);
                }
                
                // Perform translations
                for (int i = 0; i < values.size(); i++) {
                    String value = values.getString(i);
                    JsonObject translation = new JsonObject()
                        .put("original", value);
                    
                    if (enums != null && !enums.isEmpty()) {
                        if ("code_to_description".equals(finalDirection)) {
                            EnumMapping mapping = enums.get(value.toUpperCase());
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
                            for (EnumMapping mapping : enums.values()) {
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
                result.put("totalMappings", enums != null ? enums.size() : 0);
                
                return result;
            });
        })
        .onSuccess(result -> {
            sendSuccess(ctx, requestId, result);
        })
        .onFailure(error -> {
            vertx.eventBus().publish("log", "Enum translation failed: " + error.getMessage() + ",0,BusinessMappingServer,MCP,System");
            sendError(ctx, requestId, MCPResponse.ErrorCodes.INTERNAL_ERROR, 
                "Enum translation failed: " + error.getMessage());
        });
    }
    
    /**
     * Get metadata about enum tables and columns
     */
    private void getEnumMetadata(RoutingContext ctx, String requestId, JsonObject arguments) {
        String table = arguments.getString("table");
        String column = arguments.getString("column");
        
        JsonObject result = new JsonObject();
        JsonArray enumMetadata = new JsonArray();
        
        synchronized(enumCache) {
            // If specific table requested, return its metadata
            if (table != null && !table.isEmpty()) {
                Map<String, EnumMapping> mappings = enumCache.get(table.toUpperCase());
                if (mappings != null && !mappings.isEmpty()) {
                    // Get first mapping to extract column metadata
                    EnumMapping sample = mappings.values().iterator().next();
                    JsonObject tableMetadata = new JsonObject()
                        .put("table", table.toUpperCase())
                        .put("idColumn", sample.column)
                        .put("descriptionColumn", sample.descriptionColumn != null ? sample.descriptionColumn : "DESCRIPTION")
                        .put("mappingCount", mappings.size());
                    enumMetadata.add(tableMetadata);
                }
            } else {
                // Return all enum table metadata
                for (Map.Entry<String, Map<String, EnumMapping>> entry : enumCache.entrySet()) {
                    String enumTable = entry.getKey();
                    Map<String, EnumMapping> mappings = entry.getValue();
                    if (!mappings.isEmpty()) {
                        EnumMapping sample = mappings.values().iterator().next();
                        JsonObject tableMetadata = new JsonObject()
                            .put("table", enumTable)
                            .put("idColumn", sample.column)
                            .put("descriptionColumn", sample.descriptionColumn != null ? sample.descriptionColumn : "DESCRIPTION")
                            .put("mappingCount", mappings.size());
                        
                        // If column specified, only include tables that have mappings for that column pattern
                        if (column == null || column.isEmpty() || 
                            enumTable.contains(column.toUpperCase().replace("_ID", "").replace("_CODE", ""))) {
                            enumMetadata.add(tableMetadata);
                        }
                    }
                }
            }
        }
        
        result.put("enumTables", enumMetadata);
        result.put("cacheLoaded", !enumCache.isEmpty());
        
        sendSuccess(ctx, requestId, result);
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
    
    private Future<Void> loadEnumCacheIfNeeded() {
        if (System.currentTimeMillis() - enumCacheTimestamp > ENUM_CACHE_TTL || enumCache.isEmpty()) {
            // Return the Future so callers can await cache refresh
            return loadEnumCache()
                .onSuccess(v -> vertx.eventBus().publish("log", 
                    "Enum cache refresh completed,2,BusinessMappingServer,MCP,System"))
                .onFailure(error -> vertx.eventBus().publish("log", 
                    "Enum cache refresh failed: " + error.getMessage() + ",0,BusinessMappingServer,MCP,System"));
        }
        // Cache is still valid
        return Future.succeededFuture();
    }
    
    
    private Future<Void> loadEnumCache() {
        if (connectionManager == null || !connectionManager.isConnectionHealthy()) {
            vertx.eventBus().publish("log", "Database connection not available for enum cache loading,1,BusinessMappingServer,MCP,System");
            return Future.succeededFuture();
        }
        
        vertx.eventBus().publish("log", "Starting intelligent enum discovery,2,BusinessMappingServer,MCP,System");
        
        // Run discovery operations in parallel where possible
        Future<List<String>> enumTablesFuture = discoverEnumTables();
        Future<List<EnumCandidate>> enumCandidatesFuture = identifyEnumColumnsWithLLM();
        
        return CompositeFuture.join(enumTablesFuture, enumCandidatesFuture)
            .compose(composite -> {
                List<String> enumTables = composite.resultAt(0);
                List<EnumCandidate> enumCandidates = composite.resultAt(1);
                
                vertx.eventBus().publish("log", "Found " + enumTables.size() + 
                    " potential enum tables,2,BusinessMappingServer,MCP,System");
                vertx.eventBus().publish("log", "Identified " + enumCandidates.size() + 
                    " potential enum columns,2,BusinessMappingServer,MCP,System");
                
                // Process candidates and tables in parallel
                Future<Void> candidatesFuture = analyzeAndLoadEnumColumns(enumCandidates);
                Future<Void> tablesFuture = loadEnumTableMappings(enumTables);
                
                return CompositeFuture.join(candidatesFuture, tablesFuture)
                    .mapEmpty();
            })
            .onSuccess(v -> {
                // Update timestamp in same context to avoid race conditions
                enumCacheTimestamp = System.currentTimeMillis();
                vertx.eventBus().publish("log", "Enum cache loaded with " + enumCache.size() + 
                    " column mappings via intelligent discovery,2,BusinessMappingServer,MCP,System");
            })
            .onFailure(throwable -> {
                vertx.eventBus().publish("log", "Failed to load enum cache: " + 
                    throwable.getMessage() + ",0,BusinessMappingServer,MCP,System");
            })
            .mapEmpty();
    }
    
    /**
     * Process multiple enum tables in parallel
     */
    private Future<Void> loadEnumTableMappings(List<String> enumTables) {
        if (enumTables.isEmpty()) {
            return Future.succeededFuture();
        }
        
        List<Future<Void>> futures = enumTables.stream()
            .map(this::loadEnumTableMappings)
            .collect(Collectors.toList());
        
        return CompositeFuture.join(toRawFutureList(futures))
            .map(composite -> {
                vertx.eventBus().publish("log", "Processed " + enumTables.size() + 
                    " enum tables,2,BusinessMappingServer,MCP,System");
                return (Void) null;
            })
            .otherwise(throwable -> {
                // Log but don't fail - we want partial success
                vertx.eventBus().publish("log", "Some enum table loads failed: " + 
                    throwable.getMessage() + ",3,BusinessMappingServer,MCP,System");
                return null;
            });
    }
    
    /**
     * Discover tables that likely contain enum definitions
     */
    private Future<List<String>> discoverEnumTables() {
        // Database-agnostic query to find enum-like tables
        String query = """
            SELECT table_name 
            FROM user_tables 
            WHERE table_name LIKE '%ENUM%' 
               OR table_name LIKE '%LOOKUP%'
               OR table_name LIKE '%REFERENCE%'
               OR table_name LIKE '%_REF'
               OR table_name LIKE '%_TYPE%'
               OR table_name LIKE '%_STATUS%'
            ORDER BY table_name
            """;
        
        return connectionManager.executeQuery(query)
            .map(results -> {
                List<String> enumTables = new ArrayList<>();
                for (int i = 0; i < results.size(); i++) {
                    JsonObject row = results.getJsonObject(i);
                    enumTables.add(row.getString("TABLE_NAME"));
                }
                return enumTables;
            })
            .recover(throwable -> {
                vertx.eventBus().publish("log", "[BusinessMappingServer] Failed to discover enum tables: " + 
                    throwable.getMessage() + ",0,BusinessMappingServer,Database,Error");
                return Future.succeededFuture(new ArrayList<String>());
            });
    }
    
    /**
     * Use LLM to identify columns likely containing enum values
     */
    private Future<List<EnumCandidate>> identifyEnumColumnsWithLLM() {
        // Get all tables and columns
        String query = """
            SELECT t.table_name, c.column_name, c.data_type
            FROM user_tables t
            JOIN user_tab_columns c ON t.table_name = c.table_name
            WHERE t.table_name NOT LIKE 'SYS_%'
              AND t.table_name NOT LIKE 'APEX_%'
            ORDER BY t.table_name, c.column_id
            """;
        
        return connectionManager.executeQuery(query)
            .compose(results -> {
                // Build table structure for analysis
                Map<String, List<String>> tableColumns = new HashMap<>();
                for (int i = 0; i < results.size(); i++) {
                    JsonObject row = results.getJsonObject(i);
                    String tableName = row.getString("TABLE_NAME");
                    String columnName = row.getString("COLUMN_NAME");
                    String dataType = row.getString("DATA_TYPE");
                    
                    tableColumns.computeIfAbsent(tableName, k -> new ArrayList<>())
                               .add(columnName + " (" + dataType + ")");
                }
                
                // Process each table's columns
                List<Future<List<EnumCandidate>>> futures = new ArrayList<>();
                for (Map.Entry<String, List<String>> entry : tableColumns.entrySet()) {
                    futures.add(identifyEnumColumnsForTable(entry.getKey(), entry.getValue()));
                }
                
                // Combine all results
                return CompositeFuture.join(toRawFutureList(futures))
                    .map(composite -> {
                        List<EnumCandidate> allCandidates = new ArrayList<>();
                        for (int i = 0; i < composite.size(); i++) {
                            List<EnumCandidate> candidates = composite.resultAt(i);
                            if (candidates != null) {
                                allCandidates.addAll(candidates);
                            }
                        }
                        return allCandidates;
                    });
            })
            .recover(throwable -> {
                vertx.eventBus().publish("log", "[BusinessMappingServer] Failed to identify enum columns: " + 
                    throwable.getMessage() + ",0,BusinessMappingServer,Database,Error");
                return Future.succeededFuture(new ArrayList<EnumCandidate>());
            });
    }
    
    /**
     * Identify enum columns for a specific table using LLM or pattern matching
     */
    private Future<List<EnumCandidate>> identifyEnumColumnsForTable(String tableName, List<String> columns) {
        List<EnumCandidate> candidates = new ArrayList<>();
        
        // Use LLM if available for intelligent detection
        if (llmService != null && !columns.isEmpty()) {
            String prompt = "Analyze these columns from table " + tableName + " and identify which ones likely contain enumeration/categorical values:\n" +
                          "Columns: " + String.join(", ", columns) + "\n" +
                          "Look for patterns like:\n" +
                          "- Columns ending in _STATUS, _TYPE, _FLAG, _CODE, _ID (when referencing lookups)\n" +
                          "- Columns with names suggesting states, categories, or limited value sets\n" +
                          "- Short VARCHAR2 columns that likely hold codes\n" +
                          "Return ONLY column names that are likely enums, one per line. If none, return 'NONE'.";
            
            // Build messages for LLM call
            JsonArray messages = new JsonArray();
            messages.add(new JsonObject()
                .put("role", "system")
                .put("content", "You are a database schema analyzer. Identify columns that contain enumeration values."));
            messages.add(new JsonObject()
                .put("role", "user")
                .put("content", prompt));
            
            // Convert LLM CompletableFuture to Vert.x Future
            Promise<List<EnumCandidate>> promise = Promise.promise();
            
            llmService.chatCompletion(
                messages.stream().map(Object::toString).collect(Collectors.toList()),
                0.3,  // Low temperature for factual analysis
                500   // Max tokens
            ).whenComplete((llmResponse, error) -> {
                if (error != null) {
                    // Fall back to pattern matching
                    for (String colInfo : columns) {
                        String colName = colInfo.split(" ")[0];
                        if (isLikelyEnumColumn(colName)) {
                            candidates.add(new EnumCandidate(tableName, colName));
                        }
                    }
                    promise.complete(candidates);
                } else if (llmResponse != null) {
                    // Extract content from the LLM response structure
                    String responseText = "";
                    try {
                        JsonArray choices = llmResponse.getJsonArray("choices");
                        if (choices != null && choices.size() > 0) {
                            JsonObject firstChoice = choices.getJsonObject(0);
                            JsonObject message = firstChoice.getJsonObject("message");
                            responseText = message.getString("content", "");
                        }
                    } catch (Exception parseError) {
                        vertx.eventBus().publish("log", "Failed to parse LLM response: " + parseError.getMessage() + ",0,BusinessMappingServer,LLM,Error");
                        promise.complete(candidates);
                        return;
                    }
                    String[] lines = responseText.split("\n");
                    for (String line : lines) {
                        String colName = line.trim();
                        if (!colName.isEmpty() && !colName.equalsIgnoreCase("NONE")) {
                            // Verify the column exists in our list
                            for (String colInfo : columns) {
                                if (colInfo.startsWith(colName + " ")) {
                                    candidates.add(new EnumCandidate(tableName, colName));
                                    break;
                                }
                            }
                        }
                    }
                    promise.complete(candidates);
                }
            });
            
            return promise.future();
        } else {
            // No LLM available, use pattern matching
            for (String colInfo : columns) {
                String colName = colInfo.split(" ")[0];
                if (isLikelyEnumColumn(colName)) {
                    candidates.add(new EnumCandidate(tableName, colName));
                }
            }
            return Future.succeededFuture(candidates);
        }
    }
    
    /**
     * Pattern-based detection of likely enum columns
     */
    private boolean isLikelyEnumColumn(String columnName) {
        String upper = columnName.toUpperCase();
        return upper.endsWith("_STATUS") || upper.endsWith("_TYPE") ||
               upper.endsWith("_FLAG") || upper.endsWith("_CODE") ||
               upper.endsWith("_STATE") || upper.endsWith("_CATEGORY") ||
               upper.endsWith("_STATUS_ID") || upper.endsWith("_TYPE_ID") ||
               upper.equals("STATUS") || upper.equals("TYPE") ||
               upper.equals("STATE") || upper.equals("CATEGORY");
    }
    
    /**
     * Validate Oracle identifier to prevent SQL injection
     */
    private boolean isValidOracleIdentifier(String identifier) {
        if (identifier == null || identifier.isEmpty() || identifier.length() > 128) {
            return false;
        }
        // Oracle identifiers: start with letter, contain only letters, numbers, _, $, #
        // This is a simplified check - Oracle also allows quoted identifiers with spaces
        return identifier.matches("^[A-Za-z][A-Za-z0-9_$#]*$");
    }
    
    /**
     * Analyze column cardinality and load enum values if appropriate
     */
    private Future<Void> analyzeAndLoadEnumColumn(EnumCandidate candidate) {
        // Validate identifiers first to prevent SQL injection
        if (!isValidOracleIdentifier(candidate.table) || !isValidOracleIdentifier(candidate.column)) {
            vertx.eventBus().publish("log", "Invalid table or column name for enum analysis: " + 
                candidate.table + "." + candidate.column + ",2,BusinessMappingServer,MCP,System");
            return Future.succeededFuture();
        }
        
        // Check cardinality first (identifiers already validated above)
        String cardinalityQuery = String.format(
            "SELECT COUNT(DISTINCT %s) as cardinality FROM %s WHERE %s IS NOT NULL",
            candidate.column, candidate.table, candidate.column
        );
        
        return connectionManager.executeQuery(cardinalityQuery)
            .compose(results -> {
                int cardinality = 0;
                if (results.size() > 0) {
                    JsonObject row = results.getJsonObject(0);
                    cardinality = row.getInteger("CARDINALITY", 0);
                }
                
                // Only consider as enum if cardinality is low (< 50 distinct values)
                if (cardinality > 0 && cardinality < 50) {
                    vertx.eventBus().publish("log", "Loading enum values for " + candidate.table + "." + 
                        candidate.column + " (cardinality: " + cardinality + "),2,BusinessMappingServer,MCP,System");
                    
                    return loadEnumValuesWithIntelligence(candidate.table, candidate.column, cardinality)
                        .map(values -> {
                            if (!values.isEmpty()) {
                                // Store in cache with table.column as key (synchronized to prevent races)
                                String cacheKey = candidate.table.toUpperCase() + "." + candidate.column.toUpperCase();
                                synchronized(enumCache) {
                                    enumCache.put(cacheKey, values);
                                }
                                vertx.eventBus().publish("log", "Loaded " + values.size() + 
                                    " enum values for " + cacheKey + ",2,BusinessMappingServer,MCP,System");
                            }
                            return null;
                        });
                }
                return Future.<Void>succeededFuture();
            })
            .recover(throwable -> {
                vertx.eventBus().publish("log", "Failed to analyze enum column " + 
                    candidate.table + "." + candidate.column + ": " + throwable.getMessage() + 
                    ",3,BusinessMappingServer,MCP,System");
                return Future.<Void>succeededFuture();
            })
            .mapEmpty();
    }
    
    /**
     * Load mappings from a single enum table
     */
    private Future<Void> loadEnumTableMappings(String enumTable) {
        // Validate table name to prevent SQL injection
        if (!isValidOracleIdentifier(enumTable)) {
            vertx.eventBus().publish("log", "Invalid table name for enum loading: " + enumTable + ",2,BusinessMappingServer,MCP,System");
            return Future.succeededFuture();
        }
        
        // Oracle doesn't support bind variables for object names in data dictionary views
        // Use string concatenation with validation (already checked with isValidOracleIdentifier)
        String schemaQuery = "SELECT column_name FROM user_tab_columns WHERE table_name = '" + 
            enumTable.toUpperCase() + "' ORDER BY column_id";
        
        return connectionManager.executeQuery(schemaQuery)
            .compose(columns -> {
                String idColumn = null;
                String descColumn = null;
                
                for (int i = 0; i < columns.size(); i++) {
                    JsonObject row = columns.getJsonObject(i);
                    String colName = row.getString("COLUMN_NAME");
                    if (idColumn == null && (colName.endsWith("_ID") || colName.endsWith("_CODE") || colName.equals("ID"))) {
                        idColumn = colName;
                    } else if (descColumn == null && (colName.contains("DESC") || colName.contains("NAME") || colName.equals("VALUE"))) {
                        descColumn = colName;
                    }
                }
                
                if (idColumn != null && descColumn != null) {
                    // Load the enum mappings
                    final String finalIdColumn = idColumn;
                    final String finalDescColumn = descColumn;
                    String mappingQuery = String.format(
                        "SELECT %s as code, %s as description FROM %s",
                        idColumn, descColumn, enumTable
                    );
                    
                    return connectionManager.executeQuery(mappingQuery)
                        .map(results -> {
                            Map<String, EnumMapping> mappings = new HashMap<>();
                            
                            for (int i = 0; i < results.size(); i++) {
                                JsonObject row = results.getJsonObject(i);
                                String code = row.getString("CODE");
                                String desc = row.getString("DESCRIPTION");
                                if (code != null && desc != null) {
                                    mappings.put(code.toUpperCase(), 
                                        new EnumMapping(code, desc, enumTable, finalIdColumn, finalDescColumn));
                                }
                            }
                            
                            if (!mappings.isEmpty()) {
                                // Store with table name as key for reference (synchronized to prevent races)
                                synchronized(enumCache) {
                                    enumCache.put(enumTable.toUpperCase(), mappings);
                                }
                                vertx.eventBus().publish("log", "Loaded " + mappings.size() + 
                                    " enum mappings from table " + enumTable + " (ID: " + finalIdColumn + ", Desc: " + finalDescColumn + "),2,BusinessMappingServer,MCP,System");
                            }
                            return null;
                        });
                }
                return Future.<Void>succeededFuture();
            })
            .recover(throwable -> {
                vertx.eventBus().publish("log", "Failed to load enum table " + enumTable + ": " + 
                    throwable.getMessage() + ",3,BusinessMappingServer,MCP,System");
                return Future.<Void>succeededFuture();
            })
            .mapEmpty();
    }
    
    /**
     * Process multiple enum candidates in parallel
     */
    private Future<Void> analyzeAndLoadEnumColumns(List<EnumCandidate> candidates) {
        if (candidates.isEmpty()) {
            return Future.succeededFuture();
        }
        
        List<Future<Void>> futures = candidates.stream()
            .map(this::analyzeAndLoadEnumColumn)
            .collect(Collectors.toList());
        
        return CompositeFuture.join(toRawFutureList(futures))
            .map(composite -> {
                vertx.eventBus().publish("log", "Analyzed " + candidates.size() + 
                    " enum candidates,2,BusinessMappingServer,MCP,System");
                return (Void) null;
            })
            .otherwise(throwable -> {
                // Log but don't fail - we want partial success
                vertx.eventBus().publish("log", "Some enum analyses failed: " + 
                    throwable.getMessage() + ",3,BusinessMappingServer,MCP,System");
                return null;
            });
    }
    
    /**
     * Load enum values with intelligent description generation
     */
    private Future<Map<String, EnumMapping>> loadEnumValuesWithIntelligence(String table, String column, int cardinality) {
        // Validate identifiers to prevent SQL injection
        if (!isValidOracleIdentifier(table) || !isValidOracleIdentifier(column)) {
            vertx.eventBus().publish("log", "Invalid identifiers for enum value loading: " + 
                table + "." + column + ",2,BusinessMappingServer,MCP,System");
            return Future.succeededFuture(new HashMap<>());
        }
        
        // Get all distinct values (identifiers already validated above)
        String query = String.format(
            "SELECT DISTINCT %s as value, COUNT(*) as frequency " +
            "FROM %s " +
            "WHERE %s IS NOT NULL " +
            "GROUP BY %s " +
            "ORDER BY frequency DESC",
            column, table, column, column
        );
        
        return connectionManager.executeQuery(query)
            .compose(results -> {
                List<String> enumValues = new ArrayList<>();
                for (int i = 0; i < results.size(); i++) {
                    JsonObject row = results.getJsonObject(i);
                    String value = row.getString("VALUE");
                    if (value != null && value.length() <= 20) { // Reasonable length for enum
                        enumValues.add(value);
                    }
                }
                
                // Use LLM to generate descriptions if available
                if (llmService != null && !enumValues.isEmpty()) {
                    return generateEnumDescriptionsWithLLM(table, column, enumValues);
                } else {
                    // Fallback to heuristic generation
                    Map<String, EnumMapping> values = new HashMap<>();
                    for (String value : enumValues) {
                        String description = generateEnumDescription(value);
                        values.put(value.toUpperCase(), new EnumMapping(value, description, table, column));
                    }
                    return Future.succeededFuture(values);
                }
            })
            .recover(throwable -> {
                vertx.eventBus().publish("log", "Failed to load enum values for " + 
                    table + "." + column + ": " + throwable.getMessage() + ",3,BusinessMappingServer,MCP,System");
                return Future.succeededFuture(new HashMap<String, EnumMapping>());
            });
    }
    
    /**
     * Generate enum descriptions using LLM
     */
    private Future<Map<String, EnumMapping>> generateEnumDescriptionsWithLLM(String table, String column, List<String> enumValues) {
        String prompt = String.format(
            "For the database column %s.%s, these are the possible values: %s\n" +
            "Generate human-readable descriptions for each value.\n" +
            "Consider the column name '%s' as context.\n" +
            "Return as JSON: {\"value1\": \"description1\", ...}",
            table, column, enumValues, column
        );
        
        JsonArray messages = new JsonArray();
        messages.add(new JsonObject()
            .put("role", "system")
            .put("content", "You are a database enum descriptor. Generate clear descriptions for enum values."));
        messages.add(new JsonObject()
            .put("role", "user")
            .put("content", prompt));
        
        Promise<Map<String, EnumMapping>> promise = Promise.promise();
        
        llmService.chatCompletion(
            messages.stream().map(Object::toString).collect(Collectors.toList()),
            0.3,
            500
        ).whenComplete((llmResponse, error) -> {
            Map<String, EnumMapping> values = new HashMap<>();
            
            if (error == null && llmResponse != null) {
                try {
                    // Extract content from the LLM response structure
                    String responseText = "";
                    JsonArray choices = llmResponse.getJsonArray("choices");
                    if (choices != null && choices.size() > 0) {
                        JsonObject firstChoice = choices.getJsonObject(0);
                        JsonObject message = firstChoice.getJsonObject("message");
                        responseText = message.getString("content", "");
                    }
                    // Try to parse as JSON
                    JsonObject descriptions = new JsonObject(responseText);
                    for (String value : enumValues) {
                        String description = descriptions.getString(value, generateEnumDescription(value));
                        values.put(value.toUpperCase(), new EnumMapping(value, description, table, column));
                    }
                } catch (Exception e) {
                    // JSON parsing failed, use heuristic
                    for (String value : enumValues) {
                        String description = generateEnumDescription(value);
                        values.put(value.toUpperCase(), new EnumMapping(value, description, table, column));
                    }
                }
            } else {
                // LLM failed, use heuristic
                for (String value : enumValues) {
                    String description = generateEnumDescription(value);
                    values.put(value.toUpperCase(), new EnumMapping(value, description, table, column));
                }
            }
            
            promise.complete(values);
        });
        
        return promise.future();
    }
    /**
     * Helper method to convert typed Future list to raw Future list for CompositeFuture.join()
     */
    @SuppressWarnings("rawtypes")
    private List<Future> toRawFutureList(List<? extends Future> futures) {
        return futures.stream().map(f -> (Future) f).collect(Collectors.toList());
    }
    
    /**
     * Helper class for enum candidates
     */
    private static class EnumCandidate {
        final String table;
        final String column;
        
        EnumCandidate(String table, String column) {
            this.table = table;
            this.column = column;
        }
    }
    
    private Future<Map<String, EnumMapping>> loadEnumsForColumn(String table, String column) {
        // Validate identifiers to prevent SQL injection
        if (!isValidOracleIdentifier(table) || !isValidOracleIdentifier(column)) {
            vertx.eventBus().publish("log", "Invalid table or column name for enum loading: " + 
                table + "." + column + ",2,BusinessMappingServer,MCP,System");
            return Future.succeededFuture(new HashMap<>());
        }
        
        // Try to find distinct values - only practical for small cardinality
        String query = String.format(
            "SELECT DISTINCT %s as code FROM %s WHERE %s IS NOT NULL AND ROWNUM <= 100",
            column, table, column
        );
        
        return connectionManager.executeQuery(query)
            .map(results -> {
                Map<String, EnumMapping> enums = new HashMap<>();
                
                for (int i = 0; i < results.size(); i++) {
                    JsonObject row = results.getJsonObject(i);
                    String code = row.getString("CODE");
                    if (code != null && code.length() <= 10) { // Likely an enum if short
                        // Generate description based on code
                        String description = generateEnumDescription(code);
                        enums.put(code.toUpperCase(), new EnumMapping(code, description, table, column));
                    }
                }
                
                return enums;
            })
            .recover(throwable -> {
                vertx.eventBus().publish("log", "Could not load enums for " + table + "." + column + 
                    ": " + throwable.getMessage() + ",3,BusinessMappingServer,MCP,System");
                return Future.succeededFuture(new HashMap<String, EnumMapping>());
            });
    }
    
    /**
     * Tool method to load enum cache on demand
     */
    private void loadEnumCacheTool(RoutingContext ctx, String requestId, JsonObject arguments) {
        boolean forceRefresh = arguments.getBoolean("force_refresh", false);
        
        // Check if cache needs refresh
        if (!forceRefresh && System.currentTimeMillis() - enumCacheTimestamp < ENUM_CACHE_TTL && !enumCache.isEmpty()) {
            JsonObject result = new JsonObject()
                .put("status", "cache_valid")
                .put("message", "Enum cache is still valid")
                .put("cache_size", enumCache.size())
                .put("cache_age_ms", System.currentTimeMillis() - enumCacheTimestamp);
            sendSuccess(ctx, requestId, result);
            return;
        }
        
        // Load the cache
        loadEnumCache()
            .onSuccess(v -> {
                JsonObject result = new JsonObject()
                    .put("status", "cache_loaded")
                    .put("message", "Enum cache loaded successfully")
                    .put("cache_size", enumCache.size())
                    .put("discovered_tables", enumCache.keySet().stream()
                        .filter(k -> !k.contains("."))
                        .collect(Collectors.toList()))
                    .put("discovered_columns", enumCache.keySet().stream()
                        .filter(k -> k.contains("."))
                        .collect(Collectors.toList()));
                sendSuccess(ctx, requestId, result);
            })
            .onFailure(error -> {
                vertx.eventBus().publish("log", "Enum cache loading failed: " + error.getMessage() + ",0,BusinessMappingServer,MCP,System");
                sendError(ctx, requestId, MCPResponse.ErrorCodes.INTERNAL_ERROR, 
                    "Failed to load enum cache: " + error.getMessage());
            });
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
    
    @Override
    public void stop(Promise<Void> stopPromise) {
        // Connection manager handles its own cleanup
        vertx.eventBus().publish("log", "BusinessMappingServer stopped,2,BusinessMappingServer,MCP,System");
        try {
            super.stop(stopPromise);
        } catch (Exception e) {
            stopPromise.fail(e);
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
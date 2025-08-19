package AgentsMCPHost.mcp.utils;

import io.vertx.core.Vertx;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;

import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.regex.Pattern;

/**
 * EnumerationMapper - Handles bidirectional translation between codes and descriptions
 * for enumeration tables in Oracle database.
 * 
 * Automatically detects enumeration tables based on:
 * - Naming patterns (tables ending with _enum, _type, _status, _code)
 * - Table structure (having id, code, description columns)
 * - Foreign key references from other tables
 */
public class EnumerationMapper {
    
    private static EnumerationMapper instance;
    private final OracleConnectionManager oracleManager;
    private Vertx vertx;
    
    // Cache for enumeration mappings
    private final Map<String, EnumMapping> enumCache = new ConcurrentHashMap<>();
    private static final long CACHE_DURATION_MS = 10 * 60 * 1000; // 10 minutes
    
    // Patterns for detecting enum tables
    private static final Pattern ENUM_TABLE_PATTERN = Pattern.compile(
        ".*(ENUM|TYPE|STATUS|CODE|CATEGORY|CLASS)$", Pattern.CASE_INSENSITIVE);
    
    // Common enum column patterns
    private static final Pattern CODE_COLUMN_PATTERN = Pattern.compile(
        ".*_(CODE|CD|KEY|ID)$", Pattern.CASE_INSENSITIVE);
    private static final Pattern DESC_COLUMN_PATTERN = Pattern.compile(
        ".*_(DESC|DESCRIPTION|NAME|LABEL|TEXT)$", Pattern.CASE_INSENSITIVE);
    
    /**
     * Inner class to hold enumeration mapping data
     */
    private static class EnumMapping {
        final String tableName;
        final String codeColumn;
        final String descColumn;
        final Map<String, String> codeToDesc = new ConcurrentHashMap<>();
        final Map<String, String> descToCode = new ConcurrentHashMap<>();
        final long cachedAt;
        
        EnumMapping(String tableName, String codeColumn, String descColumn) {
            this.tableName = tableName;
            this.codeColumn = codeColumn;
            this.descColumn = descColumn;
            this.cachedAt = System.currentTimeMillis();
        }
        
        boolean isExpired() {
            return System.currentTimeMillis() - cachedAt > CACHE_DURATION_MS;
        }
    }
    
    private EnumerationMapper() {
        this.oracleManager = OracleConnectionManager.getInstance();
    }
    
    /**
     * Get singleton instance
     */
    public static synchronized EnumerationMapper getInstance() {
        if (instance == null) {
            instance = new EnumerationMapper();
        }
        return instance;
    }
    
    /**
     * Initialize with Vertx instance
     */
    public void initialize(Vertx vertx) {
        this.vertx = vertx;
    }
    
    /**
     * Detect all enumeration tables in the database
     */
    public Future<JsonArray> detectEnumerationTables() {
        Promise<JsonArray> promise = Promise.promise();
        
        // Query to find potential enum tables
        String query = "SELECT table_name FROM user_tables ORDER BY table_name";
        
        oracleManager.executeQuery(query)
            .compose(tables -> {
                List<Future<JsonObject>> checks = new ArrayList<>();
                
                for (int i = 0; i < tables.size(); i++) {
                    String tableName = tables.getJsonObject(i).getString("TABLE_NAME");
                    
                    // Check if table name matches enum pattern
                    if (ENUM_TABLE_PATTERN.matcher(tableName).matches()) {
                        checks.add(analyzeEnumTable(tableName));
                    }
                }
                
                return Future.all(checks);
            })
            .onSuccess(results -> {
                JsonArray enumerations = new JsonArray();
                for (Object result : results.list()) {
                    if (result != null) {
                        enumerations.add((JsonObject) result);
                    }
                }
                promise.complete(enumerations);
            })
            .onFailure(promise::fail);
        
        return promise.future();
    }
    
    /**
     * Analyze a table to determine if it's an enumeration
     */
    private Future<JsonObject> analyzeEnumTable(String tableName) {
        Promise<JsonObject> promise = Promise.promise();
        
        oracleManager.getTableMetadata(tableName)
            .onSuccess(metadata -> {
                JsonArray columns = metadata.getJsonArray("columns");
                
                String codeColumn = null;
                String descColumn = null;
                boolean hasId = false;
                
                // Analyze columns
                for (int i = 0; i < columns.size(); i++) {
                    JsonObject column = columns.getJsonObject(i);
                    String colName = column.getString("name");
                    
                    if (colName.equalsIgnoreCase("ID")) {
                        hasId = true;
                    } else if (CODE_COLUMN_PATTERN.matcher(colName).matches()) {
                        codeColumn = colName;
                    } else if (DESC_COLUMN_PATTERN.matcher(colName).matches()) {
                        descColumn = colName;
                    }
                }
                
                // Check if this looks like an enum table
                if (codeColumn != null && descColumn != null) {
                    // Make variables final for lambda
                    final String finalCodeColumn = codeColumn;
                    final String finalDescColumn = descColumn;
                    
                    // Count rows to verify it's a small lookup table
                    String countQuery = "SELECT COUNT(*) as cnt FROM " + tableName;
                    oracleManager.executeQuery(countQuery)
                        .onSuccess(countResult -> {
                            int rowCount = countResult.getJsonObject(0).getInteger("CNT", 0);
                            
                            // Typically enum tables have fewer than 100 rows
                            if (rowCount > 0 && rowCount < 100) {
                                JsonObject enumInfo = new JsonObject()
                                    .put("tableName", tableName)
                                    .put("codeColumn", finalCodeColumn)
                                    .put("descColumn", finalDescColumn)
                                    .put("rowCount", rowCount)
                                    .put("isEnumeration", true);
                                
                                promise.complete(enumInfo);
                            } else {
                                promise.complete(null);
                            }
                        })
                        .onFailure(err -> promise.complete(null));
                } else {
                    promise.complete(null);
                }
            })
            .onFailure(err -> promise.complete(null));
        
        return promise.future();
    }
    
    /**
     * Load enumeration mapping for a specific table
     */
    public Future<EnumMapping> loadEnumeration(String tableName, String codeColumn, String descColumn) {
        Promise<EnumMapping> promise = Promise.promise();
        
        // Check cache first
        EnumMapping cached = enumCache.get(tableName);
        if (cached != null && !cached.isExpired()) {
            promise.complete(cached);
            return promise.future();
        }
        
        // Load from database
        String query = String.format(
            "SELECT %s, %s FROM %s",
            codeColumn, descColumn, tableName
        );
        
        oracleManager.executeQuery(query)
            .onSuccess(rows -> {
                EnumMapping mapping = new EnumMapping(tableName, codeColumn, descColumn);
                
                for (int i = 0; i < rows.size(); i++) {
                    JsonObject row = rows.getJsonObject(i);
                    String code = String.valueOf(row.getValue(codeColumn));
                    String desc = String.valueOf(row.getValue(descColumn));
                    
                    if (code != null && desc != null) {
                        mapping.codeToDesc.put(code.toUpperCase(), desc);
                        mapping.descToCode.put(desc.toUpperCase(), code);
                    }
                }
                
                // Cache the mapping
                enumCache.put(tableName, mapping);
                promise.complete(mapping);
            })
            .onFailure(promise::fail);
        
        return promise.future();
    }
    
    /**
     * Translate a code to its description
     */
    public Future<String> translateToDescription(String tableName, String code) {
        Promise<String> promise = Promise.promise();
        
        EnumMapping mapping = enumCache.get(tableName);
        if (mapping != null && !mapping.isExpired()) {
            String desc = mapping.codeToDesc.get(code.toUpperCase());
            promise.complete(desc != null ? desc : code);
        } else {
            // Need to detect and load the enum
            detectEnumStructure(tableName)
                .compose(structure -> {
                    if (structure != null) {
                        return loadEnumeration(tableName, 
                            structure.getString("codeColumn"),
                            structure.getString("descColumn"));
                    }
                    return Future.failedFuture("Not an enumeration table");
                })
                .onSuccess(loaded -> {
                    String desc = loaded.codeToDesc.get(code.toUpperCase());
                    promise.complete(desc != null ? desc : code);
                })
                .onFailure(err -> promise.complete(code)); // Return original code if translation fails
        }
        
        return promise.future();
    }
    
    /**
     * Translate a description to its code
     */
    public Future<String> translateToCode(String tableName, String description) {
        Promise<String> promise = Promise.promise();
        
        EnumMapping mapping = enumCache.get(tableName);
        if (mapping != null && !mapping.isExpired()) {
            String code = mapping.descToCode.get(description.toUpperCase());
            promise.complete(code != null ? code : description);
        } else {
            // Need to detect and load the enum
            detectEnumStructure(tableName)
                .compose(structure -> {
                    if (structure != null) {
                        return loadEnumeration(tableName,
                            structure.getString("codeColumn"),
                            structure.getString("descColumn"));
                    }
                    return Future.failedFuture("Not an enumeration table");
                })
                .onSuccess(loaded -> {
                    String code = loaded.descToCode.get(description.toUpperCase());
                    promise.complete(code != null ? code : description);
                })
                .onFailure(err -> promise.complete(description)); // Return original if translation fails
        }
        
        return promise.future();
    }
    
    /**
     * Detect enum structure of a table
     */
    private Future<JsonObject> detectEnumStructure(String tableName) {
        return analyzeEnumTable(tableName);
    }
    
    /**
     * Check if a column is an enumeration reference
     */
    public Future<Boolean> isEnumColumn(String tableName, String columnName) {
        Promise<Boolean> promise = Promise.promise();
        
        // Check foreign keys to see if this column references an enum table
        oracleManager.getTableMetadata(tableName)
            .onSuccess(metadata -> {
                JsonArray foreignKeys = metadata.getJsonArray("foreignKeys");
                
                for (int i = 0; i < foreignKeys.size(); i++) {
                    JsonObject fk = foreignKeys.getJsonObject(i);
                    if (fk.getString("column").equals(columnName)) {
                        String referencedTable = fk.getString("referencedTable");
                        
                        // Check if referenced table is an enum
                        if (ENUM_TABLE_PATTERN.matcher(referencedTable).matches()) {
                            promise.complete(true);
                            return;
                        }
                    }
                }
                
                promise.complete(false);
            })
            .onFailure(err -> promise.complete(false));
        
        return promise.future();
    }
    
    /**
     * Translate all enum values in a result set
     */
    public Future<JsonArray> translateResults(JsonArray results, String tableName) {
        Promise<JsonArray> promise = Promise.promise();
        
        if (results == null || results.isEmpty()) {
            promise.complete(results);
            return promise.future();
        }
        
        // Get table metadata to find enum columns
        oracleManager.getTableMetadata(tableName)
            .compose(metadata -> {
                JsonArray foreignKeys = metadata.getJsonArray("foreignKeys");
                Map<String, String> enumColumns = new HashMap<>();
                
                // Identify enum columns
                for (int i = 0; i < foreignKeys.size(); i++) {
                    JsonObject fk = foreignKeys.getJsonObject(i);
                    String column = fk.getString("column");
                    String refTable = fk.getString("referencedTable");
                    
                    if (ENUM_TABLE_PATTERN.matcher(refTable).matches()) {
                        enumColumns.put(column, refTable);
                    }
                }
                
                // Translate enum values in results
                List<Future<?>> translations = new ArrayList<>();
                JsonArray translatedResults = new JsonArray();
                
                for (int i = 0; i < results.size(); i++) {
                    JsonObject row = results.getJsonObject(i).copy();
                    translatedResults.add(row);
                    
                    for (Map.Entry<String, String> entry : enumColumns.entrySet()) {
                        String column = entry.getKey();
                        String enumTable = entry.getValue();
                        Object value = row.getValue(column);
                        
                        if (value != null) {
                            int rowIndex = i;
                            translations.add(
                                translateToDescription(enumTable, value.toString())
                                    .map(desc -> {
                                        translatedResults.getJsonObject(rowIndex)
                                            .put(column + "_DESC", desc);
                                        return desc;
                                    })
                            );
                        }
                    }
                }
                
                return Future.all(translations)
                    .map(v -> translatedResults);
            })
            .onSuccess(promise::complete)
            .onFailure(err -> promise.complete(results)); // Return original on error
        
        return promise.future();
    }
    
    /**
     * Get all cached enumeration mappings
     */
    public JsonObject getCachedEnumerations() {
        JsonObject cached = new JsonObject();
        
        enumCache.forEach((tableName, mapping) -> {
            if (!mapping.isExpired()) {
                cached.put(tableName, new JsonObject()
                    .put("codeColumn", mapping.codeColumn)
                    .put("descColumn", mapping.descColumn)
                    .put("mappingCount", mapping.codeToDesc.size())
                    .put("cachedAt", mapping.cachedAt));
            }
        });
        
        return cached;
    }
    
    /**
     * Clear enumeration cache
     */
    public void clearCache() {
        enumCache.clear();
    }
}
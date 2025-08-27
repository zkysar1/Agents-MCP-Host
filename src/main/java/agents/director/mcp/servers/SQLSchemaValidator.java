package agents.director.mcp.servers;

import agents.director.services.OracleConnectionManager;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SQLSchemaValidator - Validates SQL queries against actual database schema.
 * 
 * This validator ensures that generated SQL only references tables and columns
 * that actually exist in the database. It provides detailed error messages
 * and suggestions for corrections when mismatches are found.
 * 
 * Key features:
 * - Parses SQL to extract all table and column references
 * - Validates against cached schema information
 * - Suggests closest matches for invalid references
 * - Provides detailed validation reports
 */
public class SQLSchemaValidator {
    
    private final OracleConnectionManager oracleManager;
    
    // Schema cache to avoid repeated queries
    private final Map<String, JsonObject> schemaCache = new ConcurrentHashMap<>();
    private volatile long lastSchemaRefresh = 0;
    private static final long SCHEMA_CACHE_TTL = 300000; // 5 minutes
    
    // SQL parsing patterns
    private static final Pattern TABLE_PATTERN = Pattern.compile(
        "(?:FROM|JOIN|INTO|UPDATE|DELETE\\s+FROM)\\s+([\\w\\.]+)(?:\\s+(?:AS\\s+)?(\\w+))?",
        Pattern.CASE_INSENSITIVE
    );
    
    private static final Pattern COLUMN_PATTERN = Pattern.compile(
        "(?:SELECT\\s+|WHERE\\s+|ON\\s+|SET\\s+|ORDER\\s+BY\\s+|GROUP\\s+BY\\s+|HAVING\\s+)" +
        "(?:[^;]+?)([\\w\\.]+)\\s*(?:=|>|<|>=|<=|<>|!=|LIKE|IN|BETWEEN)",
        Pattern.CASE_INSENSITIVE
    );
    
    private static final Pattern SELECT_COLUMNS_PATTERN = Pattern.compile(
        "SELECT\\s+(.+?)\\s+FROM",
        Pattern.CASE_INSENSITIVE | Pattern.DOTALL
    );
    
    public SQLSchemaValidator(OracleConnectionManager oracleManager) {
        this.oracleManager = oracleManager;
    }
    
    /**
     * Validate SQL against database schema
     */
    public Future<JsonObject> validateSQL(String sql, JsonObject schemaContext) {
        if (sql == null || sql.trim().isEmpty()) {
            return Future.succeededFuture(new JsonObject()
                .put("valid", false)
                .put("error", "No SQL provided"));
        }
        
        // First ensure we have current schema information
        return ensureSchemaLoaded()
            .compose(schema -> performValidation(sql, schema, schemaContext));
    }
    
    /**
     * Ensure schema information is loaded and current
     */
    private Future<Map<String, JsonObject>> ensureSchemaLoaded() {
        long now = System.currentTimeMillis();
        
        // Check if cache is still valid
        if (!schemaCache.isEmpty() && (now - lastSchemaRefresh) < SCHEMA_CACHE_TTL) {
            return Future.succeededFuture(schemaCache);
        }
        
        // Refresh schema cache
        return oracleManager.listTables()
            .compose(tables -> {
                // Get metadata for each table
                List<Future<JsonObject>> metadataFutures = new ArrayList<>();
                
                for (int i = 0; i < tables.size(); i++) {
                    JsonObject table = tables.getJsonObject(i);
                    String tableName = table.getString("name", "");
                    if (!tableName.isEmpty()) {
                        Future<JsonObject> metadataFuture = oracleManager.getTableMetadata(tableName)
                            .map(metadata -> {
                                metadata.put("tableName", tableName.toUpperCase());
                                return metadata;
                            });
                        metadataFutures.add(metadataFuture);
                    }
                }
                
                return Future.all(metadataFutures);
            })
            .map(compositeFuture -> {
                schemaCache.clear();
                
                List<Future<JsonObject>> futures = compositeFuture.list();
                for (Future<JsonObject> future : futures) {
                    if (future.succeeded()) {
                        JsonObject tableWithColumns = future.result();
                        String tableName = tableWithColumns.getString("tableName", "").toUpperCase();
                        if (!tableName.isEmpty()) {
                            schemaCache.put(tableName, tableWithColumns);
                        }
                    }
                }
                
                lastSchemaRefresh = now;
                return schemaCache;
            })
            .recover(err -> {
                // On error, use existing cache if available
                if (!schemaCache.isEmpty()) {
                    return Future.succeededFuture(schemaCache);
                }
                return Future.failedFuture(err);
            });
    }
    
    /**
     * Perform the actual SQL validation
     */
    private Future<JsonObject> performValidation(String sql, Map<String, JsonObject> schema, 
                                                JsonObject schemaContext) {
        JsonObject result = new JsonObject()
            .put("sql", sql)
            .put("validationTime", System.currentTimeMillis());
        
        // Extract table references from SQL
        Map<String, String> tableAliases = new HashMap<>();
        Set<String> referencedTables = extractTableReferences(sql, tableAliases);
        
        // Extract column references
        Set<ColumnReference> referencedColumns = extractColumnReferences(sql, tableAliases);
        
        // Validate tables
        JsonArray tableValidations = new JsonArray();
        boolean allTablesValid = true;
        
        for (String table : referencedTables) {
            JsonObject validation = validateTable(table, schema);
            tableValidations.add(validation);
            if (!validation.getBoolean("valid", false)) {
                allTablesValid = false;
            }
        }
        
        // Validate columns
        JsonArray columnValidations = new JsonArray();
        boolean allColumnsValid = true;
        
        for (ColumnReference colRef : referencedColumns) {
            JsonObject validation = validateColumn(colRef, schema, tableAliases);
            columnValidations.add(validation);
            if (!validation.getBoolean("valid", false)) {
                allColumnsValid = false;
            }
        }
        
        // Overall validation result
        boolean isValid = allTablesValid && allColumnsValid;
        result.put("valid", isValid);
        result.put("tableValidations", tableValidations);
        result.put("columnValidations", columnValidations);
        
        // Add suggested corrections if not valid
        if (!isValid) {
            JsonObject corrections = generateCorrections(tableValidations, columnValidations, 
                                                       schemaContext);
            result.put("suggestedCorrections", corrections);
        }
        
        // Add summary
        result.put("summary", generateValidationSummary(tableValidations, columnValidations));
        
        return Future.succeededFuture(result);
    }
    
    /**
     * Extract table references from SQL
     */
    private Set<String> extractTableReferences(String sql, Map<String, String> aliases) {
        Set<String> tables = new HashSet<>();
        
        // Remove comments and normalize whitespace
        String cleanSql = sql.replaceAll("--.*", "")
                            .replaceAll("/\\*.*?\\*/", "")
                            .replaceAll("\\s+", " ");
        
        Matcher matcher = TABLE_PATTERN.matcher(cleanSql);
        while (matcher.find()) {
            String tableName = matcher.group(1).trim().toUpperCase();
            
            // Handle schema prefix (e.g., SCHEMA.TABLE)
            if (tableName.contains(".")) {
                tableName = tableName.substring(tableName.lastIndexOf('.') + 1);
            }
            
            tables.add(tableName);
            
            // Capture alias if present
            if (matcher.groupCount() >= 2 && matcher.group(2) != null) {
                String alias = matcher.group(2).trim().toUpperCase();
                aliases.put(alias, tableName);
            }
        }
        
        return tables;
    }
    
    /**
     * Extract column references from SQL
     */
    private Set<ColumnReference> extractColumnReferences(String sql, Map<String, String> aliases) {
        Set<ColumnReference> columns = new HashSet<>();
        
        // First extract SELECT columns
        Matcher selectMatcher = SELECT_COLUMNS_PATTERN.matcher(sql);
        if (selectMatcher.find()) {
            String selectClause = selectMatcher.group(1);
            parseSelectColumns(selectClause, columns, aliases);
        }
        
        // Then extract columns from WHERE, JOIN, etc.
        Matcher columnMatcher = COLUMN_PATTERN.matcher(sql);
        while (columnMatcher.find()) {
            String columnRef = columnMatcher.group(1).trim().toUpperCase();
            ColumnReference ref = parseColumnReference(columnRef, aliases);
            if (ref != null) {
                columns.add(ref);
            }
        }
        
        return columns;
    }
    
    /**
     * Parse SELECT clause columns
     */
    private void parseSelectColumns(String selectClause, Set<ColumnReference> columns,
                                   Map<String, String> aliases) {
        // Handle SELECT *
        if (selectClause.trim().equals("*")) {
            return; // Can't validate * without specific context
        }
        
        // Split by comma (simple parsing - doesn't handle nested functions perfectly)
        String[] parts = selectClause.split(",");
        
        for (String part : parts) {
            part = part.trim();
            
            // Skip aggregates and functions for now
            if (part.contains("(") || part.contains(")")) {
                // Try to extract column references from functions
                Pattern colPattern = Pattern.compile("([\\w\\.]+)(?:\\s*[,)])", Pattern.CASE_INSENSITIVE);
                Matcher matcher = colPattern.matcher(part);
                while (matcher.find()) {
                    String colRef = matcher.group(1).toUpperCase();
                    ColumnReference ref = parseColumnReference(colRef, aliases);
                    if (ref != null) {
                        columns.add(ref);
                    }
                }
            } else {
                // Direct column reference
                String colRef = part.split("\\s+")[0]; // Remove alias
                ColumnReference ref = parseColumnReference(colRef.toUpperCase(), aliases);
                if (ref != null) {
                    columns.add(ref);
                }
            }
        }
    }
    
    /**
     * Parse a column reference (handles table.column format)
     */
    private ColumnReference parseColumnReference(String columnRef, Map<String, String> aliases) {
        if (columnRef == null || columnRef.isEmpty() || columnRef.equals("*")) {
            return null;
        }
        
        String table = null;
        String column = columnRef;
        
        if (columnRef.contains(".")) {
            String[] parts = columnRef.split("\\.", 2);
            String tableOrAlias = parts[0];
            column = parts[1];
            
            // Resolve alias if needed
            table = aliases.getOrDefault(tableOrAlias, tableOrAlias);
        }
        
        return new ColumnReference(table, column);
    }
    
    /**
     * Validate a table exists
     */
    private JsonObject validateTable(String tableName, Map<String, JsonObject> schema) {
        JsonObject validation = new JsonObject()
            .put("table", tableName);
        
        if (schema.containsKey(tableName)) {
            validation.put("valid", true);
            validation.put("exists", true);
        } else {
            validation.put("valid", false);
            validation.put("exists", false);
            validation.put("error", "Table " + tableName + " does not exist");
            
            // Find closest matches
            JsonArray suggestions = findClosestTables(tableName, schema.keySet());
            if (!suggestions.isEmpty()) {
                validation.put("suggestions", suggestions);
            }
        }
        
        return validation;
    }
    
    /**
     * Validate a column exists in its table
     */
    private JsonObject validateColumn(ColumnReference colRef, Map<String, JsonObject> schema,
                                     Map<String, String> aliases) {
        JsonObject validation = new JsonObject()
            .put("column", colRef.column);
        
        if (colRef.table != null) {
            validation.put("table", colRef.table);
        }
        
        // If table is specified, validate against that table
        if (colRef.table != null) {
            JsonObject tableSchema = schema.get(colRef.table);
            if (tableSchema == null) {
                validation.put("valid", false);
                validation.put("error", "Referenced table " + colRef.table + " does not exist");
                return validation;
            }
            
            if (columnExistsInTable(colRef.column, tableSchema)) {
                validation.put("valid", true);
                validation.put("exists", true);
            } else {
                validation.put("valid", false);
                validation.put("exists", false);
                validation.put("error", "Column " + colRef.column + " does not exist in table " + colRef.table);
                
                // Find closest column matches
                JsonArray suggestions = findClosestColumns(colRef.column, tableSchema);
                if (!suggestions.isEmpty()) {
                    validation.put("suggestions", suggestions);
                }
            }
        } else {
            // No table specified - check all tables
            JsonArray foundIn = new JsonArray();
            
            for (Map.Entry<String, JsonObject> entry : schema.entrySet()) {
                if (columnExistsInTable(colRef.column, entry.getValue())) {
                    foundIn.add(entry.getKey());
                }
            }
            
            if (foundIn.isEmpty()) {
                validation.put("valid", false);
                validation.put("exists", false);
                validation.put("error", "Column " + colRef.column + " not found in any table");
            } else if (foundIn.size() == 1) {
                validation.put("valid", true);
                validation.put("exists", true);
                validation.put("foundIn", foundIn);
            } else {
                // Ambiguous column reference
                validation.put("valid", false);
                validation.put("exists", true);
                validation.put("error", "Column " + colRef.column + " is ambiguous - found in multiple tables");
                validation.put("foundIn", foundIn);
            }
        }
        
        return validation;
    }
    
    /**
     * Check if column exists in table schema
     */
    private boolean columnExistsInTable(String columnName, JsonObject tableSchema) {
        JsonArray columns = tableSchema.getJsonArray("columns", new JsonArray());
        
        for (int i = 0; i < columns.size(); i++) {
            JsonObject col = columns.getJsonObject(i);
            String colName = col.getString("name", "").toUpperCase();
            if (colName.equals(columnName)) {
                return true;
            }
        }
        
        return false;
    }
    
    /**
     * Find closest matching tables
     */
    private JsonArray findClosestTables(String tableName, Set<String> availableTables) {
        JsonArray suggestions = new JsonArray();
        
        for (String available : availableTables) {
            double similarity = calculateSimilarity(tableName, available);
            if (similarity > 0.5) {
                suggestions.add(new JsonObject()
                    .put("table", available)
                    .put("similarity", similarity));
            }
        }
        
        // Sort by similarity
        List<JsonObject> sorted = new ArrayList<>();
        for (int i = 0; i < suggestions.size(); i++) {
            sorted.add(suggestions.getJsonObject(i));
        }
        sorted.sort((a, b) -> Double.compare(
            b.getDouble("similarity"), 
            a.getDouble("similarity")
        ));
        
        JsonArray result = new JsonArray();
        for (int i = 0; i < Math.min(3, sorted.size()); i++) {
            result.add(sorted.get(i).getString("table"));
        }
        
        return result;
    }
    
    /**
     * Find closest matching columns in a table
     */
    private JsonArray findClosestColumns(String columnName, JsonObject tableSchema) {
        JsonArray suggestions = new JsonArray();
        JsonArray columns = tableSchema.getJsonArray("columns", new JsonArray());
        
        for (int i = 0; i < columns.size(); i++) {
            JsonObject col = columns.getJsonObject(i);
            String colName = col.getString("name", "").toUpperCase();
            
            double similarity = calculateSimilarity(columnName, colName);
            if (similarity > 0.5) {
                suggestions.add(new JsonObject()
                    .put("column", colName)
                    .put("similarity", similarity));
            }
        }
        
        // Sort by similarity
        List<JsonObject> sorted = new ArrayList<>();
        for (int i = 0; i < suggestions.size(); i++) {
            sorted.add(suggestions.getJsonObject(i));
        }
        sorted.sort((a, b) -> Double.compare(
            b.getDouble("similarity"), 
            a.getDouble("similarity")
        ));
        
        JsonArray result = new JsonArray();
        for (int i = 0; i < Math.min(3, sorted.size()); i++) {
            result.add(sorted.get(i).getString("column"));
        }
        
        return result;
    }
    
    /**
     * Calculate string similarity (simple Levenshtein-based)
     */
    private double calculateSimilarity(String s1, String s2) {
        if (s1.equals(s2)) return 1.0;
        
        // Normalize
        s1 = s1.toLowerCase().replace("_", "");
        s2 = s2.toLowerCase().replace("_", "");
        
        if (s1.equals(s2)) return 0.9;
        if (s1.contains(s2) || s2.contains(s1)) return 0.7;
        
        // Simple character overlap
        Set<Character> chars1 = new HashSet<>();
        Set<Character> chars2 = new HashSet<>();
        
        for (char c : s1.toCharArray()) chars1.add(c);
        for (char c : s2.toCharArray()) chars2.add(c);
        
        Set<Character> intersection = new HashSet<>(chars1);
        intersection.retainAll(chars2);
        
        Set<Character> union = new HashSet<>(chars1);
        union.addAll(chars2);
        
        return (double) intersection.size() / union.size();
    }
    
    /**
     * Generate correction suggestions
     */
    private JsonObject generateCorrections(JsonArray tableValidations, JsonArray columnValidations,
                                         JsonObject schemaContext) {
        JsonObject corrections = new JsonObject();
        JsonArray tableCorrections = new JsonArray();
        JsonArray columnCorrections = new JsonArray();
        
        // Table corrections
        for (int i = 0; i < tableValidations.size(); i++) {
            JsonObject validation = tableValidations.getJsonObject(i);
            if (!validation.getBoolean("valid", true)) {
                JsonObject correction = new JsonObject()
                    .put("invalid", validation.getString("table"))
                    .put("suggestions", validation.getJsonArray("suggestions", new JsonArray()));
                tableCorrections.add(correction);
            }
        }
        
        // Column corrections
        for (int i = 0; i < columnValidations.size(); i++) {
            JsonObject validation = columnValidations.getJsonObject(i);
            if (!validation.getBoolean("valid", true)) {
                JsonObject correction = new JsonObject()
                    .put("invalid", validation.getString("column"))
                    .put("table", validation.getString("table", ""))
                    .put("suggestions", validation.getJsonArray("suggestions", new JsonArray()))
                    .put("foundIn", validation.getJsonArray("foundIn", new JsonArray()));
                columnCorrections.add(correction);
            }
        }
        
        corrections.put("tables", tableCorrections);
        corrections.put("columns", columnCorrections);
        
        // Add context-aware suggestions if available
        if (schemaContext != null && schemaContext.containsKey("match_result")) {
            JsonObject matchResult = schemaContext.getJsonObject("match_result");
            corrections.put("contextualHint", "Consider using tables/columns from the schema match result");
            corrections.put("matchedTables", matchResult.getJsonArray("tableMatches", new JsonArray()));
        }
        
        return corrections;
    }
    
    /**
     * Generate validation summary
     */
    private JsonObject generateValidationSummary(JsonArray tableValidations, JsonArray columnValidations) {
        int validTables = 0, invalidTables = 0;
        int validColumns = 0, invalidColumns = 0;
        
        for (int i = 0; i < tableValidations.size(); i++) {
            if (tableValidations.getJsonObject(i).getBoolean("valid", false)) {
                validTables++;
            } else {
                invalidTables++;
            }
        }
        
        for (int i = 0; i < columnValidations.size(); i++) {
            if (columnValidations.getJsonObject(i).getBoolean("valid", false)) {
                validColumns++;
            } else {
                invalidColumns++;
            }
        }
        
        return new JsonObject()
            .put("totalTables", validTables + invalidTables)
            .put("validTables", validTables)
            .put("invalidTables", invalidTables)
            .put("totalColumns", validColumns + invalidColumns)
            .put("validColumns", validColumns)
            .put("invalidColumns", invalidColumns)
            .put("overallValid", invalidTables == 0 && invalidColumns == 0);
    }
    
    /**
     * Helper class to represent column references
     */
    private static class ColumnReference {
        final String table;
        final String column;
        
        ColumnReference(String table, String column) {
            this.table = table;
            this.column = column;
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ColumnReference that = (ColumnReference) o;
            return Objects.equals(table, that.table) && 
                   Objects.equals(column, that.column);
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(table, column);
        }
    }
}
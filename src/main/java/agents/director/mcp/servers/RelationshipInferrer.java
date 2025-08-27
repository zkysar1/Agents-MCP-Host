package agents.director.mcp.servers;

import agents.director.services.OracleConnectionManager;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.*;

/**
 * RelationshipInferrer - Discovers relationships between tables even without foreign keys.
 * 
 * This tool infers relationships by analyzing:
 * - Column names and types that match across tables
 * - Data patterns and overlapping values
 * - Naming conventions (table_id pattern)
 * - Business logic patterns
 * 
 * Works with ANY dataset by discovering patterns dynamically.
 */
public class RelationshipInferrer {
    
    private final OracleConnectionManager oracleManager;
    
    // Cache for discovered relationships
    private final Map<String, List<Relationship>> relationshipCache = new HashMap<>();
    
    public RelationshipInferrer(OracleConnectionManager oracleManager) {
        this.oracleManager = oracleManager;
    }
    
    /**
     * Infer relationships between tables based on query needs
     */
    public Future<JsonObject> inferRelationships(JsonArray tables, JsonObject queryAnalysis) {
        if (tables == null || tables.isEmpty()) {
            return Future.succeededFuture(new JsonObject()
                .put("relationships", new JsonArray())
                .put("join_paths", new JsonArray()));
        }
        
        // Get metadata for all tables
        List<Future<TableInfo>> tableInfoFutures = new ArrayList<>();
        
        for (int i = 0; i < tables.size(); i++) {
            String tableName = tables.getString(i);
            if (tableName != null) {
                tableInfoFutures.add(getTableInfo(tableName));
            }
        }
        
        return Future.all(tableInfoFutures)
            .compose(result -> {
                List<TableInfo> tableInfos = new ArrayList<>();
                for (Future<TableInfo> future : tableInfoFutures) {
                    if (future.succeeded() && future.result() != null) {
                        tableInfos.add(future.result());
                    }
                }
                
                // Discover all possible relationships
                List<Relationship> relationships = discoverRelationships(tableInfos);
                
                // Find optimal join paths based on query needs
                List<JoinPath> joinPaths = findJoinPaths(relationships, queryAnalysis);
                
                return Future.succeededFuture(new JsonObject()
                    .put("relationships", relationshipsToJson(relationships))
                    .put("join_paths", joinPathsToJson(joinPaths))
                    .put("statistics", gatherStatistics(relationships, joinPaths)));
            });
    }
    
    /**
     * Get comprehensive table information
     */
    private Future<TableInfo> getTableInfo(String tableName) {
        return oracleManager.getTableMetadata(tableName)
            .map(metadata -> {
                TableInfo info = new TableInfo();
                info.tableName = tableName;
                info.columns = metadata.getJsonArray("columns", new JsonArray());
                info.primaryKey = metadata.getString("primaryKey");
                info.foreignKeys = metadata.getJsonArray("foreignKeys", new JsonArray());
                
                // Extract column info for quick access
                for (int i = 0; i < info.columns.size(); i++) {
                    JsonObject col = info.columns.getJsonObject(i);
                    info.columnMap.put(
                        col.getString("name").toUpperCase(),
                        col.getString("type")
                    );
                }
                
                return info;
            })
            .recover((Throwable err) -> {
                // Return empty info on error
                TableInfo info = new TableInfo();
                info.tableName = tableName;
                info.error = err.getMessage();
                return Future.succeededFuture(info);
            });
    }
    
    /**
     * Discover all possible relationships between tables
     */
    private List<Relationship> discoverRelationships(List<TableInfo> tables) {
        List<Relationship> relationships = new ArrayList<>();
        
        // Check each pair of tables
        for (int i = 0; i < tables.size(); i++) {
            for (int j = i + 1; j < tables.size(); j++) {
                TableInfo table1 = tables.get(i);
                TableInfo table2 = tables.get(j);
                
                if (table1.error == null && table2.error == null) {
                    // Find relationships in both directions
                    relationships.addAll(findRelationshipsBetween(table1, table2));
                    relationships.addAll(findRelationshipsBetween(table2, table1));
                }
            }
        }
        
        // Score and sort relationships by confidence
        for (Relationship rel : relationships) {
            rel.confidence = calculateConfidence(rel);
        }
        
        relationships.sort((a, b) -> Double.compare(b.confidence, a.confidence));
        
        return relationships;
    }
    
    /**
     * Find relationships from table1 to table2
     */
    private List<Relationship> findRelationshipsBetween(TableInfo table1, TableInfo table2) {
        List<Relationship> relationships = new ArrayList<>();
        
        // 1. Check declared foreign keys
        for (int i = 0; i < table1.foreignKeys.size(); i++) {
            JsonObject fk = table1.foreignKeys.getJsonObject(i);
            if (fk.getString("referencedTable", "").equalsIgnoreCase(table2.tableName)) {
                Relationship rel = new Relationship();
                rel.fromTable = table1.tableName;
                rel.fromColumn = fk.getString("column");
                rel.toTable = table2.tableName;
                rel.toColumn = fk.getString("referencedColumn");
                rel.type = "foreign_key";
                rel.isDeclared = true;
                relationships.add(rel);
            }
        }
        
        // 2. Infer by naming patterns (table_id pattern)
        String expectedFkName = table2.tableName.toLowerCase() + "_id";
        String expectedFkName2 = table2.tableName.toLowerCase().replaceAll("s$", "") + "_id"; // Remove plural
        
        for (String colName : table1.columnMap.keySet()) {
            String lowerCol = colName.toLowerCase();
            
            if (lowerCol.equals(expectedFkName) || lowerCol.equals(expectedFkName2)) {
                // Check if table2 has an ID column
                if (table2.primaryKey != null || table2.columnMap.containsKey("ID")) {
                    Relationship rel = new Relationship();
                    rel.fromTable = table1.tableName;
                    rel.fromColumn = colName;
                    rel.toTable = table2.tableName;
                    rel.toColumn = table2.primaryKey != null ? table2.primaryKey : "ID";
                    rel.type = "inferred_by_name";
                    rel.isDeclared = false;
                    relationships.add(rel);
                }
            }
        }
        
        // 3. Infer by matching column names and types
        for (Map.Entry<String, String> col1 : table1.columnMap.entrySet()) {
            String colName1 = col1.getKey();
            String colType1 = col1.getValue();
            
            // Skip if already found as FK
            boolean alreadyFound = relationships.stream()
                .anyMatch(r -> r.fromColumn.equalsIgnoreCase(colName1));
            
            if (!alreadyFound && isLikelyForeignKey(colName1, colType1)) {
                // Look for matching column in table2
                for (Map.Entry<String, String> col2 : table2.columnMap.entrySet()) {
                    String colName2 = col2.getKey();
                    String colType2 = col2.getValue();
                    
                    if (isPossibleMatch(colName1, colType1, colName2, colType2, 
                                       table1.tableName, table2.tableName)) {
                        Relationship rel = new Relationship();
                        rel.fromTable = table1.tableName;
                        rel.fromColumn = colName1;
                        rel.toTable = table2.tableName;
                        rel.toColumn = colName2;
                        rel.type = "inferred_by_pattern";
                        rel.isDeclared = false;
                        relationships.add(rel);
                    }
                }
            }
        }
        
        return relationships;
    }
    
    /**
     * Check if a column is likely a foreign key based on name and type
     */
    private boolean isLikelyForeignKey(String colName, String colType) {
        String lower = colName.toLowerCase();
        
        // Common FK patterns
        if (lower.endsWith("_id") || lower.endsWith("_key") || 
            lower.endsWith("_code") || lower.endsWith("_ref")) {
            return true;
        }
        
        // Type-based inference
        if (colType.contains("NUMBER") || colType.contains("INT")) {
            if (lower.contains("id") || lower.contains("key") || 
                lower.contains("ref") || lower.contains("parent")) {
                return true;
            }
        }
        
        return false;
    }
    
    /**
     * Check if two columns could be related
     */
    private boolean isPossibleMatch(String col1Name, String col1Type,
                                  String col2Name, String col2Type,
                                  String table1, String table2) {
        // Types must be compatible
        if (!areTypesCompatible(col1Type, col2Type)) {
            return false;
        }
        
        String lower1 = col1Name.toLowerCase();
        String lower2 = col2Name.toLowerCase();
        
        // Direct match
        if (lower1.equals(lower2)) {
            return true;
        }
        
        // Table prefix match (e.g., CUSTOMER_ID matches ID in CUSTOMER table)
        String tablePrefix = table2.toLowerCase().replaceAll("s$", ""); // Remove plural
        if (lower1.startsWith(tablePrefix + "_") && 
            lower1.substring(tablePrefix.length() + 1).equals(lower2)) {
            return true;
        }
        
        // Common ID patterns
        if (lower2.equals("id") && lower1.endsWith("_id")) {
            return true;
        }
        
        return false;
    }
    
    /**
     * Check if two column types are compatible for joining
     */
    private boolean areTypesCompatible(String type1, String type2) {
        // Normalize types
        String norm1 = normalizeType(type1);
        String norm2 = normalizeType(type2);
        
        // Exact match
        if (norm1.equals(norm2)) {
            return true;
        }
        
        // Both numeric
        if (isNumericType(norm1) && isNumericType(norm2)) {
            return true;
        }
        
        // Both string
        if (isStringType(norm1) && isStringType(norm2)) {
            return true;
        }
        
        return false;
    }
    
    /**
     * Normalize data type for comparison
     */
    private String normalizeType(String type) {
        return type.toUpperCase().replaceAll("\\(.*\\)", "").trim();
    }
    
    /**
     * Check if type is numeric
     */
    private boolean isNumericType(String type) {
        return type.contains("NUMBER") || type.contains("INT") || 
               type.contains("DECIMAL") || type.contains("FLOAT");
    }
    
    /**
     * Check if type is string
     */
    private boolean isStringType(String type) {
        return type.contains("VARCHAR") || type.contains("CHAR") || 
               type.contains("TEXT") || type.contains("STRING");
    }
    
    /**
     * Calculate confidence score for a relationship
     */
    private double calculateConfidence(Relationship rel) {
        double confidence = 0.0;
        
        // Declared FK has highest confidence
        if (rel.isDeclared) {
            confidence = 1.0;
        } else {
            switch (rel.type) {
                case "inferred_by_name":
                    confidence = 0.8; // Strong naming pattern
                    break;
                case "inferred_by_pattern":
                    confidence = 0.6; // Pattern matching
                    break;
                default:
                    confidence = 0.5;
            }
            
            // Boost for exact column name match
            if (rel.fromColumn.equalsIgnoreCase(rel.toColumn)) {
                confidence += 0.1;
            }
            
            // Boost for primary key reference
            if (rel.toColumn.equalsIgnoreCase("ID") || 
                rel.toColumn.contains("_ID")) {
                confidence += 0.1;
            }
        }
        
        return Math.min(1.0, confidence);
    }
    
    /**
     * Find optimal join paths based on query needs
     */
    private List<JoinPath> findJoinPaths(List<Relationship> relationships, 
                                       JsonObject queryAnalysis) {
        List<JoinPath> paths = new ArrayList<>();
        
        // Group relationships by table pairs
        Map<String, List<Relationship>> relsByPair = new HashMap<>();
        for (Relationship rel : relationships) {
            String key = rel.fromTable + "->" + rel.toTable;
            relsByPair.computeIfAbsent(key, k -> new ArrayList<>()).add(rel);
        }
        
        // Create join paths for each table pair
        for (Map.Entry<String, List<Relationship>> entry : relsByPair.entrySet()) {
            List<Relationship> rels = entry.getValue();
            
            // Choose best relationship for this pair
            Relationship best = rels.get(0); // Already sorted by confidence
            
            JoinPath path = new JoinPath();
            path.fromTable = best.fromTable;
            path.toTable = best.toTable;
            path.joinCondition = String.format("%s.%s = %s.%s",
                best.fromTable, best.fromColumn,
                best.toTable, best.toColumn);
            path.confidence = best.confidence;
            path.relationship = best;
            
            paths.add(path);
        }
        
        return paths;
    }
    
    /**
     * Convert relationships to JSON
     */
    private JsonArray relationshipsToJson(List<Relationship> relationships) {
        JsonArray json = new JsonArray();
        
        for (Relationship rel : relationships) {
            json.add(new JsonObject()
                .put("from_table", rel.fromTable)
                .put("from_column", rel.fromColumn)
                .put("to_table", rel.toTable)
                .put("to_column", rel.toColumn)
                .put("type", rel.type)
                .put("is_declared", rel.isDeclared)
                .put("confidence", rel.confidence));
        }
        
        return json;
    }
    
    /**
     * Convert join paths to JSON
     */
    private JsonArray joinPathsToJson(List<JoinPath> paths) {
        JsonArray json = new JsonArray();
        
        for (JoinPath path : paths) {
            json.add(new JsonObject()
                .put("from_table", path.fromTable)
                .put("to_table", path.toTable)
                .put("join_condition", path.joinCondition)
                .put("confidence", path.confidence)
                .put("join_type", "INNER")); // Default to inner join
        }
        
        return json;
    }
    
    /**
     * Gather statistics about discovered relationships
     */
    private JsonObject gatherStatistics(List<Relationship> relationships, 
                                      List<JoinPath> joinPaths) {
        int declaredCount = 0;
        int inferredCount = 0;
        Set<String> connectedTables = new HashSet<>();
        
        for (Relationship rel : relationships) {
            if (rel.isDeclared) {
                declaredCount++;
            } else {
                inferredCount++;
            }
            connectedTables.add(rel.fromTable);
            connectedTables.add(rel.toTable);
        }
        
        return new JsonObject()
            .put("total_relationships", relationships.size())
            .put("declared_fks", declaredCount)
            .put("inferred_relationships", inferredCount)
            .put("join_paths", joinPaths.size())
            .put("connected_tables", connectedTables.size());
    }
    
    /**
     * Helper class to store table information
     */
    private static class TableInfo {
        String tableName;
        JsonArray columns = new JsonArray();
        JsonArray foreignKeys = new JsonArray();
        String primaryKey;
        Map<String, String> columnMap = new HashMap<>(); // NAME -> TYPE
        String error;
    }
    
    /**
     * Helper class to represent a relationship
     */
    private static class Relationship {
        String fromTable;
        String fromColumn;
        String toTable;
        String toColumn;
        String type; // foreign_key, inferred_by_name, inferred_by_pattern
        boolean isDeclared;
        double confidence;
    }
    
    /**
     * Helper class to represent a join path
     */
    private static class JoinPath {
        String fromTable;
        String toTable;
        String joinCondition;
        double confidence;
        Relationship relationship;
    }
}
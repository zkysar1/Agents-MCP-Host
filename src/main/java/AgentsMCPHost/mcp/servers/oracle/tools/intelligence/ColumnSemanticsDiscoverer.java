package AgentsMCPHost.mcp.servers.oracle.tools.intelligence;

import AgentsMCPHost.mcp.servers.oracle.utils.OracleConnectionManager;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.*;

/**
 * ColumnSemanticsDiscoverer - Intelligently discovers what columns contain based on the query.
 * 
 * This tool analyzes table schemas and sample data to understand:
 * - Which columns contain the data types needed for the query
 * - Semantic meaning of columns beyond their names
 * - Data patterns and formats within columns
 * - Which columns are likely identifiers, measures, or dimensions
 * 
 * Works with ANY dataset by analyzing actual data patterns.
 */
public class ColumnSemanticsDiscoverer {
    
    private final OracleConnectionManager oracleManager;
    
    public ColumnSemanticsDiscoverer(OracleConnectionManager oracleManager) {
        this.oracleManager = oracleManager;
    }
    
    /**
     * Discover column semantics for given tables based on query requirements
     */
    public Future<JsonObject> discoverSemantics(JsonArray tables, JsonObject queryAnalysis) {
        if (tables == null || tables.isEmpty()) {
            return Future.succeededFuture(new JsonObject()
                .put("error", "No tables provided")
                .put("discoveries", new JsonArray()));
        }
        
        // Extract what we're looking for from the query analysis
        JsonArray entities = queryAnalysis.getJsonArray("entities", new JsonArray());
        JsonArray attributes = queryAnalysis.getJsonArray("attributes", new JsonArray());
        JsonArray aggregations = queryAnalysis.getJsonArray("aggregations", new JsonArray());
        String intent = queryAnalysis.getString("intent", "query");
        
        // Analyze each table
        List<Future<JsonObject>> analysisFutures = new ArrayList<>();
        
        for (int i = 0; i < tables.size() && i < IntelligenceConfig.MAX_TABLES_TO_ANALYZE; i++) {
            String tableName = tables.getString(i);
            if (tableName != null && !tableName.trim().isEmpty()) {
                analysisFutures.add(analyzeTableSemantics(tableName, entities, attributes, aggregations, intent));
            }
        }
        
        return Future.all(analysisFutures)
            .map(compositeFuture -> {
                JsonArray discoveries = new JsonArray();
                for (Future<JsonObject> future : analysisFutures) {
                    if (future.succeeded() && future.result() != null) {
                        discoveries.add(future.result());
                    }
                }
                
                return new JsonObject()
                    .put("discoveries", discoveries)
                    .put("summary", summarizeDiscoveries(discoveries, queryAnalysis));
            });
    }
    
    /**
     * Analyze semantics of a single table
     */
    private Future<JsonObject> analyzeTableSemantics(String tableName, JsonArray entities, 
                                                     JsonArray attributes, JsonArray aggregations,
                                                     String intent) {
        Promise<JsonObject> promise = Promise.promise();
        
        // Get table metadata and sample data (timeouts handled at transport level)
        Future<JsonObject> metadataFuture = oracleManager.getTableMetadata(tableName);
        Future<JsonArray> sampleDataFuture = oracleManager.executeQuery(
            "SELECT * FROM " + tableName + " WHERE ROWNUM <= " + IntelligenceConfig.DEFAULT_SAMPLE_SIZE
        );
        
        Future.all(metadataFuture, sampleDataFuture)
            .onSuccess(result -> {
                JsonObject metadata = metadataFuture.result();
                JsonArray sampleData = sampleDataFuture.result();
                
                JsonArray columns = metadata.getJsonArray("columns", new JsonArray());
                JsonObject tableAnalysis = new JsonObject()
                    .put("table", tableName)
                    .put("columns", new JsonArray());
                
                // Analyze each column
                for (int i = 0; i < columns.size(); i++) {
                    JsonObject column = columns.getJsonObject(i);
                    JsonObject columnAnalysis = analyzeColumn(column, sampleData, 
                                                            entities, attributes, 
                                                            aggregations, intent);
                    tableAnalysis.getJsonArray("columns").add(columnAnalysis);
                }
                
                // Calculate table relevance score
                double relevanceScore = calculateTableRelevance(tableAnalysis, entities, attributes);
                tableAnalysis.put("relevance_score", relevanceScore);
                
                promise.complete(tableAnalysis);
            })
            .onFailure(err -> {
                promise.complete(new JsonObject()
                    .put("table", tableName)
                    .put("error", err.getMessage())
                    .put("relevance_score", 0.0));
            });
        
        return promise.future();
    }
    
    /**
     * Analyze a single column's semantics
     */
    private JsonObject analyzeColumn(JsonObject columnMeta, JsonArray sampleData,
                                   JsonArray entities, JsonArray attributes,
                                   JsonArray aggregations, String intent) {
        // Defensive null checks
        if (columnMeta == null) {
            return new JsonObject().put("error", "No column metadata provided");
        }
        
        String columnName = columnMeta.getString("name", "");
        String columnType = columnMeta.getString("type", "UNKNOWN");
        
        if (columnName.isEmpty()) {
            return new JsonObject().put("error", "Column has no name");
        }
        
        JsonObject analysis = new JsonObject()
            .put("name", columnName)
            .put("type", columnType)
            .put("nullable", columnMeta.getBoolean("nullable", true));
        
        // Analyze data patterns from samples
        JsonArray sampleValues = new JsonArray();
        Set<String> uniqueValues = new HashSet<>();
        boolean hasNulls = false;
        boolean isNumeric = isNumericType(columnType);
        boolean isDate = isDateType(columnType);
        
        // Extract sample values for this column
        for (int i = 0; i < sampleData.size(); i++) {
            JsonObject row = sampleData.getJsonObject(i);
            if (row == null) continue;
            
            Object value = row.getValue(columnName);
            
            if (value == null) {
                hasNulls = true;
            } else {
                String strValue = value.toString();
                uniqueValues.add(strValue);
                if (i < 5) { // Keep first 5 samples
                    sampleValues.add(strValue);
                }
            }
        }
        
        analysis.put("sample_values", sampleValues);
        analysis.put("has_nulls", hasNulls);
        analysis.put("unique_count", uniqueValues.size());
        analysis.put("selectivity", (double) uniqueValues.size() / Math.max(1, sampleData.size()));
        
        // Determine semantic type
        String semanticType = inferSemanticType(columnName, columnType, uniqueValues, 
                                               analysis.getDouble("selectivity"));
        analysis.put("semantic_type", semanticType);
        
        // Check relevance to query
        double relevance = calculateColumnRelevance(columnName, semanticType, 
                                                   entities, attributes, aggregations, intent);
        analysis.put("relevance", relevance);
        
        // Suggest usage based on type and intent
        JsonArray suggestedUsage = suggestColumnUsage(semanticType, columnType, intent, relevance);
        analysis.put("suggested_usage", suggestedUsage);
        
        return analysis;
    }
    
    /**
     * Infer semantic type of column based on name, type, and data
     */
    private String inferSemanticType(String columnName, String dataType, 
                                    Set<String> uniqueValues, double selectivity) {
        String lowerName = columnName.toLowerCase();
        
        // Check for ID columns
        if (lowerName.endsWith("_id") || lowerName.equals("id") || 
            lowerName.endsWith("_key") || lowerName.endsWith("_pk")) {
            return "identifier";
        }
        
        // Check for name/description columns
        if (lowerName.contains("name") || lowerName.contains("desc") || 
            lowerName.contains("title") || lowerName.contains("label")) {
            return "descriptor";
        }
        
        // Check for date/time columns
        if (isDateType(dataType) || lowerName.contains("date") || 
            lowerName.contains("time") || lowerName.contains("created") ||
            lowerName.contains("updated") || lowerName.contains("modified")) {
            return "temporal";
        }
        
        // Check for status/type columns (low cardinality)
        if ((lowerName.contains("status") || lowerName.contains("type") || 
             lowerName.contains("category") || lowerName.contains("class")) &&
            selectivity < 0.1) {
            return "category";
        }
        
        // Check for amount/quantity columns
        if (isNumericType(dataType)) {
            if (lowerName.contains("amount") || lowerName.contains("price") || 
                lowerName.contains("cost") || lowerName.contains("value") ||
                lowerName.contains("total") || lowerName.contains("sum")) {
                return "monetary";
            }
            if (lowerName.contains("count") || lowerName.contains("qty") || 
                lowerName.contains("quantity") || lowerName.contains("number")) {
                return "quantity";
            }
            if (lowerName.contains("rate") || lowerName.contains("percent") || 
                lowerName.contains("ratio")) {
                return "percentage";
            }
            // High selectivity numeric = likely a measure
            if (selectivity > 0.8) {
                return "measure";
            }
        }
        
        // Check for boolean/flag columns
        if (uniqueValues.size() == 2 || lowerName.startsWith("is_") || 
            lowerName.startsWith("has_") || lowerName.contains("flag")) {
            return "boolean";
        }
        
        // Check for code columns (short strings with pattern)
        if (dataType.contains("CHAR") && uniqueValues.size() > 0) {
            boolean allSameLength = true;
            int firstLength = uniqueValues.iterator().next().length();
            for (String val : uniqueValues) {
                if (val.length() != firstLength) {
                    allSameLength = false;
                    break;
                }
            }
            if (allSameLength && firstLength < 10) {
                return "code";
            }
        }
        
        // Default based on selectivity
        if (selectivity < 0.1) {
            return "dimension"; // Low cardinality = good for grouping
        } else if (selectivity > 0.9) {
            return "unique_text"; // High cardinality text
        }
        
        return "general";
    }
    
    /**
     * Calculate how relevant this column is to the query
     */
    private double calculateColumnRelevance(String columnName, String semanticType,
                                          JsonArray entities, JsonArray attributes,
                                          JsonArray aggregations, String intent) {
        double score = 0.0;
        String lowerName = columnName.toLowerCase().replace("_", " ");
        
        // Check against entities
        for (int i = 0; i < entities.size(); i++) {
            String entity = entities.getString(i).toLowerCase();
            if (lowerName.contains(entity) || entity.contains(lowerName)) {
                score += 0.4;
            }
        }
        
        // Check against attributes
        for (int i = 0; i < attributes.size(); i++) {
            String attr = attributes.getString(i).toLowerCase();
            if (lowerName.contains(attr) || attr.contains(lowerName)) {
                score += 0.3;
            }
        }
        
        // Check semantic type relevance to intent
        if (intent.contains("count") && semanticType.equals("identifier")) {
            score += 0.2;
        } else if (intent.contains("sum") && (semanticType.equals("monetary") || 
                                              semanticType.equals("measure"))) {
            score += 0.3;
        } else if (intent.contains("average") && semanticType.equals("measure")) {
            score += 0.3;
        } else if (intent.contains("group") && semanticType.equals("dimension")) {
            score += 0.2;
        }
        
        // Check aggregation needs
        for (int i = 0; i < aggregations.size(); i++) {
            JsonObject agg = aggregations.getJsonObject(i);
            if (agg != null) {
                String aggType = agg.getString("type", "");
                if (aggType.equals("sum") && semanticType.equals("monetary")) {
                    score += 0.2;
                } else if (aggType.equals("count") && semanticType.equals("identifier")) {
                    score += 0.2;
                }
            }
        }
        
        return Math.min(1.0, score);
    }
    
    /**
     * Suggest how this column should be used in the query
     */
    private JsonArray suggestColumnUsage(String semanticType, String dataType, 
                                       String intent, double relevance) {
        JsonArray usage = new JsonArray();
        
        switch (semanticType) {
            case "identifier":
                usage.add("COUNT_DISTINCT");
                if (intent.contains("list")) usage.add("SELECT");
                break;
                
            case "descriptor":
                usage.add("SELECT");
                usage.add("DISPLAY");
                if (relevance > 0.5) usage.add("SEARCH");
                break;
                
            case "temporal":
                usage.add("FILTER_DATE");
                usage.add("GROUP_BY_PERIOD");
                if (intent.contains("recent")) usage.add("ORDER_BY_DESC");
                break;
                
            case "category":
            case "dimension":
                usage.add("GROUP_BY");
                usage.add("FILTER");
                if (relevance > 0.3) usage.add("PIVOT");
                break;
                
            case "monetary":
            case "measure":
                usage.add("SUM");
                usage.add("AVG");
                usage.add("MIN_MAX");
                if (intent.contains("compare")) usage.add("COMPARE");
                break;
                
            case "quantity":
                usage.add("SUM");
                usage.add("COUNT");
                break;
                
            case "boolean":
                usage.add("FILTER");
                usage.add("COUNT_IF");
                break;
                
            case "code":
                usage.add("FILTER");
                usage.add("GROUP_BY");
                break;
                
            default:
                if (relevance > 0.5) {
                    usage.add("SELECT");
                    if (isNumericType(dataType)) usage.add("AGGREGATE");
                }
        }
        
        return usage;
    }
    
    /**
     * Calculate overall table relevance
     */
    private double calculateTableRelevance(JsonObject tableAnalysis, 
                                         JsonArray entities, JsonArray attributes) {
        JsonArray columns = tableAnalysis.getJsonArray("columns", new JsonArray());
        
        double maxRelevance = 0.0;
        double avgRelevance = 0.0;
        int relevantColumns = 0;
        
        for (int i = 0; i < columns.size(); i++) {
            JsonObject col = columns.getJsonObject(i);
            double relevance = col.getDouble("relevance", 0.0);
            
            maxRelevance = Math.max(maxRelevance, relevance);
            avgRelevance += relevance;
            
            if (relevance > 0.3) {
                relevantColumns++;
            }
        }
        
        if (columns.size() > 0) {
            avgRelevance /= columns.size();
        }
        
        // Weighted score: max relevance + average + column count factor
        return (maxRelevance * 0.5) + (avgRelevance * 0.3) + 
               (Math.min(relevantColumns / 5.0, 1.0) * 0.2);
    }
    
    /**
     * Summarize discoveries across all tables
     */
    private JsonObject summarizeDiscoveries(JsonArray discoveries, JsonObject queryAnalysis) {
        JsonObject summary = new JsonObject();
        
        // Find most relevant table
        JsonObject bestTable = null;
        double bestScore = 0.0;
        
        // Collect all relevant columns
        JsonArray relevantColumns = new JsonArray();
        
        for (int i = 0; i < discoveries.size(); i++) {
            JsonObject table = discoveries.getJsonObject(i);
            double score = table.getDouble("relevance_score", 0.0);
            
            if (score > bestScore) {
                bestScore = score;
                bestTable = table;
            }
            
            // Collect relevant columns
            JsonArray columns = table.getJsonArray("columns", new JsonArray());
            for (int j = 0; j < columns.size(); j++) {
                JsonObject col = columns.getJsonObject(j);
                if (col.getDouble("relevance", 0.0) > 0.3) {
                    relevantColumns.add(new JsonObject()
                        .put("table", table.getString("table"))
                        .put("column", col.getString("name"))
                        .put("type", col.getString("semantic_type"))
                        .put("relevance", col.getDouble("relevance"))
                        .put("usage", col.getJsonArray("suggested_usage")));
                }
            }
        }
        
        summary.put("best_table", bestTable != null ? bestTable.getString("table") : null);
        summary.put("best_score", bestScore);
        summary.put("relevant_columns", relevantColumns);
        summary.put("total_tables_analyzed", discoveries.size());
        
        return summary;
    }
    
    /**
     * Check if data type is numeric
     */
    private boolean isNumericType(String dataType) {
        String upper = dataType.toUpperCase();
        return upper.contains("NUMBER") || upper.contains("INT") || 
               upper.contains("DECIMAL") || upper.contains("FLOAT") ||
               upper.contains("DOUBLE") || upper.contains("NUMERIC");
    }
    
    /**
     * Check if data type is date/time
     */
    private boolean isDateType(String dataType) {
        String upper = dataType.toUpperCase();
        return upper.contains("DATE") || upper.contains("TIME") || 
               upper.contains("TIMESTAMP");
    }
}
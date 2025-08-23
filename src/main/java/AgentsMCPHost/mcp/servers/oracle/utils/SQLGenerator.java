package AgentsMCPHost.mcp.servers.oracle.utils;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

/**
 * SQLGenerator - Converts natural language queries to SQL statements.
 * Handles JOIN generation, WHERE clauses, and enumeration translations.
 * 
 * Supports various query types:
 * - Simple SELECT queries
 * - JOIN queries based on foreign keys
 * - Aggregation queries (COUNT, SUM, AVG)
 * - Time-based queries with date filtering
 * - Sorting and limiting results
 */
public class SQLGenerator {
    
    // Query intent patterns
    private static final Pattern AGGREGATION_PATTERN = Pattern.compile(
        "\\b(count|sum|average|avg|total|max|min|mean)\\b", Pattern.CASE_INSENSITIVE);
    private static final Pattern TIME_PATTERN = Pattern.compile(
        "\\b(today|yesterday|this week|last week|this month|last month|this year|last year)\\b", 
        Pattern.CASE_INSENSITIVE);
    private static final Pattern COMPARISON_PATTERN = Pattern.compile(
        "\\b(greater than|less than|more than|fewer than|between|above|below|over|under)\\b", 
        Pattern.CASE_INSENSITIVE);
    private static final Pattern SORT_PATTERN = Pattern.compile(
        "\\b(top|bottom|highest|lowest|most|least|best|worst|first|last)\\s+(\\d+)?\\b", 
        Pattern.CASE_INSENSITIVE);
    
    // Common business term mappings
    private static final Map<String, String> TERM_MAPPINGS = new HashMap<>();
    static {
        // Common business terms to column mappings
        TERM_MAPPINGS.put("customer", "customer_id");
        TERM_MAPPINGS.put("order", "order_id");
        TERM_MAPPINGS.put("product", "product_id");
        TERM_MAPPINGS.put("amount", "amount");
        TERM_MAPPINGS.put("price", "price");
        TERM_MAPPINGS.put("quantity", "qty");
        TERM_MAPPINGS.put("date", "created_date");
        TERM_MAPPINGS.put("status", "status_id");
        TERM_MAPPINGS.put("type", "type_id");
        TERM_MAPPINGS.put("category", "category_id");
    }
    
    private final OracleConnectionManager oracleManager;
    private final EnumerationMapper enumMapper;
    
    // Cache for table relationships
    private final Map<String, JsonArray> relationshipCache = new ConcurrentHashMap<>();
    
    public SQLGenerator() {
        this.oracleManager = OracleConnectionManager.getInstance();
        this.enumMapper = EnumerationMapper.getInstance();
    }
    
    /**
     * Generate SQL from natural language query
     */
    public SQLGenerationResult generateSQL(String naturalQuery, JsonObject context) {
        SQLGenerationResult result = new SQLGenerationResult();
        
        try {
            // 1. Detect query intent
            QueryIntent intent = detectIntent(naturalQuery);
            result.intent = intent;
            
            // 2. Extract entities (tables, columns, values)
            QueryEntities entities = extractEntities(naturalQuery, context);
            result.entities = entities;
            
            // 3. Build SQL based on intent and entities
            String sql = buildSQL(intent, entities, naturalQuery);
            result.sql = sql;
            result.success = true;
            
            // 4. Add metadata
            result.requiresEnumTranslation = entities.hasEnumReferences;
            result.confidence = calculateConfidence(intent, entities);
            
        } catch (Exception e) {
            result.success = false;
            result.error = e.getMessage();
        }
        
        return result;
    }
    
    /**
     * Detect the intent of the query
     */
    private QueryIntent detectIntent(String query) {
        QueryIntent intent = new QueryIntent();
        
        if (AGGREGATION_PATTERN.matcher(query).find()) {
            intent.type = QueryType.AGGREGATION;
            intent.aggregationFunction = extractAggregationFunction(query);
        } else if (TIME_PATTERN.matcher(query).find()) {
            intent.type = QueryType.TIME_BASED;
            intent.timePeriod = extractTimePeriod(query);
        } else if (SORT_PATTERN.matcher(query).find()) {
            intent.type = QueryType.RANKING;
            intent.sortOrder = extractSortOrder(query);
            intent.limit = extractLimit(query);
        } else if (COMPARISON_PATTERN.matcher(query).find()) {
            intent.type = QueryType.COMPARISON;
            intent.comparisonOperator = extractComparisonOperator(query);
        } else {
            intent.type = QueryType.SIMPLE_SELECT;
        }
        
        return intent;
    }
    
    /**
     * Extract entities from the query
     */
    private QueryEntities extractEntities(String query, JsonObject context) {
        QueryEntities entities = new QueryEntities();
        
        // Get available tables from context
        JsonArray availableTables = context.getJsonArray("tables", new JsonArray());
        
        // Extract table references
        for (int i = 0; i < availableTables.size(); i++) {
            String tableName = availableTables.getString(i);
            if (query.toLowerCase().contains(tableName.toLowerCase().replace("_", " "))) {
                entities.tables.add(tableName);
            }
        }
        
        // Extract column references using term mappings
        for (Map.Entry<String, String> entry : TERM_MAPPINGS.entrySet()) {
            if (query.toLowerCase().contains(entry.getKey())) {
                entities.columns.add(entry.getValue());
            }
        }
        
        // Extract literal values (numbers, quoted strings)
        Pattern valuePattern = Pattern.compile("'([^']+)'|\"([^\"]+)\"|(\\d+)");
        Matcher matcher = valuePattern.matcher(query);
        while (matcher.find()) {
            String value = matcher.group(1) != null ? matcher.group(1) :
                          matcher.group(2) != null ? matcher.group(2) : matcher.group(3);
            entities.values.add(value);
        }
        
        // Check for enum references
        entities.hasEnumReferences = query.toLowerCase().contains("status") ||
                                   query.toLowerCase().contains("type") ||
                                   query.toLowerCase().contains("category");
        
        return entities;
    }
    
    /**
     * Build SQL statement based on intent and entities
     */
    private String buildSQL(QueryIntent intent, QueryEntities entities, String originalQuery) {
        StringBuilder sql = new StringBuilder();
        
        switch (intent.type) {
            case AGGREGATION:
                return buildAggregationQuery(intent, entities);
                
            case TIME_BASED:
                return buildTimeBasedQuery(intent, entities);
                
            case RANKING:
                return buildRankingQuery(intent, entities);
                
            case COMPARISON:
                return buildComparisonQuery(intent, entities);
                
            case SIMPLE_SELECT:
            default:
                return buildSimpleSelectQuery(entities);
        }
    }
    
    /**
     * Build a simple SELECT query
     */
    private String buildSimpleSelectQuery(QueryEntities entities) {
        StringBuilder sql = new StringBuilder("SELECT ");
        
        // Select columns
        if (entities.columns.isEmpty()) {
            sql.append("*");
        } else {
            sql.append(String.join(", ", entities.columns));
        }
        
        // FROM clause
        if (!entities.tables.isEmpty()) {
            sql.append(" FROM ").append(entities.tables.get(0));
            
            // Add JOINs if multiple tables
            if (entities.tables.size() > 1) {
                for (int i = 1; i < entities.tables.size(); i++) {
                    String joinClause = generateJoinClause(
                        entities.tables.get(0), 
                        entities.tables.get(i)
                    );
                    sql.append(" ").append(joinClause);
                }
            }
        }
        
        // WHERE clause
        String whereClause = buildWhereClause(entities);
        if (!whereClause.isEmpty()) {
            sql.append(" WHERE ").append(whereClause);
        }
        
        // Default limit
        sql.append(" FETCH FIRST 100 ROWS ONLY");
        
        return sql.toString();
    }
    
    /**
     * Build an aggregation query
     */
    private String buildAggregationQuery(QueryIntent intent, QueryEntities entities) {
        StringBuilder sql = new StringBuilder("SELECT ");
        
        // Add aggregation function
        String aggFunc = intent.aggregationFunction.toUpperCase();
        String aggColumn = entities.columns.isEmpty() ? "*" : entities.columns.get(0);
        
        if (aggFunc.equals("COUNT")) {
            sql.append("COUNT(*)");
        } else {
            sql.append(aggFunc).append("(").append(aggColumn).append(")");
        }
        sql.append(" AS result");
        
        // FROM clause
        if (!entities.tables.isEmpty()) {
            sql.append(" FROM ").append(entities.tables.get(0));
        }
        
        // WHERE clause
        String whereClause = buildWhereClause(entities);
        if (!whereClause.isEmpty()) {
            sql.append(" WHERE ").append(whereClause);
        }
        
        return sql.toString();
    }
    
    /**
     * Build a time-based query
     */
    private String buildTimeBasedQuery(QueryIntent intent, QueryEntities entities) {
        StringBuilder sql = new StringBuilder("SELECT ");
        
        // Select columns
        if (entities.columns.isEmpty()) {
            sql.append("*");
        } else {
            sql.append(String.join(", ", entities.columns));
        }
        
        // FROM clause
        if (!entities.tables.isEmpty()) {
            sql.append(" FROM ").append(entities.tables.get(0));
        }
        
        // Add time-based WHERE clause
        String timeCondition = getTimeCondition(intent.timePeriod);
        sql.append(" WHERE ").append(timeCondition);
        
        // Add other conditions
        String whereClause = buildWhereClause(entities);
        if (!whereClause.isEmpty()) {
            sql.append(" AND ").append(whereClause);
        }
        
        return sql.toString();
    }
    
    /**
     * Build a ranking query
     */
    private String buildRankingQuery(QueryIntent intent, QueryEntities entities) {
        StringBuilder sql = new StringBuilder("SELECT ");
        
        // Select columns
        if (entities.columns.isEmpty()) {
            sql.append("*");
        } else {
            sql.append(String.join(", ", entities.columns));
        }
        
        // FROM clause
        if (!entities.tables.isEmpty()) {
            sql.append(" FROM ").append(entities.tables.get(0));
        }
        
        // WHERE clause
        String whereClause = buildWhereClause(entities);
        if (!whereClause.isEmpty()) {
            sql.append(" WHERE ").append(whereClause);
        }
        
        // ORDER BY clause
        if (!entities.columns.isEmpty()) {
            sql.append(" ORDER BY ").append(entities.columns.get(0));
            sql.append(intent.sortOrder.equals("DESC") ? " DESC" : " ASC");
        }
        
        // LIMIT clause
        int limit = intent.limit > 0 ? intent.limit : 10;
        sql.append(" FETCH FIRST ").append(limit).append(" ROWS ONLY");
        
        return sql.toString();
    }
    
    /**
     * Build a comparison query
     */
    private String buildComparisonQuery(QueryIntent intent, QueryEntities entities) {
        StringBuilder sql = new StringBuilder("SELECT ");
        
        // Select columns
        if (entities.columns.isEmpty()) {
            sql.append("*");
        } else {
            sql.append(String.join(", ", entities.columns));
        }
        
        // FROM clause
        if (!entities.tables.isEmpty()) {
            sql.append(" FROM ").append(entities.tables.get(0));
        }
        
        // WHERE clause with comparison
        sql.append(" WHERE ");
        if (!entities.columns.isEmpty() && !entities.values.isEmpty()) {
            sql.append(entities.columns.get(0));
            sql.append(" ").append(intent.comparisonOperator).append(" ");
            sql.append(entities.values.get(0));
        }
        
        return sql.toString();
    }
    
    /**
     * Build WHERE clause from entities
     */
    private String buildWhereClause(QueryEntities entities) {
        List<String> conditions = new ArrayList<>();
        
        // Add conditions based on values
        for (int i = 0; i < Math.min(entities.columns.size(), entities.values.size()); i++) {
            String column = entities.columns.get(i);
            String value = entities.values.get(i);
            
            // Check if value is numeric
            if (value.matches("\\d+")) {
                conditions.add(column + " = " + value);
            } else {
                conditions.add(column + " = '" + value + "'");
            }
        }
        
        return String.join(" AND ", conditions);
    }
    
    /**
     * Generate JOIN clause between two tables
     */
    private String generateJoinClause(String table1, String table2) {
        // Simple heuristic: look for common column patterns
        // In real implementation, this would use foreign key metadata
        
        // Common join patterns
        if (table1.equals("orders") && table2.equals("customers")) {
            return "JOIN customers ON orders.customer_id = customers.customer_id";
        }
        if (table1.equals("order_items") && table2.equals("products")) {
            return "JOIN products ON order_items.product_id = products.product_id";
        }
        
        // Generic pattern: assume table2_id exists in table1
        String joinColumn = table2.toLowerCase() + "_id";
        return String.format("JOIN %s ON %s.%s = %s.%s",
            table2, table1, joinColumn, table2, joinColumn);
    }
    
    /**
     * Get time condition for date filtering
     */
    private String getTimeCondition(String timePeriod) {
        String dateColumn = "created_date"; // Default date column
        
        switch (timePeriod.toLowerCase()) {
            case "today":
                return dateColumn + " = TRUNC(SYSDATE)";
            case "yesterday":
                return dateColumn + " = TRUNC(SYSDATE - 1)";
            case "this week":
                return dateColumn + " >= TRUNC(SYSDATE, 'IW')";
            case "last week":
                return dateColumn + " >= TRUNC(SYSDATE - 7, 'IW') AND " + 
                       dateColumn + " < TRUNC(SYSDATE, 'IW')";
            case "this month":
                return dateColumn + " >= TRUNC(SYSDATE, 'MM')";
            case "last month":
                return dateColumn + " >= ADD_MONTHS(TRUNC(SYSDATE, 'MM'), -1) AND " +
                       dateColumn + " < TRUNC(SYSDATE, 'MM')";
            case "this year":
                return dateColumn + " >= TRUNC(SYSDATE, 'YY')";
            case "last year":
                return dateColumn + " >= ADD_MONTHS(TRUNC(SYSDATE, 'YY'), -12) AND " +
                       dateColumn + " < TRUNC(SYSDATE, 'YY')";
            default:
                return "1=1"; // Always true
        }
    }
    
    /**
     * Extract aggregation function from query
     */
    private String extractAggregationFunction(String query) {
        if (query.toLowerCase().contains("count")) return "COUNT";
        if (query.toLowerCase().contains("sum") || query.toLowerCase().contains("total")) return "SUM";
        if (query.toLowerCase().contains("avg") || query.toLowerCase().contains("average")) return "AVG";
        if (query.toLowerCase().contains("max") || query.toLowerCase().contains("highest")) return "MAX";
        if (query.toLowerCase().contains("min") || query.toLowerCase().contains("lowest")) return "MIN";
        return "COUNT";
    }
    
    /**
     * Extract time period from query
     */
    private String extractTimePeriod(String query) {
        Matcher matcher = TIME_PATTERN.matcher(query);
        if (matcher.find()) {
            return matcher.group();
        }
        return "today";
    }
    
    /**
     * Extract sort order from query
     */
    private String extractSortOrder(String query) {
        if (query.toLowerCase().contains("top") || query.toLowerCase().contains("highest") ||
            query.toLowerCase().contains("most") || query.toLowerCase().contains("best")) {
            return "DESC";
        }
        return "ASC";
    }
    
    /**
     * Extract limit from query
     */
    private int extractLimit(String query) {
        Pattern numberPattern = Pattern.compile("\\b(\\d+)\\b");
        Matcher matcher = numberPattern.matcher(query);
        if (matcher.find()) {
            return Integer.parseInt(matcher.group(1));
        }
        return 10; // Default
    }
    
    /**
     * Extract comparison operator from query
     */
    private String extractComparisonOperator(String query) {
        if (query.toLowerCase().contains("greater than") || query.toLowerCase().contains("more than") ||
            query.toLowerCase().contains("above") || query.toLowerCase().contains("over")) {
            return ">";
        }
        if (query.toLowerCase().contains("less than") || query.toLowerCase().contains("fewer than") ||
            query.toLowerCase().contains("below") || query.toLowerCase().contains("under")) {
            return "<";
        }
        if (query.toLowerCase().contains("between")) {
            return "BETWEEN";
        }
        return "=";
    }
    
    /**
     * Calculate confidence score for the generated SQL
     */
    private double calculateConfidence(QueryIntent intent, QueryEntities entities) {
        double confidence = 0.5; // Base confidence
        
        // Increase confidence based on recognized elements
        if (!entities.tables.isEmpty()) confidence += 0.2;
        if (!entities.columns.isEmpty()) confidence += 0.15;
        if (intent.type != QueryType.SIMPLE_SELECT) confidence += 0.1;
        if (!entities.values.isEmpty()) confidence += 0.05;
        
        return Math.min(confidence, 1.0);
    }
    
    /**
     * Query intent types
     */
    public enum QueryType {
        SIMPLE_SELECT,
        AGGREGATION,
        TIME_BASED,
        RANKING,
        COMPARISON
    }
    
    /**
     * Query intent information
     */
    public static class QueryIntent {
        public QueryType type = QueryType.SIMPLE_SELECT;
        public String aggregationFunction;
        public String timePeriod;
        public String sortOrder;
        public int limit;
        public String comparisonOperator;
    }
    
    /**
     * Extracted entities from query
     */
    public static class QueryEntities {
        public List<String> tables = new ArrayList<>();
        public List<String> columns = new ArrayList<>();
        public List<String> values = new ArrayList<>();
        public boolean hasEnumReferences = false;
    }
    
    /**
     * SQL generation result
     */
    public static class SQLGenerationResult {
        public boolean success;
        public String sql;
        public QueryIntent intent;
        public QueryEntities entities;
        public boolean requiresEnumTranslation;
        public double confidence;
        public String error;
        
        public JsonObject toJson() {
            JsonObject json = new JsonObject()
                .put("success", success)
                .put("sql", sql)
                .put("requiresEnumTranslation", requiresEnumTranslation)
                .put("confidence", confidence);
            
            if (error != null) {
                json.put("error", error);
            }
            
            if (intent != null) {
                json.put("intent", new JsonObject()
                    .put("type", intent.type.toString())
                    .put("aggregationFunction", intent.aggregationFunction)
                    .put("timePeriod", intent.timePeriod)
                    .put("sortOrder", intent.sortOrder)
                    .put("limit", intent.limit));
            }
            
            if (entities != null) {
                json.put("entities", new JsonObject()
                    .put("tables", new JsonArray(entities.tables))
                    .put("columns", new JsonArray(entities.columns))
                    .put("values", new JsonArray(entities.values)));
            }
            
            return json;
        }
    }
}
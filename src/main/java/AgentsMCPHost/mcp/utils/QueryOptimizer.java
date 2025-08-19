package AgentsMCPHost.mcp.utils;

import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;

import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * QueryOptimizer - Validates, optimizes, and secures SQL queries.
 * 
 * Features:
 * - SQL injection prevention
 * - Query validation and syntax checking
 * - Read-only enforcement
 * - Query optimization suggestions
 * - Result size limiting
 * - Timeout management
 */
public class QueryOptimizer {
    
    // Security patterns
    private static final Pattern DANGEROUS_KEYWORDS = Pattern.compile(
        "\\b(DROP|DELETE|INSERT|UPDATE|ALTER|CREATE|TRUNCATE|EXEC|EXECUTE|GRANT|REVOKE)\\b",
        Pattern.CASE_INSENSITIVE
    );
    
    private static final Pattern SQL_INJECTION_PATTERNS = Pattern.compile(
        "(;|--|/\\*|\\*/|xp_|sp_|0x|\\bUNION\\b.*\\bSELECT\\b|\\bOR\\b.*=.*\\bOR\\b)",
        Pattern.CASE_INSENSITIVE
    );
    
    private static final Pattern VALID_SELECT = Pattern.compile(
        "^\\s*SELECT\\s+.+\\s+FROM\\s+.+",
        Pattern.CASE_INSENSITIVE | Pattern.DOTALL
    );
    
    // Query limits
    private static final int DEFAULT_ROW_LIMIT = 1000;
    private static final int MAX_ROW_LIMIT = 10000;
    private static final int DEFAULT_TIMEOUT_SECONDS = 30;
    private static final int MAX_JOINS = 5;
    
    // Common optimization patterns
    private static final Map<String, String> OPTIMIZATION_RULES = new HashMap<>();
    static {
        OPTIMIZATION_RULES.put("SELECT \\* FROM", "Consider selecting specific columns instead of *");
        OPTIMIZATION_RULES.put("NOT IN", "Consider using NOT EXISTS for better performance");
        OPTIMIZATION_RULES.put("OR", "Multiple ORs might benefit from UNION");
        OPTIMIZATION_RULES.put("LIKE '%", "Leading wildcard prevents index usage");
        OPTIMIZATION_RULES.put("DISTINCT", "Ensure DISTINCT is necessary, it can be expensive");
    }
    
    /**
     * Validate and optimize a SQL query
     */
    public ValidationResult validateAndOptimize(String sql, QueryOptions options) {
        ValidationResult result = new ValidationResult();
        result.originalQuery = sql;
        
        // 1. Security validation
        SecurityCheck security = checkSecurity(sql);
        if (!security.isSafe) {
            result.isValid = false;
            result.errors.add("Security violation: " + security.reason);
            return result;
        }
        
        // 2. Syntax validation
        SyntaxCheck syntax = checkSyntax(sql);
        if (!syntax.isValid) {
            result.isValid = false;
            result.errors.addAll(syntax.errors);
            return result;
        }
        
        // 3. Apply optimizations
        String optimizedSql = applyOptimizations(sql, options);
        result.optimizedQuery = optimizedSql;
        
        // 4. Add limit if not present
        if (!hasLimit(optimizedSql)) {
            int limit = options != null && options.maxRows > 0 ? 
                       Math.min(options.maxRows, MAX_ROW_LIMIT) : DEFAULT_ROW_LIMIT;
            optimizedSql = addLimit(optimizedSql, limit);
            result.optimizedQuery = optimizedSql;
            result.warnings.add("Added row limit of " + limit);
        }
        
        // 5. Generate optimization suggestions
        List<String> suggestions = generateSuggestions(sql);
        result.suggestions.addAll(suggestions);
        
        // 6. Estimate query cost
        QueryCost cost = estimateCost(optimizedSql);
        result.estimatedCost = cost;
        
        result.isValid = true;
        return result;
    }
    
    /**
     * Check query for security issues
     */
    private SecurityCheck checkSecurity(String sql) {
        SecurityCheck check = new SecurityCheck();
        
        // Check for dangerous keywords
        Matcher dangerousMatcher = DANGEROUS_KEYWORDS.matcher(sql);
        if (dangerousMatcher.find()) {
            check.isSafe = false;
            check.reason = "Query contains prohibited operation: " + dangerousMatcher.group();
            return check;
        }
        
        // Check for SQL injection patterns
        Matcher injectionMatcher = SQL_INJECTION_PATTERNS.matcher(sql);
        if (injectionMatcher.find()) {
            check.isSafe = false;
            check.reason = "Potential SQL injection detected";
            return check;
        }
        
        // Ensure it's a SELECT statement
        if (!sql.trim().toUpperCase().startsWith("SELECT")) {
            check.isSafe = false;
            check.reason = "Only SELECT statements are allowed";
            return check;
        }
        
        check.isSafe = true;
        return check;
    }
    
    /**
     * Check SQL syntax
     */
    private SyntaxCheck checkSyntax(String sql) {
        SyntaxCheck check = new SyntaxCheck();
        
        // Basic SELECT validation
        if (!VALID_SELECT.matcher(sql).find()) {
            check.isValid = false;
            check.errors.add("Invalid SELECT statement structure");
            return check;
        }
        
        // Check for balanced parentheses
        int openParens = 0;
        for (char c : sql.toCharArray()) {
            if (c == '(') openParens++;
            if (c == ')') openParens--;
            if (openParens < 0) {
                check.isValid = false;
                check.errors.add("Unbalanced parentheses");
                return check;
            }
        }
        if (openParens != 0) {
            check.isValid = false;
            check.errors.add("Unbalanced parentheses");
            return check;
        }
        
        // Check for required keywords in correct order
        String upperSql = sql.toUpperCase();
        int selectPos = upperSql.indexOf("SELECT");
        int fromPos = upperSql.indexOf("FROM");
        
        if (selectPos == -1 || fromPos == -1 || selectPos > fromPos) {
            check.isValid = false;
            check.errors.add("SELECT must come before FROM");
            return check;
        }
        
        // Check JOIN count
        int joinCount = countOccurrences(upperSql, "JOIN");
        if (joinCount > MAX_JOINS) {
            check.isValid = false;
            check.errors.add("Too many JOINs (max " + MAX_JOINS + ")");
            return check;
        }
        
        check.isValid = true;
        return check;
    }
    
    /**
     * Apply query optimizations
     */
    private String applyOptimizations(String sql, QueryOptions options) {
        String optimized = sql;
        
        // Remove unnecessary whitespace
        optimized = optimized.replaceAll("\\s+", " ").trim();
        
        // Add table aliases if not present
        optimized = addTableAliases(optimized);
        
        // Optimize WHERE clause
        optimized = optimizeWhereClause(optimized);
        
        // Add index hints if beneficial
        if (options != null && options.useIndexHints) {
            optimized = addIndexHints(optimized);
        }
        
        return optimized;
    }
    
    /**
     * Add table aliases for readability and performance
     */
    private String addTableAliases(String sql) {
        // Simple implementation - in production would be more sophisticated
        Pattern tablePattern = Pattern.compile("FROM\\s+(\\w+)(?!\\s+\\w+\\s+)", Pattern.CASE_INSENSITIVE);
        Matcher matcher = tablePattern.matcher(sql);
        
        if (matcher.find()) {
            String tableName = matcher.group(1);
            String alias = tableName.substring(0, 1).toLowerCase();
            sql = sql.replaceFirst("FROM\\s+" + tableName, "FROM " + tableName + " " + alias);
            
            // Update column references
            sql = sql.replaceAll("\\b" + tableName + "\\.", alias + ".");
        }
        
        return sql;
    }
    
    /**
     * Optimize WHERE clause
     */
    private String optimizeWhereClause(String sql) {
        // Convert IN with single value to =
        sql = sql.replaceAll("\\bIN\\s*\\(\\s*([^,)]+)\\s*\\)", "= $1");
        
        // Convert BETWEEN for single value
        Pattern betweenPattern = Pattern.compile("BETWEEN\\s+(\\w+)\\s+AND\\s+\\1", Pattern.CASE_INSENSITIVE);
        sql = betweenPattern.matcher(sql).replaceAll("= $1");
        
        return sql;
    }
    
    /**
     * Add index hints where beneficial
     */
    private String addIndexHints(String sql) {
        // This would require metadata about available indexes
        // Simplified implementation
        if (sql.contains("WHERE") && sql.contains("_id")) {
            // Assume _id columns are indexed
            sql = sql.replace("FROM", "FROM /*+ INDEX */");
        }
        return sql;
    }
    
    /**
     * Check if query has a limit
     */
    private boolean hasLimit(String sql) {
        String upper = sql.toUpperCase();
        return upper.contains("FETCH") || upper.contains("ROWNUM") || 
               upper.contains("LIMIT") || upper.contains("TOP");
    }
    
    /**
     * Add limit to query
     */
    private String addLimit(String sql, int limit) {
        // Oracle syntax
        if (!sql.toUpperCase().contains("ORDER BY")) {
            return sql + " FETCH FIRST " + limit + " ROWS ONLY";
        } else {
            // Insert before ORDER BY
            int orderByPos = sql.toUpperCase().indexOf("ORDER BY");
            return sql.substring(0, orderByPos) + 
                   "FETCH FIRST " + limit + " ROWS ONLY " +
                   sql.substring(orderByPos);
        }
    }
    
    /**
     * Generate optimization suggestions
     */
    private List<String> generateSuggestions(String sql) {
        List<String> suggestions = new ArrayList<>();
        
        for (Map.Entry<String, String> rule : OPTIMIZATION_RULES.entrySet()) {
            Pattern pattern = Pattern.compile(rule.getKey(), Pattern.CASE_INSENSITIVE);
            if (pattern.matcher(sql).find()) {
                suggestions.add(rule.getValue());
            }
        }
        
        // Check for missing WHERE clause
        if (!sql.toUpperCase().contains("WHERE")) {
            suggestions.add("Consider adding WHERE clause to filter results");
        }
        
        // Check for SELECT *
        if (sql.contains("*")) {
            suggestions.add("Select specific columns instead of * for better performance");
        }
        
        return suggestions;
    }
    
    /**
     * Estimate query cost
     */
    private QueryCost estimateCost(String sql) {
        QueryCost cost = new QueryCost();
        
        // Simple heuristic-based estimation
        String upper = sql.toUpperCase();
        
        // Base cost
        cost.estimatedRows = DEFAULT_ROW_LIMIT;
        cost.estimatedTimeMs = 100;
        
        // Adjust for JOINs
        int joins = countOccurrences(upper, "JOIN");
        cost.estimatedTimeMs += joins * 200;
        cost.complexity = joins > 2 ? "HIGH" : joins > 0 ? "MEDIUM" : "LOW";
        
        // Adjust for WHERE clause
        if (!upper.contains("WHERE")) {
            cost.estimatedRows *= 10;
            cost.estimatedTimeMs *= 5;
        }
        
        // Adjust for aggregations
        if (upper.contains("GROUP BY")) {
            cost.estimatedTimeMs += 300;
            cost.complexity = "HIGH";
        }
        
        // Adjust for DISTINCT
        if (upper.contains("DISTINCT")) {
            cost.estimatedTimeMs += 200;
        }
        
        return cost;
    }
    
    /**
     * Count occurrences of substring
     */
    private int countOccurrences(String str, String substr) {
        int count = 0;
        int idx = 0;
        while ((idx = str.indexOf(substr, idx)) != -1) {
            count++;
            idx += substr.length();
        }
        return count;
    }
    
    /**
     * Parse query to extract components
     */
    public QueryComponents parseQuery(String sql) {
        QueryComponents components = new QueryComponents();
        
        String upper = sql.toUpperCase();
        
        // Extract SELECT clause
        int selectStart = upper.indexOf("SELECT") + 6;
        int fromStart = upper.indexOf("FROM");
        if (selectStart > 5 && fromStart > selectStart) {
            components.selectClause = sql.substring(selectStart, fromStart).trim();
        }
        
        // Extract FROM clause
        int fromEnd = upper.indexOf("WHERE");
        if (fromEnd == -1) fromEnd = upper.indexOf("GROUP BY");
        if (fromEnd == -1) fromEnd = upper.indexOf("ORDER BY");
        if (fromEnd == -1) fromEnd = sql.length();
        
        if (fromStart > -1) {
            components.fromClause = sql.substring(fromStart + 4, fromEnd).trim();
        }
        
        // Extract WHERE clause
        int whereStart = upper.indexOf("WHERE");
        if (whereStart > -1) {
            int whereEnd = upper.indexOf("GROUP BY");
            if (whereEnd == -1) whereEnd = upper.indexOf("ORDER BY");
            if (whereEnd == -1) whereEnd = sql.length();
            components.whereClause = sql.substring(whereStart + 5, whereEnd).trim();
        }
        
        // Extract GROUP BY clause
        int groupByStart = upper.indexOf("GROUP BY");
        if (groupByStart > -1) {
            int groupByEnd = upper.indexOf("ORDER BY");
            if (groupByEnd == -1) groupByEnd = sql.length();
            components.groupByClause = sql.substring(groupByStart + 8, groupByEnd).trim();
        }
        
        // Extract ORDER BY clause
        int orderByStart = upper.indexOf("ORDER BY");
        if (orderByStart > -1) {
            components.orderByClause = sql.substring(orderByStart + 8).trim();
        }
        
        return components;
    }
    
    /**
     * Query validation result
     */
    public static class ValidationResult {
        public boolean isValid;
        public String originalQuery;
        public String optimizedQuery;
        public List<String> errors = new ArrayList<>();
        public List<String> warnings = new ArrayList<>();
        public List<String> suggestions = new ArrayList<>();
        public QueryCost estimatedCost;
        
        public JsonObject toJson() {
            return new JsonObject()
                .put("isValid", isValid)
                .put("originalQuery", originalQuery)
                .put("optimizedQuery", optimizedQuery)
                .put("errors", new JsonArray(errors))
                .put("warnings", new JsonArray(warnings))
                .put("suggestions", new JsonArray(suggestions))
                .put("estimatedCost", estimatedCost != null ? estimatedCost.toJson() : null);
        }
    }
    
    /**
     * Security check result
     */
    private static class SecurityCheck {
        boolean isSafe;
        String reason;
    }
    
    /**
     * Syntax check result
     */
    private static class SyntaxCheck {
        boolean isValid = true;
        List<String> errors = new ArrayList<>();
    }
    
    /**
     * Query cost estimation
     */
    public static class QueryCost {
        public int estimatedRows;
        public long estimatedTimeMs;
        public String complexity; // LOW, MEDIUM, HIGH
        
        public JsonObject toJson() {
            return new JsonObject()
                .put("estimatedRows", estimatedRows)
                .put("estimatedTimeMs", estimatedTimeMs)
                .put("complexity", complexity);
        }
    }
    
    /**
     * Query components
     */
    public static class QueryComponents {
        public String selectClause;
        public String fromClause;
        public String whereClause;
        public String groupByClause;
        public String orderByClause;
        
        public JsonObject toJson() {
            return new JsonObject()
                .put("select", selectClause)
                .put("from", fromClause)
                .put("where", whereClause)
                .put("groupBy", groupByClause)
                .put("orderBy", orderByClause);
        }
    }
    
    /**
     * Query execution options
     */
    public static class QueryOptions {
        public int maxRows = DEFAULT_ROW_LIMIT;
        public int timeoutSeconds = DEFAULT_TIMEOUT_SECONDS;
        public boolean useIndexHints = false;
        public boolean explainPlan = false;
        
        public static QueryOptions fromJson(JsonObject json) {
            QueryOptions options = new QueryOptions();
            if (json != null) {
                options.maxRows = json.getInteger("maxRows", DEFAULT_ROW_LIMIT);
                options.timeoutSeconds = json.getInteger("timeoutSeconds", DEFAULT_TIMEOUT_SECONDS);
                options.useIndexHints = json.getBoolean("useIndexHints", false);
                options.explainPlan = json.getBoolean("explainPlan", false);
            }
            return options;
        }
    }
}
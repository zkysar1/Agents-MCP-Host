package agents.director.mcp.servers;

import agents.director.services.OracleConnectionManager;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * SmartSQLOptimizer - Intelligently optimizes SQL queries using EXPLAIN PLAN.
 * 
 * Key features:
 * - Properly uses EXPLAIN PLAN with executeUpdate
 * - Calculates query complexity to skip optimization for simple queries
 * - Provides actionable optimization suggestions
 * - Handles errors gracefully
 */
public class SmartSQLOptimizer {
    
    private final OracleConnectionManager oracleManager;
    
    // Bounded LRU cache for execution plans
    private final Map<String, JsonObject> planCache;
    
    public SmartSQLOptimizer(OracleConnectionManager oracleManager) {
        this.oracleManager = oracleManager;
        
        // Create thread-safe bounded LRU cache
        this.planCache = Collections.synchronizedMap(
            new LinkedHashMap<String, JsonObject>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, JsonObject> eldest) {
                    return size() > IntelligenceConfig.MAX_CACHE_SIZE;
                }
            }
        );
    }
    
    /**
     * Optimize SQL with intelligent complexity checking
     */
    public Future<JsonObject> optimize(String sql, double complexityThreshold) {
        if (sql == null || sql.trim().isEmpty()) {
            return Future.succeededFuture(new JsonObject()
                .put("error", "No SQL provided")
                .put("optimized_sql", sql));
        }
        
        // Calculate query complexity
        double complexity = calculateComplexity(sql);
        
        // Skip optimization for simple queries
        if (complexity < complexityThreshold) {
            return Future.succeededFuture(new JsonObject()
                .put("optimized_sql", sql)
                .put("complexity", complexity)
                .put("skipped", true)
                .put("reason", "Query too simple to benefit from optimization"));
        }
        
        // For complex queries, get execution plan
        return getExecutionPlan(sql)
            .compose(plan -> analyzeAndOptimize(sql, plan, complexity));
    }
    
    /**
     * Calculate query complexity based on various factors
     */
    private double calculateComplexity(String sql) {
        String upperSql = sql.toUpperCase();
        double complexity = 0.0;
        
        // Base complexity for operation type
        if (upperSql.contains("SELECT")) complexity += 0.1;
        if (upperSql.contains("INSERT")) complexity += 0.2;
        if (upperSql.contains("UPDATE")) complexity += 0.3;
        if (upperSql.contains("DELETE")) complexity += 0.2;
        
        // Joins add significant complexity
        int joinCount = countOccurrences(upperSql, "JOIN");
        complexity += joinCount * 0.3;
        
        // Subqueries add complexity
        int subqueryCount = countOccurrences(upperSql, "SELECT") - 1; // -1 for main SELECT
        complexity += Math.max(0, subqueryCount) * 0.4;
        
        // Complex clauses
        if (upperSql.contains("GROUP BY")) complexity += 0.2;
        if (upperSql.contains("HAVING")) complexity += 0.2;
        if (upperSql.contains("ORDER BY")) complexity += 0.1;
        if (upperSql.contains("UNION")) complexity += 0.3;
        if (upperSql.contains("INTERSECT")) complexity += 0.3;
        if (upperSql.contains("MINUS")) complexity += 0.3;
        
        // Window functions
        if (upperSql.contains("OVER")) complexity += 0.4;
        if (upperSql.contains("PARTITION BY")) complexity += 0.2;
        
        // Complex WHERE conditions
        int andCount = countOccurrences(upperSql, " AND ");
        int orCount = countOccurrences(upperSql, " OR ");
        complexity += (andCount + orCount) * 0.05;
        
        // Functions and expressions
        if (upperSql.contains("CASE")) complexity += 0.15;
        if (upperSql.contains("DECODE")) complexity += 0.15;
        if (upperSql.contains("NVL")) complexity += 0.05;
        
        return Math.min(1.0, complexity); // Cap at 1.0
    }
    
    /**
     * Count occurrences of a substring
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
     * Get execution plan using proper Oracle method
     */
    private Future<JsonObject> getExecutionPlan(String sql) {
        // Check cache first
        String cacheKey = sql.hashCode() + "_plan";
        JsonObject cached = planCache.get(cacheKey);
        
        if (cached != null) {
            long cachedTime = cached.getLong("cached_at", 0L);
            if (System.currentTimeMillis() - cachedTime < IntelligenceConfig.PLAN_CACHE_TTL_MS) {
                return Future.succeededFuture(cached);
            } else {
                planCache.remove(cacheKey);
            }
        }
        
        // Generate a unique statement ID to avoid conflicts
        String statementId = "STMT_" + System.currentTimeMillis();
        
        // First check if PLAN_TABLE exists
        String checkPlanTableSql = 
            "SELECT COUNT(*) AS CNT FROM user_tables WHERE table_name = 'PLAN_TABLE'";
        
        return oracleManager.executeQuery(checkPlanTableSql)
            .compose(result -> {
                // executeQuery returns JsonArray, check if PLAN_TABLE exists
                if (result.isEmpty() || result.getJsonObject(0).getInteger("CNT", 0) == 0) {
                    // PLAN_TABLE doesn't exist, return error
                    return Future.succeededFuture(new JsonObject()
                        .put("error", "PLAN_TABLE does not exist. Run @?/rdbms/admin/utlxplan.sql to create it.")
                        .put("lines", new JsonArray()));
                }
                
                // PLAN_TABLE exists, proceed with EXPLAIN PLAN
                String explainSql = String.format(
                    "EXPLAIN PLAN SET STATEMENT_ID = '%s' FOR %s", 
                    statementId, sql
                );
                
                return oracleManager.executeUpdate(explainSql)
                    .compose(rowsAffected -> {
                        // Now fetch the plan using DBMS_XPLAN
                        String fetchPlanSql = String.format(
                            "SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY(NULL, '%s', 'TYPICAL'))",
                            statementId
                        );
                        return oracleManager.executeQuery(fetchPlanSql);
                    })
                    .map(planRows -> {
                        // Convert plan rows to readable format
                        JsonObject planResult = new JsonObject();
                        JsonArray planLines = new JsonArray();
                        
                        for (int i = 0; i < planRows.size(); i++) {
                            JsonObject row = planRows.getJsonObject(i);
                            String planLine = row.getString("PLAN_TABLE_OUTPUT");
                            if (planLine != null) {
                                planLines.add(planLine);
                            }
                        }
                        
                        planResult.put("lines", planLines);
                        planResult.put("raw", planRows);
                        planResult.put("cached_at", System.currentTimeMillis());
                        
                        // Cache the result
                        planCache.put(cacheKey, planResult);
                        
                        return planResult;
                    });
            })
            .recover(err -> {
                // If EXPLAIN PLAN fails, return error details
                return Future.succeededFuture(new JsonObject()
                    .put("error", "Failed to generate execution plan: " + err.getMessage())
                    .put("lines", new JsonArray()));
            });
    }
    
    /**
     * Analyze execution plan and suggest optimizations
     */
    private Future<JsonObject> analyzeAndOptimize(String sql, JsonObject plan, double complexity) {
        JsonObject result = new JsonObject()
            .put("original_sql", sql)
            .put("complexity", complexity);
        
        // Extract plan details
        JsonArray planLines = plan.getJsonArray("lines", new JsonArray());
        StringBuilder planText = new StringBuilder();
        
        boolean hasFullTableScan = false;
        boolean hasNestedLoops = false;
        boolean hasHighCost = false;
        
        for (int i = 0; i < planLines.size(); i++) {
            String line = planLines.getString(i);
            planText.append(line).append("\n");
            
            // Analyze plan for common issues
            if (line.contains("TABLE ACCESS FULL")) {
                hasFullTableScan = true;
            }
            if (line.contains("NESTED LOOPS")) {
                hasNestedLoops = true;
            }
            if (line.contains("Cost=") && extractCost(line) > 1000) {
                hasHighCost = true;
            }
        }
        
        result.put("execution_plan", planText.toString());
        
        // Generate optimization suggestions
        JsonArray suggestions = new JsonArray();
        
        if (hasFullTableScan) {
            suggestions.add("Consider adding indexes on columns used in WHERE clause to avoid full table scans");
        }
        
        if (hasNestedLoops && complexity > 0.6) {
            suggestions.add("Nested loops detected in complex query - consider using hash joins for large datasets");
        }
        
        if (hasHighCost) {
            suggestions.add("High cost operation detected - review query structure and consider breaking into smaller queries");
        }
        
        // Add general suggestions based on complexity
        if (complexity > 0.7) {
            suggestions.add("Complex query detected - consider using materialized views for frequently accessed data");
            suggestions.add("Review if all joins are necessary - eliminate redundant joins");
        }
        
        // Smart SQL rewriting (for now, return original - could be enhanced with LLM)
        result.put("optimized_sql", sql);
        result.put("suggestions", suggestions);
        result.put("has_issues", hasFullTableScan || hasNestedLoops || hasHighCost);
        
        return Future.succeededFuture(result);
    }
    
    /**
     * Extract cost value from execution plan line
     */
    private int extractCost(String line) {
        try {
            int costIdx = line.indexOf("Cost=");
            if (costIdx != -1) {
                int startIdx = costIdx + 5;
                int endIdx = line.indexOf(' ', startIdx);
                if (endIdx == -1) endIdx = line.indexOf(')', startIdx);
                if (endIdx != -1) {
                    return Integer.parseInt(line.substring(startIdx, endIdx));
                }
            }
        } catch (Exception e) {
            // Ignore parsing errors
        }
        return 0;
    }
}
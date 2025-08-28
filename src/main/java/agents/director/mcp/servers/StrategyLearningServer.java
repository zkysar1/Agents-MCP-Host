package agents.director.mcp.servers;

import agents.director.mcp.base.MCPServerBase;
import agents.director.mcp.base.MCPTool;
import agents.director.mcp.base.MCPResponse;
import agents.director.services.LlmAPIService;
import agents.director.services.LogUtil;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.web.RoutingContext;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * MCP Server for learning from strategy execution history.
 * Tracks execution patterns and suggests improvements for future strategies.
 */
public class StrategyLearningServer extends MCPServerBase {
    
    private LlmAPIService llmService;
    
    // In-memory storage for execution history (in production, would use a database)
    private final Map<String, ExecutionRecord> executionHistory = new ConcurrentHashMap<>();
    private final Map<String, PatternStatistics> patternStats = new ConcurrentHashMap<>();
    private final Map<String, List<String>> strategyTypeHistory = new ConcurrentHashMap<>();
    
    // Learning configuration
    private static final int MIN_EXECUTIONS_FOR_PATTERN = 3;
    private static final double SUCCESS_THRESHOLD = 0.8;
    private static final long HISTORY_RETENTION_MS = 7 * 24 * 60 * 60 * 1000L; // 7 days
    
    public StrategyLearningServer() {
        super("StrategyLearningServer", "/mcp/servers/strategy-learning");
    }
    
    @Override
    public void start(Promise<Void> startPromise) {
        llmService = LlmAPIService.getInstance();
        
        if (!llmService.isInitialized()) {
            LogUtil.logWarning(vertx, "LLM service not initialized - learning insights will be limited", 
                "StrategyLearningServer", "Init", "Warning");
        }
        
        // Start periodic cleanup of old history
        vertx.setPeriodic(3600000, id -> cleanupOldHistory()); // Every hour
        
        // Start periodic pattern analysis
        vertx.setPeriodic(300000, id -> analyzePatterns()); // Every 5 minutes
        
        super.start(startPromise);
    }
    
    @Override
    protected void initializeTools() {
        // Register record_execution tool
        registerTool(new MCPTool(
            "strategy_learning__record_execution",
            "Record strategy execution results for learning and pattern analysis",
            new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("strategy", new JsonObject()
                        .put("type", "object")
                        .put("description", "The strategy that was executed"))
                    .put("execution_results", new JsonObject()
                        .put("type", "object")
                        .put("description", "Detailed execution results")
                        .put("properties", new JsonObject()
                            .put("success", new JsonObject().put("type", "boolean"))
                            .put("total_duration", new JsonObject().put("type", "integer"))
                            .put("steps_completed", new JsonObject().put("type", "integer"))
                            .put("steps_failed", new JsonObject().put("type", "integer"))
                            .put("error_messages", new JsonObject()
                                .put("type", "array")
                                .put("items", new JsonObject().put("type", "string")))))
                    .put("performance_metrics", new JsonObject()
                        .put("type", "object")
                        .put("description", "Performance measurements")
                        .put("properties", new JsonObject()
                            .put("query_complexity", new JsonObject().put("type", "number"))
                            .put("data_volume", new JsonObject().put("type", "string"))
                            .put("response_time", new JsonObject().put("type", "integer"))
                            .put("resource_usage", new JsonObject().put("type", "object"))))
                    .put("user_satisfaction", new JsonObject()
                        .put("type", "number")
                        .put("description", "User satisfaction score 0-1")
                        .put("minimum", 0)
                        .put("maximum", 1)))
                .put("required", new JsonArray().add("strategy").add("execution_results"))
        ));
        
        // Register suggest_improvements tool
        registerTool(new MCPTool(
            "strategy_learning__suggest_improvements",
            "Suggest strategy improvements based on historical execution data",
            new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("strategy_type", new JsonObject()
                        .put("type", "string")
                        .put("description", "Type of strategy (e.g., 'complex_analytical', 'simple_query')")
                        .put("enum", new JsonArray()
                            .add("simple_query")
                            .add("complex_analytical")
                            .add("exploratory")
                            .add("performance_critical")))
                    .put("similar_executions", new JsonObject()
                        .put("type", "array")
                        .put("description", "Recent similar execution records")
                        .put("items", new JsonObject().put("type", "object")))
                    .put("failure_patterns", new JsonObject()
                        .put("type", "array")
                        .put("description", "Common failure patterns observed")
                        .put("items", new JsonObject().put("type", "object")))
                    .put("context", new JsonObject()
                        .put("type", "object")
                        .put("description", "Additional context for improvement suggestions")
                        .put("properties", new JsonObject()
                            .put("user_expertise", new JsonObject().put("type", "string"))
                            .put("time_constraints", new JsonObject().put("type", "boolean")))))
                .put("required", new JsonArray().add("strategy_type"))
        ));
        
        // Register analyze_patterns tool
        registerTool(new MCPTool(
            "strategy_learning__analyze_patterns",
            "Analyze execution patterns to identify optimization opportunities",
            new JsonObject()
                .put("type", "object")
                .put("properties", new JsonObject()
                    .put("time_window", new JsonObject()
                        .put("type", "string")
                        .put("description", "Time window for analysis")
                        .put("enum", new JsonArray()
                            .add("last_hour")
                            .add("last_day")
                            .add("last_week")))
                    .put("focus_area", new JsonObject()
                        .put("type", "string")
                        .put("description", "Specific area to focus on")
                        .put("enum", new JsonArray()
                            .add("performance")
                            .add("reliability")
                            .add("resource_usage")
                            .add("user_satisfaction"))))
                .put("required", new JsonArray())
        ));
    }
    
    @Override
    protected void executeTool(RoutingContext ctx, String requestId, String toolName, JsonObject arguments) {
        switch (toolName) {
            case "strategy_learning__record_execution":
                recordExecution(ctx, requestId, arguments);
                break;
            case "strategy_learning__suggest_improvements":
                suggestImprovements(ctx, requestId, arguments);
                break;
            case "strategy_learning__analyze_patterns":
                analyzePatterns(ctx, requestId, arguments);
                break;
            default:
                sendError(ctx, requestId, MCPResponse.ErrorCodes.METHOD_NOT_FOUND, 
                    "Unknown tool: " + toolName);
        }
    }
    
    private void recordExecution(RoutingContext ctx, String requestId, JsonObject arguments) {
        JsonObject strategy = arguments.getJsonObject("strategy");
        JsonObject executionResults = arguments.getJsonObject("execution_results");
        JsonObject performanceMetrics = arguments.getJsonObject("performance_metrics", new JsonObject());
        Float userSatisfaction = arguments.getFloat("user_satisfaction");
        
        String recordId = UUID.randomUUID().toString();
        String strategyName = strategy.getString("name", "Unknown Strategy");
        
        LogUtil.logDebug(vertx, "Recording execution for strategy: " + strategyName,
            "StrategyLearningServer", "Record", "Execution");
        
        // Create execution record
        ExecutionRecord record = new ExecutionRecord(
            recordId,
            strategy,
            executionResults,
            performanceMetrics,
            userSatisfaction,
            System.currentTimeMillis()
        );
        
        // Store record
        executionHistory.put(recordId, record);
        
        // Update strategy type history
        String strategyType = categorizeStrategy(strategy);
        strategyTypeHistory.computeIfAbsent(strategyType, k -> new ArrayList<>()).add(recordId);
        
        // Extract key insights
        JsonArray keyInsights = extractKeyInsights(record);
        
        // Detect patterns
        JsonArray patternsDetected = detectPatterns(strategyType, record);
        
        // Update pattern statistics
        updatePatternStatistics(strategyType, record);
        
        JsonObject result = new JsonObject()
            .put("recording_id", recordId)
            .put("key_insights", keyInsights)
            .put("patterns_detected", patternsDetected)
            .put("strategy_type", strategyType)
            .put("historical_performance", getHistoricalPerformance(strategyType));
        
        sendSuccess(ctx, requestId, new JsonObject().put("result", result));
    }
    
    private void suggestImprovements(RoutingContext ctx, String requestId, JsonObject arguments) {
        String strategyType = arguments.getString("strategy_type");
        JsonArray similarExecutions = arguments.getJsonArray("similar_executions", new JsonArray());
        JsonArray failurePatterns = arguments.getJsonArray("failure_patterns", new JsonArray());
        JsonObject context = arguments.getJsonObject("context", new JsonObject());
        
        LogUtil.logDebug(vertx, "Suggesting improvements for strategy type: " + strategyType,
            "StrategyLearningServer", "Suggest", "Improvements");
        
        // Get historical data for this strategy type
        List<ExecutionRecord> historicalRecords = getHistoricalRecords(strategyType);
        
        // Analyze performance patterns
        JsonObject performanceAnalysis = analyzePerformance(historicalRecords);
        
        // Generate improvements
        JsonArray improvements = new JsonArray();
        JsonArray confidenceScores = new JsonArray();
        
        // Rule-based improvements
        generateRuleBasedImprovements(strategyType, performanceAnalysis, improvements, confidenceScores);
        
        // If LLM available, generate intelligent suggestions
        if (llmService.isInitialized() && !historicalRecords.isEmpty()) {
            generateLLMImprovements(strategyType, historicalRecords, failurePatterns, context)
                .onComplete(ar -> {
                    if (ar.succeeded()) {
                        JsonArray llmImprovements = ar.result();
                        improvements.addAll(llmImprovements);
                        
                        // Add high confidence for LLM suggestions
                        for (int i = 0; i < llmImprovements.size(); i++) {
                            confidenceScores.add(0.8f);
                        }
                    }
                    
                    sendImprovementResponse(ctx, requestId, improvements, confidenceScores, performanceAnalysis);
                });
        } else {
            sendImprovementResponse(ctx, requestId, improvements, confidenceScores, performanceAnalysis);
        }
    }
    
    private void sendImprovementResponse(RoutingContext ctx, String requestId, 
                                       JsonArray improvements, JsonArray confidenceScores,
                                       JsonObject performanceAnalysis) {
        JsonObject result = new JsonObject()
            .put("improvements", improvements)
            .put("confidence_scores", confidenceScores)
            .put("expected_impact", calculateExpectedImpact(improvements))
            .put("performance_baseline", performanceAnalysis);
        
        sendSuccess(ctx, requestId, new JsonObject().put("result", result));
    }
    
    private void analyzePatterns(RoutingContext ctx, String requestId, JsonObject arguments) {
        String timeWindow = arguments.getString("time_window", "last_day");
        String focusArea = arguments.getString("focus_area", "performance");
        
        LogUtil.logInfo(vertx, "Analyzing patterns for " + focusArea + " in " + timeWindow,
            "StrategyLearningServer", "Pattern", "Analysis", false);
        
        // Filter records by time window
        long cutoffTime = calculateCutoffTime(timeWindow);
        List<ExecutionRecord> relevantRecords = executionHistory.values().stream()
            .filter(record -> record.timestamp > cutoffTime)
            .collect(Collectors.toList());
        
        // Analyze based on focus area
        JsonObject analysis = new JsonObject();
        
        switch (focusArea) {
            case "performance":
                analysis = analyzePerformancePatterns(relevantRecords);
                break;
            case "reliability":
                analysis = analyzeReliabilityPatterns(relevantRecords);
                break;
            case "resource_usage":
                analysis = analyzeResourcePatterns(relevantRecords);
                break;
            case "user_satisfaction":
                analysis = analyzeSatisfactionPatterns(relevantRecords);
                break;
        }
        
        // Add summary statistics
        analysis.put("total_executions", relevantRecords.size());
        analysis.put("time_window", timeWindow);
        analysis.put("focus_area", focusArea);
        
        sendSuccess(ctx, requestId, new JsonObject().put("result", analysis));
    }
    
    // Helper methods
    
    private String categorizeStrategy(JsonObject strategy) {
        int stepCount = strategy.getJsonArray("steps", new JsonArray()).size();
        String name = strategy.getString("name", "").toLowerCase();
        
        if (name.contains("simple") || stepCount <= 4) {
            return "simple_query";
        } else if (name.contains("complex") || stepCount >= 8) {
            return "complex_analytical";
        } else if (name.contains("explor") || name.contains("discover")) {
            return "exploratory";
        } else if (name.contains("performance") || name.contains("optim")) {
            return "performance_critical";
        } else {
            return "standard";
        }
    }
    
    private JsonArray extractKeyInsights(ExecutionRecord record) {
        JsonArray insights = new JsonArray();
        
        // Success rate insight
        if (record.executionResults.getBoolean("success", false)) {
            insights.add("Successful execution in " + record.executionResults.getInteger("total_duration") + "ms");
        } else {
            insights.add("Failed execution with " + record.executionResults.getInteger("steps_failed", 0) + " failed steps");
        }
        
        // Performance insight
        float complexity = record.performanceMetrics.getFloat("query_complexity", 0.5f);
        int responseTime = record.performanceMetrics.getInteger("response_time", 0);
        if (complexity > 0.7 && responseTime < 5000) {
            insights.add("Efficient handling of complex query");
        } else if (complexity < 0.3 && responseTime > 2000) {
            insights.add("Simple query took longer than expected");
        }
        
        // User satisfaction insight
        if (record.userSatisfaction != null) {
            if (record.userSatisfaction > 0.8) {
                insights.add("High user satisfaction (" + String.format("%.0f%%", record.userSatisfaction * 100) + ")");
            } else if (record.userSatisfaction < 0.5) {
                insights.add("Low user satisfaction - needs improvement");
            }
        }
        
        return insights;
    }
    
    private JsonArray detectPatterns(String strategyType, ExecutionRecord record) {
        JsonArray patterns = new JsonArray();
        
        // Get recent records of same type
        List<ExecutionRecord> recentSimilar = getRecentRecords(strategyType, 10);
        
        // Pattern: Consistent failures
        long failures = recentSimilar.stream()
            .filter(r -> !r.executionResults.getBoolean("success", true))
            .count();
        if (failures > recentSimilar.size() * 0.3) {
            patterns.add(new JsonObject()
                .put("pattern", "high_failure_rate")
                .put("confidence", 0.8)
                .put("details", "Failure rate " + (failures * 100 / recentSimilar.size()) + "%"));
        }
        
        // Pattern: Performance degradation
        if (recentSimilar.size() >= 3) {
            List<Integer> recentTimes = recentSimilar.stream()
                .map(r -> r.executionResults.getInteger("total_duration", 0))
                .collect(Collectors.toList());
            
            if (isIncreasingTrend(recentTimes)) {
                patterns.add(new JsonObject()
                    .put("pattern", "performance_degradation")
                    .put("confidence", 0.7)
                    .put("details", "Response times increasing"));
            }
        }
        
        // Pattern: Specific step failures
        Map<String, Integer> stepFailures = new HashMap<>();
        for (ExecutionRecord r : recentSimilar) {
            JsonArray errors = r.executionResults.getJsonArray("error_messages", new JsonArray());
            for (int i = 0; i < errors.size(); i++) {
                String error = errors.getString(i);
                if (error.contains("step")) {
                    stepFailures.merge(error, 1, Integer::sum);
                }
            }
        }
        
        stepFailures.forEach((step, count) -> {
            if (count > recentSimilar.size() * 0.2) {
                patterns.add(new JsonObject()
                    .put("pattern", "recurring_step_failure")
                    .put("confidence", 0.9)
                    .put("details", step + " failed " + count + " times"));
            }
        });
        
        return patterns;
    }
    
    private void updatePatternStatistics(String strategyType, ExecutionRecord record) {
        PatternStatistics stats = patternStats.computeIfAbsent(strategyType, 
            k -> new PatternStatistics(strategyType));
        
        stats.totalExecutions++;
        if (record.executionResults.getBoolean("success", false)) {
            stats.successfulExecutions++;
        }
        
        int duration = record.executionResults.getInteger("total_duration", 0);
        stats.totalDuration += duration;
        stats.maxDuration = Math.max(stats.maxDuration, duration);
        stats.minDuration = stats.minDuration == 0 ? duration : Math.min(stats.minDuration, duration);
        
        if (record.userSatisfaction != null) {
            stats.totalSatisfaction += record.userSatisfaction;
            stats.satisfactionCount++;
        }
        
        stats.lastUpdated = System.currentTimeMillis();
    }
    
    private JsonObject getHistoricalPerformance(String strategyType) {
        PatternStatistics stats = patternStats.get(strategyType);
        if (stats == null || stats.totalExecutions == 0) {
            return new JsonObject()
                .put("success_rate", "N/A")
                .put("avg_duration", "N/A")
                .put("sample_size", 0);
        }
        
        return new JsonObject()
            .put("success_rate", (float) stats.successfulExecutions / stats.totalExecutions)
            .put("avg_duration", stats.totalDuration / stats.totalExecutions)
            .put("min_duration", stats.minDuration)
            .put("max_duration", stats.maxDuration)
            .put("avg_satisfaction", stats.satisfactionCount > 0 ? 
                stats.totalSatisfaction / stats.satisfactionCount : "N/A")
            .put("sample_size", stats.totalExecutions);
    }
    
    private List<ExecutionRecord> getHistoricalRecords(String strategyType) {
        List<String> recordIds = strategyTypeHistory.getOrDefault(strategyType, new ArrayList<>());
        return recordIds.stream()
            .map(executionHistory::get)
            .filter(Objects::nonNull)
            .sorted((a, b) -> Long.compare(b.timestamp, a.timestamp))
            .limit(50)
            .collect(Collectors.toList());
    }
    
    private List<ExecutionRecord> getRecentRecords(String strategyType, int limit) {
        return getHistoricalRecords(strategyType).stream()
            .limit(limit)
            .collect(Collectors.toList());
    }
    
    private JsonObject analyzePerformance(List<ExecutionRecord> records) {
        if (records.isEmpty()) {
            return new JsonObject().put("status", "no_data");
        }
        
        IntSummaryStatistics durationStats = records.stream()
            .mapToInt(r -> r.executionResults.getInteger("total_duration", 0))
            .summaryStatistics();
        
        long successCount = records.stream()
            .filter(r -> r.executionResults.getBoolean("success", false))
            .count();
        
        return new JsonObject()
            .put("avg_duration", durationStats.getAverage())
            .put("min_duration", durationStats.getMin())
            .put("max_duration", durationStats.getMax())
            .put("success_rate", (float) successCount / records.size())
            .put("total_executions", records.size());
    }
    
    private void generateRuleBasedImprovements(String strategyType, JsonObject performanceAnalysis,
                                             JsonArray improvements, JsonArray confidenceScores) {
        double avgDuration = performanceAnalysis.getDouble("avg_duration", 0);
        double successRate = performanceAnalysis.getDouble("success_rate", 1.0);
        
        // Performance improvements
        if (avgDuration > 10000) { // > 10 seconds
            improvements.add(new JsonObject()
                .put("type", "performance")
                .put("suggestion", "Enable parallel execution for independent steps")
                .put("reason", "Average execution time is " + (avgDuration / 1000) + "s"));
            confidenceScores.add(0.9f);
        }
        
        // Reliability improvements
        if (successRate < 0.8) {
            improvements.add(new JsonObject()
                .put("type", "reliability")
                .put("suggestion", "Add retry logic for frequently failing steps")
                .put("reason", "Success rate is only " + String.format("%.0f%%", successRate * 100)));
            confidenceScores.add(0.85f);
        }
        
        // Strategy-specific improvements
        switch (strategyType) {
            case "complex_analytical":
                if (avgDuration > 15000) {
                    improvements.add(new JsonObject()
                        .put("type", "optimization")
                        .put("suggestion", "Consider caching intermediate results for complex queries")
                        .put("reason", "Complex queries averaging " + (avgDuration / 1000) + "s"));
                    confidenceScores.add(0.7f);
                }
                break;
                
            case "simple_query":
                if (avgDuration > 3000) {
                    improvements.add(new JsonObject()
                        .put("type", "simplification")
                        .put("suggestion", "Remove optional validation steps for simple queries")
                        .put("reason", "Simple queries should complete under 3s"));
                    confidenceScores.add(0.8f);
                }
                break;
        }
    }
    
    private Future<JsonArray> generateLLMImprovements(String strategyType, List<ExecutionRecord> history,
                                                     JsonArray failurePatterns, JsonObject context) {
        Promise<JsonArray> promise = Promise.promise();
        
        String prompt = buildImprovementPrompt(strategyType, history, failurePatterns, context);
        
        llmService.complete(prompt, new JsonObject()
            .put("temperature", 0.3)
            .put("max_tokens", 1000))
            .onComplete(ar -> {
                if (ar.succeeded()) {
                    try {
                        JsonArray improvements = parseImprovementsFromLLM(ar.result());
                        promise.complete(improvements);
                    } catch (Exception e) {
                        LogUtil.logError(vertx, "Failed to parse LLM improvements", e,
                            "StrategyLearningServer", "Improvements", "Parse", false);
                        promise.complete(new JsonArray());
                    }
                } else {
                    promise.complete(new JsonArray());
                }
            });
        
        return promise.future();
    }
    
    private String buildImprovementPrompt(String strategyType, List<ExecutionRecord> history,
                                        JsonArray failurePatterns, JsonObject context) {
        // Summarize recent performance
        JsonObject recentPerf = analyzePerformance(history.stream().limit(10).collect(Collectors.toList()));
        
        return String.format("""
            Analyze execution history and suggest improvements for %s strategies.
            
            Recent Performance:
            - Success Rate: %.1f%%
            - Average Duration: %.1fs
            - Sample Size: %d executions
            
            Common Failure Patterns:
            %s
            
            Context:
            - User Expertise: %s
            - Time Constraints: %s
            
            Based on this data, suggest 2-3 specific improvements to enhance:
            1. Reliability (reduce failures)
            2. Performance (reduce execution time)
            3. User satisfaction
            
            Output as JSON array:
            [
              {
                "type": "category (performance/reliability/user_experience)",
                "suggestion": "specific actionable improvement",
                "reason": "data-driven justification",
                "implementation": "brief how-to"
              }
            ]
            """,
            strategyType,
            recentPerf.getDouble("success_rate", 0) * 100,
            recentPerf.getDouble("avg_duration", 0) / 1000,
            history.size(),
            failurePatterns.encodePrettily(),
            context.getString("user_expertise", "intermediate"),
            context.getBoolean("time_constraints", false) ? "Yes" : "No");
    }
    
    private JsonArray parseImprovementsFromLLM(String llmResponse) {
        String response = llmResponse.trim();
        int start = response.indexOf("[");
        int end = response.lastIndexOf("]");
        
        if (start >= 0 && end > start) {
            String jsonStr = response.substring(start, end + 1);
            return new JsonArray(jsonStr);
        }
        
        throw new IllegalArgumentException("No valid JSON array found in LLM response");
    }
    
    private JsonObject calculateExpectedImpact(JsonArray improvements) {
        float performanceImpact = 0.0f;
        float reliabilityImpact = 0.0f;
        float satisfactionImpact = 0.0f;
        
        for (int i = 0; i < improvements.size(); i++) {
            JsonObject improvement = improvements.getJsonObject(i);
            String type = improvement.getString("type", "");
            
            switch (type) {
                case "performance":
                    performanceImpact += 0.15f;
                    break;
                case "reliability":
                    reliabilityImpact += 0.2f;
                    break;
                case "user_experience":
                    satisfactionImpact += 0.1f;
                    break;
            }
        }
        
        return new JsonObject()
            .put("performance_improvement", Math.min(performanceImpact, 0.5f))
            .put("reliability_improvement", Math.min(reliabilityImpact, 0.4f))
            .put("satisfaction_improvement", Math.min(satisfactionImpact, 0.3f));
    }
    
    private long calculateCutoffTime(String timeWindow) {
        long now = System.currentTimeMillis();
        switch (timeWindow) {
            case "last_hour":
                return now - 3600000L;
            case "last_day":
                return now - 86400000L;
            case "last_week":
                return now - 604800000L;
            default:
                return now - 86400000L; // Default to last day
        }
    }
    
    private JsonObject analyzePerformancePatterns(List<ExecutionRecord> records) {
        if (records.isEmpty()) {
            return new JsonObject().put("status", "no_data");
        }
        
        // Group by hour
        Map<Integer, List<ExecutionRecord>> byHour = records.stream()
            .collect(Collectors.groupingBy(r -> 
                (int) ((System.currentTimeMillis() - r.timestamp) / 3600000)));
        
        // Find peak hours
        int peakHour = -1;
        double peakAvgDuration = 0;
        
        for (Map.Entry<Integer, List<ExecutionRecord>> entry : byHour.entrySet()) {
            double avgDuration = entry.getValue().stream()
                .mapToInt(r -> r.executionResults.getInteger("total_duration", 0))
                .average()
                .orElse(0);
            
            if (avgDuration > peakAvgDuration) {
                peakAvgDuration = avgDuration;
                peakHour = entry.getKey();
            }
        }
        
        return new JsonObject()
            .put("pattern_type", "performance")
            .put("total_executions", records.size())
            .put("peak_hour", peakHour + " hours ago")
            .put("peak_avg_duration", peakAvgDuration)
            .put("recommendation", peakAvgDuration > 5000 ? 
                "Consider scaling resources during peak times" : "Performance is stable");
    }
    
    private JsonObject analyzeReliabilityPatterns(List<ExecutionRecord> records) {
        Map<String, Integer> errorTypes = new HashMap<>();
        int totalFailures = 0;
        
        for (ExecutionRecord record : records) {
            if (!record.executionResults.getBoolean("success", true)) {
                totalFailures++;
                JsonArray errors = record.executionResults.getJsonArray("error_messages", new JsonArray());
                for (int i = 0; i < errors.size(); i++) {
                    String error = categorizeError(errors.getString(i));
                    errorTypes.merge(error, 1, Integer::sum);
                }
            }
        }
        
        // Find most common error
        String mostCommonError = errorTypes.entrySet().stream()
            .max(Map.Entry.comparingByValue())
            .map(Map.Entry::getKey)
            .orElse("unknown");
        
        return new JsonObject()
            .put("pattern_type", "reliability")
            .put("failure_rate", records.isEmpty() ? 0 : (float) totalFailures / records.size())
            .put("total_failures", totalFailures)
            .put("error_distribution", new JsonObject(errorTypes))
            .put("most_common_error", mostCommonError)
            .put("recommendation", totalFailures > records.size() * 0.2 ? 
                "Focus on fixing " + mostCommonError + " errors" : "Reliability is acceptable");
    }
    
    private JsonObject analyzeResourcePatterns(List<ExecutionRecord> records) {
        // Analyze resource usage patterns
        double avgMemoryUsage = 0;
        double avgCpuUsage = 0;
        int resourceDataCount = 0;
        
        for (ExecutionRecord record : records) {
            JsonObject resources = record.performanceMetrics.getJsonObject("resource_usage", new JsonObject());
            if (resources.containsKey("memory_mb")) {
                avgMemoryUsage += resources.getDouble("memory_mb", 0);
                resourceDataCount++;
            }
            if (resources.containsKey("cpu_percent")) {
                avgCpuUsage += resources.getDouble("cpu_percent", 0);
                resourceDataCount++;
            }
        }
        
        if (resourceDataCount > 0) {
            avgMemoryUsage /= resourceDataCount;
            avgCpuUsage /= resourceDataCount;
        }
        
        return new JsonObject()
            .put("pattern_type", "resource_usage")
            .put("avg_memory_mb", avgMemoryUsage)
            .put("avg_cpu_percent", avgCpuUsage)
            .put("data_points", resourceDataCount)
            .put("recommendation", avgMemoryUsage > 1000 ? 
                "Consider memory optimization strategies" : "Resource usage is reasonable");
    }
    
    private JsonObject analyzeSatisfactionPatterns(List<ExecutionRecord> records) {
        List<Float> satisfactionScores = records.stream()
            .map(r -> r.userSatisfaction)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
        
        if (satisfactionScores.isEmpty()) {
            return new JsonObject()
                .put("pattern_type", "user_satisfaction")
                .put("status", "no_satisfaction_data");
        }
        
        double avgSatisfaction = satisfactionScores.stream()
            .mapToDouble(Float::doubleValue)
            .average()
            .orElse(0);
        
        // Find correlation with execution time
        double timeCorrelation = calculateTimeToSatisfactionCorrelation(records);
        
        return new JsonObject()
            .put("pattern_type", "user_satisfaction")
            .put("avg_satisfaction", avgSatisfaction)
            .put("sample_size", satisfactionScores.size())
            .put("time_correlation", timeCorrelation)
            .put("recommendation", avgSatisfaction < 0.7 ? 
                "User satisfaction needs improvement" : 
                timeCorrelation < -0.5 ? "Users value faster responses" : "Satisfaction is good");
    }
    
    private String categorizeError(String errorMessage) {
        String lower = errorMessage.toLowerCase();
        
        if (lower.contains("timeout")) return "timeout";
        if (lower.contains("validation")) return "validation";
        if (lower.contains("permission") || lower.contains("access")) return "permission";
        if (lower.contains("not found") || lower.contains("404")) return "not_found";
        if (lower.contains("syntax") || lower.contains("parse")) return "syntax";
        if (lower.contains("connection")) return "connection";
        
        return "other";
    }
    
    private boolean isIncreasingTrend(List<Integer> values) {
        if (values.size() < 3) return false;
        
        int increases = 0;
        for (int i = 1; i < values.size(); i++) {
            if (values.get(i) > values.get(i - 1)) {
                increases++;
            }
        }
        
        return increases > values.size() * 0.6;
    }
    
    private double calculateTimeToSatisfactionCorrelation(List<ExecutionRecord> records) {
        List<Double> times = new ArrayList<>();
        List<Double> satisfactions = new ArrayList<>();
        
        for (ExecutionRecord record : records) {
            if (record.userSatisfaction != null) {
                times.add((double) record.executionResults.getInteger("total_duration", 0));
                satisfactions.add(record.userSatisfaction.doubleValue());
            }
        }
        
        if (times.size() < 3) return 0;
        
        // Simple correlation calculation
        double meanTime = times.stream().mapToDouble(Double::doubleValue).average().orElse(0);
        double meanSat = satisfactions.stream().mapToDouble(Double::doubleValue).average().orElse(0);
        
        double numerator = 0;
        double denomTime = 0;
        double denomSat = 0;
        
        for (int i = 0; i < times.size(); i++) {
            double timeDiff = times.get(i) - meanTime;
            double satDiff = satisfactions.get(i) - meanSat;
            numerator += timeDiff * satDiff;
            denomTime += timeDiff * timeDiff;
            denomSat += satDiff * satDiff;
        }
        
        double denominator = Math.sqrt(denomTime * denomSat);
        return denominator > 0 ? numerator / denominator : 0;
    }
    
    private void cleanupOldHistory() {
        long cutoff = System.currentTimeMillis() - HISTORY_RETENTION_MS;
        
        List<String> toRemove = executionHistory.entrySet().stream()
            .filter(entry -> entry.getValue().timestamp < cutoff)
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());
        
        toRemove.forEach(executionHistory::remove);
        
        if (!toRemove.isEmpty()) {
            LogUtil.logDebug(vertx, "Cleaned up " + toRemove.size() + " old execution records",
                "StrategyLearningServer", "Cleanup", "History");
        }
    }
    
    private void analyzePatterns() {
        // This runs periodically to update pattern statistics
        patternStats.forEach((strategyType, stats) -> {
            if (stats.lastUpdated > System.currentTimeMillis() - 300000) { // Updated in last 5 min
                LogUtil.logDebug(vertx, "Pattern analysis for " + strategyType + 
                    ": success=" + ((float) stats.successfulExecutions / stats.totalExecutions),
                    "StrategyLearningServer", "Pattern", "Update");
            }
        });
    }
    
    // Inner classes
    
    private static class ExecutionRecord {
        final String id;
        final JsonObject strategy;
        final JsonObject executionResults;
        final JsonObject performanceMetrics;
        final Float userSatisfaction;
        final long timestamp;
        
        ExecutionRecord(String id, JsonObject strategy, JsonObject executionResults,
                       JsonObject performanceMetrics, Float userSatisfaction, long timestamp) {
            this.id = id;
            this.strategy = strategy;
            this.executionResults = executionResults;
            this.performanceMetrics = performanceMetrics;
            this.userSatisfaction = userSatisfaction;
            this.timestamp = timestamp;
        }
    }
    
    private static class PatternStatistics {
        final String strategyType;
        int totalExecutions = 0;
        int successfulExecutions = 0;
        long totalDuration = 0;
        int maxDuration = 0;
        int minDuration = 0;
        float totalSatisfaction = 0;
        int satisfactionCount = 0;
        long lastUpdated;
        
        PatternStatistics(String strategyType) {
            this.strategyType = strategyType;
            this.lastUpdated = System.currentTimeMillis();
        }
    }
}
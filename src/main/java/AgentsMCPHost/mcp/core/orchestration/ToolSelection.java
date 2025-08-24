package AgentsMCPHost.mcp.core.orchestration;

import AgentsMCPHost.llm.LlmAPIService;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.core.eventbus.Message;
import io.vertx.core.file.FileSystem;
import AgentsMCPHost.streaming.StreamingEventPublisher;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Unified Tool Selection Verticle - Central decision point for all tool routing.
 * 
 * This verticle replaces the fragmented tool detection logic previously spread
 * across ConversationVerticle. It uses LLM-powered analysis to intelligently
 * select the appropriate tool(s) or agent flow for any user query.
 * 
 * Key improvements:
 * - Single responsibility for tool selection
 * - LLM-based understanding vs keyword matching
 * - Configurable tool capabilities
 * - Treats Oracle Agent as part of the tool ecosystem
 * - Supports tool combinations and complex strategies
 */
public class ToolSelection extends AbstractVerticle {
    
    // Tool registry loaded from configuration
    private JsonObject toolRegistry;
    
    // Available tools tracked from MCP infrastructure
    private final Map<String, JsonObject> availableTools = new ConcurrentHashMap<>();
    
    // LLM service for intelligent analysis
    private LlmAPIService llmService;
    
    // Cache for recent decisions (helps with similar queries)
    private final Map<String, ToolDecision> decisionCache = new ConcurrentHashMap<>();
    private static final int CACHE_SIZE = 100;
    
    // Configuration
    private boolean llmValidationEnabled = true;
    private double defaultConfidenceThreshold = 0.6;
    
    @Override
    public void start(Promise<Void> startPromise) {
        // Initialize LLM service
        llmService = LlmAPIService.getInstance();
        
        // Load tool registry configuration
        loadToolRegistry()
            .compose(v -> {
                // Register event bus consumers
                registerEventBusConsumers();
                
                // Subscribe to tool updates from MCP infrastructure
                subscribeToToolUpdates();
                
                System.out.println("ToolSelectionVerticle started - unified tool selection ready");
                return Future.succeededFuture();
            })
            .onSuccess(v -> startPromise.complete())
            .onFailure(startPromise::fail);
    }
    
    /**
     * Load tool registry from configuration file
     */
    private Future<Void> loadToolRegistry() {
        FileSystem fs = vertx.fileSystem();
        String configPath = "src/main/resources/tool-registry.json";
        
        return fs.readFile(configPath)
            .compose(buffer -> {
                // Use executeBlocking with Callable to prevent Jackson initialization from blocking event loop
                return vertx.<JsonObject>executeBlocking(() -> {
                    // This JSON parsing triggers Jackson class loading - must be on worker thread
                    return new JsonObject(buffer);
                }, false);  // Unordered - JSON parsing doesn't need ordering
            })
            .<Void>map(jsonObject -> {
                toolRegistry = jsonObject;
                System.out.println("Loaded tool registry with " + 
                                 toolRegistry.getJsonObject("tools", new JsonObject()).size() + 
                                 " tool definitions");
                return null;
            })
            .recover(err -> {
                // If file doesn't exist, use default registry
                System.out.println("Tool registry not found, using defaults");
                // Also wrap default creation in executeBlocking to be consistent
                return vertx.<JsonObject>executeBlocking(() -> {
                    return createDefaultRegistry();
                }, false)  // Unordered - default creation doesn't need ordering
                .<Void>map(defaultRegistry -> {
                    toolRegistry = defaultRegistry;
                    System.out.println("Using default tool registry with " + 
                                     defaultRegistry.getJsonObject("tools", new JsonObject()).size() + 
                                     " tool definitions");
                    return null;
                });
            });
    }
    
    /**
     * Create default tool registry if configuration file is missing
     */
    private JsonObject createDefaultRegistry() {
        return new JsonObject()
            .put("tools", new JsonObject()
                // Oracle tools
                .put("oracle__list_tables", new JsonObject()
                    .put("capabilities", new JsonArray()
                        .add("list").add("tables").add("schema").add("metadata"))
                    .put("intents", new JsonArray()
                        .add("discover").add("explore").add("list").add("show"))
                    .put("entities", new JsonArray()
                        .add("table").add("schema").add("database"))
                    .put("confidence_threshold", 0.7)
                    .put("description", "List all database tables"))
                
                .put("oracle__describe_table", new JsonObject()
                    .put("capabilities", new JsonArray()
                        .add("describe").add("structure").add("columns").add("metadata"))
                    .put("intents", new JsonArray()
                        .add("describe").add("show").add("structure"))
                    .put("entities", new JsonArray()
                        .add("table").add("column").add("field"))
                    .put("confidence_threshold", 0.7)
                    .put("description", "Get table structure and column details"))
                
                .put("oracle__execute_query", new JsonObject()
                    .put("capabilities", new JsonArray()
                        .add("query").add("select").add("retrieve").add("filter"))
                    .put("intents", new JsonArray()
                        .add("get").add("find").add("search").add("select"))
                    .put("entities", new JsonArray()
                        .add("data").add("records").add("rows"))
                    .put("confidence_threshold", 0.6)
                    .put("description", "Execute SQL SELECT queries"))
                
                // Orchestration strategies (not tools, but complex flows)
                .put("_orchestration_oracle", new JsonObject()
                    .put("type", "orchestration")
                    .put("strategy", "oracle_full_pipeline")
                    .put("capabilities", new JsonArray()
                        .add("complex_query").add("natural_language_sql")
                        .add("multi_table").add("discovery").add("intelligent"))
                    .put("intents", new JsonArray()
                        .add("*")) // Catch-all for complex queries
                    .put("entities", new JsonArray()
                        .add("*")) // Any entities
                    .put("confidence_threshold", 0.5)
                    .put("priority", "low") // Use when specific tools don't match well
                    .put("description", "Full Oracle pipeline with discovery and LLM"))
            )
            .put("settings", new JsonObject()
                .put("llm_validation_enabled", true)
                .put("default_confidence_threshold", 0.6)
                .put("cache_similar_queries", true));
    }
    
    /**
     * Register event bus consumers for tool selection requests
     */
    private void registerEventBusConsumers() {
        // Main tool selection endpoint
        vertx.eventBus().consumer("tool.selection.analyze", this::handleToolSelectionRequest);
        
        // Configuration updates
        vertx.eventBus().consumer("tool.selection.update_config", this::handleConfigUpdate);
        
        // Cache management
        vertx.eventBus().consumer("tool.selection.clear_cache", msg -> {
            decisionCache.clear();
            msg.reply(new JsonObject().put("cleared", true));
        });
    }
    
    /**
     * Subscribe to tool availability updates from MCP infrastructure
     */
    private void subscribeToToolUpdates() {
        vertx.eventBus().consumer("mcp.tools.aggregated", msg -> {
            JsonObject update = (JsonObject) msg.body();
            System.out.println("Tool registry updated: " + update.getInteger("totalTools") + " tools available");
        });
        
        vertx.eventBus().consumer("mcp.tools.discovered", msg -> {
            JsonObject discovery = (JsonObject) msg.body();
            JsonArray tools = discovery.getJsonArray("tools");
            String serverName = discovery.getString("server");
            
            for (int i = 0; i < tools.size(); i++) {
                JsonObject tool = tools.getJsonObject(i);
                String toolName = serverName + "__" + tool.getString("name");
                availableTools.put(toolName, tool);
            }
        });
    }
    
    /**
     * Handle tool selection request - Main entry point
     */
    private void handleToolSelectionRequest(Message<JsonObject> msg) {
        JsonObject request = msg.body();
        String query = request.getString("query");
        JsonArray conversationHistory = request.getJsonArray("history", new JsonArray());
        String sessionId = request.getString("sessionId");
        
        System.out.println("[ToolSelection] Analyzing query: " + query);
        
        // Publish tool selection start event if streaming
        if (sessionId != null) {
            StreamingEventPublisher publisher = new StreamingEventPublisher(vertx, sessionId);
            publisher.publishProgress("tool_analysis", "Analyzing query for tool requirements",
                new JsonObject()
                    .put("phase", "tool_analysis")
                    .put("query", query)
                    .put("status", "analyzing"));
        }
        
        // Check cache for similar recent queries
        String cacheKey = generateCacheKey(query);
        ToolDecision cachedDecision = decisionCache.get(cacheKey);
        if (cachedDecision != null && cachedDecision.isValid()) {
            System.out.println("[ToolSelection] Using cached decision: " + cachedDecision.strategy);
            msg.reply(cachedDecision.toJson());
            return;
        }
        
        // Perform tool selection analysis
        analyzeAndSelectTools(query, conversationHistory, sessionId)
            .onSuccess(decision -> {
                // Cache the decision
                cacheDecision(cacheKey, decision);
                
                // Log the final decision
                System.out.println("[ToolSelection] Final decision: " + decision.strategy + 
                    (decision.primaryTool != null ? " (tool: " + decision.primaryTool + ")" : "") +
                    (decision.orchestrationName != null ? " (orchestration: " + decision.orchestrationName + ")" : ""));
                
                // Publish tool selection completion if streaming
                if (sessionId != null) {
                    StreamingEventPublisher publisher = new StreamingEventPublisher(vertx, sessionId);
                    publisher.publishProgress("tool_analysis", "Tool selection completed",
                        new JsonObject()
                            .put("phase", "tool_analysis")
                            .put("strategy", decision.strategy.name())
                            .put("primaryTool", decision.primaryTool != null ? decision.primaryTool : "none")
                            .put("confidence", decision.confidence)
                            .put("status", "completed"));
                }
                
                // Reply with decision
                msg.reply(decision.toJson());
            })
            .onFailure(err -> {
                System.err.println("[ToolSelection] Failed: " + err.getMessage());
                msg.fail(500, "Tool selection failed: " + err.getMessage());
            });
    }
    
    /**
     * Main tool selection algorithm
     */
    private Future<ToolDecision> analyzeAndSelectTools(String query, JsonArray history, String sessionId) {
        // Publish analysis phase if streaming
        if (sessionId != null) {
            StreamingEventPublisher publisher = new StreamingEventPublisher(vertx, sessionId);
            publisher.publishProgress("intent_analysis", "Analyzing query intent",
                new JsonObject()
                    .put("phase", "intent_analysis")
                    .put("method", llmService.isInitialized() ? "llm" : "pattern")
                    .put("status", "processing"));
        }
        
        // Step 1: Analyze query with LLM (or fallback to pattern matching)
        return analyzeQueryIntent(query, history)
            .compose(analysis -> {
                // Step 2: Find candidate tools based on analysis
                List<ToolCandidate> candidates = findCandidateTools(analysis);
                
                // Step 3: Validate and refine selection with LLM if enabled
                if (llmValidationEnabled && llmService.isInitialized()) {
                    return validateToolSelection(query, analysis, candidates);
                } else {
                    // Fallback to score-based selection
                    return Future.succeededFuture(selectByScore(candidates, analysis));
                }
            });
    }
    
    /**
     * Analyze query intent using LLM
     */
    private Future<QueryAnalysis> analyzeQueryIntent(String query, JsonArray history) {
        if (!llmService.isInitialized()) {
            // Fallback to pattern-based analysis
            return Future.succeededFuture(analyzeQueryWithPatterns(query));
        }
        
        String prompt = buildIntentAnalysisPrompt(query, history);
        
        JsonArray messages = new JsonArray()
            .add(new JsonObject()
                .put("role", "system")
                .put("content", "You are an expert at analyzing user queries to determine what tools or capabilities are needed."))
            .add(new JsonObject()
                .put("role", "user")
                .put("content", prompt));
        
        return llmService.chatCompletion(messages)
            .map(response -> {
                JsonArray choices = response.getJsonArray("choices");
                if (choices != null && !choices.isEmpty()) {
                    String content = choices.getJsonObject(0)
                        .getJsonObject("message")
                        .getString("content");
                    
                    try {
                        JsonObject analysisJson = new JsonObject(content);
                        return QueryAnalysis.fromJson(analysisJson);
                    } catch (Exception e) {
                        // If JSON parsing fails, fallback to pattern analysis
                        return analyzeQueryWithPatterns(query);
                    }
                }
                return analyzeQueryWithPatterns(query);
            })
            .recover(err -> {
                System.err.println("LLM analysis failed, using patterns: " + err.getMessage());
                return Future.succeededFuture(analyzeQueryWithPatterns(query));
            });
    }
    
    /**
     * Pattern-based query analysis (fallback when LLM unavailable)
     */
    private QueryAnalysis analyzeQueryWithPatterns(String query) {
        QueryAnalysis analysis = new QueryAnalysis();
        String lower = query.toLowerCase();
        
        // Detect intent
        if (lower.contains("list") || lower.contains("show")) {
            analysis.intent = "list";
        } else if (lower.contains("count") || lower.contains("how many")) {
            analysis.intent = "count";
        } else if (lower.contains("describe") || lower.contains("structure")) {
            analysis.intent = "describe";
        } else if (lower.contains("find") || lower.contains("search") || lower.contains("get")) {
            analysis.intent = "search";
        } else {
            analysis.intent = "query";
        }
        
        // Extract entities
        if (lower.contains("table")) analysis.entities.add("table");
        if (lower.contains("orders")) analysis.entities.add("orders");
        if (lower.contains("customers")) analysis.entities.add("customers");
        if (lower.contains("products")) analysis.entities.add("products");
        if (lower.contains("pending")) analysis.entities.add("pending");
        if (lower.contains("california")) analysis.entities.add("california");
        
        // Detect capabilities needed
        if (lower.contains("join") || (analysis.entities.size() > 1)) {
            analysis.capabilities.add("multi_table");
        }
        if (lower.contains("sql") || lower.contains("query")) {
            analysis.capabilities.add("query");
        }
        
        // Set complexity
        analysis.complexity = analysis.entities.size() > 2 || 
                             analysis.capabilities.contains("multi_table") ? 
                             "complex" : "simple";
        
        analysis.confidence = 0.7; // Pattern matching confidence
        
        return analysis;
    }
    
    /**
     * Find candidate tools based on query analysis
     */
    private List<ToolCandidate> findCandidateTools(QueryAnalysis analysis) {
        List<ToolCandidate> candidates = new ArrayList<>();
        JsonObject tools = toolRegistry.getJsonObject("tools", new JsonObject());
        
        for (String toolName : tools.fieldNames()) {
            JsonObject toolDef = tools.getJsonObject(toolName);
            double score = scoreToolMatch(toolDef, analysis);
            
            if (score > 0) {
                ToolCandidate candidate = new ToolCandidate();
                candidate.toolName = toolName;
                candidate.score = score;
                candidate.capabilities = toolDef.getJsonArray("capabilities", new JsonArray());
                candidate.description = toolDef.getString("description", "");
                candidate.priority = toolDef.getString("priority", "normal");
                candidates.add(candidate);
            }
        }
        
        // Sort by score (descending) and priority
        candidates.sort((a, b) -> {
            if (Math.abs(a.score - b.score) < 0.01) {
                // Similar scores, check priority
                return a.priority.equals("high") ? -1 : 
                       b.priority.equals("high") ? 1 : 0;
            }
            return Double.compare(b.score, a.score);
        });
        
        return candidates;
    }
    
    /**
     * Score how well a tool matches the query analysis
     */
    private double scoreToolMatch(JsonObject toolDef, QueryAnalysis analysis) {
        double score = 0.0;
        double maxScore = 0.0;
        
        // Check intent match
        JsonArray intents = toolDef.getJsonArray("intents", new JsonArray());
        maxScore += 1.0;
        if (intents.contains("*") || intents.contains(analysis.intent)) {
            score += 1.0;
        } else {
            // Partial match
            for (int i = 0; i < intents.size(); i++) {
                if (analysis.intent.contains(intents.getString(i))) {
                    score += 0.5;
                    break;
                }
            }
        }
        
        // Check entity match
        JsonArray entities = toolDef.getJsonArray("entities", new JsonArray());
        if (!entities.contains("*")) {
            for (String queryEntity : analysis.entities) {
                maxScore += 0.5;
                if (entities.contains(queryEntity)) {
                    score += 0.5;
                }
            }
        } else {
            // Wildcard entities - give base score
            score += 0.3 * analysis.entities.size();
            maxScore += 0.3 * analysis.entities.size();
        }
        
        // Check capability match
        JsonArray capabilities = toolDef.getJsonArray("capabilities", new JsonArray());
        for (String requiredCap : analysis.capabilities) {
            maxScore += 0.7;
            if (capabilities.contains(requiredCap)) {
                score += 0.7;
            }
        }
        
        // Handle complexity
        if (analysis.complexity.equals("complex")) {
            if (capabilities.contains("complex_query") || capabilities.contains("intelligent")) {
                score += 1.0;
            }
            maxScore += 1.0;
        }
        
        // Normalize score
        return maxScore > 0 ? score / maxScore : 0;
    }
    
    /**
     * Validate tool selection with LLM
     */
    private Future<ToolDecision> validateToolSelection(String query, QueryAnalysis analysis, List<ToolCandidate> candidates) {
        String prompt = buildValidationPrompt(query, analysis, candidates);
        
        JsonArray messages = new JsonArray()
            .add(new JsonObject()
                .put("role", "system")
                .put("content", "You are an expert at selecting the right tools for database queries. " +
                              "Choose the optimal tool or combination of tools."))
            .add(new JsonObject()
                .put("role", "user")
                .put("content", prompt));
        
        return llmService.chatCompletion(messages)
            .map(response -> {
                JsonArray choices = response.getJsonArray("choices");
                if (choices != null && !choices.isEmpty()) {
                    String content = choices.getJsonObject(0)
                        .getJsonObject("message")
                        .getString("content");
                    
                    System.out.println("[ToolSelection] LLM validation response: " + content);
                    
                    try {
                        JsonObject decisionJson = new JsonObject(content);
                        ToolDecision decision = ToolDecision.fromJson(decisionJson);
                        System.out.println("[ToolSelection] LLM decision parsed: " + decision.strategy + 
                            " (tool: " + decision.primaryTool + ", orchestration: " + decision.orchestrationName + ")");
                        return decision;
                    } catch (Exception e) {
                        System.err.println("[ToolSelection] Failed to parse LLM response, using score-based: " + e.getMessage());
                        // Fallback to score-based selection
                        return selectByScore(candidates, analysis);
                    }
                }
                System.out.println("[ToolSelection] No LLM response, using score-based selection");
                return selectByScore(candidates, analysis);
            })
            .recover(err -> {
                System.err.println("[ToolSelection] LLM validation failed, using score-based: " + err.getMessage());
                return Future.succeededFuture(selectByScore(candidates, analysis));
            });
    }
    
    /**
     * Select tool by score (fallback when LLM validation fails)
     */
    private ToolDecision selectByScore(List<ToolCandidate> candidates, QueryAnalysis analysis) {
        ToolDecision decision = new ToolDecision();
        
        if (candidates.isEmpty()) {
            decision.strategy = ToolStrategy.STANDARD_LLM;
            decision.reasoning = "No matching tools found";
            return decision;
        }
        
        ToolCandidate best = candidates.get(0);
        
        // Check if this is the oracle_agent which should trigger orchestration
        if (best.toolName.equals("oracle_agent")) {
            // Oracle agent now means use the orchestration strategy
            decision.strategy = ToolStrategy.ORCHESTRATION;
            decision.orchestrationName = "oracle_full_pipeline";
            decision.reasoning = "Database query requiring Oracle orchestration pipeline";
        } else if (best.toolName.startsWith("_orchestration_")) {
            // This is an explicit orchestration strategy marker
            JsonObject strategyDef = toolRegistry.getJsonObject("tools").getJsonObject(best.toolName);
            decision.strategy = ToolStrategy.ORCHESTRATION;
            decision.orchestrationName = strategyDef.getString("strategy", "oracle_full_pipeline");
            decision.reasoning = "Using orchestration strategy: " + decision.orchestrationName;
        } else if (analysis.complexity.equals("complex") || best.score < 0.5) {
            // Complex query or low confidence - check if it's database related
            boolean isDatabaseQuery = false;
            for (String entity : analysis.entities) {
                if (entity.contains("order") || entity.contains("customer") || 
                    entity.contains("product") || entity.contains("table") ||
                    entity.contains("database") || entity.contains("sql")) {
                    isDatabaseQuery = true;
                    break;
                }
            }
            
            if (isDatabaseQuery) {
                decision.strategy = ToolStrategy.ORCHESTRATION;
                decision.orchestrationName = "oracle_full_pipeline";
                decision.reasoning = "Complex database query requiring full pipeline orchestration";
            } else {
                // Not database related, use standard LLM
                decision.strategy = ToolStrategy.STANDARD_LLM;
                decision.reasoning = "Complex non-database query, using standard LLM";
            }
        } else {
            // Use specific tool
            decision.strategy = ToolStrategy.SINGLE_TOOL;
            decision.primaryTool = best.toolName;
            decision.reasoning = "Direct tool match with score: " + best.score;
        }
        
        decision.confidence = best.score;
        decision.analysis = analysis;
        
        return decision;
    }
    
    /**
     * Build intent analysis prompt for LLM
     */
    private String buildIntentAnalysisPrompt(String query, JsonArray history) {
        StringBuilder prompt = new StringBuilder();
        prompt.append("Analyze this database query and extract structured information:\n\n");
        prompt.append("Query: \"").append(query).append("\"\n\n");
        
        if (!history.isEmpty()) {
            prompt.append("Recent conversation context:\n");
            int start = Math.max(0, history.size() - 3);
            for (int i = start; i < history.size(); i++) {
                JsonObject msg = history.getJsonObject(i);
                prompt.append("- ").append(msg.getString("role")).append(": ");
                prompt.append(msg.getString("content", "")).append("\n");
            }
            prompt.append("\n");
        }
        
        prompt.append("Extract the following in JSON format:\n");
        prompt.append("1. intent: Primary action (list, count, search, describe, aggregate, compare)\n");
        prompt.append("2. entities: Business entities and concepts mentioned\n");
        prompt.append("3. capabilities: Required capabilities (query, multi_table, aggregate, filter)\n");
        prompt.append("4. complexity: 'simple' or 'complex'\n");
        prompt.append("5. confidence: Your confidence level (0.0 to 1.0)\n\n");
        prompt.append("Return ONLY valid JSON.");
        
        return prompt.toString();
    }
    
    /**
     * Build validation prompt for LLM
     */
    private String buildValidationPrompt(String query, QueryAnalysis analysis, List<ToolCandidate> candidates) {
        StringBuilder prompt = new StringBuilder();
        prompt.append("Select the best tool(s) for this query:\n\n");
        prompt.append("Query: \"").append(query).append("\"\n\n");
        prompt.append("Analysis:\n").append(analysis.toJson().encodePrettily()).append("\n\n");
        prompt.append("Available tools (sorted by score):\n");
        
        for (int i = 0; i < Math.min(5, candidates.size()); i++) {
            ToolCandidate c = candidates.get(i);
            prompt.append(i + 1).append(". ").append(c.toolName);
            prompt.append(" (score: ").append(String.format("%.2f", c.score)).append(")\n");
            prompt.append("   ").append(c.description).append("\n");
        }
        
        prompt.append("\nReturn JSON with:\n");
        prompt.append("{\n");
        prompt.append("  \"strategy\": \"SINGLE_TOOL|MULTIPLE_TOOLS|ORCHESTRATION|STANDARD_LLM\",\n");
        prompt.append("  \"primaryTool\": \"tool_name (for SINGLE_TOOL)\",\n");
        prompt.append("  \"orchestrationName\": \"strategy_name (for ORCHESTRATION)\",\n");
        prompt.append("  \"additionalTools\": [],\n");
        prompt.append("  \"reasoning\": \"explanation\",\n");
        prompt.append("  \"confidence\": 0.0-1.0\n");
        prompt.append("}\n");
        
        return prompt.toString();
    }
    
    /**
     * Cache a tool decision
     */
    private void cacheDecision(String key, ToolDecision decision) {
        // Simple cache management - remove oldest if too large
        if (decisionCache.size() >= CACHE_SIZE) {
            String oldest = decisionCache.keySet().iterator().next();
            decisionCache.remove(oldest);
        }
        decisionCache.put(key, decision);
    }
    
    /**
     * Generate cache key for query
     */
    private String generateCacheKey(String query) {
        // Simple normalization for cache key
        return query.toLowerCase().trim().replaceAll("\\s+", " ");
    }
    
    /**
     * Handle configuration updates
     */
    private void handleConfigUpdate(Message<JsonObject> msg) {
        JsonObject update = msg.body();
        
        if (update.containsKey("llm_validation_enabled")) {
            llmValidationEnabled = update.getBoolean("llm_validation_enabled");
        }
        if (update.containsKey("default_confidence_threshold")) {
            defaultConfidenceThreshold = update.getDouble("default_confidence_threshold");
        }
        
        msg.reply(new JsonObject().put("updated", true));
    }
    
    /**
     * Query analysis result
     */
    private static class QueryAnalysis {
        String intent = "query";
        List<String> entities = new ArrayList<>();
        List<String> capabilities = new ArrayList<>();
        String complexity = "simple";
        double confidence = 0.5;
        
        static QueryAnalysis fromJson(JsonObject json) {
            QueryAnalysis analysis = new QueryAnalysis();
            analysis.intent = json.getString("intent", "query");
            analysis.confidence = json.getDouble("confidence", 0.5);
            analysis.complexity = json.getString("complexity", "simple");
            
            JsonArray entitiesArray = json.getJsonArray("entities", new JsonArray());
            for (int i = 0; i < entitiesArray.size(); i++) {
                analysis.entities.add(entitiesArray.getString(i));
            }
            
            JsonArray capsArray = json.getJsonArray("capabilities", new JsonArray());
            for (int i = 0; i < capsArray.size(); i++) {
                analysis.capabilities.add(capsArray.getString(i));
            }
            
            return analysis;
        }
        
        JsonObject toJson() {
            return new JsonObject()
                .put("intent", intent)
                .put("entities", new JsonArray(entities))
                .put("capabilities", new JsonArray(capabilities))
                .put("complexity", complexity)
                .put("confidence", confidence);
        }
    }
    
    /**
     * Tool candidate with score
     */
    private static class ToolCandidate {
        String toolName;
        double score;
        JsonArray capabilities;
        String description;
        String priority;
    }
    
    /**
     * Tool selection strategy
     */
    public enum ToolStrategy {
        SINGLE_TOOL,      // Use a single specific tool
        MULTIPLE_TOOLS,   // Use multiple tools in sequence
        ORCHESTRATION,    // Use an orchestration strategy (e.g., oracle_full_pipeline)
        STANDARD_LLM      // No tools needed, use standard LLM
    }
    
    /**
     * Tool selection decision
     */
    public static class ToolDecision {
        ToolStrategy strategy = ToolStrategy.STANDARD_LLM;
        String primaryTool;
        String orchestrationName;  // Name of orchestration strategy to use
        List<String> additionalTools = new ArrayList<>();
        String reasoning;
        double confidence = 0.5;
        QueryAnalysis analysis;
        long timestamp = System.currentTimeMillis();
        
        static ToolDecision fromJson(JsonObject json) {
            ToolDecision decision = new ToolDecision();
            
            String strategyStr = json.getString("strategy", "STANDARD_LLM");
            try {
                decision.strategy = ToolStrategy.valueOf(strategyStr);
            } catch (Exception e) {
                decision.strategy = ToolStrategy.STANDARD_LLM;
            }
            
            decision.primaryTool = json.getString("primaryTool", null);
            decision.orchestrationName = json.getString("orchestrationName", null);
            decision.reasoning = json.getString("reasoning");
            decision.confidence = json.getDouble("confidence", 0.5);
            
            JsonArray additional = json.getJsonArray("additionalTools", new JsonArray());
            for (int i = 0; i < additional.size(); i++) {
                decision.additionalTools.add(additional.getString(i));
            }
            
            // CRITICAL FIX: Convert oracle_agent from SINGLE_TOOL to ORCHESTRATION
            if (decision.strategy == ToolStrategy.SINGLE_TOOL && 
                "oracle_agent".equals(decision.primaryTool)) {
                System.out.println("[ToolSelection] Converting oracle_agent from SINGLE_TOOL to ORCHESTRATION");
                decision.strategy = ToolStrategy.ORCHESTRATION;
                decision.orchestrationName = "oracle_full_pipeline";
                decision.primaryTool = null; // Clear tool since we're using orchestration
                decision.reasoning = "Oracle agent requires full pipeline orchestration";
            }
            
            return decision;
        }
        
        public JsonObject toJson() {
            JsonObject json = new JsonObject()
                .put("strategy", strategy.toString())
                .put("reasoning", reasoning)
                .put("confidence", confidence)
                .put("timestamp", timestamp);
            
            if (primaryTool != null) {
                json.put("primaryTool", primaryTool);
            }
            
            if (orchestrationName != null) {
                json.put("orchestrationName", orchestrationName);
            }
            
            if (!additionalTools.isEmpty()) {
                json.put("additionalTools", new JsonArray(additionalTools));
            }
            
            if (analysis != null) {
                json.put("analysis", analysis.toJson());
            }
            
            return json;
        }
        
        boolean isValid() {
            // Cache validity - 5 minutes
            return (System.currentTimeMillis() - timestamp) < 300000;
        }
    }
}
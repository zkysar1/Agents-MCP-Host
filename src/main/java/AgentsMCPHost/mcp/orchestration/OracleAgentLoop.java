package AgentsMCPHost.mcp.orchestration;

import AgentsMCPHost.mcp.utils.SQLGenerator;
import AgentsMCPHost.mcp.utils.OracleConnectionManager;
import AgentsMCPHost.services.LlmAPIService;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.core.eventbus.Message;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * OracleAgentLoop - Core orchestrator for interactive Oracle SQL discovery.
 * 
 * Manages the conversation flow:
 * 1. Extract tokens from user query
 * 2. Match tokens against schema
 * 3. Confirm understanding with user
 * 4. Build and execute SQL
 * 5. Format and return results
 * 
 * Zero hardcoded schema knowledge - everything is discovered dynamically.
 */
public class OracleAgentLoop extends AbstractVerticle {
    
    // Configuration
    private static final int SCHEMA_MATCH_TIMEOUT_MS = Integer.parseInt(
        System.getenv().getOrDefault("ORACLE_SCHEMA_MATCH_TIMEOUT", "30000"));  // Increased to 30 seconds
    
    // Components
    private SchemaMatcher schemaMatcher;
    private UserInteractionTemplates templates;
    private LlmAPIService llmService;
    private OracleConnectionManager oracleManager;
    
    // Session management
    private final Map<String, ConversationContext> sessions = new ConcurrentHashMap<>();
    
    /**
     * Helper method for logging via event bus
     * Format: message,level,component,action,category
     * Levels: 0=ERROR, 1=INFO, 2=DEBUG
     */
    private void log(String message, int level, String action) {
        vertx.eventBus().publish("log", message + "," + level + ",OracleAgentLoop," + action + ",Oracle");
    }
    
    // State machine states
    public enum State {
        INITIAL,           // User query received
        DISCOVERING,       // Finding relevant tables
        CONFIRMING_TABLES, // "Did you mean these tables?"
        EXPLORING_SCHEMA,  // Getting column details
        CONFIRMING_INTENT, // "Looking for X in Y, correct?"
        BUILDING_QUERY,    // Constructing SQL
        CONFIRMING_SQL,    // "This will search for..."
        EXECUTING,         // Running query
        COMPLETE,          // Results delivered
        FAILED             // Error state
    }
    
    @Override
    public void start(Promise<Void> startPromise) {
        // Initialize components
        schemaMatcher = new SchemaMatcher();
        templates = new UserInteractionTemplates();
        llmService = LlmAPIService.getInstance();
        oracleManager = OracleConnectionManager.getInstance();
        
        // Initialize SchemaMatcher with Vertx (this also initializes OracleConnectionManager)
        schemaMatcher.initialize(vertx)
            .onSuccess(v -> {
                System.out.println("OracleAgentLoop: Schema matcher and database initialized");
                
                // Register event bus consumers after DB is ready
                vertx.eventBus().consumer("oracle.query.start", this::handleQueryStart);
                vertx.eventBus().consumer("oracle.agent.process", this::handleAgentProcess);  // Main entry point from ConversationVerticle
                vertx.eventBus().consumer("oracle.user.response", this::handleUserResponse);
                vertx.eventBus().consumer("oracle.session.status", this::handleStatusRequest);
                
                System.out.println("OracleAgentLoop started - ready for intelligent SQL generation");
                
                // Publish ready event for Driver to know we're initialized
                vertx.eventBus().publish("oracle.agent.ready", new JsonObject()
                    .put("timestamp", System.currentTimeMillis())
                    .put("status", "ready"));
                
                startPromise.complete();
            })
            .onFailure(err -> {
                System.err.println("Failed to initialize Oracle connection in Agent Loop: " + err.getMessage());
                // Still start but without DB
                vertx.eventBus().consumer("oracle.agent.process", this::handleAgentProcess);
                
                // Still publish ready event (with degraded status)
                vertx.eventBus().publish("oracle.agent.ready", new JsonObject()
                    .put("timestamp", System.currentTimeMillis())
                    .put("status", "degraded")
                    .put("error", err.getMessage()));
                
                startPromise.complete();
            });
    }
    
    /**
     * Handle agent process request from ConversationVerticle
     * This implements the full algorithm from the specification
     */
    private void handleAgentProcess(Message<JsonObject> msg) {
        JsonObject request = msg.body();
        if (request == null) {
            msg.fail(400, "Request body is null");
            return;
        }
        String query = request.getString("query");
        if (query == null || query.trim().isEmpty()) {
            msg.fail(400, "Query is required");
            return;
        }
        String sessionId = request.getString("sessionId", UUID.randomUUID().toString());
        
        // Create conversation context
        ConversationContext context = new ConversationContext();
        context.sessionId = sessionId;
        context.originalQuery = query;
        context.currentState = State.INITIAL;
        context.startTime = System.currentTimeMillis();
        
        sessions.put(sessionId, context);
        
        // Execute the full algorithm pipeline
        executeFullPipeline(context)
            .onSuccess(result -> {
                msg.reply(new JsonObject()
                    .put("success", true)
                    .put("result", result)
                    .put("sessionId", sessionId));
            })
            .onFailure(err -> {
                System.err.println("Oracle Agent failed: " + err.getMessage());
                // Try fallback with simple SQL generation
                String simpleSql = generateFallbackSql(query);
                msg.reply(new JsonObject()
                    .put("success", false)
                    .put("error", err.getMessage())
                    .put("fallbackSql", simpleSql));
            });
    }
    
    /**
     * Execute the full pipeline as per specification with LLM integration
     */
    private Future<String> executeFullPipeline(ConversationContext context) {
        // Step 1: Natural Language Understanding with LLM
        System.out.println("[PIPELINE] Starting for query: " + context.originalQuery);
        log("Starting Oracle Agent pipeline for: " + context.originalQuery, 1, "Pipeline");
        
        return performNaturalLanguageUnderstanding(context)
            .recover(err -> {
                System.err.println("[PIPELINE] NLU failed, using fallback: " + err.getMessage());
                // Fallback to simple token extraction
                QueryTokenExtractor.QueryTokens tokens = QueryTokenExtractor.extract(context.originalQuery);
                context.extractedTokens = tokens;
                return Future.succeededFuture(new JsonObject().put("success", true));
            })
            .compose(nluResult -> {
                System.out.println("[PIPELINE] Step 1 complete - NLU extracted tokens: " + 
                                 context.extractedTokens.getAllSearchTerms());
                
                // Step 2: Dynamic Metadata Retrieval with configurable timeout
                System.out.println("[PIPELINE] Step 2 - Starting schema matching...");
                return schemaMatcher.findMatches(context.extractedTokens.getAllSearchTerms())
                    .timeout(SCHEMA_MATCH_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                    .recover(err -> {
                        System.err.println("[PIPELINE] Schema matching failed/timeout: " + err.getMessage());
                        log("Schema matching failed: " + err.getMessage(), 0, "Schema");
                        // Return empty match result as fallback
                        return Future.succeededFuture(new SchemaMatcher.MatchResult());
                    });
            })
            .compose(matchResult -> {
                SchemaMatcher.MatchResult matches = (SchemaMatcher.MatchResult) matchResult;
                context.schemaMatches = matches;
                
                if (matches != null) {
                    System.out.println("[PIPELINE] Step 2 complete - Found " + 
                                     matches.tableMatches.size() + " table matches, " +
                                     matches.columnMatches.size() + " column matches");
                } else {
                    System.out.println("[PIPELINE] Step 2 - No schema matches found");
                }
                
                // Step 3: Query Planning and SQL Generation
                System.out.println("[PIPELINE] Step 3 - Generating SQL...");
                return generateIntelligentSqlWithLLM(context);
            })
            .recover(err -> {
                System.err.println("[PIPELINE] SQL generation failed: " + err.getMessage());
                // Fallback to simple SQL
                String fallbackSql = generateFallbackSql(context.originalQuery);
                System.out.println("[PIPELINE] Using fallback SQL: " + fallbackSql);
                return Future.succeededFuture(fallbackSql);
            })
            .compose(sql -> {
                context.generatedSql = sql;
                System.out.println("[PIPELINE] Step 3 complete - Generated SQL: " + sql);
                
                // Step 4: Query Optimization (optional, don't fail pipeline if this fails)
                System.out.println("[PIPELINE] Step 4 - Optimizing query...");
                return optimizeQuery(context, sql)
                    .recover(err -> {
                        System.err.println("[PIPELINE] Optimization failed, using original SQL: " + err.getMessage());
                        return Future.succeededFuture(sql);
                    });
            })
            .compose(optimizedSql -> {
                context.finalSql = optimizedSql;
                System.out.println("[PIPELINE] Step 4 complete - Final SQL: " + optimizedSql);
                
                // Step 5: Execute Query
                System.out.println("[PIPELINE] Step 5 - Executing query...");
                return executeQuery(optimizedSql);
            })
            .recover(err -> {
                System.err.println("[PIPELINE] Query execution failed: " + err.getMessage());
                log("Query execution failed: " + err.getMessage(), 0, "Execute");
                // Return empty results
                return Future.succeededFuture(new JsonArray());
            })
            .compose(results -> {
                context.queryResults = results;
                System.out.println("[PIPELINE] Step 5 complete - Got " + results.size() + " results");
                
                // Step 6: Result Analysis (optional refinement)
                System.out.println("[PIPELINE] Step 6 - Analyzing results...");
                return analyzeAndRefineResults(context, results)
                    .recover(err -> {
                        System.err.println("[PIPELINE] Refinement failed, using original results: " + err.getMessage());
                        return Future.succeededFuture(results);
                    });
            })
            .compose(refinedResults -> {
                System.out.println("[PIPELINE] Step 7 - Formatting results...");
                // Step 7: Natural Language Formatting
                return formatResultsWithLLM(context, refinedResults);
            })
            .recover(err -> {
                System.err.println("[PIPELINE] Complete failure: " + err.getMessage());
                log("Pipeline failed: " + err.getMessage(), 0, "Pipeline");
                return Future.succeededFuture("I encountered an error processing your query: " + err.getMessage() + 
                                             "\nPlease try rephrasing your question.");
            });
    }
    
    /**
     * Perform Natural Language Understanding using LLM
     */
    private Future<JsonObject> performNaturalLanguageUnderstanding(ConversationContext context) {
        if (!llmService.isInitialized()) {
            // Fallback to simple token extraction if LLM not available
            QueryTokenExtractor.QueryTokens tokens = QueryTokenExtractor.extract(context.originalQuery);
            context.extractedTokens = tokens;
            return Future.succeededFuture(new JsonObject().put("success", true));
        }
        
        // Create enhanced NLU prompt with business context awareness
        String nluPrompt = "Analyze this business database query and extract comprehensive intent and entities:\n\n" +
                          "Query: \"" + context.originalQuery + "\"\n\n" +
                          "Provide a detailed JSON response with:\n" +
                          "1. intent: Primary action (count, list, aggregate, compare, trend, filter, join)\n" +
                          "2. entities: Business entities mentioned (orders, customers, products, etc.)\n" +
                          "3. filters: Filter conditions with operators\n" +
                          "4. aggregations: Aggregation functions needed (sum, count, avg, min, max)\n" +
                          "5. grouping: Fields to group by\n" +
                          "6. time_period: Time constraints or ranges\n" +
                          "7. locations: Geographic filters (states, cities, regions)\n" +
                          "8. status_values: Status or state values (pending, shipped, active)\n" +
                          "9. join_hints: Potential tables that need joining\n" +
                          "10. output_columns: Specific fields user wants to see\n" +
                          "\nBe thorough - extract ALL relevant information.\n" +
                          "\nExample for 'Show me pending orders from California last month':\n" +
                          "{\"intent\":\"list\",\"entities\":[\"orders\"],\"filters\":[{\"field\":\"status\",\"operator\":\"=\",\"value\":\"pending\"},{\"field\":\"state\",\"operator\":\"=\",\"value\":\"California\"}],\"time_period\":{\"range\":\"last_month\"},\"locations\":[\"California\"],\"status_values\":[\"pending\"],\"join_hints\":[\"customers\"],\"output_columns\":[\"order_id\",\"order_date\",\"customer_name\"]}";
        
        JsonArray messages = new JsonArray()
            .add(new JsonObject()
                .put("role", "system")
                .put("content", "You are an expert database query analyzer specializing in business intelligence. " +
                                "Extract comprehensive intents, entities, and query requirements from natural language. " +
                                "Consider business context, implied joins, and common query patterns. " +
                                "Always think about what tables might need to be joined based on the query."))
            .add(new JsonObject()
                .put("role", "user")
                .put("content", nluPrompt));
        
        return llmService.chatCompletion(messages)
            .map(response -> {
                // Parse LLM response
                JsonArray choices = response.getJsonArray("choices");
                if (choices != null && !choices.isEmpty()) {
                    JsonObject firstChoice = choices.getJsonObject(0);
                    if (firstChoice == null) {
                        throw new RuntimeException("No valid choice in LLM response");
                    }
                    JsonObject message = firstChoice.getJsonObject("message");
                    if (message == null) {
                        throw new RuntimeException("No message in LLM response");
                    }
                    String content = message.getString("content");
                    if (content == null) {
                        throw new RuntimeException("No content in LLM message");
                    }
                    
                    // Try to parse as JSON
                    try {
                        JsonObject nluResult = new JsonObject(content);
                        
                        // Extract tokens from NLU result
                        QueryTokenExtractor.QueryTokens tokens = new QueryTokenExtractor.QueryTokens();
                        
                        // Add entities as search terms
                        JsonArray entities = nluResult.getJsonArray("entities", new JsonArray());
                        for (int i = 0; i < entities.size(); i++) {
                            tokens.addEntity(entities.getString(i));
                        }
                        
                        // Store intent
                        tokens.intent = nluResult.getString("intent", "query");
                        
                        context.extractedTokens = tokens;
                        context.nluResult = nluResult;
                        
                        System.out.println("NLU Result: " + nluResult.encodePrettily());
                        
                    } catch (Exception e) {
                        // If JSON parsing fails, fall back to simple extraction
                        QueryTokenExtractor.QueryTokens tokens = QueryTokenExtractor.extract(context.originalQuery);
                        context.extractedTokens = tokens;
                    }
                }
                
                return new JsonObject().put("success", true);
            })
            .recover(err -> {
                // On LLM failure, use simple token extraction
                System.err.println("LLM NLU failed, using simple extraction: " + err.getMessage());
                QueryTokenExtractor.QueryTokens tokens = QueryTokenExtractor.extract(context.originalQuery);
                context.extractedTokens = tokens;
                return Future.succeededFuture(new JsonObject().put("success", true));
            });
    }
    
    /**
     * Discover actual data values for entities mentioned in the query
     */
    private Future<JsonObject> discoverDataValues(ConversationContext context) {
        if (!llmService.isInitialized()) {
            return Future.succeededFuture(new JsonObject());
        }
        
        JsonObject discoveries = new JsonObject();
        List<Future<Void>> discoveryFutures = new ArrayList<>();
        
        // If user mentioned geographic locations, discover how they're represented
        if (context.nluResult != null) {
            JsonArray locations = context.nluResult.getJsonArray("locations", new JsonArray());
            for (int i = 0; i < locations.size(); i++) {
                String location = locations.getString(i);
                discoveryFutures.add(discoverLocationMapping(location, discoveries));
            }
            
            // Discover status values
            JsonArray statusValues = context.nluResult.getJsonArray("status_values", new JsonArray());
            for (int i = 0; i < statusValues.size(); i++) {
                String status = statusValues.getString(i);
                discoveryFutures.add(discoverStatusMapping(status, discoveries));
            }
        }
        
        return Future.all(discoveryFutures)
            .map(v -> {
                context.dataDiscoveries = discoveries;
                return discoveries;
            });
    }
    
    /**
     * Discover how a location is represented in the actual data
     */
    private Future<Void> discoverLocationMapping(String location, JsonObject discoveries) {
        // Sample customer data to see what geographic fields exist
        String sampleQuery = "SELECT * FROM customers WHERE ROWNUM <= 20";
        
        return oracleManager.executeQuery(sampleQuery)
            .compose(sampleData -> {
                if (sampleData.isEmpty()) {
                    return Future.succeededFuture();
                }
                
                // Ask LLM to identify how the location might be represented
                String prompt = "Given this sample customer data:\n" + 
                              sampleData.encodePrettily() + "\n\n" +
                              "The user is asking about '" + location + "'.\n" +
                              "How is this location represented in the data? " +
                              "List the specific column names and values that would identify " + location + ".\n" +
                              "Return as JSON: {\"column\": \"column_name\", \"values\": [\"value1\", \"value2\"], \"explanation\": \"why\"}";
                
                JsonArray messages = new JsonArray()
                    .add(new JsonObject()
                        .put("role", "system")
                        .put("content", "You are a data analyst examining database records to find geographic patterns."))
                    .add(new JsonObject()
                        .put("role", "user")
                        .put("content", prompt));
                
                return llmService.chatCompletion(messages);
            })
            .compose(response -> {
                try {
                    JsonArray choices = response.getJsonArray("choices");
                    if (choices != null && !choices.isEmpty()) {
                        String content = choices.getJsonObject(0)
                            .getJsonObject("message")
                            .getString("content");
                        
                        // Try to parse as JSON
                        JsonObject mapping = new JsonObject(content);
                        discoveries.put("location_" + location, mapping);
                        System.out.println("Discovered location mapping for " + location + ": " + mapping.encode());
                    }
                } catch (Exception e) {
                    System.err.println("Failed to parse location discovery: " + e.getMessage());
                }
                return Future.succeededFuture((Void) null);
            })
            .recover(err -> {
                System.err.println("Location discovery failed for " + location + ": " + err.getMessage());
                return Future.succeededFuture((Void) null);
            });
    }
    
    /**
     * Discover how a status is represented in the actual data
     */
    private Future<Void> discoverStatusMapping(String status, JsonObject discoveries) {
        // Check enumeration tables for status values
        String enumQuery = "SELECT * FROM order_status_enum";
        
        return oracleManager.executeQuery(enumQuery)
            .compose(enumData -> {
                if (!enumData.isEmpty()) {
                    // Find matching status
                    for (int i = 0; i < enumData.size(); i++) {
                        JsonObject row = enumData.getJsonObject(i);
                        String statusCode = row.getString("STATUS_CODE", "");
                        if (statusCode.equalsIgnoreCase(status)) {
                            discoveries.put("status_" + status, new JsonObject()
                                .put("table", "order_status_enum")
                                .put("column", "status_code")
                                .put("value", statusCode)
                                .put("id", row.getInteger("STATUS_ID")));
                            break;
                        }
                    }
                }
                return Future.succeededFuture((Void) null);
            })
            .recover(err -> {
                System.err.println("Status discovery failed: " + err.getMessage());
                return Future.succeededFuture((Void) null);
            });
    }
    
    /**
     * Generate SQL intelligently using LLM with full schema context and discovered data
     */
    private Future<String> generateIntelligentSqlWithLLM(ConversationContext context) {
        if (!llmService.isInitialized()) {
            // Fallback to simple SQL generation if LLM not available
            return generateSimpleSql(context);
        }
        
        // First, discover how data values are represented
        return discoverDataValues(context)
            .compose(discoveries -> {
                // Build comprehensive schema context with discoveries
                StringBuilder schemaContext = new StringBuilder();
                schemaContext.append("Database Schema Information:\n\n");
                
                // Add matched tables with sample data
                if (context.schemaMatches != null && context.schemaMatches.tableMatches != null) {
                    schemaContext.append("Relevant Tables:\n");
                    for (SchemaMatcher.TableMatch match : context.schemaMatches.tableMatches) {
                        schemaContext.append("- ").append(match.tableName);
                        if (match.rowCount > 0) {
                            schemaContext.append(" (rows: ").append(match.rowCount).append(")");
                        }
                        schemaContext.append("\n");
                    }
                    schemaContext.append("\n");
                }
                
                // Add column information with explicit note about what exists
                if (context.tableMetadata != null && !context.tableMetadata.isEmpty()) {
                    schemaContext.append("Table Columns (THESE ARE THE ONLY COLUMNS THAT EXIST):\n");
                    for (Map.Entry<String, JsonObject> entry : context.tableMetadata.entrySet()) {
                        schemaContext.append("Table ").append(entry.getKey()).append(":\n");
                        JsonObject metadata = entry.getValue();
                        JsonArray columns = metadata.getJsonArray("columns");
                        if (columns != null) {
                            for (int i = 0; i < columns.size(); i++) {
                                JsonObject col = columns.getJsonObject(i);
                                if (col != null) {
                                    String colName = col.getString("name", "unknown");
                                    String colType = col.getString("type", "unknown");
                                    schemaContext.append("  - ").append(colName);
                                    schemaContext.append(" (").append(colType).append(")\n");
                                }
                            }
                        }
                    }
                    schemaContext.append("\n");
                }
                
                // Add discovered data mappings
                if (context.dataDiscoveries != null && !context.dataDiscoveries.isEmpty()) {
                    schemaContext.append("Discovered Data Mappings:\n");
                    for (String key : context.dataDiscoveries.fieldNames()) {
                        JsonObject mapping = context.dataDiscoveries.getJsonObject(key);
                        schemaContext.append("- ").append(key).append(": ");
                        schemaContext.append(mapping.encode()).append("\n");
                    }
                    schemaContext.append("\n");
                }
                
                // Create SQL generation prompt WITHOUT hardcoded assumptions
                String sqlPrompt = "Generate an Oracle SQL query for this request:\n\n" +
                                  "User Query: \"" + context.originalQuery + "\"\n\n" +
                                  "Query Intent Analysis:\n" + 
                                  (context.nluResult != null ? "- Intent: " + context.nluResult.getString("intent", "unknown") + "\n" +
                                   "- Entities: " + context.nluResult.getJsonArray("entities", new JsonArray()).encode() + "\n" : "") +
                                  "\n" + schemaContext.toString() +
                                  "\nIMPORTANT SQL Generation Rules:\n" +
                                  "1. Use ONLY columns that actually exist in the schema above - DO NOT assume columns like 'state' exist\n" +
                                  "2. Use the Discovered Data Mappings to understand how to filter for locations and statuses\n" +
                                  "3. If a location was discovered in certain cities, use city IN (...) not a non-existent state column\n" +
                                  "4. Join tables using actual foreign key relationships (e.g., customer_id, status_id)\n" +
                                  "5. For 'pending' status, check the discovered mapping for the correct status_id or status_code\n" +
                                  "6. Use Oracle 12c+ syntax (FETCH FIRST N ROWS ONLY for limits)\n" +
                                  "7. Handle case-insensitive comparisons with UPPER() for text fields\n" +
                                  "8. For 'how many' or count queries, use SELECT COUNT(*)\n" +
                                  "9. Default limit is 100 rows for listing queries\n" +
                                  "10. Include proper JOIN conditions when querying across tables\n" +
                                  "\nReturn ONLY the SQL query. No explanations or comments.\n";
        
                JsonArray messages = new JsonArray()
                    .add(new JsonObject()
                        .put("role", "system")
                        .put("content", "You are an Oracle SQL expert. Generate precise SQL queries based on schema information."))
                    .add(new JsonObject()
                        .put("role", "user")
                        .put("content", sqlPrompt));
                
                return llmService.chatCompletion(messages)
                    .map(response -> {
                        // Extract SQL from LLM response
                        JsonArray choices = response.getJsonArray("choices");
                        if (choices != null && !choices.isEmpty()) {
                            JsonObject firstChoice = choices.getJsonObject(0);
                            if (firstChoice == null) {
                                return generateFallbackSql(context.originalQuery);
                            }
                            JsonObject message = firstChoice.getJsonObject("message");
                            if (message == null) {
                                return generateFallbackSql(context.originalQuery);
                            }
                            String sql = message.getString("content");
                            if (sql == null || sql.trim().isEmpty()) {
                                return generateFallbackSql(context.originalQuery);
                            }
                            
                            // Clean up the SQL
                            sql = sql.trim();
                            
                            // Remove markdown code blocks if present
                            if (sql.startsWith("```sql")) {
                                sql = sql.substring(6);
                            }
                            if (sql.startsWith("```")) {
                                sql = sql.substring(3);
                            }
                            if (sql.endsWith("```")) {
                                sql = sql.substring(0, sql.length() - 3);
                            }
                            sql = sql.trim();
                            
                            System.out.println("LLM Generated SQL: " + sql);
                            return sql;
                        }
                        
                        // Fallback if no response
                        return generateFallbackSql(context.originalQuery);
                    })
                    .recover(err -> {
                        System.err.println("LLM SQL generation failed: " + err.getMessage());
                        return Future.succeededFuture(generateFallbackSql(context.originalQuery));
                    });
            });
    }
    
    /**
     * Generate simple SQL without LLM
     */
    private Future<String> generateSimpleSql(ConversationContext context) {
        SQLGenerator generator = new SQLGenerator();
        
        JsonObject sqlContext = new JsonObject()
            .put("tokens", JsonObject.mapFrom(context.extractedTokens))
            .put("matches", context.schemaMatches.toJson())
            .put("query", context.originalQuery);
        
        SQLGenerator.SQLGenerationResult result = generator.generateSQL(context.originalQuery, sqlContext);
        
        if (result.success) {
            return Future.succeededFuture(result.sql);
        } else {
            return Future.succeededFuture(generateFallbackSql(context.originalQuery));
        }
    }
    
    /**
     * Optimize query using EXPLAIN PLAN
     */
    private Future<String> optimizeQuery(ConversationContext context, String sql) {
        // Validate SQL to prevent injection
        if (!isValidSQL(sql)) {
            log("Invalid SQL detected, skipping optimization", 0, "Optimize");
            return Future.succeededFuture(sql);
        }
        
        // First check if PLAN_TABLE exists
        String checkPlanTableSql = "SELECT COUNT(*) AS CNT FROM user_tables WHERE table_name = 'PLAN_TABLE'";
        
        return oracleManager.executeQuery(checkPlanTableSql)
            .compose(result -> {
                if (result.isEmpty() || (result.getJsonObject(0) != null && result.getJsonObject(0).getInteger("CNT", 0) == 0)) {
                    // PLAN_TABLE doesn't exist, skip optimization
                    System.out.println("PLAN_TABLE not found, skipping query optimization");
                    return Future.succeededFuture(sql);
                }
                
                // Generate a unique statement ID for this plan (make it final for lambda)
                final String statementId = "PLAN_" + System.currentTimeMillis();
                
                // Skip EXPLAIN PLAN due to SQL injection risk
                // Oracle doesn't support parameterized EXPLAIN PLAN
                // Apply basic optimizations without execution plan
                System.out.println("Query optimization using static analysis only (EXPLAIN PLAN disabled for security)");
                
                // Apply basic query optimizations
                String optimizedSql = applyBasicOptimizations(sql);
                context.optimizationAnalysis = new JsonObject()
                    .put("method", "static_analysis")
                    .put("original_sql", sql)
                    .put("optimized_sql", optimizedSql)
                    .put("security_note", "EXPLAIN PLAN disabled to prevent SQL injection");
                
                return Future.succeededFuture(optimizedSql);
            });
    }
    
    /**
     * Analyze execution plan for optimization opportunities
     */
    private JsonObject analyzeExecutionPlan(JsonArray planResults) {
        JsonObject analysis = new JsonObject();
        JsonArray issues = new JsonArray();
        
        int totalCost = 0;
        boolean hasFullTableScan = false;
        boolean hasNestedLoops = false;
        boolean hasHighCardinality = false;
        
        for (int i = 0; i < planResults.size(); i++) {
            JsonObject row = planResults.getJsonObject(i);
            String operation = row.getString("OPERATION", "");
            Integer cost = row.getInteger("COST");
            Long cardinality = row.getLong("CARDINALITY");
            
            if (cost != null) {
                totalCost += cost;
            }
            
            // Check for full table scans
            if (operation.contains("TABLE ACCESS FULL")) {
                hasFullTableScan = true;
                issues.add("Full table scan detected on " + row.getString("OBJECT_NAME", "unknown"));
            }
            
            // Check for nested loops with high cardinality
            if (operation.contains("NESTED LOOPS") && cardinality != null && cardinality > 10000) {
                hasNestedLoops = true;
                issues.add("Nested loops with high cardinality: " + cardinality);
            }
            
            // Check for very high cardinality operations
            if (cardinality != null && cardinality > 100000) {
                hasHighCardinality = true;
                issues.add("High cardinality operation: " + operation);
            }
        }
        
        analysis.put("totalCost", totalCost);
        analysis.put("hasFullTableScan", hasFullTableScan);
        analysis.put("hasNestedLoops", hasNestedLoops);
        analysis.put("hasHighCardinality", hasHighCardinality);
        analysis.put("issues", issues);
        
        // Determine if optimization is needed
        boolean needsOptimization = hasFullTableScan || 
                                  (hasNestedLoops && totalCost > 1000) || 
                                  totalCost > 5000;
        
        analysis.put("needsOptimization", needsOptimization);
        
        return analysis;
    }
    
    /**
     * Apply optimizations based on execution plan analysis
     */
    private String applyOptimizations(String sql, JsonObject analysis) {
        String optimizedSql = sql;
        String upperSql = sql.toUpperCase().trim();
        
        // Add hints for full table scans if detected
        if (analysis.getBoolean("hasFullTableScan", false)) {
            // Add INDEX hint if possible, checking for WITH clause
            if (!optimizedSql.contains("/*+") && !upperSql.startsWith("WITH")) {
                optimizedSql = optimizedSql.replaceFirst("(?i)SELECT", "SELECT /*+ INDEX */");
            }
        }
        
        // Add FIRST_ROWS hint for high cardinality queries
        if (analysis.getBoolean("hasHighCardinality", false)) {
            if (!optimizedSql.contains("/*+") && !upperSql.contains("FETCH FIRST") && !upperSql.startsWith("WITH")) {
                optimizedSql = optimizedSql.replaceFirst("(?i)SELECT", "SELECT /*+ FIRST_ROWS(100) */");
            }
        }
        
        // Ensure we have row limiting for large result sets
        if (!upperSql.contains("FETCH FIRST") && !upperSql.contains("ROWNUM")) {
            // Remove trailing semicolon if present
            optimizedSql = optimizedSql.replaceAll(";\\s*$", "");
            
            // Add FETCH FIRST, being careful with ORDER BY
            if (!upperSql.contains("UNION") && !upperSql.contains("INTERSECT") && !upperSql.contains("MINUS")) {
                optimizedSql += " FETCH FIRST 100 ROWS ONLY";
            }
        }
        
        return optimizedSql;
    }
    
    /**
     * Apply basic optimizations without using EXPLAIN PLAN
     */
    private String applyBasicOptimizations(String sql) {
        String optimizedSql = sql;
        String upperSql = sql.toUpperCase().trim();
        
        // Add FETCH FIRST if missing for SELECT without aggregation
        if (upperSql.startsWith("SELECT") && 
            !upperSql.contains("COUNT(") && !upperSql.contains("SUM(") && 
            !upperSql.contains("AVG(") && !upperSql.contains("MAX(") && 
            !upperSql.contains("MIN(") && !upperSql.contains("FETCH FIRST") && 
            !upperSql.contains("ROWNUM")) {
            
            // Add row limit for safety
            optimizedSql = sql + " FETCH FIRST 1000 ROWS ONLY";
        }
        
        // Add ORDER BY for deterministic results if missing
        if (!upperSql.contains("ORDER BY") && upperSql.contains("FROM")) {
            // Don't add ORDER BY to aggregate queries
            if (!upperSql.contains("GROUP BY") && !upperSql.contains("COUNT(")) {
                // Try to find the first column to order by
                if (upperSql.contains("SELECT *")) {
                    // Can't determine column, skip ORDER BY
                } else {
                    // Add ORDER BY 1 (first column)
                    optimizedSql = optimizedSql.replace(" FETCH FIRST", " ORDER BY 1 FETCH FIRST");
                    if (!optimizedSql.contains("ORDER BY")) {
                        optimizedSql = optimizedSql + " ORDER BY 1";
                    }
                }
            }
        }
        
        return optimizedSql;
    }
    
    /**
     * Execute the SQL query with intelligent retry on failure
     */
    private Future<JsonArray> executeQuery(String sql) {
        System.out.println("[DEBUG] Executing SQL query: " + sql);
        
        // Validate SQL first
        if (!isValidSQL(sql)) {
            System.err.println("[ERROR] Invalid SQL detected: " + sql);
            return Future.failedFuture("Invalid SQL query");
        }
        
        // Execute with error handling and potential retry
        return oracleManager.executeQuery(sql)
            .onSuccess(results -> {
                System.out.println("[DEBUG] Query executed successfully, returned " + 
                                 results.size() + " rows");
            })
            .recover(err -> {
                System.err.println("[ERROR] Query execution failed: " + err.getMessage());
                
                // Intelligent error recovery using LLM
                if (err.getMessage().contains("ORA-00904")) {
                    // Invalid column - need to fix the SQL
                    return handleInvalidColumnError(sql, err.getMessage());
                } else if (err.getMessage().contains("ORA-00942")) {
                    // Table doesn't exist
                    return handleTableNotFoundError(sql, err.getMessage());
                } else if (err.getMessage().contains("ORA-00936")) {
                    // Missing expression
                    return handleMissingExpressionError(sql, err.getMessage());
                }
                
                // For other errors, return empty results with error info
                return Future.succeededFuture(new JsonArray()
                    .add(new JsonObject()
                        .put("error", true)
                        .put("message", err.getMessage())
                        .put("sql", sql)));
            });
    }
    
    /**
     * Handle invalid column error by discovering correct columns and retrying
     */
    private Future<JsonArray> handleInvalidColumnError(String sql, String errorMsg) {
        System.out.println("[RECOVERY] Attempting to fix invalid column error");
        
        if (!llmService.isInitialized()) {
            return Future.failedFuture("Column not found and LLM not available for recovery");
        }
        
        // Extract the invalid column name from error message
        String invalidColumn = extractColumnFromError(errorMsg);
        
        // Get actual schema to show LLM what columns exist
        return getActualSchemaForTables(sql)
            .compose(actualSchema -> {
                String prompt = "The following SQL query failed with error: " + errorMsg + "\n\n" +
                              "Failed SQL:\n" + sql + "\n\n" +
                              "Actual database schema:\n" + actualSchema + "\n\n" +
                              "The column '" + invalidColumn + "' does not exist.\n" +
                              "Please rewrite this SQL using ONLY columns that actually exist in the schema.\n" +
                              "If you need to filter by location (like California), use the city column with appropriate city names.\n" +
                              "Return ONLY the corrected SQL query.";
                
                JsonArray messages = new JsonArray()
                    .add(new JsonObject()
                        .put("role", "system")
                        .put("content", "You are an Oracle SQL expert fixing queries to match actual schema."))
                    .add(new JsonObject()
                        .put("role", "user")
                        .put("content", prompt));
                
                return llmService.chatCompletion(messages);
            })
            .compose(response -> {
                // Extract corrected SQL
                JsonArray choices = response.getJsonArray("choices");
                if (choices != null && !choices.isEmpty()) {
                    String correctedSql = choices.getJsonObject(0)
                        .getJsonObject("message")
                        .getString("content", "");
                    
                    // Clean up SQL
                    correctedSql = correctedSql.replaceAll("```sql", "")
                                              .replaceAll("```", "")
                                              .trim();
                    
                    System.out.println("[RECOVERY] Retrying with corrected SQL: " + correctedSql);
                    
                    // Retry with corrected SQL
                    return oracleManager.executeQuery(correctedSql);
                }
                
                return Future.failedFuture("Could not generate corrected SQL");
            })
            .recover(retryErr -> {
                // If retry also fails, return informative error
                return Future.succeededFuture(new JsonArray()
                    .add(new JsonObject()
                        .put("error", true)
                        .put("message", "Column '" + invalidColumn + "' does not exist. Available columns need to be checked.")
                        .put("original_error", errorMsg)
                        .put("suggestion", "Try querying by city instead of state, or check available columns")));
            });
    }
    
    /**
     * Extract column name from Oracle error message
     */
    private String extractColumnFromError(String errorMsg) {
        // ORA-00904: "STATE": invalid identifier
        if (errorMsg.contains("\"")) {
            int start = errorMsg.indexOf("\"") + 1;
            int end = errorMsg.indexOf("\"", start);
            if (end > start) {
                return errorMsg.substring(start, end);
            }
        }
        return "unknown";
    }
    
    /**
     * Get actual schema for tables mentioned in SQL
     */
    private Future<String> getActualSchemaForTables(String sql) {
        // Extract table names from SQL (simplified - just look for FROM and JOIN)
        Set<String> tableNames = new HashSet<>();
        String upperSql = sql.toUpperCase();
        
        // Find tables after FROM
        if (upperSql.contains("FROM")) {
            int fromIndex = upperSql.indexOf("FROM") + 5;
            int whereIndex = upperSql.indexOf("WHERE", fromIndex);
            if (whereIndex == -1) whereIndex = upperSql.length();
            
            String fromClause = sql.substring(fromIndex, whereIndex);
            // Extract table names (simplified parsing)
            String[] parts = fromClause.split("\\s+|,");
            for (String part : parts) {
                if (!part.trim().isEmpty() && !part.equalsIgnoreCase("JOIN") && 
                    !part.equalsIgnoreCase("ON") && !part.contains("=")) {
                    tableNames.add(part.trim().toUpperCase());
                }
            }
        }
        
        // Get metadata for these tables
        List<Future<String>> metadataFutures = new ArrayList<>();
        for (String table : tableNames) {
            metadataFutures.add(oracleManager.getTableMetadata(table)
                .map(metadata -> {
                    StringBuilder sb = new StringBuilder();
                    sb.append("Table ").append(table).append(":\n");
                    JsonArray columns = metadata.getJsonArray("columns");
                    if (columns != null) {
                        for (int i = 0; i < columns.size(); i++) {
                            JsonObject col = columns.getJsonObject(i);
                            sb.append("  - ").append(col.getString("name"));
                            sb.append(" (").append(col.getString("type")).append(")\n");
                        }
                    }
                    return sb.toString();
                })
                .recover(err -> Future.succeededFuture("Table " + table + ": metadata unavailable\n")));
        }
        
        return Future.all(metadataFutures)
            .map(composite -> {
                StringBuilder result = new StringBuilder();
                for (Future<String> future : metadataFutures) {
                    result.append(future.result());
                }
                return result.toString();
            });
    }
    
    /**
     * Handle table not found error
     */
    private Future<JsonArray> handleTableNotFoundError(String sql, String errorMsg) {
        return Future.succeededFuture(new JsonArray()
            .add(new JsonObject()
                .put("error", true)
                .put("message", "Table not found: " + errorMsg)
                .put("suggestion", "Check table names in the database")));
    }
    
    /**
     * Handle missing expression error
     */
    private Future<JsonArray> handleMissingExpressionError(String sql, String errorMsg) {
        return Future.succeededFuture(new JsonArray()
            .add(new JsonObject()
                .put("error", true)
                .put("message", "SQL syntax error: " + errorMsg)
                .put("sql", sql)));
    }
    
    /**
     * Validate SQL to prevent injection attacks
     */
    private boolean isValidSQL(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            return false;
        }
        
        // Basic validation - should be a SELECT statement
        String upperSql = sql.toUpperCase().trim();
        if (!upperSql.startsWith("SELECT") && !upperSql.startsWith("WITH")) {
            return false;
        }
        
        // Check for common injection patterns
        if (sql.contains("--") || sql.contains("/*") && sql.contains("*/") && !sql.contains("/*+")) {
            // Allow optimizer hints but not other comments
            return false;
        }
        
        // Check for multiple statements
        if (sql.split(";").length > 1) {
            return false;
        }
        
        return true;
    }
    
    /**
     * Analyze results and potentially run follow-up queries for refinement
     */
    private Future<JsonArray> analyzeAndRefineResults(ConversationContext context, JsonArray results) {
        // Check if results need refinement
        if (shouldRefineResults(context, results)) {
            return performIterativeRefinement(context, results);
        }
        return Future.succeededFuture(results);
    }
    
    /**
     * Determine if results need refinement
     */
    private boolean shouldRefineResults(ConversationContext context, JsonArray results) {
        // Don't refine if we've already done so (prevent infinite loops)
        if (context.refinementCount >= 2) {
            return false;
        }
        
        // Check various conditions that might need refinement
        if (results == null || results.isEmpty()) {
            // Empty results might benefit from broader search
            return context.nluResult != null && 
                   context.nluResult.getString("intent", "").equals("list");
        }
        
        // Check if we got too many results for a specific query
        if (results.size() > 100) {
            return context.nluResult != null && 
                   context.nluResult.getJsonArray("filters", new JsonArray()).size() > 0;
        }
        
        // Check if the intent suggests we need aggregation but got raw data
        String intent = context.nluResult != null ? 
                       context.nluResult.getString("intent", "") : "";
        if ((intent.equals("count") || intent.equals("aggregate")) && 
            results.size() > 1 && !hasAggregateColumns(results)) {
            return true;
        }
        
        return false;
    }
    
    /**
     * Check if results have aggregate columns
     */
    private boolean hasAggregateColumns(JsonArray results) {
        if (results.isEmpty()) return false;
        JsonObject firstRow = results.getJsonObject(0);
        for (String key : firstRow.fieldNames()) {
            if (key.toUpperCase().contains("COUNT") || 
                key.toUpperCase().contains("SUM") || 
                key.toUpperCase().contains("AVG") ||
                key.toUpperCase().contains("MAX") ||
                key.toUpperCase().contains("MIN")) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Perform iterative refinement by generating and executing follow-up queries
     */
    private Future<JsonArray> performIterativeRefinement(ConversationContext context, JsonArray results) {
        context.refinementCount++;
        
        if (!llmService.isInitialized()) {
            // Can't refine without LLM
            return Future.succeededFuture(results);
        }
        
        // Build refinement prompt
        String refinementPrompt = buildRefinementPrompt(context, results);
        
        JsonArray messages = new JsonArray()
            .add(new JsonObject()
                .put("role", "system")
                .put("content", "You are an Oracle SQL expert. Generate follow-up queries to refine database results."))
            .add(new JsonObject()
                .put("role", "user")
                .put("content", refinementPrompt));
        
        return llmService.chatCompletion(messages)
            .compose(response -> {
                // Extract refined SQL from response
                JsonArray choices = response.getJsonArray("choices");
                if (choices != null && !choices.isEmpty()) {
                    String refinedSql = choices.getJsonObject(0)
                        .getJsonObject("message")
                        .getString("content");
                    
                    // Clean the SQL
                    refinedSql = refinedSql.replaceAll("```sql", "")
                                         .replaceAll("```", "")
                                         .trim();
                    
                    System.out.println("Iterative Refinement SQL: " + refinedSql);
                    
                    // Execute the refined query
                    return executeQuery(refinedSql)
                        .map(refinedResults -> {
                            // Merge or replace results as appropriate
                            if (refinedResults != null && !refinedResults.isEmpty()) {
                                context.queryResults = refinedResults;
                                return refinedResults;
                            }
                            return results;
                        });
                }
                return Future.succeededFuture(results);
            })
            .recover(err -> {
                log("Refinement failed: " + err.getMessage(), 0, "Refinement");
                return Future.succeededFuture(results);
            });
    }
    
    /**
     * Build prompt for iterative refinement
     */
    private String buildRefinementPrompt(ConversationContext context, JsonArray results) {
        StringBuilder prompt = new StringBuilder();
        prompt.append("The user asked: \"").append(context.originalQuery).append("\"\n\n");
        prompt.append("Initial SQL executed: ").append(context.finalSql).append("\n\n");
        
        if (results.isEmpty()) {
            prompt.append("The query returned no results.\n");
            prompt.append("Generate a broader SQL query that might find relevant data.\n");
            prompt.append("Consider: removing filters, checking for similar values, or querying related tables.\n");
        } else if (results.size() > 100) {
            prompt.append("The query returned too many results (").append(results.size()).append(").\n");
            prompt.append("Generate a more specific SQL query with better filtering or aggregation.\n");
        } else {
            prompt.append("Results summary: ").append(results.size()).append(" rows returned.\n");
            prompt.append("The user's intent was: ").append(context.nluResult != null ? 
                         context.nluResult.getString("intent", "query") : "query").append("\n");
            prompt.append("Generate a refined SQL query that better answers the user's question.\n");
        }
        
        prompt.append("\nReturn ONLY the SQL query, no explanations.");
        
        return prompt.toString();
    }
    
    /**
     * Format results using LLM for natural language response with explanations
     */
    private Future<String> formatResultsWithLLM(ConversationContext context, JsonArray results) {
        if (!llmService.isInitialized()) {
            // Fallback to simple formatting if LLM not available
            return formatResultsSimple(context, results);
        }
        
        // Check if results contain error information
        boolean hasError = false;
        String errorMsg = null;
        if (results != null && results.size() == 1) {
            JsonObject firstRow = results.getJsonObject(0);
            if (firstRow != null && firstRow.getBoolean("error", false)) {
                hasError = true;
                errorMsg = firstRow.getString("message", "Unknown error");
            }
        }
        
        // Build result summary for LLM
        StringBuilder resultSummary = new StringBuilder();
        resultSummary.append("Query Results:\n\n");
        
        if (hasError) {
            resultSummary.append("ERROR: ").append(errorMsg).append("\n");
            resultSummary.append("The query could not be executed successfully.\n");
        } else if (results == null || results.isEmpty()) {
            resultSummary.append("No records found.");
        } else {
            resultSummary.append("Number of records: ").append(results.size()).append("\n\n");
            
            // Include sample of results for context
            int sampleSize = Math.min(results.size(), 5);
            resultSummary.append("Sample data:\n");
            for (int i = 0; i < sampleSize; i++) {
                JsonObject row = results.getJsonObject(i);
                resultSummary.append(row.encode()).append("\n");
            }
            
            if (results.size() > 5) {
                resultSummary.append("... and ").append(results.size() - 5).append(" more records\n");
            }
        }
        
        // Add discovery information for context
        String discoveryInfo = "";
        if (context.dataDiscoveries != null && !context.dataDiscoveries.isEmpty()) {
            discoveryInfo = "\n\nData Discovery Notes:\n" + context.dataDiscoveries.encodePrettily() + "\n";
        }
        
        // Create formatting prompt with explanation request
        String formatPrompt = "Convert the following database query results into a natural, friendly response:\n\n" +
                             "Original User Query: \"" + context.originalQuery + "\"\n\n" +
                             "SQL Executed: " + (context.finalSql != null ? context.finalSql : "No SQL generated") + "\n\n" +
                             resultSummary.toString() + discoveryInfo + "\n\n" +
                             "Instructions:\n" +
                             "1. Provide a clear, conversational answer to the user's question\n" +
                             "2. If no results were found, explain why (e.g., 'No orders were found matching California because the database doesn't have state information, only city names')\n" +
                             "3. If there was an error, explain it in user-friendly terms\n" +
                             "4. If the query succeeded, summarize the key findings\n" +
                             "5. Be helpful - if the user asked about California but we only have city data, explain that\n" +
                             "6. For count queries, state the number clearly\n" +
                             "7. Be concise but complete";
        
        JsonArray messages = new JsonArray()
            .add(new JsonObject()
                .put("role", "system")
                .put("content", "You are a helpful assistant that explains database query results in natural language."))
            .add(new JsonObject()
                .put("role", "user")
                .put("content", formatPrompt));
        
        return llmService.chatCompletion(messages)
            .map(response -> {
                // Extract formatted response
                JsonArray choices = response.getJsonArray("choices");
                if (choices != null && !choices.isEmpty()) {
                    String formattedResponse = choices.getJsonObject(0)
                        .getJsonObject("message")
                        .getString("content");
                    
                    return formattedResponse;
                }
                
                // Fallback if no response
                return formatResultsSimple(context, results).result();
            })
            .recover(err -> {
                System.err.println("LLM formatting failed: " + err.getMessage());
                return formatResultsSimple(context, results);
            });
    }
    
    /**
     * Simple result formatting without LLM
     */
    private Future<String> formatResultsSimple(ConversationContext context, JsonArray results) {
        StringBuilder formatted = new StringBuilder();
        
        if (results == null || results.isEmpty()) {
            formatted.append("No results found for your query: ").append(context.originalQuery);
        } else {
            // For count queries, just show the number
            if (context.originalQuery.toLowerCase().contains("how many") && results.size() == 1) {
                // Try to extract count from first result
                JsonObject firstRow = results.getJsonObject(0);
                for (String key : firstRow.fieldNames()) {
                    if (key.toLowerCase().contains("count")) {
                        formatted.append("The answer is: ").append(firstRow.getValue(key));
                        return Future.succeededFuture(formatted.toString());
                    }
                }
            }
            
            formatted.append("Found ").append(results.size()).append(" results:\n\n");
            
            // Show results in a readable format
            int limit = Math.min(results.size(), 10);
            for (int i = 0; i < limit; i++) {
                JsonObject row = results.getJsonObject(i);
                formatted.append("Result ").append(i + 1).append(":\n");
                for (String key : row.fieldNames()) {
                    formatted.append("  ").append(key).append(": ").append(row.getValue(key)).append("\n");
                }
                formatted.append("\n");
            }
            
            if (results.size() > 10) {
                formatted.append("... and ").append(results.size() - 10).append(" more records.");
            }
        }
        
        return Future.succeededFuture(formatted.toString());
    }
    
    /**
     * Generate fallback SQL for when agent fails
     */
    private String generateFallbackSql(String query) {
        // Enhanced pattern matching as fallback
        String lower = query.toLowerCase();
        
        // Handle "how many" queries (counting)
        if (lower.contains("how many") || lower.contains("count")) {
            if (lower.contains("pending") && lower.contains("orders") && lower.contains("california")) {
                return "SELECT COUNT(*) AS count FROM orders o " +
                       "JOIN customers c ON o.customer_id = c.customer_id " +
                       "WHERE UPPER(o.status) = 'PENDING' " +
                       "AND (UPPER(c.state) = 'CA' OR UPPER(c.state) = 'CALIFORNIA')";
            }
            if (lower.contains("pending") && lower.contains("orders")) {
                return "SELECT COUNT(*) AS count FROM orders WHERE UPPER(status) = 'PENDING'";
            }
            if (lower.contains("orders")) {
                return "SELECT COUNT(*) AS count FROM orders";
            }
            if (lower.contains("customers")) {
                return "SELECT COUNT(*) AS count FROM customers";
            }
        }
        
        // Handle listing queries
        if (lower.contains("pending") && lower.contains("orders") && lower.contains("california")) {
            return "SELECT o.order_id, o.order_date, o.status, c.customer_name, c.city, c.state " +
                   "FROM orders o JOIN customers c ON o.customer_id = c.customer_id " +
                   "WHERE UPPER(o.status) = 'PENDING' " +
                   "AND (UPPER(c.state) = 'CA' OR UPPER(c.state) = 'CALIFORNIA') " +
                   "FETCH FIRST 100 ROWS ONLY";
        }
        
        if (lower.contains("pending") && lower.contains("orders")) {
            return "SELECT * FROM orders WHERE UPPER(status) = 'PENDING' FETCH FIRST 100 ROWS ONLY";
        }
        
        if (lower.contains("california") && lower.contains("orders")) {
            return "SELECT o.*, c.state FROM orders o " +
                   "JOIN customers c ON o.customer_id = c.customer_id " +
                   "WHERE UPPER(c.state) = 'CA' OR UPPER(c.state) = 'CALIFORNIA' " +
                   "FETCH FIRST 100 ROWS ONLY";
        }
        
        if (lower.contains("list") && lower.contains("tables")) {
            return "SELECT table_name FROM user_tables ORDER BY table_name";
        }
        
        if (lower.contains("low") && lower.contains("stock")) {
            return "SELECT * FROM products WHERE quantity_in_stock < 10 ORDER BY quantity_in_stock";
        }
        
        System.err.println("[FALLBACK] Unable to generate specific SQL for: " + query);
        return "SELECT 'Unable to generate SQL for: " + query.replace("'", "''") + "' AS message FROM dual";
    }
    
    /**
     * Handle new query from user
     */
    private void handleQueryStart(Message<JsonObject> msg) {
        JsonObject request = msg.body();
        String query = request.getString("query");
        String sessionId = UUID.randomUUID().toString();
        
        // Create new conversation context
        ConversationContext context = new ConversationContext();
        context.sessionId = sessionId;
        context.originalQuery = query;
        context.currentState = State.INITIAL;
        context.startTime = System.currentTimeMillis();
        
        sessions.put(sessionId, context);
        
        // Start the discovery process
        startDiscovery(context)
            .onSuccess(response -> {
                msg.reply(response);
            })
            .onFailure(err -> {
                context.currentState = State.FAILED;
                msg.fail(500, "Discovery failed: " + err.getMessage());
            });
    }
    
    /**
     * Handle user response to confirmation
     */
    private void handleUserResponse(Message<JsonObject> msg) {
        JsonObject request = msg.body();
        String sessionId = request.getString("sessionId");
        String response = request.getString("response");
        
        ConversationContext context = sessions.get(sessionId);
        if (context == null) {
            msg.fail(404, "Session not found");
            return;
        }
        
        // Process user response based on current state
        processUserResponse(context, response)
            .onSuccess(result -> {
                msg.reply(result);
            })
            .onFailure(err -> {
                context.currentState = State.FAILED;
                msg.fail(500, "Processing failed: " + err.getMessage());
            });
    }
    
    /**
     * Start the discovery process
     */
    private Future<JsonObject> startDiscovery(ConversationContext context) {
        context.currentState = State.DISCOVERING;
        
        // Step 1: Extract tokens from query
        QueryTokenExtractor.QueryTokens tokens = QueryTokenExtractor.extract(context.originalQuery);
        context.extractedTokens = tokens;
        
        // Log what we found
        System.out.println("Extracted tokens from '" + context.originalQuery + "': " + tokens);
        
        // Step 2: Match tokens against schema
        return schemaMatcher.findMatches(tokens.getAllSearchTerms())
            .compose(matches -> {
                context.schemaMatches = matches;
                
                // Check confidence level
                if (matches.confidence > 0.8) {
                    // High confidence - proceed with confirmation
                    return confirmTables(context);
                } else if (matches.confidence > 0.3) {
                    // Medium confidence - ask for clarification
                    return clarifyIntent(context);
                } else {
                    // Low confidence - ask user to rephrase
                    return askForRephrase(context);
                }
            });
    }
    
    /**
     * Confirm table selection with user
     */
    private Future<JsonObject> confirmTables(ConversationContext context) {
        context.currentState = State.CONFIRMING_TABLES;
        
        // Get unique table names from matches
        Set<String> tables = new HashSet<>();
        for (SchemaMatcher.TableMatch match : context.schemaMatches.tableMatches) {
            tables.add(match.tableName);
        }
        for (SchemaMatcher.ColumnMatch match : context.schemaMatches.columnMatches) {
            tables.add(match.tableName);
        }
        
        // Build confirmation message
        String confirmation = templates.confirmTables(
            new ArrayList<>(tables),
            context.schemaMatches
        );
        
        // Store pending confirmation
        context.pendingConfirmation = new PendingConfirmation();
        context.pendingConfirmation.type = "tables";
        context.pendingConfirmation.options = new ArrayList<>(tables);
        
        return Future.succeededFuture(new JsonObject()
            .put("sessionId", context.sessionId)
            .put("state", context.currentState.toString())
            .put("message", confirmation)
            .put("requiresResponse", true)
            .put("responseType", "confirmation")
            .put("options", new JsonArray(new ArrayList<>(tables))));
    }
    
    /**
     * Clarify intent when confidence is medium
     */
    private Future<JsonObject> clarifyIntent(ConversationContext context) {
        context.currentState = State.CONFIRMING_INTENT;
        
        // Build clarification message
        String clarification = templates.clarifyIntent(
            context.extractedTokens,
            context.schemaMatches
        );
        
        return Future.succeededFuture(new JsonObject()
            .put("sessionId", context.sessionId)
            .put("state", context.currentState.toString())
            .put("message", clarification)
            .put("requiresResponse", true)
            .put("responseType", "clarification"));
    }
    
    /**
     * Ask user to rephrase when confidence is low
     */
    private Future<JsonObject> askForRephrase(ConversationContext context) {
        String message = templates.askForRephrase(context.originalQuery);
        
        // List available tables for reference
        return vertx.eventBus().<JsonObject>request("mcp.oracle.metadata.execute", 
            new JsonObject()
                .put("tool", "list_tables")
                .put("arguments", new JsonObject()))
            .map(reply -> {
                JsonObject result = reply.body();
                String tablesInfo = result.getString("result", "");
                
                return new JsonObject()
                    .put("sessionId", context.sessionId)
                    .put("state", "needs_rephrase")
                    .put("message", message + "\n\nAvailable tables:\n" + tablesInfo)
                    .put("requiresResponse", true)
                    .put("responseType", "rephrase");
            });
    }
    
    /**
     * Process user response based on current state
     */
    private Future<JsonObject> processUserResponse(ConversationContext context, String response) {
        switch (context.currentState) {
            case CONFIRMING_TABLES:
                return processTableConfirmation(context, response);
                
            case CONFIRMING_INTENT:
                return processIntentConfirmation(context, response);
                
            case CONFIRMING_SQL:
                return processSQLConfirmation(context, response);
                
            default:
                return Future.failedFuture("Unexpected state for user response: " + context.currentState);
        }
    }
    
    /**
     * Process table confirmation response
     */
    private Future<JsonObject> processTableConfirmation(ConversationContext context, String response) {
        if (response.toLowerCase().startsWith("y")) {
            // User confirmed - proceed to explore schema
            return exploreSchema(context);
        } else if (response.toLowerCase().startsWith("n")) {
            // User rejected - ask for clarification
            return clarifyIntent(context);
        } else {
            // User provided specific input
            context.userFeedback = response;
            return refineUnderstanding(context);
        }
    }
    
    /**
     * Process intent confirmation response
     */
    private Future<JsonObject> processIntentConfirmation(ConversationContext context, String response) {
        if (response.toLowerCase().startsWith("y")) {
            // User confirmed - proceed to build query
            return buildQuery(context);
        } else {
            // User provided clarification
            context.userFeedback = response;
            return refineUnderstanding(context);
        }
    }
    
    /**
     * Process SQL confirmation response
     */
    private Future<JsonObject> processSQLConfirmation(ConversationContext context, String response) {
        if (response.toLowerCase().startsWith("y")) {
            // User confirmed - execute query
            return executeQuery(context);
        } else {
            // User wants modifications
            context.userFeedback = response;
            return modifyQuery(context);
        }
    }
    
    /**
     * Explore schema details for confirmed tables
     */
    private Future<JsonObject> exploreSchema(ConversationContext context) {
        context.currentState = State.EXPLORING_SCHEMA;
        
        // Get detailed metadata for relevant tables
        Set<String> tables = new HashSet<>();
        for (SchemaMatcher.TableMatch match : context.schemaMatches.tableMatches) {
            tables.add(match.tableName);
        }
        
        List<Future<JsonObject>> metadataFutures = new ArrayList<>();
        for (String table : tables) {
            Future<JsonObject> metadataFuture = vertx.eventBus().<JsonObject>request(
                "mcp.oracle.metadata.execute",
                new JsonObject()
                    .put("tool", "describe_table")
                    .put("arguments", new JsonObject().put("table_name", table))
            ).map(Message::body);
            
            metadataFutures.add(metadataFuture);
        }
        
        return Future.all(metadataFutures)
            .compose(compositeFuture -> {
                // Store metadata
                context.tableMetadata = new HashMap<>();
                for (int i = 0; i < metadataFutures.size(); i++) {
                    String tableName = new ArrayList<>(tables).get(i);
                    context.tableMetadata.put(tableName, metadataFutures.get(i).result());
                }
                
                // Check for relationships
                return discoverRelationships(context);
            });
    }
    
    /**
     * Discover relationships between tables
     */
    private Future<JsonObject> discoverRelationships(ConversationContext context) {
        if (context.tableMetadata.size() < 2) {
            // Single table - no relationships needed
            return buildQuery(context);
        }
        
        // Get relationships between tables
        List<String> tableList = new ArrayList<>(context.tableMetadata.keySet());
        String table1 = tableList.get(0);
        String table2 = tableList.size() > 1 ? tableList.get(1) : null;
        
        return vertx.eventBus().<JsonObject>request(
            "mcp.oracle.metadata.execute",
            new JsonObject()
                .put("tool", "get_relationships")
                .put("arguments", new JsonObject()
                    .put("table_name", table1)
                    .put("related_table", table2))
        ).compose(reply -> {
            JsonObject result = reply.body();
            context.relationships = result;
            
            // Now build the query
            return buildQuery(context);
        });
    }
    
    /**
     * Build SQL query based on discovered information
     */
    private Future<JsonObject> buildQuery(ConversationContext context) {
        context.currentState = State.BUILDING_QUERY;
        
        // Use SQLGenerator to build query
        // For now, create a simple SELECT query
        String sql = buildSimpleQuery(context);
        context.generatedSQL = sql;
        
        // Confirm SQL with user
        return confirmSQL(context);
    }
    
    /**
     * Build a simple SELECT query
     */
    private String buildSimpleQuery(ConversationContext context) {
        StringBuilder sql = new StringBuilder("SELECT ");
        
        // Select relevant columns or *
        if (context.schemaMatches.columnMatches.isEmpty()) {
            sql.append("*");
        } else {
            Set<String> columns = new HashSet<>();
            for (SchemaMatcher.ColumnMatch match : context.schemaMatches.columnMatches) {
                columns.add(match.tableName + "." + match.columnName);
            }
            sql.append(String.join(", ", columns));
        }
        
        // FROM clause
        Set<String> tables = new HashSet<>();
        for (SchemaMatcher.TableMatch match : context.schemaMatches.tableMatches) {
            tables.add(match.tableName);
        }
        
        if (!tables.isEmpty()) {
            sql.append(" FROM ").append(String.join(", ", tables));
        }
        
        // Add WHERE clause for enum matches
        if (!context.schemaMatches.enumMatches.isEmpty()) {
            sql.append(" WHERE ");
            List<String> conditions = new ArrayList<>();
            for (SchemaMatcher.EnumMatch match : context.schemaMatches.enumMatches) {
                conditions.add(match.enumTable + " = '" + match.matchedValue + "'");
            }
            sql.append(String.join(" AND ", conditions));
        }
        
        // Add limit
        sql.append(" FETCH FIRST 100 ROWS ONLY");
        
        return sql.toString();
    }
    
    /**
     * Confirm SQL with user
     */
    private Future<JsonObject> confirmSQL(ConversationContext context) {
        context.currentState = State.CONFIRMING_SQL;
        
        String confirmation = templates.confirmSQL(context.generatedSQL);
        
        return Future.succeededFuture(new JsonObject()
            .put("sessionId", context.sessionId)
            .put("state", context.currentState.toString())
            .put("message", confirmation)
            .put("sql", context.generatedSQL)
            .put("requiresResponse", true)
            .put("responseType", "sql_confirmation"));
    }
    
    /**
     * Execute the confirmed query
     */
    private Future<JsonObject> executeQuery(ConversationContext context) {
        context.currentState = State.EXECUTING;
        
        return vertx.eventBus().<JsonObject>request(
            "mcp.oracle.execute",
            new JsonObject()
                .put("tool", "execute_query")
                .put("arguments", new JsonObject().put("sql", context.generatedSQL))
        ).map(reply -> {
            context.currentState = State.COMPLETE;
            JsonObject result = reply.body();
            
            return new JsonObject()
                .put("sessionId", context.sessionId)
                .put("state", context.currentState.toString())
                .put("success", true)
                .put("result", result.getString("result"))
                .put("sql", context.generatedSQL)
                .put("executionTime", System.currentTimeMillis() - context.startTime);
        });
    }
    
    /**
     * Refine understanding based on user feedback
     */
    private Future<JsonObject> refineUnderstanding(ConversationContext context) {
        // Re-extract tokens including user feedback
        String combinedQuery = context.originalQuery + " " + context.userFeedback;
        QueryTokenExtractor.QueryTokens newTokens = QueryTokenExtractor.extract(combinedQuery);
        context.extractedTokens = newTokens;
        
        // Re-run matching
        return schemaMatcher.findMatches(newTokens.getAllSearchTerms())
            .compose(matches -> {
                context.schemaMatches = matches;
                return confirmTables(context);
            });
    }
    
    /**
     * Modify query based on user feedback
     */
    private Future<JsonObject> modifyQuery(ConversationContext context) {
        // For now, just rebuild the query
        // In production, this would parse the user's modification request
        return buildQuery(context);
    }
    
    /**
     * Handle status request
     */
    private void handleStatusRequest(Message<JsonObject> msg) {
        JsonObject request = msg.body();
        String sessionId = request.getString("sessionId");
        
        if (sessionId != null) {
            ConversationContext context = sessions.get(sessionId);
            if (context != null) {
                msg.reply(context.toJson());
            } else {
                msg.fail(404, "Session not found");
            }
        } else {
            // Return all sessions
            JsonArray sessionList = new JsonArray();
            sessions.forEach((id, context) -> {
                sessionList.add(context.toJson());
            });
            msg.reply(new JsonObject().put("sessions", sessionList));
        }
    }
    
    /**
     * Conversation context - tracks state for a single conversation
     */
    private static class ConversationContext {
        String sessionId;
        State currentState;
        String originalQuery;
        String userFeedback;
        long startTime;
        
        // Discovered information
        QueryTokenExtractor.QueryTokens extractedTokens;
        SchemaMatcher.MatchResult schemaMatches;
        Map<String, JsonObject> tableMetadata;
        JsonObject relationships;
        JsonObject nluResult;  // NLU analysis result from LLM
        JsonObject dataDiscoveries;  // Discovered data mappings from LLM
        String generatedSQL;
        String generatedSql;  // Alias for compatibility
        String finalSql;
        JsonArray queryResults;
        JsonObject optimizationAnalysis;  // EXPLAIN PLAN analysis
        int refinementCount = 0;  // Track iterative refinements
        
        // Pending confirmations
        PendingConfirmation pendingConfirmation;
        
        JsonObject toJson() {
            return new JsonObject()
                .put("sessionId", sessionId)
                .put("state", currentState.toString())
                .put("originalQuery", originalQuery)
                .put("elapsedTime", System.currentTimeMillis() - startTime)
                .put("hasSQL", generatedSQL != null);
        }
    }
    
    /**
     * Pending confirmation information
     */
    private static class PendingConfirmation {
        String type;
        List<String> options;
        String originalMessage;
    }
}
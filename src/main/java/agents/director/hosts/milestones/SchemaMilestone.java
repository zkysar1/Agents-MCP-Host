package agents.director.hosts.milestones;

import agents.director.hosts.base.MilestoneContext;
import agents.director.hosts.base.MilestoneManager;
import agents.director.mcp.clients.OracleSchemaIntelligenceClient;
import agents.director.mcp.clients.BusinessMappingClient;
import agents.director.mcp.clients.SessionSchemaResolverClient;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import java.util.*;

/**
 * Milestone 2: Schema Exploration
 * 
 * Explores the database schema to find relevant tables and views based on the intent.
 * Uses OracleSchemaIntelligenceServer and BusinessMappingServer by deploying MCP clients directly.
 * 
 * Output shared with user: List of relevant tables/views and their purpose
 */
public class SchemaMilestone extends MilestoneManager {
    
    private static final String SCHEMA_CLIENT = "schema";
    private static final String BUSINESS_CLIENT = "business";
    private static final String SESSION_RESOLVER_CLIENT = "sessionresolver";
    private final Map<String, String> deploymentIds = new HashMap<>();
    
    public SchemaMilestone(Vertx vertx, String baseUrl) {
        super(vertx, baseUrl, 2, "SchemaMilestone", 
              "Explore database schema and identify relevant tables");
    }
    
    @Override
    public Future<Void> initialize() {
        Promise<Void> promise = Promise.promise();
        
        // Deploy the MCP clients directly
        List<Future> deploymentFutures = new ArrayList<>();
        
        OracleSchemaIntelligenceClient schemaClient = new OracleSchemaIntelligenceClient(baseUrl);
        BusinessMappingClient businessClient = new BusinessMappingClient(baseUrl);
        SessionSchemaResolverClient resolverClient = new SessionSchemaResolverClient(baseUrl);
        
        deploymentFutures.add(deployClient(SCHEMA_CLIENT, schemaClient));
        deploymentFutures.add(deployClient(BUSINESS_CLIENT, businessClient));
        deploymentFutures.add(deployClient(SESSION_RESOLVER_CLIENT, resolverClient));
        
        CompositeFuture.all(deploymentFutures)
            .onSuccess(v -> {
                log("Schema milestone initialized successfully", 2);
                promise.complete();
            })
            .onFailure(err -> {
                log("Failed to initialize schema milestone: " + err.getMessage(), 0);
                promise.fail(err);
            });
        
        return promise.future();
    }
    
    @Override
    public Future<MilestoneContext> execute(MilestoneContext context) {
        Promise<MilestoneContext> promise = Promise.promise();
        
        log("Starting schema exploration for intent: " + context.getIntent(), 3);
        
        // Publish progress event at start
        if (context.isStreaming() && context.getSessionId() != null) {
            publishProgressEvent(context.getConversationId(),
                "Step 2: Schema Exploration",
                "Finding relevant tables in database...",
                new JsonObject()
                    .put("phase", "schema_exploration")
                    .put("intent", context.getIntent()));
        }
        
        // Initialize session schema resolver if sessionId is available
        Future<JsonObject> initFuture;
        if (context.getSessionId() != null) {
            initFuture = callTool(SESSION_RESOLVER_CLIENT, "discover_available_schemas",
                new JsonObject().put("sessionId", context.getSessionId()))
                .onSuccess(result -> {
                    log("Session schema resolver initialized with " + 
                        result.getJsonArray("schemas", new JsonArray()).size() + " schemas", 3);
                })
                .onFailure(err -> {
                    log("Failed to initialize session schema resolver: " + err.getMessage(), 2);
                });
        } else {
            initFuture = Future.succeededFuture(new JsonObject());
        }
        
        // Build search context from intent
        JsonObject searchContext = new JsonObject()
            .put("query", context.getQuery())
            .put("intent", context.getIntent())
            .put("intent_type", context.getIntentType())
            .put("intent_details", context.getIntentDetails() != null ? 
                 context.getIntentDetails() : new JsonObject());
        
        // After initialization, discover relevant schema elements using clients directly
        initFuture.compose(v -> discoverRelevantSchema(context.getQuery(), searchContext, context.getSessionId()))
            .onSuccess(schemaResult -> {
                // Extract tables and their descriptions
                JsonArray tables = schemaResult.getJsonArray("tables");
                JsonArray views = schemaResult.getJsonArray("views");
                JsonObject relationships = schemaResult.getJsonObject("relationships");
                JsonObject businessTerms = schemaResult.getJsonObject("businessTerms");
                
                // Process tables
                List<String> relevantTables = new ArrayList<>();
                Map<String, String> tableDescriptions = new HashMap<>();
                
                if (tables != null) {
                    for (int i = 0; i < tables.size(); i++) {
                    JsonObject table = tables.getJsonObject(i);
                    String tableName = table.getString("name");
                    String description = table.getString("description", "");
                    
                    relevantTables.add(tableName);
                    if (!description.isEmpty()) {
                        tableDescriptions.put(tableName, description);
                    }
                    
                    // Extract and store complete column information
                    JsonArray columns = table.getJsonArray("columns");
                    if (columns != null) {
                        // Store the complete column objects with all metadata (even if empty)
                        context.setTableColumns(tableName, columns);
                    }
                    }
                }
                
                // Add views as well
                if (views != null) {
                    for (int i = 0; i < views.size(); i++) {
                    JsonObject view = views.getJsonObject(i);
                    String viewName = view.getString("name");
                    String description = view.getString("description", "View: " + viewName);
                    
                    relevantTables.add(viewName);
                    tableDescriptions.put(viewName, description);
                    }
                }
                
                // Update context with schema information
                context.setRelevantTables(relevantTables);
                for (Map.Entry<String, String> entry : tableDescriptions.entrySet()) {
                    context.setTableDescription(entry.getKey(), entry.getValue());
                }
                
                context.setSchemaDetails(new JsonObject()
                    .put("tables", tables)
                    .put("views", views)
                    .put("relationships", relationships)
                    .put("businessTerms", businessTerms)
                    .put("confidence", schemaResult.getDouble("confidence", 0.8)));
                
                // Mark milestone as complete
                context.completeMilestone(2);
                
                // Publish streaming event if applicable
                if (context.isStreaming() && context.getSessionId() != null) {
                    publishStreamingEvent(context.getConversationId(), "milestone.schema_complete",
                        getShareableResult(context));
                }
                
                log("Schema exploration complete: found " + relevantTables.size() + " relevant tables", 2);
                promise.complete(context);
            })
            .recover(err -> {
                // Log the failure explicitly at WARNING level
                log("Schema exploration failed, using degraded mode: " + err.getMessage(), 1);
                
                // Mark context as degraded
                context.setMilestoneDegraded(2, "Schema discovery failed: " + err.getMessage());
                
                // Publish degradation event
                publishDegradationEvent(context, "schema_discovery", err.getMessage());
                
                // Fallback: Try to extract table names from the query itself
                List<String> fallbackTables = extractTableNamesFromQuery(context.getQuery());
                
                // Log what we're doing as fallback
                log("Using query parsing fallback, found potential tables: " + fallbackTables, 1);
                
                context.setRelevantTables(fallbackTables);
                context.setSchemaDetails(new JsonObject()
                    .put("degraded", true)
                    .put("degradation_reason", err.getMessage())
                    .put("fallback_method", "query_parsing")
                    .put("tables", new JsonArray(fallbackTables))
                    .put("confidence", 0.2)); // Very low confidence for query parsing
                
                // Mark milestone as complete (but degraded)
                context.completeMilestone(2);
                
                // Publish streaming event about degradation
                if (context.isStreaming() && context.getSessionId() != null) {
                    JsonObject degradedResult = getShareableResult(context);
                    degradedResult.put("degraded", true)
                        .put("message", "⚠️ Schema discovery degraded: Using query parsing to guess tables");
                    publishStreamingEvent(context.getConversationId(), "milestone.schema_complete", degradedResult);
                    
                    // Also publish a specific warning about the fallback
                    publishStreamingEvent(context.getConversationId(), "degradation_warning", new JsonObject()
                        .put("milestone", 2)
                        .put("operation", "schema_discovery")
                        .put("fallback_tables", new JsonArray(fallbackTables))
                        .put("message", "Could not connect to database schema. Guessing table names from query.")
                        .put("severity", "WARNING"));
                }
                
                // Return degraded result to continue pipeline
                return Future.succeededFuture(new JsonObject()
                    .put("tables", new JsonArray(fallbackTables))
                    .put("views", new JsonArray())
                    .put("relationships", new JsonObject())
                    .put("degraded", true)
                    .put("confidence", 0.2));
            });
        
        return promise.future();
    }
    
    @Override
    public JsonObject getShareableResult(MilestoneContext context) {
        JsonArray tablesArray = new JsonArray();
        
        for (String table : context.getRelevantTables()) {
            JsonObject tableInfo = new JsonObject()
                .put("name", table);
            
            String description = context.getTableDescriptions().get(table);
            if (description != null && !description.isEmpty()) {
                tableInfo.put("description", description);
            }
            
            tablesArray.add(tableInfo);
        }
        
        String message;
        if (context.getRelevantTables().isEmpty()) {
            message = "No specific tables identified yet. Will explore the schema further.";
        } else if (context.getRelevantTables().size() == 1) {
            message = "Found 1 relevant table: " + context.getRelevantTables().get(0);
        } else {
            message = "Found " + context.getRelevantTables().size() + " relevant tables in the schema";
        }
        
        return new JsonObject()
            .put("milestone", 2)
            .put("milestone_name", "Schema Exploration")
            .put("tables", tablesArray)
            .put("table_count", context.getRelevantTables().size())
            .put("message", message);
    }
    
    /**
     * Simple fallback to extract potential table names from query
     */
    private List<String> extractTableNamesFromQuery(String query) {
        List<String> tables = new ArrayList<>();
        String lower = query.toLowerCase();
        
        // Common table name patterns
        String[] commonTables = {
            "orders", "customers", "products", "users", "transactions",
            "sales", "inventory", "employees", "departments", "accounts",
            "invoices", "payments", "items", "suppliers", "shipments"
        };
        
        for (String table : commonTables) {
            if (lower.contains(table)) {
                tables.add(table.toUpperCase());
            }
        }
        
        // If no tables found, return a generic placeholder
        if (tables.isEmpty()) {
            tables.add("MAIN_TABLE");
        }
        
        return tables;
    }
    
    /**
     * Discover relevant schema elements (replaces manager method)
     */
    private Future<JsonObject> discoverRelevantSchema(String query, JsonObject searchContext, String sessionId) {
        // Extract terms from query for business mapping
        JsonArray terms = extractTermsFromQuery(query);
        
        // First map business terms
        Future<JsonObject> businessTermsFuture = callTool(BUSINESS_CLIENT, "map_business_terms",
            new JsonObject()
                .put("terms", terms)
                .put("context", searchContext));
        
        // Then match to Oracle schema
        Future<JsonObject> schemaMatchFuture = businessTermsFuture
            .compose(mappedTerms -> {
                // Create analysis object for match_oracle_schema
                JsonObject analysis = new JsonObject()
                    .put("intent", searchContext.getString("intent", ""))
                    .put("queryType", searchContext.getString("intent_type", "query"))
                    .put("entities", extractTermsFromQuery(query))
                    .put("businessTerms", mappedTerms)
                    .put("originalQuery", query);  // Include original query for term extraction
                
                return callTool(SCHEMA_CLIENT, "match_oracle_schema",
                    new JsonObject()
                        .put("analysis", analysis)
                        .put("maxSuggestions", 5)
                        .put("confidenceThreshold", 0.5));
            })
            .map(matchResult -> {
                // Transform matches array to tables array for compatibility
                JsonArray matches = matchResult.getJsonArray("matches", new JsonArray());
                JsonArray tables = new JsonArray();
                
                for (int i = 0; i < matches.size(); i++) {
                    JsonObject match = matches.getJsonObject(i);
                    JsonObject table = match.getJsonObject("table");
                    if (table != null) {
                        // Create a copy to avoid mutating the original
                        table = table.copy();
                        // Always set 'name' from 'tableName' for downstream compatibility
                        // (OracleSchemaIntelligenceServer returns 'tableName' but downstream expects 'name')
                        String tableName = table.getString("tableName");
                        table.put("name", tableName);
                        // Add relevant columns from the match
                        table.put("relevantColumns", match.getJsonArray("relevantColumns", new JsonArray()));
                        table.put("confidence", match.getDouble("confidence", 0.5));
                        table.put("matchReason", match.getString("matchReason", ""));
                        tables.add(table);
                    }
                }
                
                // Return transformed result with tables array
                matchResult.put("tables", tables);
                return matchResult;
            });
        
        // Infer relationships if tables found
        Future<JsonObject> relationshipsFuture = schemaMatchFuture
            .compose(match -> {
                JsonArray tables = match.getJsonArray("tables");
                if (tables != null && tables.size() > 0) {
                    return callTool(SCHEMA_CLIENT, "infer_table_relationships",
                        new JsonObject().put("tables", tables));
                }
                return Future.succeededFuture(new JsonObject());
            });
        
        // Resolve schema prefixes for discovered tables if sessionId is available
        Future<JsonObject> schemaResolutionFuture = schemaMatchFuture
            .compose(match -> {
                if (sessionId == null) {
                    return Future.succeededFuture(new JsonObject());
                }
                
                JsonArray tables = match.getJsonArray("tables");
                if (tables == null || tables.isEmpty()) {
                    return Future.succeededFuture(new JsonObject());
                }
                
                // Resolve schema for each table and collect results
                List<Future> resolutionFutures = new ArrayList<>();
                JsonObject resolvedSchemas = new JsonObject();
                
                for (int i = 0; i < tables.size(); i++) {
                    JsonObject table = tables.getJsonObject(i);
                    String tableName = table.getString("name");
                    if (tableName != null) {
                        Future<JsonObject> resolveFuture = callTool(SESSION_RESOLVER_CLIENT, "resolve_table_schema",
                            new JsonObject()
                                .put("tableName", tableName)
                                .put("sessionId", sessionId)
                                .put("queryContext", new JsonObject()
                                    .put("queryType", searchContext.getString("intent_type"))))
                            .onSuccess(result -> {
                                String schema = result.getString("schema");
                                if (schema != null) {
                                    resolvedSchemas.put(tableName, schema);
                                    // Learn from successful resolution
                                    callTool(SESSION_RESOLVER_CLIENT, "learn_from_success",
                                        new JsonObject()
                                            .put("tableName", tableName)
                                            .put("schema", schema)
                                            .put("sessionId", sessionId))
                                        .onFailure(err -> {
                                            log("Failed to record learning for " + tableName + ": " + err.getMessage(), 3);
                                        });
                                }
                            });
                        resolutionFutures.add(resolveFuture);
                    }
                }
                
                return CompositeFuture.join(resolutionFutures)
                    .map(v -> resolvedSchemas);
            });
        
        return CompositeFuture.join(businessTermsFuture, schemaMatchFuture, relationshipsFuture, schemaResolutionFuture)
            .map(composite -> {
                // Handle partial failures - check each result individually
                JsonObject businessTerms = composite.succeeded(0) ? 
                    composite.resultAt(0) : new JsonObject().put("error", "Business term mapping failed");
                JsonObject schemaMatch = composite.succeeded(1) ? 
                    composite.resultAt(1) : new JsonObject().put("error", "Schema matching failed");
                JsonObject relationships = composite.succeeded(2) ? 
                    composite.resultAt(2) : new JsonObject().put("error", "Relationship inference failed");
                JsonObject resolvedSchemas = composite.succeeded(3) ? 
                    composite.resultAt(3) : new JsonObject();
                
                // Enhance tables with resolved schema prefixes
                JsonArray tables = schemaMatch.getJsonArray("tables");
                if (tables != null && !resolvedSchemas.isEmpty()) {
                    for (int i = 0; i < tables.size(); i++) {
                        JsonObject table = tables.getJsonObject(i);
                        String tableName = table.getString("name");
                        String resolvedSchema = resolvedSchemas.getString(tableName);
                        if (resolvedSchema != null) {
                            table.put("schema", resolvedSchema);
                            table.put("qualifiedName", resolvedSchema + "." + tableName);
                        }
                    }
                }
                
                return new JsonObject()
                    .put("tables", tables)
                    .put("views", schemaMatch.getJsonArray("views"))
                    .put("relationships", relationships)
                    .put("businessTerms", businessTerms)
                    .put("resolvedSchemas", resolvedSchemas)
                    .put("confidence", schemaMatch.getDouble("confidence", 0.8));
            });
    }
    
    /**
     * Deploy a clients and track it
     */
    private Future<String> deployClient(String clientName, AbstractVerticle client) {
        Promise<String> promise = Promise.promise();
        
        vertx.deployVerticle(client, ar -> {
            if (ar.succeeded()) {
                String deploymentId = ar.result();
                deploymentIds.put(clientName, deploymentId);
                log("Deployed " + clientName + " clients", 3);
                promise.complete(deploymentId);
            } else {
                promise.fail(ar.cause());
            }
        });
        
        return promise.future();
    }
    
    /**
     * Call a tool on a deployed clients
     */
    private Future<JsonObject> callTool(String clientName, String toolName, JsonObject params) {
        Promise<JsonObject> promise = Promise.promise();
        
        // Map clients names to normalized server names
        String serverName;
        switch (clientName) {
            case SCHEMA_CLIENT:
                serverName = "oracleschemaintelligence";
                break;
            case BUSINESS_CLIENT:
                serverName = "businessmapping";
                break;
            case SESSION_RESOLVER_CLIENT:
                serverName = "sessionschemaresolver";
                break;
            default:
                serverName = clientName.toLowerCase();
        }
        
        String address = "mcp.clients." + serverName + "." + toolName;
        vertx.eventBus().<JsonObject>request(address, params, ar -> {
            if (ar.succeeded()) {
                promise.complete(ar.result().body());
            } else {
                promise.fail(ar.cause());
            }
        });
        
        return promise.future();
    }
    
    /**
     * Extract key terms from a query for business mapping
     */
    private JsonArray extractTermsFromQuery(String query) {
        JsonArray terms = new JsonArray();
        String lower = query.toLowerCase();
        
        // Remove common SQL keywords and stop words
        String[] stopWords = {"select", "from", "where", "and", "or", "the", "is", "are", 
                              "in", "of", "to", "for", "with", "how", "many", "what", 
                              "show", "get", "find", "list", "all", "by", "group", "order"};
        
        // Split query into words and filter
        String[] words = lower.replaceAll("[^a-z0-9\\s]", " ").split("\\s+");
        Set<String> uniqueTerms = new HashSet<>();
        
        for (String word : words) {
            word = word.trim();
            if (word.length() > 2) {  // Skip very short words
                boolean isStopWord = false;
                for (String stopWord : stopWords) {
                    if (word.equals(stopWord)) {
                        isStopWord = true;
                        break;
                    }
                }
                if (!isStopWord) {
                    uniqueTerms.add(word);
                }
            }
        }
        
        // Add unique terms to array
        for (String term : uniqueTerms) {
            terms.add(term);
        }
        
        // If no terms found, add some generic ones based on query
        if (terms.isEmpty()) {
            if (lower.contains("order")) terms.add("orders");
            if (lower.contains("customer")) terms.add("customers");
            if (lower.contains("product")) terms.add("products");
            if (terms.isEmpty()) terms.add("data");  // Fallback
        }
        
        return terms;
    }
    
    @Override
    public Future<Void> cleanup() {
        List<Future> undeployFutures = new ArrayList<>();
        
        // Undeploy all clients
        for (Map.Entry<String, String> entry : deploymentIds.entrySet()) {
            Promise<Void> promise = Promise.promise();
            vertx.undeploy(entry.getValue(), ar -> {
                if (ar.succeeded()) {
                    log("Undeployed " + entry.getKey() + " clients", 3);
                    promise.complete();
                } else {
                    promise.fail(ar.cause());
                }
            });
            undeployFutures.add(promise.future());
        }
        
        // Also call parent cleanup
        undeployFutures.add(super.cleanup());
        
        return CompositeFuture.all(undeployFutures).mapEmpty();
    }
}
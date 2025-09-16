package agents.director.hosts.milestones;

import agents.director.hosts.base.MilestoneContext;
import agents.director.hosts.base.MilestoneManager;
import agents.director.mcp.clients.OracleQueryAnalysisClient;
import agents.director.mcp.clients.OracleSchemaIntelligenceClient;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import java.util.*;

/**
 * Milestone 3: Data Statistics Analysis
 * 
 * Analyzes table columns and data statistics related to the intent and schema.
 * Uses OracleQueryAnalysisServer and OracleSchemaIntelligenceServer.
 * 
 * Output shared with user: Column information and data statistics
 * 
 * Note: This milestone deploys MCP clients directly rather than using a manager
 * because it needs fine-grained control over the analysis and mapping operations.
 */
public class DataStatsMilestone extends MilestoneManager {
    
    private static final String ANALYSIS_CLIENT = "query_analysis";
    private static final String SCHEMA_CLIENT = "schema";
    private final Map<String, String> deploymentIds = new HashMap<>();
    
    public DataStatsMilestone(Vertx vertx, String baseUrl) {
        super(vertx, baseUrl, 3, "DataStatsMilestone", 
              "Analyze table columns and data statistics");
    }
    
    @Override
    public Future<Void> initialize() {
        Promise<Void> promise = Promise.promise();
        
        // Deploy the analysis and mapping clients directly
        List<Future> deploymentFutures = new ArrayList<>();
        
        OracleQueryAnalysisClient analysisClient = new OracleQueryAnalysisClient(baseUrl);
        OracleSchemaIntelligenceClient schemaClient = new OracleSchemaIntelligenceClient(baseUrl);

        deploymentFutures.add(deployClient(ANALYSIS_CLIENT, analysisClient));
        deploymentFutures.add(deployClient(SCHEMA_CLIENT, schemaClient));
        
        CompositeFuture.all(deploymentFutures)
            .onSuccess(v -> {
                log("Data stats milestone initialized successfully", 2);
                promise.complete();
            })
            .onFailure(err -> {
                log("Failed to initialize data stats milestone: " + err.getMessage(), 0);
                promise.fail(err);
            });
        
        return promise.future();
    }
    
    @Override
    public Future<MilestoneContext> execute(MilestoneContext context) {
        Promise<MilestoneContext> promise = Promise.promise();
        
        log("Starting data statistics analysis for tables: " + context.getRelevantTables(), 3);
        
        // Publish progress event at start
        if (context.isStreaming() && context.getSessionId() != null) {
            publishProgressEvent(context.getConversationId(),
                "Step 3: Data Analysis",
                "Analyzing table structures and statistics...",
                new JsonObject()
                    .put("phase", "data_analysis")
                    .put("tables", new JsonArray(context.getRelevantTables())));
        }
        
        // If no tables were found, skip this milestone
        if (context.getRelevantTables().isEmpty()) {
            log("No tables to analyze, skipping data stats milestone", 2);
            context.completeMilestone(3);
            promise.complete(context);
            return promise.future();
        }
        
        // Load enum cache first (important for proper value resolution)
        Future<JsonObject> enumCacheFuture = loadEnumCache();
        
        // Then analyze each table after cache is ready
        enumCacheFuture
            .compose(cacheResult -> {
                log("Enum cache loaded: " + cacheResult.getString("status", "unknown"), 2);

                List<Future> analysisFutures = new ArrayList<>();

                for (String table : context.getRelevantTables()) {
                    // Analyze table structure
                    Future<JsonObject> structureFuture = analyzeTableStructure(table, context);

                    // Map business terms to columns
                    Future<JsonObject> mappingFuture = mapBusinessTerms(table, context);

                    analysisFutures.add(structureFuture);
                    analysisFutures.add(mappingFuture);
                }

                return CompositeFuture.join(analysisFutures);
            })
            .map(results -> {
                // Aggregate results - fail fast if any operation failed
                Map<String, List<String>> tableColumns = new HashMap<>();
                Map<String, JsonObject> columnStats = new HashMap<>();
                JsonObject dataProfile = new JsonObject();

                int resultIndex = 0;
                for (String table : context.getRelevantTables()) {
                    // Structure result
                    if (!results.succeeded(resultIndex)) {
                        throw new RuntimeException("Table structure analysis failed for " + table + ": " +
                            results.cause(resultIndex).getMessage());
                    }

                    JsonObject structure = (JsonObject) results.resultAt(resultIndex);
                    if (structure != null) {
                        JsonArray columns = structure.getJsonArray("columns");
                        List<String> columnNames = new ArrayList<>();

                        if (columns != null) {
                            for (int i = 0; i < columns.size(); i++) {
                                JsonObject col = columns.getJsonObject(i);
                                // Check both "columnName" and "name" for compatibility
                                String colName = col.getString("columnName");
                                if (colName == null) {
                                    colName = col.getString("name");
                                }

                                // Only add non-null column names
                                if (colName != null) {
                                    columnNames.add(colName);

                                    // Store column statistics
                                    String statKey = table + "." + colName;
                                    columnStats.put(statKey, col);
                                }
                            }
                        }

                        tableColumns.put(table, columnNames);
                    }
                    resultIndex++;

                    // Mapping result
                    if (!results.succeeded(resultIndex)) {
                        throw new RuntimeException("Business term mapping failed for " + table + ": " +
                            results.cause(resultIndex).getMessage());
                    }

                    JsonObject mapping = (JsonObject) results.resultAt(resultIndex);
                    if (mapping != null) {
                        dataProfile.put(table + "_mapping", mapping);
                    }
                    resultIndex++;
                }
                
                // Don't overwrite column information from SchemaMilestone
                // Only set columns if they weren't already discovered
                for (Map.Entry<String, List<String>> entry : tableColumns.entrySet()) {
                    if (!context.getTableColumns().containsKey(entry.getKey())) {
                        // Convert List<String> to JsonArray of simple column objects
                        JsonArray columns = new JsonArray();
                        for (String colName : entry.getValue()) {
                            // Only add non-null column names
                            if (colName != null) {
                                columns.add(new JsonObject().put("columnName", colName));
                            }
                        }
                        context.setTableColumns(entry.getKey(), columns);
                    }
                }
                
                for (Map.Entry<String, JsonObject> entry : columnStats.entrySet()) {
                    context.setColumnStats(entry.getKey(), entry.getValue());
                }
                
                context.setDataProfile(dataProfile);
                
                // Mark milestone as complete
                context.completeMilestone(3);
                
                // Publish streaming event if applicable
                if (context.isStreaming() && context.getSessionId() != null) {
                    publishStreamingEvent(context.getConversationId(), "milestone.data_stats_complete",
                        getShareableResult(context));
                }

                log("Data statistics analysis complete", 2);
                return context;
            })
            .onComplete(ar -> {
                if (ar.succeeded()) {
                    promise.complete(ar.result());
                } else {
                    promise.fail(ar.cause());
                }
            });
        
        return promise.future();
    }
    
    @Override
    public JsonObject getShareableResult(MilestoneContext context) {
        JsonArray tablesInfo = new JsonArray();
        
        for (String table : context.getRelevantTables()) {
            JsonObject tableInfo = new JsonObject()
                .put("table", table);
            
            JsonArray columns = context.getTableColumns().get(table);
            if (columns != null && !columns.isEmpty()) {
                // Extract column names for display
                JsonArray columnNames = new JsonArray();
                for (int i = 0; i < columns.size(); i++) {
                    JsonObject col = columns.getJsonObject(i);
                    String colName = col.getString("columnName");
                    columnNames.add(colName);  // Add even if null - let downstream handle it
                }
                tableInfo.put("columns", columnNames);
                tableInfo.put("column_count", columns.size());
            }
            
            tablesInfo.add(tableInfo);
        }
        
        int totalColumns = context.getTableColumns().values().stream()
            .mapToInt(JsonArray::size)
            .sum();
        
        String message = "Analyzed " + context.getRelevantTables().size() + 
                        " table(s) with " + totalColumns + " total columns";
        
        return new JsonObject()
            .put("milestone", 3)
            .put("milestone_name", "Data Statistics Analysis")
            .put("tables", tablesInfo)
            .put("total_columns", totalColumns)
            .put("message", message);
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
     * Analyze table structure using the query analysis clients
     */
    private Future<JsonObject> analyzeTableStructure(String table, MilestoneContext context) {
        Promise<JsonObject> promise = Promise.promise();
        
        // Build context array with table information
        JsonArray analysisContext = new JsonArray()
            .add(new JsonObject()
                .put("role", "system")
                .put("content", "Analyzing requirements for table: " + table))
            .add(new JsonObject()
                .put("role", "user")
                .put("content", "Intent: " + context.getIntent()));
        
        JsonObject request = new JsonObject()
            .put("query", context.getQuery())
            .put("context", analysisContext);
        
        // Call the analysis tool
        vertx.eventBus().<JsonObject>request(
            "mcp.clients.oracle_query_analysis.analyze_query",
            request,
            ar -> {
                if (ar.succeeded()) {
                    promise.complete(ar.result().body());
                } else {
                    // Fail fast - no degraded mode
                    promise.fail(new RuntimeException("Table structure analysis failed for " + table + ": " +
                        ar.cause().getMessage()));
                }
            });
        
        return promise.future();
    }
    
    /**
     * Map business terms to columns using the business mapping clients
     */
    private Future<JsonObject> mapBusinessTerms(String table, MilestoneContext context) {
        Promise<JsonObject> promise = Promise.promise();
        
        // Extract terms from the query (simple tokenization)
        JsonArray terms = new JsonArray();
        String[] words = context.getQuery().toLowerCase().split("\\s+");
        for (String word : words) {
            // Skip common words and short words
            if (word.length() > 2 && !isCommonWord(word)) {
                terms.add(word);
            }
        }

        // Process ALL terms, not just the first one
        // Since resolve_synonym expects a single term, we'll batch process them
        if (terms.isEmpty()) {
            // No terms to resolve, return empty result immediately
            promise.complete(new JsonObject()
                .put("table", table)
                .put("mappings", new JsonObject()));
            return promise.future();
        }

        // Process all terms and aggregate results
        List<Future> termFutures = new ArrayList<>();
        for (int i = 0; i < terms.size(); i++) {
            String term = terms.getString(i);
            JsonObject termRequest = new JsonObject().put("term", term);

            Promise<JsonObject> termPromise = Promise.promise();
            vertx.eventBus().<JsonObject>request(
                "mcp.clients.oracleschemaintelligence.resolve_synonym",
                termRequest,
                ar -> {
                    if (ar.succeeded()) {
                        termPromise.complete(ar.result().body());
                    } else {
                        termPromise.fail(ar.cause());
                    }
                });
            termFutures.add(termPromise.future());
        }

        CompositeFuture.join(termFutures)
            .onComplete(ar -> {
                // Process results even if some failed
                JsonObject allMappings = new JsonObject();
                CompositeFuture composite = ar.result();

                for (int i = 0; i < termFutures.size(); i++) {
                    if (composite.succeeded(i)) {
                        JsonObject termResult = composite.resultAt(i);
                        JsonArray matches = termResult.getJsonArray("matches", new JsonArray());
                        String term = terms.getString(i);
                        if (!matches.isEmpty()) {
                            allMappings.put(term, matches);
                        }
                    }
                }

                // Complete with whatever mappings we got
                promise.complete(new JsonObject()
                    .put("table", table)
                    .put("mappings", allMappings));
            });

        return promise.future();
    }
    
    /**
     * Check if a word is a common word that should be skipped
     */
    private boolean isCommonWord(String word) {
        // Common SQL/query words to skip
        String[] commonWords = {
            "the", "and", "or", "is", "are", "was", "were", "been", "have", "has", "had",
            "do", "does", "did", "will", "would", "could", "should", "may", "might",
            "a", "an", "of", "in", "on", "at", "to", "for", "from", "with", "by",
            "how", "what", "where", "when", "why", "who", "which", "there", "many", "much"
        };
        
        for (String common : commonWords) {
            if (word.equalsIgnoreCase(common)) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Load the enum cache for value resolution
     */
    private Future<JsonObject> loadEnumCache() {
        Promise<JsonObject> promise = Promise.promise();

        // Call get_enum_metadata to check if enum data is available
        vertx.eventBus().<JsonObject>request(
            "mcp.clients.oracleschemaintelligence.get_enum_metadata",
            new JsonObject(),
            ar -> {
                if (ar.succeeded()) {
                    JsonObject result = ar.result().body();
                    JsonArray enumTables = result.getJsonArray("enumTables", new JsonArray());
                    log("Enum metadata loaded with " + enumTables.size() + " enum tables", 2);
                    promise.complete(new JsonObject()
                        .put("status", "loaded")
                        .put("cache_size", enumTables.size()));
                } else {
                    log("Failed to load enum metadata: " + ar.cause().getMessage(), 1);
                    promise.fail(ar.cause());
                }
            });

        return promise.future();
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
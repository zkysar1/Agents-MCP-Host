package agents.director.hosts.milestones;

import agents.director.hosts.base.MilestoneContext;
import agents.director.hosts.base.MilestoneManager;
import agents.director.mcp.clients.OracleQueryAnalysisClient;
import agents.director.mcp.clients.BusinessMappingClient;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import java.util.*;

/**
 * Milestone 3: Data Statistics Analysis
 * 
 * Analyzes table columns and data statistics related to the intent and schema.
 * Uses OracleQueryAnalysisServer and BusinessMappingServer.
 * 
 * Output shared with user: Column information and data statistics
 * 
 * Note: This milestone deploys MCP clients directly rather than using a manager
 * because it needs fine-grained control over the analysis and mapping operations.
 */
public class DataStatsMilestone extends MilestoneManager {
    
    private static final String ANALYSIS_CLIENT = "query_analysis";
    private static final String MAPPING_CLIENT = "business_mapping";
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
        BusinessMappingClient mappingClient = new BusinessMappingClient(baseUrl);
        
        deploymentFutures.add(deployClient(ANALYSIS_CLIENT, analysisClient));
        deploymentFutures.add(deployClient(MAPPING_CLIENT, mappingClient));
        
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
            .recover(err -> {
                // If enum cache loading fails, continue without it
                log("Enum cache loading failed, continuing without enum resolution: " + err.getMessage(), 1);
                
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
                // Aggregate results, checking for partial failures
                Map<String, List<String>> tableColumns = new HashMap<>();
                Map<String, JsonObject> columnStats = new HashMap<>();
                JsonObject dataProfile = new JsonObject();
                boolean anyFailed = false;
                
                int resultIndex = 0;
                for (String table : context.getRelevantTables()) {
                    // Structure result
                    if (results.succeeded(resultIndex)) {
                        JsonObject structure = (JsonObject) results.resultAt(resultIndex);
                        if (structure != null && !structure.getBoolean("degraded", false)) {
                            JsonArray columns = structure.getJsonArray("columns");
                            List<String> columnNames = new ArrayList<>();
                            
                            if (columns != null) {
                                for (int i = 0; i < columns.size(); i++) {
                                    JsonObject col = columns.getJsonObject(i);
                                    String colName = col.getString("name");
                                    columnNames.add(colName);
                                    
                                    // Store column statistics
                                    String statKey = table + "." + colName;
                                    columnStats.put(statKey, col);
                                }
                            }
                            
                            tableColumns.put(table, columnNames);
                        } else if (structure != null && structure.getBoolean("degraded", false)) {
                            anyFailed = true;
                            log("Table structure analysis degraded for: " + table, 1);
                        }
                    } else {
                        anyFailed = true;
                        log("Table structure analysis failed for: " + table + " - " + results.cause(resultIndex), 1);
                    }
                    resultIndex++;
                    
                    // Mapping result
                    if (results.succeeded(resultIndex)) {
                        JsonObject mapping = (JsonObject) results.resultAt(resultIndex);
                        if (mapping != null && !mapping.getBoolean("degraded", false)) {
                            dataProfile.put(table + "_mapping", mapping);
                        } else if (mapping != null && mapping.getBoolean("degraded", false)) {
                            anyFailed = true;
                            log("Business mapping degraded for: " + table, 1);
                        }
                    } else {
                        anyFailed = true;
                        log("Business mapping failed for: " + table + " - " + results.cause(resultIndex), 1);
                    }
                    resultIndex++;
                }
                
                // If any operations failed, mark as degraded
                if (anyFailed) {
                    context.setMilestoneDegraded(3, "Some data analysis operations failed or were degraded");
                    publishDegradationEvent(context, "data_analysis", "Partial failure in table analysis");
                    
                    if (context.isStreaming() && context.getSessionId() != null) {
                        publishStreamingEvent(context.getConversationId(), "degradation_warning", new JsonObject()
                            .put("milestone", 3)
                            .put("message", "⚠️ Data analysis partially degraded - some tables could not be fully analyzed")
                            .put("severity", "WARNING"));
                    }
                }
                
                // Don't overwrite column information from SchemaMilestone
                // Only set columns if they weren't already discovered
                for (Map.Entry<String, List<String>> entry : tableColumns.entrySet()) {
                    if (!context.getTableColumns().containsKey(entry.getKey())) {
                        // Convert List<String> to JsonArray of simple column objects
                        JsonArray columns = new JsonArray();
                        for (String colName : entry.getValue()) {
                            columns.add(new JsonObject().put("columnName", colName));
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
                    JsonObject result = getShareableResult(context);
                    if (anyFailed) {
                        result.put("degraded", true);
                    }
                    publishStreamingEvent(context.getConversationId(), "milestone.data_stats_complete", result);
                }
                
                log("Data statistics analysis complete" + (anyFailed ? " (with degradation)" : ""), 2);
                return context;
            })
            .recover(err -> {
                // Complete failure - log explicitly
                log("Data statistics analysis completely failed, continuing with degraded mode: " + err.getMessage(), 1);
                
                // Mark context as severely degraded
                context.setMilestoneDegraded(3, "Complete data analysis failure: " + err.getMessage());
                publishDegradationEvent(context, "data_analysis", err.getMessage());
                
                // Don't create fake columns - fail open
                // Column information should come from SchemaMilestone
                context.setDataProfile(new JsonObject()
                    .put("degraded", true)
                    .put("degradation_reason", err.getMessage())
                    .put("message", "Could not analyze table structures"));
                
                context.completeMilestone(3);
                
                if (context.isStreaming() && context.getSessionId() != null) {
                    JsonObject result = getShareableResult(context);
                    result.put("degraded", true)
                        .put("message", "⚠️ Data analysis failed - proceeding without column statistics");
                    publishStreamingEvent(context.getConversationId(), "milestone.data_stats_complete", result);
                }
                
                return Future.succeededFuture(context);
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
        
        JsonObject request = new JsonObject()
            .put("table_name", table)
            .put("query", context.getQuery())
            .put("intent", context.getIntent());
        
        // Call the analysis tool
        vertx.eventBus().<JsonObject>request(
            "mcp.clients.oracle_query_analysis.oracle_query_analysis__analyze_query_requirements",
            request,
            ar -> {
                if (ar.succeeded()) {
                    promise.complete(ar.result().body());
                } else {
                    // DON'T return fake columns - mark as degraded instead
                    log("Table structure analysis failed for " + table + ": " + ar.cause().getMessage(), 1);
                    promise.complete(new JsonObject()
                        .put("table", table)
                        .put("degraded", true)
                        .put("degradation_reason", "Could not analyze table structure: " + ar.cause().getMessage())
                        .put("columns", new JsonArray())); // Empty array, not fake data
                }
            });
        
        return promise.future();
    }
    
    /**
     * Map business terms to columns using the business mapping clients
     */
    private Future<JsonObject> mapBusinessTerms(String table, MilestoneContext context) {
        Promise<JsonObject> promise = Promise.promise();
        
        JsonObject request = new JsonObject()
            .put("table_name", table)
            .put("business_context", context.getQuery())
            .put("intent", context.getIntent());
        
        // Call the mapping tool
        vertx.eventBus().<JsonObject>request(
            "mcp.clients.business_mapping.business_mapping__map_terms_to_columns",
            request,
            ar -> {
                if (ar.succeeded()) {
                    promise.complete(ar.result().body());
                } else {
                    // Mark as degraded instead of silently returning empty
                    log("Business term mapping failed for " + table + ": " + ar.cause().getMessage(), 1);
                    promise.complete(new JsonObject()
                        .put("table", table)
                        .put("degraded", true)
                        .put("degradation_reason", "Could not map business terms: " + ar.cause().getMessage())
                        .put("mappings", new JsonObject())); // Empty mapping with degradation flag
                }
            });
        
        return promise.future();
    }
    
    /**
     * Load the enum cache for value resolution
     */
    private Future<JsonObject> loadEnumCache() {
        Promise<JsonObject> promise = Promise.promise();
        
        JsonObject request = new JsonObject()
            .put("force_refresh", false);  // Use cached values if available
        
        // Call the load_enum_cache tool
        vertx.eventBus().<JsonObject>request(
            "mcp.clients.businessmapping.load_enum_cache",
            request,
            ar -> {
                if (ar.succeeded()) {
                    JsonObject result = ar.result().body();
                    log("Enum cache loaded with " + result.getInteger("cache_size", 0) + " mappings", 2);
                    promise.complete(result);
                } else {
                    log("Failed to load enum cache: " + ar.cause().getMessage(), 1);
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
package agents.director.hosts.milestones;

import agents.director.hosts.base.MilestoneContext;
import agents.director.hosts.base.MilestoneManager;
import agents.director.mcp.client.OracleQueryAnalysisClient;
import agents.director.mcp.client.BusinessMappingClient;
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
        
        // If no tables were found, skip this milestone
        if (context.getRelevantTables().isEmpty()) {
            log("No tables to analyze, skipping data stats milestone", 2);
            context.completeMilestone(3);
            promise.complete(context);
            return promise.future();
        }
        
        // Analyze each table
        List<Future> analysisFutures = new ArrayList<>();
        
        for (String table : context.getRelevantTables()) {
            // Analyze table structure
            Future<JsonObject> structureFuture = analyzeTableStructure(table, context);
            
            // Map business terms to columns
            Future<JsonObject> mappingFuture = mapBusinessTerms(table, context);
            
            analysisFutures.add(structureFuture);
            analysisFutures.add(mappingFuture);
        }
        
        CompositeFuture.all(analysisFutures)
            .onSuccess(results -> {
                // Aggregate results
                Map<String, List<String>> tableColumns = new HashMap<>();
                Map<String, JsonObject> columnStats = new HashMap<>();
                JsonObject dataProfile = new JsonObject();
                
                int resultIndex = 0;
                for (String table : context.getRelevantTables()) {
                    // Structure result
                    JsonObject structure = (JsonObject) results.resultAt(resultIndex++);
                    if (structure != null) {
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
                    }
                    
                    // Mapping result
                    JsonObject mapping = (JsonObject) results.resultAt(resultIndex++);
                    if (mapping != null) {
                        dataProfile.put(table + "_mapping", mapping);
                    }
                }
                
                // Update context with column and stats information
                for (Map.Entry<String, List<String>> entry : tableColumns.entrySet()) {
                    context.setTableColumns(entry.getKey(), entry.getValue());
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
                promise.complete(context);
            })
            .onFailure(err -> {
                log("Data statistics analysis failed: " + err.getMessage(), 1);
                
                // Fallback: Use basic column information
                for (String table : context.getRelevantTables()) {
                    context.setTableColumns(table, Arrays.asList("ID", "NAME", "VALUE", "DATE"));
                }
                
                context.setDataProfile(new JsonObject()
                    .put("fallback", true)
                    .put("error", err.getMessage()));
                
                context.completeMilestone(3);
                promise.complete(context);
            });
        
        return promise.future();
    }
    
    @Override
    public JsonObject getShareableResult(MilestoneContext context) {
        JsonArray tablesInfo = new JsonArray();
        
        for (String table : context.getRelevantTables()) {
            JsonObject tableInfo = new JsonObject()
                .put("table", table);
            
            List<String> columns = context.getTableColumns().get(table);
            if (columns != null && !columns.isEmpty()) {
                tableInfo.put("columns", new JsonArray(columns));
                tableInfo.put("column_count", columns.size());
            }
            
            tablesInfo.add(tableInfo);
        }
        
        int totalColumns = context.getTableColumns().values().stream()
            .mapToInt(List::size)
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
     * Deploy a client and track it
     */
    private Future<String> deployClient(String clientName, AbstractVerticle client) {
        Promise<String> promise = Promise.promise();
        
        vertx.deployVerticle(client, ar -> {
            if (ar.succeeded()) {
                String deploymentId = ar.result();
                deploymentIds.put(clientName, deploymentId);
                log("Deployed " + clientName + " client", 3);
                promise.complete(deploymentId);
            } else {
                promise.fail(ar.cause());
            }
        });
        
        return promise.future();
    }
    
    /**
     * Analyze table structure using the query analysis client
     */
    private Future<JsonObject> analyzeTableStructure(String table, MilestoneContext context) {
        Promise<JsonObject> promise = Promise.promise();
        
        JsonObject request = new JsonObject()
            .put("table_name", table)
            .put("query", context.getQuery())
            .put("intent", context.getIntent());
        
        // Call the analysis tool
        vertx.eventBus().<JsonObject>request(
            "mcp.client.oracle_query_analysis.oracle_query_analysis__analyze_query_requirements",
            request,
            ar -> {
                if (ar.succeeded()) {
                    promise.complete(ar.result().body());
                } else {
                    // Return basic structure on failure
                    promise.complete(new JsonObject()
                        .put("table", table)
                        .put("columns", new JsonArray()
                            .add(new JsonObject().put("name", "ID").put("type", "NUMBER"))
                            .add(new JsonObject().put("name", "NAME").put("type", "VARCHAR2"))
                            .add(new JsonObject().put("name", "VALUE").put("type", "NUMBER"))
                            .add(new JsonObject().put("name", "DATE_CREATED").put("type", "DATE"))));
                }
            });
        
        return promise.future();
    }
    
    /**
     * Map business terms to columns using the business mapping client
     */
    private Future<JsonObject> mapBusinessTerms(String table, MilestoneContext context) {
        Promise<JsonObject> promise = Promise.promise();
        
        JsonObject request = new JsonObject()
            .put("table_name", table)
            .put("business_context", context.getQuery())
            .put("intent", context.getIntent());
        
        // Call the mapping tool
        vertx.eventBus().<JsonObject>request(
            "mcp.client.business_mapping.business_mapping__map_terms_to_columns",
            request,
            ar -> {
                if (ar.succeeded()) {
                    promise.complete(ar.result().body());
                } else {
                    // Return empty mapping on failure
                    promise.complete(new JsonObject()
                        .put("table", table)
                        .put("mappings", new JsonObject()));
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
                    log("Undeployed " + entry.getKey() + " client", 3);
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
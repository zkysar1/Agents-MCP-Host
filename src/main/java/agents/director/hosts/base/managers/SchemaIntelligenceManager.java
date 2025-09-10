package agents.director.hosts.base.managers;

import agents.director.mcp.client.OracleSchemaIntelligenceClient;
import agents.director.mcp.client.BusinessMappingClient;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;

import java.util.Arrays;
import java.util.List;

/**
 * Manager for schema intelligence clients.
 * Coordinates between SchemaIntelligence and BusinessMapping clients
 * to provide comprehensive schema understanding and business term mapping.
 */
public class SchemaIntelligenceManager extends MCPClientManager {
    
    private static final String SCHEMA_CLIENT = "schema";
    private static final String BUSINESS_CLIENT = "business";
    
    public SchemaIntelligenceManager(Vertx vertx, String baseUrl) {
        super(vertx, baseUrl);
    }
    
    @Override
    public Future<Void> initialize() {
        List<Future> deploymentFutures = Arrays.asList(
            deployClient(SCHEMA_CLIENT, new OracleSchemaIntelligenceClient(baseUrl)),
            deployClient(BUSINESS_CLIENT, new BusinessMappingClient(baseUrl))
        );
        
        return CompositeFuture.all(deploymentFutures).mapEmpty();
    }
    
    /**
     * Map business terms to technical database schema
     * @param businessTerms Array of business terms to map
     * @return Future with mapped schema elements
     */
    public Future<JsonObject> mapBusinessToTechnical(JsonArray businessTerms) {
        // First map business terms
        return callClientTool(BUSINESS_CLIENT, "map_business_terms",
            new JsonObject().put("terms", businessTerms))
            .compose(mappedTerms -> {
                // Then match to Oracle schema
                return callClientTool(SCHEMA_CLIENT, "match_oracle_schema",
                    new JsonObject()
                        .put("mappedTerms", mappedTerms)
                        .put("businessTerms", businessTerms))
                    .map(schemaMatch -> {
                        // Combine results
                        return new JsonObject()
                            .put("businessTerms", businessTerms)
                            .put("mappedTerms", mappedTerms)
                            .put("schemaElements", schemaMatch);
                    });
            });
    }
    
    /**
     * Discover relationships between tables
     * @param tables Array of table names
     * @return Future with discovered relationships
     */
    public Future<JsonObject> discoverRelationships(JsonArray tables) {
        return callClientTool(SCHEMA_CLIENT, "infer_table_relationships",
            new JsonObject().put("tables", tables));
    }
    
    /**
     * Suggest optimal query strategy based on query and schema
     * @param query The user's query
     * @param schemaContext Current schema context
     * @return Future with suggested strategy
     */
    public Future<JsonObject> suggestOptimalStrategy(String query, JsonObject schemaContext) {
        // Get strategy suggestion from schema intelligence
        Future<JsonObject> strategyFuture = callClientTool(SCHEMA_CLIENT, "suggest_strategy",
            new JsonObject()
                .put("query", query)
                .put("schemaContext", schemaContext));
        
        // Extract business terms from query
        Future<JsonObject> businessTermsFuture = callClientTool(BUSINESS_CLIENT, "map_business_terms",
            new JsonObject().put("query", query));
        
        return CompositeFuture.all(strategyFuture, businessTermsFuture)
            .map(composite -> {
                JsonObject strategy = composite.resultAt(0);
                JsonObject businessTerms = composite.resultAt(1);
                
                // Enhance strategy with business context
                strategy.put("businessContext", businessTerms);
                return strategy;
            });
    }
    
    /**
     * Translate enum values between business and technical terms
     * @param enumType The type of enum
     * @param values The values to translate
     * @param direction "business_to_technical" or "technical_to_business"
     * @return Future with translated values
     */
    public Future<JsonObject> translateEnumValues(String enumType, JsonArray values, String direction) {
        return callClientTool(BUSINESS_CLIENT, "translate_enum",
            new JsonObject()
                .put("enumType", enumType)
                .put("values", values)
                .put("direction", direction));
    }
    
    /**
     * Get comprehensive schema intelligence for a query
     * @param query The user's query
     * @return Future with full schema intelligence
     */
    public Future<JsonObject> getComprehensiveIntelligence(String query) {
        // Extract business terms
        Future<JsonObject> businessTermsFuture = callClientTool(BUSINESS_CLIENT, "map_business_terms",
            new JsonObject().put("query", query));
        
        // Match to schema
        Future<JsonObject> schemaMatchFuture = businessTermsFuture
            .compose(terms -> callClientTool(SCHEMA_CLIENT, "match_oracle_schema",
                new JsonObject()
                    .put("query", query)
                    .put("businessTerms", terms)));
        
        // Infer relationships
        Future<JsonObject> relationshipsFuture = schemaMatchFuture
            .compose(match -> {
                JsonArray tables = match.getJsonArray("tables", new JsonArray());
                if (tables.size() > 0) {
                    return callClientTool(SCHEMA_CLIENT, "infer_table_relationships",
                        new JsonObject().put("tables", tables));
                }
                return Future.succeededFuture(new JsonObject());
            });
        
        // Suggest strategy
        Future<JsonObject> strategyFuture = callClientTool(SCHEMA_CLIENT, "suggest_strategy",
            new JsonObject().put("query", query));
        
        return CompositeFuture.all(businessTermsFuture, schemaMatchFuture, relationshipsFuture, strategyFuture)
            .map(composite -> new JsonObject()
                .put("businessTerms", composite.resultAt(0))
                .put("schemaMatch", composite.resultAt(1))
                .put("relationships", composite.resultAt(2))
                .put("suggestedStrategy", composite.resultAt(3)));
    }
}
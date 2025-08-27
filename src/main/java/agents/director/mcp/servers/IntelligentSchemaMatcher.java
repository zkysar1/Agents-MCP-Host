package agents.director.mcp.servers;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.*;

/**
 * IntelligentSchemaMatcher - Combines all intelligence tools for smart schema matching.
 * 
 * This tool orchestrates:
 * - Deep query analysis to understand ALL requirements
 * - Business term mapping to database terms
 * - Column semantics discovery
 * - Relationship inference
 * 
 * It ensures we find ALL necessary columns and tables for complex queries.
 */
public class IntelligentSchemaMatcher {
    
    private final SchemaMatcher basicMatcher;
    private final DeepQueryAnalyzer queryAnalyzer;
    private final BusinessTermMapper termMapper;
    private final ColumnSemanticsDiscoverer semanticsDiscoverer;
    private final RelationshipInferrer relationshipInferrer;
    
    public IntelligentSchemaMatcher(SchemaMatcher basicMatcher,
                                  DeepQueryAnalyzer queryAnalyzer,
                                  BusinessTermMapper termMapper,
                                  ColumnSemanticsDiscoverer semanticsDiscoverer,
                                  RelationshipInferrer relationshipInferrer) {
        this.basicMatcher = basicMatcher;
        this.queryAnalyzer = queryAnalyzer;
        this.termMapper = termMapper;
        this.semanticsDiscoverer = semanticsDiscoverer;
        this.relationshipInferrer = relationshipInferrer;
    }
    
    /**
     * Perform intelligent schema matching using all available tools
     */
    public Future<JsonObject> match(String query, JsonArray conversationHistory) {
        if (query == null || query.trim().isEmpty()) {
            return Future.succeededFuture(new JsonObject()
                .put("error", "No query provided")
                .put("matches", new JsonObject()));
        }
        
        // Step 1: Deep query analysis
        long startTime = System.currentTimeMillis();
        
        return queryAnalyzer.analyze(query, conversationHistory)
            .compose(queryAnalysis -> {
                long analysisTime = System.currentTimeMillis() - startTime;
                System.out.println("[IntelligentSchemaMatcher] Query analysis complete in " + 
                                 analysisTime + "ms. Intent: " + 
                                 queryAnalysis.getString("intent"));
                
                // Step 2: Extract all terms that need mapping
                JsonArray allTerms = extractAllTerms(queryAnalysis);
                
                // Step 3: Get basic schema matches for initial context
                Set<String> termsSet = new HashSet<>();
                for (int i = 0; i < allTerms.size(); i++) {
                    termsSet.add(allTerms.getString(i));
                }
                
                return basicMatcher.findMatches(termsSet)
                    .compose(basicMatches -> {
                        long basicMatchTime = System.currentTimeMillis() - startTime;
                        System.out.println("[IntelligentSchemaMatcher] Basic matches found in " + 
                                         basicMatchTime + "ms: " + 
                                         basicMatches.tableMatches.size() + " tables, " +
                                         basicMatches.columnMatches.size() + " columns");
                        
                        // Step 4: Map business terms to database terms
                        JsonObject schemaContext = new JsonObject()
                            .put("match_result", basicMatches.toJson());
                        
                        return termMapper.mapTerms(allTerms, schemaContext)
                            .compose(termMappings -> {
                                System.out.println("[IntelligentSchemaMatcher] Term mappings complete");
                                
                                // Step 5: Discover column semantics for matched tables
                                JsonArray tables = extractTables(basicMatches);
                                
                                return semanticsDiscoverer.discoverSemantics(tables, queryAnalysis)
                                    .compose(semantics -> {
                                        System.out.println("[IntelligentSchemaMatcher] Semantics discovered");
                                        
                                        // Step 6: Infer relationships between tables
                                        return relationshipInferrer.inferRelationships(tables, queryAnalysis)
                                            .map(relationships -> {
                                                // Combine all results
                                                return combineResults(query, queryAnalysis, 
                                                                    basicMatches, termMappings, 
                                                                    semantics, relationships);
                                            });
                                    });
                            });
                    });
            })
            .recover(err -> {
                long errorTime = System.currentTimeMillis() - startTime;
                System.err.println("[IntelligentSchemaMatcher] Error after " + errorTime + 
                                 "ms: " + err.getMessage());
                err.printStackTrace();
                return Future.succeededFuture(createErrorResult(query, err.getMessage()));
            });
    }
    
    /**
     * Extract all terms from query analysis
     */
    private JsonArray extractAllTerms(JsonObject queryAnalysis) {
        Set<String> allTerms = new HashSet<>();
        
        // Add entities
        JsonArray entities = queryAnalysis.getJsonArray("entities", new JsonArray());
        for (int i = 0; i < entities.size(); i++) {
            allTerms.add(entities.getString(i));
        }
        
        // Add attributes
        JsonArray attributes = queryAnalysis.getJsonArray("attributes", new JsonArray());
        for (int i = 0; i < attributes.size(); i++) {
            allTerms.add(attributes.getString(i));
        }
        
        // Add filter concepts
        JsonArray filters = queryAnalysis.getJsonArray("filters", new JsonArray());
        for (int i = 0; i < filters.size(); i++) {
            JsonObject filter = filters.getJsonObject(i);
            if (filter.containsKey("concept")) {
                allTerms.add(filter.getString("concept"));
            }
        }
        
        // Add aggregation targets
        JsonArray aggregations = queryAnalysis.getJsonArray("aggregations", new JsonArray());
        for (int i = 0; i < aggregations.size(); i++) {
            JsonObject agg = aggregations.getJsonObject(i);
            if (agg.containsKey("of")) {
                allTerms.add(agg.getString("of"));
            }
        }
        
        return new JsonArray(new ArrayList<>(allTerms));
    }
    
    /**
     * Extract tables from basic matches
     */
    private JsonArray extractTables(SchemaMatcher.MatchResult basicMatches) {
        Set<String> tables = new HashSet<>();
        
        // From table matches
        for (SchemaMatcher.TableMatch tm : basicMatches.tableMatches) {
            tables.add(tm.tableName);
        }
        
        // From column matches
        for (SchemaMatcher.ColumnMatch cm : basicMatches.columnMatches) {
            tables.add(cm.tableName);
        }
        
        // Limit to reasonable number
        List<String> tableList = new ArrayList<>(tables);
        if (tableList.size() > 5) {
            tableList = tableList.subList(0, 5);
        }
        
        return new JsonArray(tableList);
    }
    
    /**
     * Combine all intelligence results
     */
    private JsonObject combineResults(String originalQuery,
                                    JsonObject queryAnalysis,
                                    SchemaMatcher.MatchResult basicMatches,
                                    JsonObject termMappings,
                                    JsonObject semantics,
                                    JsonObject relationships) {
        JsonObject result = new JsonObject();
        
        // Query understanding
        result.put("query", originalQuery);
        result.put("analysis", queryAnalysis);
        
        // Schema matches with enhancements
        JsonObject enhancedMatches = enhanceMatches(basicMatches, semantics, termMappings);
        result.put("schema_matches", enhancedMatches);
        
        // Discovered columns based on semantics
        JsonArray requiredColumns = extractRequiredColumns(queryAnalysis, semantics);
        result.put("required_columns", requiredColumns);
        
        // Relationships for joining
        result.put("relationships", relationships);
        
        // SQL generation hints
        JsonObject sqlHints = generateSqlHints(queryAnalysis, enhancedMatches, 
                                              requiredColumns, relationships);
        result.put("sql_hints", sqlHints);
        
        // Overall confidence
        double confidence = calculateOverallConfidence(basicMatches, semantics, relationships);
        result.put("confidence", confidence);
        
        return result;
    }
    
    /**
     * Enhance basic matches with semantic information
     */
    private JsonObject enhanceMatches(SchemaMatcher.MatchResult basicMatches,
                                    JsonObject semantics,
                                    JsonObject termMappings) {
        JsonObject enhanced = basicMatches.toJson();
        
        // Add semantic discoveries
        JsonArray discoveries = semantics.getJsonArray("discoveries", new JsonArray());
        JsonObject summary = semantics.getJsonObject("summary", new JsonObject());
        
        enhanced.put("semantic_discoveries", discoveries);
        enhanced.put("semantic_summary", summary);
        
        // Add term mappings
        enhanced.put("term_mappings", termMappings.getJsonArray("mappings", new JsonArray()));
        
        return enhanced;
    }
    
    /**
     * Extract required columns based on query analysis and semantics
     */
    private JsonArray extractRequiredColumns(JsonObject queryAnalysis, JsonObject semantics) {
        JsonArray required = new JsonArray();
        Set<String> addedColumns = new HashSet<>();
        
        String intent = queryAnalysis.getString("intent", "");
        JsonArray aggregations = queryAnalysis.getJsonArray("aggregations", new JsonArray());
        JsonObject temporal = queryAnalysis.getJsonObject("temporal", new JsonObject());
        JsonArray sorting = queryAnalysis.getJsonArray("sorting", new JsonArray());
        
        // Get relevant columns from semantic summary
        JsonObject summary = semantics.getJsonObject("summary", new JsonObject());
        JsonArray relevantColumns = summary.getJsonArray("relevant_columns", new JsonArray());
        
        for (int i = 0; i < relevantColumns.size(); i++) {
            JsonObject col = relevantColumns.getJsonObject(i);
            JsonArray usage = col.getJsonArray("usage", new JsonArray());
            
            // Determine if this column is needed
            boolean needed = false;
            String reason = "";
            
            // Check against intent
            if (intent.contains("count") && usage.contains("COUNT_DISTINCT")) {
                needed = true;
                reason = "needed for counting";
            } else if (intent.contains("sum") && usage.contains("SUM")) {
                needed = true;
                reason = "needed for sum calculation";
            } else if (intent.contains("average") && usage.contains("AVG")) {
                needed = true;
                reason = "needed for average calculation";
            } else if (intent.contains("list") && usage.contains("SELECT")) {
                needed = true;
                reason = "needed for display";
            }
            
            // Check against aggregations
            for (int j = 0; j < aggregations.size(); j++) {
                JsonObject agg = aggregations.getJsonObject(j);
                String aggType = agg.getString("type", "");
                
                if (aggType.equals("sum") && usage.contains("SUM")) {
                    needed = true;
                    reason = "needed for sum aggregation";
                } else if (aggType.equals("count") && usage.contains("COUNT_DISTINCT")) {
                    needed = true;
                    reason = "needed for count aggregation";
                }
            }
            
            // Check temporal needs
            if (!temporal.isEmpty() && usage.contains("FILTER_DATE")) {
                needed = true;
                reason = "needed for date filtering";
            }
            
            // Check sorting needs
            for (int j = 0; j < sorting.size(); j++) {
                JsonObject sort = sorting.getJsonObject(j);
                if (usage.contains("ORDER_BY_DESC") || usage.contains("SELECT")) {
                    needed = true;
                    reason = "needed for sorting";
                }
            }
            
            // Always include high relevance columns
            if (col.getDouble("relevance", 0.0) > 0.7) {
                needed = true;
                reason = "high relevance to query";
            }
            
            if (needed) {
                String key = col.getString("table") + "." + col.getString("column");
                if (!addedColumns.contains(key)) {
                    addedColumns.add(key);
                    required.add(new JsonObject()
                        .put("table", col.getString("table"))
                        .put("column", col.getString("column"))
                        .put("type", col.getString("type"))
                        .put("reason", reason)
                        .put("usage", usage));
                }
            }
        }
        
        return required;
    }
    
    /**
     * Generate SQL hints based on all analysis
     */
    private JsonObject generateSqlHints(JsonObject queryAnalysis,
                                      JsonObject enhancedMatches,
                                      JsonArray requiredColumns,
                                      JsonObject relationships) {
        JsonObject hints = new JsonObject();
        
        // SELECT clause hints
        JsonArray selectHints = new JsonArray();
        for (int i = 0; i < requiredColumns.size(); i++) {
            JsonObject col = requiredColumns.getJsonObject(i);
            JsonArray usage = col.getJsonArray("usage", new JsonArray());
            
            if (usage.contains("SUM")) {
                selectHints.add("SUM(" + col.getString("table") + "." + 
                              col.getString("column") + ")");
            } else if (usage.contains("COUNT_DISTINCT")) {
                selectHints.add("COUNT(DISTINCT " + col.getString("table") + "." + 
                              col.getString("column") + ")");
            } else if (usage.contains("AVG")) {
                selectHints.add("AVG(" + col.getString("table") + "." + 
                              col.getString("column") + ")");
            } else if (usage.contains("SELECT")) {
                selectHints.add(col.getString("table") + "." + col.getString("column"));
            }
        }
        hints.put("select_hints", selectHints);
        
        // JOIN hints from relationships
        JsonArray joinPaths = relationships.getJsonArray("join_paths", new JsonArray());
        hints.put("join_hints", joinPaths);
        
        // WHERE clause hints from filters
        JsonArray whereHints = new JsonArray();
        JsonArray filters = queryAnalysis.getJsonArray("filters", new JsonArray());
        for (int i = 0; i < filters.size(); i++) {
            JsonObject filter = filters.getJsonObject(i);
            whereHints.add(filter);
        }
        hints.put("where_hints", whereHints);
        
        // GROUP BY hints
        JsonArray groupByHints = new JsonArray();
        for (int i = 0; i < requiredColumns.size(); i++) {
            JsonObject col = requiredColumns.getJsonObject(i);
            JsonArray usage = col.getJsonArray("usage", new JsonArray());
            
            if (usage.contains("GROUP_BY")) {
                groupByHints.add(col.getString("table") + "." + col.getString("column"));
            }
        }
        hints.put("group_by_hints", groupByHints);
        
        // ORDER BY hints
        JsonArray orderByHints = new JsonArray();
        JsonArray sorting = queryAnalysis.getJsonArray("sorting", new JsonArray());
        for (int i = 0; i < sorting.size(); i++) {
            orderByHints.add(sorting.getJsonObject(i));
        }
        hints.put("order_by_hints", orderByHints);
        
        return hints;
    }
    
    /**
     * Calculate overall confidence in the match
     */
    private double calculateOverallConfidence(SchemaMatcher.MatchResult basicMatches,
                                            JsonObject semantics,
                                            JsonObject relationships) {
        double basicConfidence = basicMatches.confidence;
        
        // Semantic confidence
        JsonObject summary = semantics.getJsonObject("summary", new JsonObject());
        double semanticScore = summary.getDouble("best_score", 0.0);
        
        // Relationship confidence
        JsonObject stats = relationships.getJsonObject("statistics", new JsonObject());
        int totalRels = stats.getInteger("total_relationships", 0);
        double relConfidence = totalRels > 0 ? 0.8 : 0.5;
        
        // Weighted average
        return (basicConfidence * 0.3) + (semanticScore * 0.5) + (relConfidence * 0.2);
    }
    
    /**
     * Create error result
     */
    private JsonObject createErrorResult(String query, String error) {
        return new JsonObject()
            .put("query", query)
            .put("error", error)
            .put("matches", new JsonObject())
            .put("confidence", 0.0);
    }
}
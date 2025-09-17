package agents.director.services;

import io.vertx.core.Vertx;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import agents.director.Driver;
import static agents.director.Driver.logLevel;
import java.sql.*;
import java.util.*;

/**
 * Knowledge Graph Builder for Oracle Schema Intelligence.
 * Builds an in-memory graph at startup containing:
 * - Complete schema metadata (tables, columns, types)
 * - Foreign key relationships for join path discovery
 * - Synonym mappings for business term resolution
 * - Column indexes for disambiguation
 * Uses Vert.x async patterns for parallel loading and non-blocking operations.
 */
public class KnowledgeGraphBuilder {

    private static KnowledgeGraphBuilder instance;
    private volatile Vertx vertx;
    private OracleConnectionManager connectionManager;

    // The in-memory knowledge graph containing everything
    private volatile JsonObject knowledgeGraph;
    private volatile boolean graphBuilt = false;
    private volatile long graphBuildTimestamp = 0;

    private LlmAPIService llmService;

    private KnowledgeGraphBuilder() {
        // Private constructor for singleton
    }

    /**
     * Get singleton instance
     */
    public static synchronized KnowledgeGraphBuilder getInstance() {
        if (instance == null) {
            instance = new KnowledgeGraphBuilder();
        }
        return instance;
    }

    /**
     * Initialize and build the knowledge graph
     */
    public Future<JsonObject> initialize(Vertx vertx) {
        this.vertx = vertx;
        this.connectionManager = OracleConnectionManager.getInstance();
        this.llmService = LlmAPIService.getInstance();

        vertx.eventBus().publish("log", "[KnowledgeGraphBuilder] Starting knowledge graph initialization,1,KnowledgeGraphBuilder,Graph,Startup");

        // Check if connection manager is healthy
        if (!connectionManager.isConnectionHealthy()) {
            String errorMsg = "[KnowledgeGraphBuilder] Cannot build graph - Oracle connection not healthy";
            vertx.eventBus().publish("log", errorMsg + ",0,KnowledgeGraphBuilder,Graph,Error");
            return Future.failedFuture(new RuntimeException(errorMsg));
        }

        // Build the graph
        return buildGraph()
            .onSuccess(graph -> {
                // Set everything atomically to avoid race conditions
                this.knowledgeGraph = graph;
                this.graphBuildTimestamp = System.currentTimeMillis();
                this.graphBuilt = true; // Set this last after everything is ready

                // Keep print for debugging + add log level 4 (data)
                System.out.println(knowledgeGraph.encode());
                if (logLevel >= 4) vertx.eventBus().publish("log", "Knowledge graph JSON: " + knowledgeGraph.encode() + ",4,KnowledgeGraphBuilder,Graph,Data");

                // Log statistics
                JsonObject metadata = graph.getJsonObject("metadata");
                vertx.eventBus().publish("log", String.format(
                    "[KnowledgeGraphBuilder] Graph built successfully - Tables: %d, Relationships: %d, Synonyms: %d,1,KnowledgeGraphBuilder,Graph,Success",
                    metadata.getInteger("tableCount", 0),
                    metadata.getInteger("relationshipCount", 0),
                    metadata.getInteger("synonymCount", 0)
                ));

                // Log table names for debugging
                logGraphContents(graph);
            })
            .onFailure(error -> {
                vertx.eventBus().publish("log",
                    "[KnowledgeGraphBuilder] Failed to build knowledge graph: " + error.getMessage() + ",0,KnowledgeGraphBuilder,Graph,Error");
            });
    }

    /**
     * Build the complete knowledge graph - sequential and simple
     */
    private Future<JsonObject> buildGraph() {
        long startTime = System.currentTimeMillis();

        // Sequential loading for simplicity and to enable proper cross-referencing
        return loadTablesAndColumns()
            .compose(tables -> {
                // Store tables as we go for building references
                JsonObject tablesObj = new JsonObject();
                for (int i = 0; i < tables.size(); i++) {
                    JsonObject table = tables.getJsonObject(i);
                    String tableName = table.getString("tableName");
                    if (tableName != null) {
                        tablesObj.put(tableName.toUpperCase(), table);
                    }
                }

                return loadForeignKeys()
                    .compose(relationships ->
                        loadEnumTablesAndValues(relationships)
                            .compose(enumData ->
                                generateSynonymsFromSchema(tablesObj)
                                    .map(synonyms -> {
                                        // Assemble the final simplified graph
                                        return new JsonObject()
                                            .put("tables", tablesObj)
                                            .put("relationships", relationships)
                                            .put("synonyms", synonyms)
                                            .put("enumTables", enumData.getJsonObject("enumTables", new JsonObject()))
                                            .put("enumValues", enumData.getJsonObject("enumValues", new JsonObject()))
                                            .put("metadata", new JsonObject()
                                                .put("buildTime", System.currentTimeMillis() - startTime)
                                                .put("timestamp", System.currentTimeMillis())
                                                .put("tableCount", tablesObj.size())
                                                .put("relationshipCount", relationships.size())
                                                .put("synonymCount", synonyms.size())
                                                .put("enumTableCount", enumData.getJsonObject("enumTables", new JsonObject()).size())
                                                .put("schema", getDefaultSchema())
                                            );
                                    })
                            )
                    );
            });
    }

    /**
     * Load all tables and columns from the database
     */
    private Future<JsonArray> loadTablesAndColumns() {
        vertx.eventBus().publish("log", "[KnowledgeGraphBuilder] Loading tables and columns,2,KnowledgeGraphBuilder,Graph,Loading");

        String currentSchema = getDefaultSchema();

        // Get list of tables
        return connectionManager.listTables()
            .compose(tableList -> {
                // Load metadata for each table in parallel
                List<Future<JsonObject>> metadataFutures = new ArrayList<>();

                for (int i = 0; i < tableList.size(); i++) {
                    JsonObject tableInfo = tableList.getJsonObject(i);
                    String tableName = tableInfo.getString("name");

                    Future<JsonObject> metadataFuture = connectionManager.getTableMetadata(tableName)
                        .map(metadata -> {
                            // Enhance metadata with schema name
                            metadata.put("schema", currentSchema);
                            metadata.put("rowCount", tableInfo.getLong("row_count", 0L));
                            return metadata;
                        });

                    metadataFutures.add(metadataFuture);
                }

                // Wait for all metadata to load
                // Future.join accepts List<? extends Future<?>> so we can pass our typed list directly
                return Future.join(metadataFutures)
                    .map(composite -> {
                        JsonArray tables = new JsonArray();
                        for (Future<JsonObject> metadataFuture : metadataFutures) {
                            tables.add(metadataFuture.result());
                        }

                        vertx.eventBus().publish("log",
                            "[KnowledgeGraphBuilder] Loaded " + tables.size() + " tables,2,KnowledgeGraphBuilder,Graph,Progress");
                        return tables;
                    });
            });
    }

    /**
     * Load all foreign key relationships
     */
    private Future<JsonArray> loadForeignKeys() {
        vertx.eventBus().publish("log", "[KnowledgeGraphBuilder] Loading foreign key relationships,2,KnowledgeGraphBuilder,Graph,Loading");

        return vertx.executeBlocking(() -> {
            try {
                // Get all foreign keys in the schema

                return connectionManager.executeWithConnection(conn -> {
                    JsonArray rels = new JsonArray();

                    try {
                        DatabaseMetaData metaData = conn.getMetaData();
                        String currentSchema = getDefaultSchema().toUpperCase();

                        // Get all foreign keys in the schema
                        String query = """
                            SELECT
                                a.table_name AS fk_table,
                                a.column_name AS fk_column,
                                b.table_name AS pk_table,
                                b.column_name AS pk_column,
                                a.constraint_name
                            FROM
                                all_cons_columns a
                                JOIN all_constraints c ON a.owner = c.owner
                                    AND a.constraint_name = c.constraint_name
                                JOIN all_cons_columns b ON c.r_owner = b.owner
                                    AND c.r_constraint_name = b.constraint_name
                            WHERE
                                c.constraint_type = 'R'
                                AND a.owner = ?
                            ORDER BY
                                a.table_name, a.constraint_name, a.position
                        """;

                        try (PreparedStatement stmt = conn.prepareStatement(query)) {
                            stmt.setString(1, currentSchema);

                            try (ResultSet rs = stmt.executeQuery()) {
                                while (rs.next()) {
                                    JsonObject rel = new JsonObject()
                                        .put("type", "foreign_key")
                                        .put("fromTable", rs.getString("FK_TABLE"))
                                        .put("fromColumn", rs.getString("FK_COLUMN"))
                                        .put("toTable", rs.getString("PK_TABLE"))
                                        .put("toColumn", rs.getString("PK_COLUMN"))
                                        .put("constraintName", rs.getString("CONSTRAINT_NAME"));

                                    rels.add(rel);
                                }
                            }
                        }

                        vertx.eventBus().publish("log",
                            "[KnowledgeGraphBuilder] Loaded " + rels.size() + " foreign key relationships,2,KnowledgeGraphBuilder,Graph,Progress");

                    } catch (SQLException e) {
                        throw new RuntimeException("Failed to load foreign keys: " + e.getMessage(), e);
                    }

                    return rels;
                }).toCompletionStage().toCompletableFuture().get();

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Load enum tables and their values with proper referencedBy population
     */
    private Future<JsonObject> loadEnumTablesAndValues(JsonArray relationships) {
        vertx.eventBus().publish("log", "[KnowledgeGraphBuilder] Loading enum tables and values,2,KnowledgeGraphBuilder,Graph,Loading");

        return vertx.executeBlocking(() -> {
            try {
                JsonObject enumData = new JsonObject();
                JsonObject enumTables = new JsonObject();
                JsonObject enumValues = new JsonObject();

                // Discover enum tables
                String enumTableQuery = """
                    SELECT table_name
                    FROM user_tables
                    WHERE table_name LIKE '%ENUM%'
                       OR table_name LIKE '%LOOKUP%'
                       OR table_name LIKE '%REFERENCE%'
                       OR table_name LIKE '%_REF'
                       OR table_name LIKE '%_TYPE%'
                       OR table_name LIKE '%_STATUS%'
                       OR table_name LIKE '%_CODE%'
                    ORDER BY table_name
                """;

                JsonArray enumTableResults = connectionManager.executeQuery(enumTableQuery)
                    .toCompletionStage().toCompletableFuture().get();

                // Process each enum table
                for (int i = 0; i < enumTableResults.size(); i++) {
                    JsonObject row = enumTableResults.getJsonObject(i);
                    String tableName = row.getString("TABLE_NAME");

                    // Get table structure to identify ID and description columns
                    JsonObject tableMetadata = connectionManager.getTableMetadata(tableName)
                        .toCompletionStage().toCompletableFuture().get();

                    JsonArray columns = tableMetadata.getJsonArray("columns");
                    if (columns != null && columns.size() >= 2) {
                        // Identify ID and description columns
                        String idColumn = null;
                        String descColumn = null;

                        for (int j = 0; j < columns.size(); j++) {
                            JsonObject col = columns.getJsonObject(j);
                            String colName = col.getString("name");
                            String colType = col.getString("type");

                            // Skip if column name is null
                            if (colName == null) continue;

                            // First column is usually the ID (only assign if not already set)
                            if (idColumn == null && (j == 0 || colName.contains("ID") || colName.contains("CODE"))) {
                                idColumn = colName;
                            } else if (descColumn == null && (colName.contains("DESC") || colName.contains("NAME") ||
                                     colName.contains("LABEL") || (colType != null && colType.contains("VARCHAR")))) {
                                descColumn = colName;
                            }
                        }

                        if (idColumn != null && descColumn != null) {
                            // Validate SQL identifiers to prevent injection
                            if (isValidOracleIdentifier(tableName) &&
                                isValidOracleIdentifier(idColumn) &&
                                isValidOracleIdentifier(descColumn)) {

                                // This is a valid enum table
                                JsonObject enumTable = new JsonObject()
                                    .put("isEnum", true)
                                    .put("idColumn", idColumn)
                                    .put("descColumn", descColumn)
                                    .put("referencedBy", new JsonArray());

                                // Populate referencedBy based on foreign keys
                                JsonArray referencedBy = enumTable.getJsonArray("referencedBy");
                                for (int r = 0; r < relationships.size(); r++) {
                                    JsonObject rel = relationships.getJsonObject(r);
                                    if (tableName.equalsIgnoreCase(rel.getString("toTable"))) {
                                        String fromTable = rel.getString("fromTable");
                                        if (fromTable != null && !referencedBy.contains(fromTable)) {
                                            referencedBy.add(fromTable);
                                        }
                                    }
                                }

                                enumTables.put(tableName, enumTable);

                                // Load enum values (identifiers are now validated)
                                String valuesQuery = String.format(
                                    "SELECT %s, %s FROM %s ORDER BY %s",
                                    idColumn, descColumn, tableName, idColumn
                                );

                                try {
                                    JsonArray valueResults = connectionManager.executeQuery(valuesQuery)
                                        .toCompletionStage().toCompletableFuture().get();

                                    JsonObject tableValues = new JsonObject();
                                    for (int k = 0; k < valueResults.size(); k++) {
                                        JsonObject valueRow = valueResults.getJsonObject(k);
                                        String code = valueRow.getString(idColumn);
                                        String description = valueRow.getString(descColumn);
                                        if (code != null && description != null) {
                                            // Store with uppercase code for consistent retrieval
                                            tableValues.put(code.toUpperCase(), description);
                                        }
                                    }

                                    if (!tableValues.isEmpty()) {
                                        enumValues.put(tableName, tableValues);
                                        vertx.eventBus().publish("log",
                                            "[KnowledgeGraphBuilder] Loaded " + tableValues.size() +
                                            " enum values from " + tableName + ",3,KnowledgeGraphBuilder,Graph,Progress");
                                    }
                                } catch (Exception e) {
                                    vertx.eventBus().publish("log",
                                        "[KnowledgeGraphBuilder] Failed to load values from " + tableName + ": " +
                                        e.getMessage() + ",2,KnowledgeGraphBuilder,Graph,Warning");
                                }
                            } else {
                                vertx.eventBus().publish("log",
                                    "[KnowledgeGraphBuilder] Skipping table with invalid identifiers: " + tableName +
                                    ",1,KnowledgeGraphBuilder,Graph,Warning");
                            }
                        }
                    }
                }

                enumData.put("enumTables", enumTables);
                enumData.put("enumValues", enumValues);

                vertx.eventBus().publish("log",
                    "[KnowledgeGraphBuilder] Discovered " + enumTables.size() + " enum tables,2,KnowledgeGraphBuilder,Graph,Progress");

                return enumData;

            } catch (Exception e) {
                vertx.eventBus().publish("log",
                    "[KnowledgeGraphBuilder] Failed to load enum data: " + e.getMessage() + ",1,KnowledgeGraphBuilder,Graph,Warning");
                // Return empty but don't fail the whole graph build
                return new JsonObject()
                    .put("enumTables", new JsonObject())
                    .put("enumValues", new JsonObject());
            }
        });
    }

    /**
     * Generate synonyms dynamically from actual schema using LLM
     */
    private Future<JsonObject> generateSynonymsFromSchema(JsonObject tables) {
        vertx.eventBus().publish("log", "[KnowledgeGraphBuilder] Generating dynamic synonyms from schema,2,KnowledgeGraphBuilder,Graph,Loading");

        // Initialize LLM service if not already done
        if (llmService == null) {
            llmService = LlmAPIService.getInstance();
        }

        // If LLM is not available, fail open with empty synonyms
        if (!llmService.isInitialized()) {
            vertx.eventBus().publish("log", "[KnowledgeGraphBuilder] LLM service not available - returning empty synonyms (fail open),1,KnowledgeGraphBuilder,Graph,Warning");
            return Future.succeededFuture(new JsonObject());
        } else {
            vertx.eventBus().publish("log", "[KnowledgeGraphBuilder] LLM service available - generating dynamic synonyms from schema,2,KnowledgeGraphBuilder,Graph,Info");
        }

        return vertx.executeBlocking(() -> {
            try {
                // Extract unique terms from table and column names
                Set<String> uniqueTerms = new HashSet<>();
                JsonArray schemaElements = new JsonArray();

                for (String tableName : tables.fieldNames()) {
                    JsonObject table = tables.getJsonObject(tableName);
                    uniqueTerms.add(tableName.toLowerCase());

                    // Add table info for LLM context
                    JsonObject tableInfo = new JsonObject()
                        .put("table", tableName);

                    JsonArray columns = table.getJsonArray("columns");
                    if (columns != null) {
                        JsonArray columnNames = new JsonArray();
                        for (int i = 0; i < columns.size(); i++) {
                            JsonObject col = columns.getJsonObject(i);
                            String colName = col.getString("name");
                            if (colName != null) {
                                uniqueTerms.add(colName.toLowerCase());
                                columnNames.add(colName);
                            }
                        }
                        tableInfo.put("columns", columnNames);
                    }
                    schemaElements.add(tableInfo);
                }

                // Use LLM to generate synonyms based on actual schema
                String prompt = "Analyze this database schema and identify business term synonyms. " +
                    "For each group of related terms (like customer/client/user or order/purchase/transaction), " +
                    "create a synonym mapping. Focus on terms that actually appear in the schema.\n\n" +
                    "Schema:\n" + schemaElements.encodePrettily() + "\n\n" +
                    "Return a JSON object where keys are primary terms and values are objects with:\n" +
                    "- category: the business concept\n" +
                    "- related_terms: array of synonymous terms found in the schema\n" +
                    "- confidence: 0.0-1.0 based on how certain the relationship is\n\n" +
                    "Only include terms that actually appear in the provided schema. Return empty object {} if no clear synonyms found.";

                JsonArray messages = new JsonArray()
                    .add(new JsonObject().put("role", "system").put("content", "You are a database schema analyst. Identify business term synonyms based only on the provided schema."))
                    .add(new JsonObject().put("role", "user").put("content", prompt));

                try {
                    JsonObject llmResponse = llmService.chatCompletion(messages).toCompletionStage().toCompletableFuture().get();

                    // Parse LLM response
                    JsonArray choices = llmResponse.getJsonArray("choices");
                    if (choices != null && !choices.isEmpty()) {
                        JsonObject firstChoice = choices.getJsonObject(0);
                        JsonObject message = firstChoice.getJsonObject("message");
                        String content = message.getString("content");

                        // Log raw content for debugging if at debug level
                        if (Driver.logLevel >= 4) {
                            vertx.eventBus().publish("log",
                                "[KnowledgeGraphBuilder] Raw LLM response for synonyms: " +
                                content.substring(0, Math.min(content.length(), 500)) +
                                ",4,KnowledgeGraphBuilder,Graph,Debug");
                        }

                        // Extract and parse JSON using helper method
                        JsonObject synonyms = extractJsonFromLLMResponse(content);

                        if (!synonyms.isEmpty()) {
                            vertx.eventBus().publish("log",
                                "[KnowledgeGraphBuilder] Successfully parsed " + synonyms.size() +
                                " synonym groups from schema,2,KnowledgeGraphBuilder,Graph,Success");

                            // Log the synonym groups at debug level
                            if (Driver.logLevel >= 3) {
                                for (String key : synonyms.fieldNames()) {
                                    JsonObject group = synonyms.getJsonObject(key);
                                    vertx.eventBus().publish("log",
                                        "[KnowledgeGraphBuilder] Synonym group '" + key + "': " +
                                        group.encodePrettily().substring(0, Math.min(group.encodePrettily().length(), 200)) +
                                        ",3,KnowledgeGraphBuilder,Graph,Debug");
                                }
                            }
                        } else {
                            vertx.eventBus().publish("log",
                                "[KnowledgeGraphBuilder] No synonyms extracted from LLM response (may be empty or parsing failed),2,KnowledgeGraphBuilder,Graph,Warning");
                        }

                        return synonyms;
                    }
                } catch (Exception e) {
                    vertx.eventBus().publish("log",
                        "[KnowledgeGraphBuilder] LLM call failed: " + e.getMessage() + " - returning empty synonyms,1,KnowledgeGraphBuilder,Graph,Warning");
                }

                return new JsonObject();

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }


    /**
     * Extract JSON from LLM response content that may contain markdown formatting
     */
    private JsonObject extractJsonFromLLMResponse(String content) {
        if (content == null || content.isEmpty()) {
            return new JsonObject();
        }

        try {
            // Clean markdown formatting
            content = content.trim();

            // Remove markdown code block markers
            if (content.startsWith("```json")) {
                content = content.substring(7);
            } else if (content.startsWith("```")) {
                content = content.substring(3);
            }

            if (content.endsWith("```")) {
                content = content.substring(0, content.length() - 3);
            }

            // Trim again after removing markers
            content = content.trim();

            // Try direct parsing first
            try {
                return new JsonObject(content);
            } catch (Exception directParseEx) {
                // If direct parsing fails, try to extract JSON using brace matching
                int startIdx = content.indexOf("{");
                int endIdx = content.lastIndexOf("}");

                if (startIdx != -1 && endIdx != -1 && endIdx > startIdx) {
                    String jsonStr = content.substring(startIdx, endIdx + 1);
                    return new JsonObject(jsonStr);
                }

                // Log the actual content for debugging
                if (Driver.logLevel >= 3) {
                    vertx.eventBus().publish("log",
                        "[KnowledgeGraphBuilder] Could not extract JSON from content: " +
                        content.substring(0, Math.min(content.length(), 200)) +
                        ",3,KnowledgeGraphBuilder,Graph,Debug");
                }
            }
        } catch (Exception e) {
            vertx.eventBus().publish("log",
                "[KnowledgeGraphBuilder] Error extracting JSON from LLM response: " + e.getMessage() +
                ",2,KnowledgeGraphBuilder,Graph,Warning");
        }

        return new JsonObject();
    }

    /**
     * Find join path between two tables using BFS - now works directly with relationships array
     */
    public JsonArray findJoinPath(String fromTable, String toTable) {
        if (!graphBuilt) {
            throw new IllegalStateException("Knowledge graph not built yet");
        }

        fromTable = fromTable.toUpperCase();
        toTable = toTable.toUpperCase();

        // BFS to find shortest path
        Queue<JsonArray> queue = new LinkedList<>();
        Set<String> visited = new HashSet<>();

        queue.offer(new JsonArray().add(fromTable));
        visited.add(fromTable);

        JsonArray relationships = knowledgeGraph.getJsonArray("relationships");

        while (!queue.isEmpty()) {
            JsonArray path = queue.poll();
            String currentTable = path.getString(path.size() - 1);

            if (currentTable.equals(toTable)) {
                // Found path - convert to join information
                return buildJoinPath(path);
            }

            // Explore neighbors by scanning all relationships
            if (relationships != null) {
                for (int i = 0; i < relationships.size(); i++) {
                    JsonObject rel = relationships.getJsonObject(i);
                    String relFromTable = rel.getString("fromTable");
                    String relToTable = rel.getString("toTable");

                    // Check both directions
                    String nextTable = null;
                    if (currentTable.equalsIgnoreCase(relFromTable)) {
                        nextTable = relToTable;
                    } else if (currentTable.equalsIgnoreCase(relToTable)) {
                        nextTable = relFromTable;
                    }

                    if (nextTable != null && !visited.contains(nextTable.toUpperCase())) {
                        visited.add(nextTable.toUpperCase());
                        JsonArray newPath = path.copy();
                        newPath.add(nextTable.toUpperCase());
                        queue.offer(newPath);
                    }
                }
            }
        }

        // No path found
        return new JsonArray();
    }

    /**
     * Build join path details from table path - now scans relationships array
     */
    private JsonArray buildJoinPath(JsonArray tablePath) {
        JsonArray joinPath = new JsonArray();
        JsonArray relationships = knowledgeGraph.getJsonArray("relationships");

        for (int i = 0; i < tablePath.size() - 1; i++) {
            String fromTable = tablePath.getString(i);
            String toTable = tablePath.getString(i + 1);

            // Find the relationship by scanning all relationships
            if (relationships != null) {
                for (int j = 0; j < relationships.size(); j++) {
                    JsonObject rel = relationships.getJsonObject(j);
                    String relFromTable = rel.getString("fromTable");
                    String relToTable = rel.getString("toTable");

                    // Check if this relationship connects our tables (either direction)
                    if ((fromTable.equalsIgnoreCase(relFromTable) && toTable.equalsIgnoreCase(relToTable)) ||
                        (fromTable.equalsIgnoreCase(relToTable) && toTable.equalsIgnoreCase(relFromTable))) {

                        // Add join in correct direction
                        if (fromTable.equalsIgnoreCase(relFromTable)) {
                            joinPath.add(new JsonObject()
                                .put("fromTable", fromTable)
                                .put("fromColumn", rel.getString("fromColumn"))
                                .put("toTable", toTable)
                                .put("toColumn", rel.getString("toColumn"))
                                .put("joinType", "INNER JOIN")
                            );
                        } else {
                            // Reverse direction
                            joinPath.add(new JsonObject()
                                .put("fromTable", fromTable)
                                .put("fromColumn", rel.getString("toColumn"))
                                .put("toTable", toTable)
                                .put("toColumn", rel.getString("fromColumn"))
                                .put("joinType", "INNER JOIN")
                            );
                        }
                        break;
                    }
                }
            }
        }

        return joinPath;
    }

    /**
     * Resolve a business term to schema elements - now uses direct synonym lookup
     */
    public JsonArray resolveSynonym(String term) {
        if (!graphBuilt) {
            throw new IllegalStateException("Knowledge graph not built yet");
        }

        JsonArray results = new JsonArray();
        String lowerTerm = term.toLowerCase();

        // Check if term exists in synonyms
        JsonObject synonyms = knowledgeGraph.getJsonObject("synonyms");
        if (synonyms == null || synonyms.isEmpty()) {
            return results; // No synonyms available
        }

        // Look for the term in synonyms
        JsonObject synonymData = synonyms.getJsonObject(lowerTerm);
        if (synonymData != null) {
            String category = synonymData.getString("category");
            JsonArray relatedTerms = synonymData.getJsonArray("related_terms");
            Double confidence = synonymData.getDouble("confidence", 0.5);

            // Find tables and columns that match the related terms
            JsonObject tables = knowledgeGraph.getJsonObject("tables");
            if (tables != null) {
                for (String tableName : tables.fieldNames()) {
                    JsonObject table = tables.getJsonObject(tableName);

                    // Check if table name contains any related term
                    if (relatedTerms != null) {
                        for (int i = 0; i < relatedTerms.size(); i++) {
                            String relatedTerm = relatedTerms.getString(i);
                            if (tableName.toLowerCase().contains(relatedTerm.toLowerCase())) {
                                results.add(new JsonObject()
                                    .put("type", "table")
                                    .put("name", tableName)
                                    .put("confidence", confidence)
                                    .put("reason", "Table name matches " + category + " synonym")
                                );
                                break;
                            }
                        }
                    }

                    // Check columns
                    JsonArray columns = table.getJsonArray("columns");
                    if (columns != null && relatedTerms != null) {
                        for (int j = 0; j < columns.size(); j++) {
                            JsonObject column = columns.getJsonObject(j);
                            String columnName = column.getString("name");

                            if (columnName != null) {
                                for (int i = 0; i < relatedTerms.size(); i++) {
                                    String relatedTerm = relatedTerms.getString(i);
                                    if (columnName.toLowerCase().contains(relatedTerm.toLowerCase())) {
                                        results.add(new JsonObject()
                                            .put("type", "column")
                                            .put("table", tableName)
                                            .put("column", columnName)
                                            .put("confidence", confidence * 0.9)
                                            .put("reason", "Column name matches " + category + " synonym")
                                        );
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        return results;
    }

    /**
     * Disambiguate a column name using context - now scans tables directly
     */
    public JsonArray disambiguateColumn(String columnName, JsonObject context) {
        if (!graphBuilt) {
            throw new IllegalStateException("Knowledge graph not built yet");
        }

        JsonArray results = new JsonArray();
        String upperColumn = columnName.toUpperCase();

        // Scan all tables to find which ones contain this column
        JsonObject tables = knowledgeGraph.getJsonObject("tables");
        if (tables == null) {
            return results;
        }

        // Get context hints
        JsonArray contextTables = context.getJsonArray("tables");
        String queryIntent = context.getString("intent", "");

        for (String tableName : tables.fieldNames()) {
            JsonObject table = tables.getJsonObject(tableName);
            JsonArray columns = table.getJsonArray("columns");

            if (columns != null) {
                // Check if this table has the column
                boolean hasColumn = false;
                for (int i = 0; i < columns.size(); i++) {
                    JsonObject col = columns.getJsonObject(i);
                    String colName = col.getString("name");
                    if (colName != null && colName.equalsIgnoreCase(columnName)) {
                        hasColumn = true;
                        break;
                    }
                }

                if (hasColumn) {
                    double confidence = 0.5; // Base confidence
                    String reason = "Column exists in table";

                    // Boost confidence if table is in context
                    if (contextTables != null && contextTables.contains(tableName)) {
                        confidence = 0.9;
                        reason = "Table is in query context";
                    }

                    // Boost confidence based on query intent
                    if (!queryIntent.isEmpty()) {
                        String lowerTable = tableName.toLowerCase();
                        String lowerIntent = queryIntent.toLowerCase();

                        if (lowerIntent.contains(lowerTable) || lowerTable.contains(lowerIntent)) {
                            confidence = Math.max(confidence, 0.8);
                            reason = "Table name matches query intent";
                        }
                    }

                    results.add(new JsonObject()
                        .put("table", tableName)
                        .put("column", columnName)
                        .put("confidence", confidence)
                        .put("reason", reason)
                    );
                }
            }
        }

        // Sort by confidence
        List<JsonObject> toSort = new ArrayList<>();
        for (int i = 0; i < results.size(); i++) {
            toSort.add(results.getJsonObject(i));
        }

        toSort.sort((a, b) -> Double.compare(
            b.getDouble("confidence"),
            a.getDouble("confidence")
        ));

        JsonArray sorted = new JsonArray();
        for (JsonObject obj : toSort) {
            sorted.add(obj);
        }

        return sorted;
    }


    /**
     * Validate Oracle SQL identifier to prevent injection
     */
    private boolean isValidOracleIdentifier(String identifier) {
        if (identifier == null || identifier.isEmpty()) {
            return false;
        }
        // Oracle identifiers: start with letter, contain only letters, numbers, underscore, #, $
        // Max length 30 characters (128 in 12.2+, but we'll use 30 for compatibility)
        return identifier.matches("^[A-Za-z][A-Za-z0-9_#$]{0,29}$");
    }

    /**
     * Log the contents of the knowledge graph for debugging
     */
    private void logGraphContents(JsonObject graph) {
        try {
            JsonObject tables = graph.getJsonObject("tables");
            if (tables != null && !tables.isEmpty()) {
                // Collect all table names from the JsonObject keys
                List<String> tableNames = new ArrayList<>(tables.fieldNames());

                // Log table names
                vertx.eventBus().publish("log", String.format(
                    "[KnowledgeGraph] Tables in schema: %s,1,KnowledgeGraphBuilder,Graph,Info",
                    String.join(", ", tableNames)
                ));

                // Log detailed info for tables containing order-related terms
                for (String tableName : tables.fieldNames()) {
                    JsonObject table = tables.getJsonObject(tableName);
                    String tableNameLower = tableName.toLowerCase();
                    if (tableNameLower.contains("order") || tableNameLower.contains("purchase") ||
                        tableNameLower.contains("sale") || tableNameLower.contains("transaction")) {

                        JsonArray columns = table.getJsonArray("columns");
                        List<String> columnNames = new ArrayList<>();
                        if (columns != null) {
                            for (int j = 0; j < columns.size(); j++) {
                                JsonObject col = columns.getJsonObject(j);
                                columnNames.add(col.getString("name", ""));
                            }
                        }

                        vertx.eventBus().publish("log", String.format(
                            "[KnowledgeGraph] Order-related table: %s, Columns: %s,2,KnowledgeGraphBuilder,Graph,Detail",
                            tableName,
                            String.join(", ", columnNames)
                        ));
                    }
                }
            } else {
                vertx.eventBus().publish("log",
                    "[KnowledgeGraph] WARNING: No tables found in graph!,1,KnowledgeGraphBuilder,Graph,Warning");
            }
        } catch (Exception e) {
            vertx.eventBus().publish("log",
                "[KnowledgeGraph] Error logging graph contents: " + e.getMessage() + ",1,KnowledgeGraphBuilder,Graph,Error");
        }
    }

    /**
     * Get the default schema name
     */
    private String getDefaultSchema() {
        // This should match the DEFAULT_SCHEMA from OracleConnectionManager
        String schema = System.getProperty("DEFAULT_SCHEMA");
        if (schema == null) {
            schema = System.getenv("DEFAULT_SCHEMA");
        }

        // Validate schema name
        if (schema == null || schema.trim().isEmpty()) {
            throw new IllegalStateException("DEFAULT_SCHEMA environment variable is not set");
        }

        // Validate Oracle identifier format
        if (!schema.matches("^[A-Z][A-Z0-9_]*$")) {
            throw new IllegalStateException("Invalid schema name: '" + schema + "'. Schema names must start with a letter and contain only uppercase letters, numbers, and underscores.");
        }

        return schema;
    }

    /**
     * Check if the graph is built and ready
     */
    public boolean isGraphBuilt() {
        return graphBuilt;
    }

    /**
     * Get the complete knowledge graph
     */
    public JsonObject getKnowledgeGraph() {
        if (!graphBuilt) {
            throw new IllegalStateException("Knowledge graph not built yet");
        }
        return knowledgeGraph;
    }

    /**
     * Get graph metadata
     */
    public JsonObject getGraphMetadata() {
        if (!graphBuilt) {
            return new JsonObject().put("status", "not_built");
        }

        return knowledgeGraph.getJsonObject("metadata");
    }

    /**
     * Translate enum values between code and description - now uses direct enumValues lookup
     */
    public JsonObject translateEnum(String table, String column, JsonArray values, String direction) {
        if (!graphBuilt) {
            throw new IllegalStateException("Knowledge graph not built yet");
        }

        String upperTable = table.toUpperCase();
        JsonObject enumValues = knowledgeGraph.getJsonObject("enumValues");
        JsonObject tableValues = enumValues != null ? enumValues.getJsonObject(upperTable) : null;

        if (tableValues == null || tableValues.isEmpty()) {
            return new JsonObject()
                .put("error", "No enum values found for table: " + table)
                .put("translations", new JsonArray());
        }

        JsonArray translations = new JsonArray();

        for (int i = 0; i < values.size(); i++) {
            String value = values.getString(i);
            if (value == null) continue;

            String upperValue = value.toUpperCase();
            JsonObject translation = new JsonObject().put("original", value);

            if ("code_to_description".equals(direction)) {
                // Translate code to description
                String description = tableValues.getString(upperValue);
                if (description != null) {
                    translation.put("translated", description);
                    translation.put("found", true);
                } else {
                    translation.put("translated", value);
                    translation.put("found", false);
                }
            } else if ("description_to_code".equals(direction)) {
                // Translate description to code
                String code = null;
                for (String key : tableValues.fieldNames()) {
                    if (tableValues.getString(key).equalsIgnoreCase(value)) {
                        code = key;
                        break;
                    }
                }
                if (code != null) {
                    translation.put("translated", code);
                    translation.put("found", true);
                } else {
                    translation.put("translated", value);
                    translation.put("found", false);
                }
            } else {
                // Auto-detect direction
                if (tableValues.containsKey(upperValue)) {
                    // It's a code
                    translation.put("translated", tableValues.getString(upperValue));
                    translation.put("found", true);
                    translation.put("detectedDirection", "code_to_description");
                } else {
                    // Try as description
                    String code = null;
                    for (String key : tableValues.fieldNames()) {
                        if (tableValues.getString(key).equalsIgnoreCase(value)) {
                            code = key;
                            break;
                        }
                    }
                    if (code != null) {
                        translation.put("translated", code);
                        translation.put("found", true);
                        translation.put("detectedDirection", "description_to_code");
                    } else {
                        translation.put("translated", value);
                        translation.put("found", false);
                    }
                }
            }

            translations.add(translation);
        }

        return new JsonObject()
            .put("table", table)
            .put("column", column)
            .put("direction", direction)
            .put("translations", translations);
    }

    /**
     * Get enum metadata for tables and columns - now uses direct enumTables/enumValues lookup
     */
    public JsonObject getEnumMetadata(String table, String column) {
        if (!graphBuilt) {
            throw new IllegalStateException("Knowledge graph not built yet");
        }

        JsonObject result = new JsonObject();

        if (table != null) {
            // Get specific enum table metadata
            String upperTable = table.toUpperCase();
            JsonObject enumTables = knowledgeGraph.getJsonObject("enumTables");
            JsonObject enumTable = enumTables != null ? enumTables.getJsonObject(upperTable) : null;
            if (enumTable != null) {
                result.put("table", table);
                result.put("isEnum", true);
                result.put("idColumn", enumTable.getString("idColumn"));
                result.put("descColumn", enumTable.getString("descColumn"));
                result.put("referencedBy", enumTable.getJsonArray("referencedBy", new JsonArray()));

                // Include sample values
                JsonObject enumValues = knowledgeGraph.getJsonObject("enumValues");
                JsonObject values = enumValues != null ? enumValues.getJsonObject(upperTable) : null;
                if (values != null && !values.isEmpty()) {
                    JsonArray sampleValues = new JsonArray();
                    int count = 0;
                    for (String code : values.fieldNames()) {
                        if (count++ >= 5) break; // Only show first 5 as samples
                        sampleValues.add(new JsonObject()
                            .put("code", code)
                            .put("description", values.getString(code)));
                    }
                    result.put("sampleValues", sampleValues);
                    result.put("totalValues", values.size());
                }
            } else {
                result.put("table", table);
                result.put("isEnum", false);
            }
        } else {
            // Return all enum tables
            JsonArray enumTableList = new JsonArray();
            JsonObject enumTables = knowledgeGraph.getJsonObject("enumTables");
            JsonObject enumValues = knowledgeGraph.getJsonObject("enumValues");
            if (enumTables != null) {
                for (String enumTableName : enumTables.fieldNames()) {
                    JsonObject enumTableMeta = enumTables.getJsonObject(enumTableName);
                    JsonObject tableInfo = new JsonObject()
                        .put("tableName", enumTableName)
                        .put("metadata", enumTableMeta);

                    JsonObject values = enumValues != null ? enumValues.getJsonObject(enumTableName) : null;
                    if (values != null) {
                        tableInfo.put("valueCount", values.size());
                    }
                    enumTableList.add(tableInfo);
                }
            }
            result.put("enumTables", enumTableList);
        }

        return result;
    }
}
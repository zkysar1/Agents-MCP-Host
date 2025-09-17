package agents.director.services;

import io.vertx.core.Vertx;
import io.vertx.core.Future;
import io.vertx.core.CompositeFuture;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
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

    // Common business term patterns (shared with BusinessMappingServer)
    // Using JsonObject for thread safety and Vert.x consistency
    private static final JsonObject COMMON_TERM_PATTERNS;

    static {
        JsonObject patterns = new JsonObject();
        patterns.put("customer", new JsonArray().add("customer").add("clients").add("account").add("user").add("member"));
        patterns.put("order", new JsonArray().add("order").add("purchase").add("transaction").add("sale"));
        patterns.put("product", new JsonArray().add("product").add("item").add("sku").add("merchandise").add("goods"));
        patterns.put("employee", new JsonArray().add("employee").add("staff").add("worker").add("personnel").add("emp"));
        patterns.put("date", new JsonArray().add("date").add("datetime").add("timestamp").add("created").add("modified"));
        patterns.put("status", new JsonArray().add("status").add("state").add("flag").add("active").add("enabled"));
        patterns.put("amount", new JsonArray().add("amount").add("total").add("sum").add("price").add("cost").add("value"));
        patterns.put("name", new JsonArray().add("name").add("title").add("description").add("label"));
        patterns.put("location", new JsonArray().add("state").add("province").add("region").add("territory").add("prov"));
        patterns.put("postal", new JsonArray().add("zip").add("zipcode").add("postal").add("postal_code").add("postcode"));
        patterns.put("province", new JsonArray().add("state").add("province").add("prov"));
        patterns.put("postal_code", new JsonArray().add("zip").add("zipcode").add("postal_code"));
        patterns.put("phone", new JsonArray().add("phone").add("telephone").add("tel").add("mobile").add("cell"));
        patterns.put("address", new JsonArray().add("address").add("addr").add("street").add("location"));
        // JsonObject is immutable once created, no need for unmodifiableMap wrapper
        COMMON_TERM_PATTERNS = patterns;
    }

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
     * Build the complete knowledge graph
     */
    private Future<JsonObject> buildGraph() {
        long startTime = System.currentTimeMillis();

        // Load components in parallel where possible
        Future<JsonArray> tablesFuture = loadTablesAndColumns();
        Future<JsonArray> relationshipsFuture = loadForeignKeys();
        Future<JsonArray> synonymsFuture = loadSynonyms();
        Future<JsonObject> enumDataFuture = loadEnumTablesAndValues();

        CompositeFuture composite = Future.join(Arrays.asList(
            tablesFuture,
            relationshipsFuture,
            synonymsFuture,
            enumDataFuture
        ));

        return composite.map(ar -> {
            JsonArray tables = composite.resultAt(0);
            JsonArray relationships = composite.resultAt(1);
            JsonArray synonyms = composite.resultAt(2);
            JsonObject enumData = composite.resultAt(3);

            // Build all indexes as JsonObjects
            JsonObject indexes = buildIndexes(tables, relationships, synonyms, enumData);

            // Assemble the final graph with everything in one structure
            // All indexes stored directly in the graph

            return new JsonObject()
                .put("tables", tables)
                .put("relationships", relationships)
                .put("synonyms", synonyms)
                .put("enumTables", enumData.getJsonObject("enumTables", new JsonObject()))
                .put("enumValues", enumData.getJsonObject("enumValues", new JsonObject()))
                // All indexes stored directly in the graph
                .put("tableIndex", indexes.getJsonObject("tableIndex"))
                .put("columnIndex", indexes.getJsonObject("columnIndex"))
                .put("relationshipIndex", indexes.getJsonObject("relationshipIndex"))
                .put("synonymIndex", indexes.getJsonObject("synonymIndex"))
                .put("enumTableIndex", indexes.getJsonObject("enumTableIndex"))
                .put("enumValuesIndex", indexes.getJsonObject("enumValuesIndex"))
                .put("metadata", new JsonObject()
                    .put("buildTime", System.currentTimeMillis() - startTime)
                    .put("timestamp", System.currentTimeMillis())
                    .put("tableCount", tables.size())
                    .put("relationshipCount", relationships.size())
                    .put("synonymCount", synonyms.size())
                    .put("enumTableCount", enumData.getJsonObject("enumTables", new JsonObject()).size())
                    .put("schema", getDefaultSchema())
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
     * Load enum tables and their values
     */
    private Future<JsonObject> loadEnumTablesAndValues() {
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
     * Load synonym mappings for business terms
     */
    private Future<JsonArray> loadSynonyms() {
        vertx.eventBus().publish("log", "[KnowledgeGraphBuilder] Building synonym mappings,2,KnowledgeGraphBuilder,Graph,Loading");

        return vertx.executeBlocking(() -> {
            try {
                JsonArray synonyms = new JsonArray();

                // Build synonyms from common patterns
                for (String category : COMMON_TERM_PATTERNS.fieldNames()) {
                    JsonArray terms = COMMON_TERM_PATTERNS.getJsonArray(category);

                    for (int i = 0; i < terms.size(); i++) {
                        String term = terms.getString(i);
                        JsonObject synonym = new JsonObject()
                            .put("term", term)
                            .put("category", category)
                            .put("patterns", terms.copy())  // Use copy for safety
                            .put("confidence", 0.8);

                        synonyms.add(synonym);
                    }
                }

                vertx.eventBus().publish("log",
                    "[KnowledgeGraphBuilder] Built " + synonyms.size() + " synonym mappings,2,KnowledgeGraphBuilder,Graph,Progress");

                return synonyms;

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Build indexes for fast lookup - returns JsonObject containing all indexes
     */
    private JsonObject buildIndexes(JsonArray tables, JsonArray relationships, JsonArray synonyms, JsonObject enumData) {
        // Create index objects
        JsonObject tableIndex = new JsonObject();
        JsonObject columnToTablesIndex = new JsonObject();
        JsonObject relationshipIndex = new JsonObject();
        JsonObject synonymIndex = new JsonObject();
        JsonObject enumTableIndex = new JsonObject();
        JsonObject enumValuesIndex = new JsonObject();
        // Build table index
        for (int i = 0; i < tables.size(); i++) {
            JsonObject table = tables.getJsonObject(i);
            String tableName = table.getString("tableName");
            if (tableName != null) {
                // Ensure consistent uppercase storage
                tableIndex.put(tableName.toUpperCase(), table);

                // Build column to tables index
                JsonArray columns = table.getJsonArray("columns");
                if (columns != null) {
                    for (int j = 0; j < columns.size(); j++) {
                        JsonObject column = columns.getJsonObject(j);
                        String columnName = column.getString("name");
                        if (columnName != null) {
                            String upperColumnName = columnName.toUpperCase();
                            JsonArray tablesList = columnToTablesIndex.getJsonArray(upperColumnName);
                            if (tablesList == null) {
                                tablesList = new JsonArray();
                                columnToTablesIndex.put(upperColumnName, tablesList);
                            }
                            tablesList.add(tableName.toUpperCase());
                        }
                    }
                }
            }
        }

        // Build relationship index
        for (int i = 0; i < relationships.size(); i++) {
            JsonObject rel = relationships.getJsonObject(i);
            String fromTable = rel.getString("fromTable");
            String toTable = rel.getString("toTable");
            String fromColumn = rel.getString("fromColumn");
            String toColumn = rel.getString("toColumn");

            if (fromTable != null && toTable != null) {
                // Store with uppercase for consistency
                String upperFromTable = fromTable.toUpperCase();
                JsonArray fromRelList = relationshipIndex.getJsonArray(upperFromTable);
                if (fromRelList == null) {
                    fromRelList = new JsonArray();
                    relationshipIndex.put(upperFromTable, fromRelList);
                }
                fromRelList.add(rel);

                // Also index reverse relationship for bidirectional traversal
                // NOTE: Swap both tables AND columns for correct reverse relationship
                JsonObject reverseRel = rel.copy()
                    .put("type", "foreign_key_reverse")
                    .put("fromTable", toTable)
                    .put("toTable", fromTable)
                    .put("fromColumn", toColumn)  // Swap columns too
                    .put("toColumn", fromColumn);

                String upperToTable = toTable.toUpperCase();
                JsonArray toRelList = relationshipIndex.getJsonArray(upperToTable);
                if (toRelList == null) {
                    toRelList = new JsonArray();
                    relationshipIndex.put(upperToTable, toRelList);
                }
                toRelList.add(reverseRel);
            }
        }

        // Build synonym index
        for (int i = 0; i < synonyms.size(); i++) {
            JsonObject synonym = synonyms.getJsonObject(i);
            String term = synonym.getString("term");
            if (term != null) {
                term = term.toLowerCase();
                JsonArray synList = synonymIndex.getJsonArray(term);
                if (synList == null) {
                    synList = new JsonArray();
                    synonymIndex.put(term, synList);
                }
                synList.add(synonym);
            }
        }

        // Build enum indexes from enumData
        JsonObject enumTables = enumData.getJsonObject("enumTables");
        JsonObject enumValues = enumData.getJsonObject("enumValues");

        if (enumTables != null) {
            for (String tableName : enumTables.fieldNames()) {
                enumTableIndex.put(tableName.toUpperCase(), enumTables.getJsonObject(tableName));
            }
        }

        if (enumValues != null) {
            for (String tableName : enumValues.fieldNames()) {
                JsonObject tableValues = enumValues.getJsonObject(tableName);
                enumValuesIndex.put(tableName.toUpperCase(), tableValues);
            }
        }

        // Return all indexes as a single JsonObject
        return new JsonObject()
            .put("tableIndex", tableIndex)
            .put("columnIndex", columnToTablesIndex)
            .put("relationshipIndex", relationshipIndex)
            .put("synonymIndex", synonymIndex)
            .put("enumTableIndex", enumTableIndex)
            .put("enumValuesIndex", enumValuesIndex);
    }


    /**
     * Find join path between two tables using BFS
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

        while (!queue.isEmpty()) {
            JsonArray path = queue.poll();
            String currentTable = path.getString(path.size() - 1);

            if (currentTable.equals(toTable)) {
                // Found path - convert to join information
                return buildJoinPath(path);
            }

            // Explore neighbors
            JsonObject relationshipIndex = knowledgeGraph.getJsonObject("relationshipIndex");
            JsonArray relationships = relationshipIndex != null ? relationshipIndex.getJsonArray(currentTable) : null;
            if (relationships != null) {
                for (int i = 0; i < relationships.size(); i++) {
                    JsonObject rel = relationships.getJsonObject(i);
                    String nextTable = rel.getString("toTable");

                    if (!visited.contains(nextTable)) {
                        visited.add(nextTable);
                        JsonArray newPath = path.copy();
                        newPath.add(nextTable);
                        queue.offer(newPath);
                    }
                }
            }
        }

        // No path found
        return new JsonArray();
    }

    /**
     * Build join path details from table path
     */
    private JsonArray buildJoinPath(JsonArray tablePath) {
        JsonArray joinPath = new JsonArray();

        for (int i = 0; i < tablePath.size() - 1; i++) {
            String fromTable = tablePath.getString(i);
            String toTable = tablePath.getString(i + 1);

            // Find the relationship (use uppercase for lookup)
            JsonObject relationshipIndex = knowledgeGraph.getJsonObject("relationshipIndex");
            JsonArray rels = relationshipIndex != null ? relationshipIndex.getJsonArray(fromTable.toUpperCase()) : null;
            if (rels != null) {
                for (int j = 0; j < rels.size(); j++) {
                    JsonObject rel = rels.getJsonObject(j);
                    String relToTable = rel.getString("toTable");
                    if (relToTable != null && relToTable.equalsIgnoreCase(toTable)) {
                        joinPath.add(new JsonObject()
                            .put("fromTable", fromTable)
                            .put("fromColumn", rel.getString("fromColumn"))
                            .put("toTable", toTable)
                            .put("toColumn", rel.getString("toColumn"))
                            .put("joinType", "INNER JOIN")
                        );
                        break;
                    }
                }
            }
        }

        return joinPath;
    }

    /**
     * Resolve a business term to schema elements
     */
    public JsonArray resolveSynonym(String term) {
        if (!graphBuilt) {
            throw new IllegalStateException("Knowledge graph not built yet");
        }

        JsonArray results = new JsonArray();
        String lowerTerm = term.toLowerCase();

        // Check direct synonym match
        JsonObject synonymIndex = knowledgeGraph.getJsonObject("synonymIndex");
        JsonArray synonyms = synonymIndex != null ? synonymIndex.getJsonArray(lowerTerm) : null;
        if (synonyms != null) {
            for (int i = 0; i < synonyms.size(); i++) {
                JsonObject synonym = synonyms.getJsonObject(i);
                String category = synonym.getString("category");

                // Find matching tables and columns
                JsonObject tableIndex = knowledgeGraph.getJsonObject("tableIndex");
                if (tableIndex != null) {
                    for (String tableName : tableIndex.fieldNames()) {
                        JsonObject table = tableIndex.getJsonObject(tableName);

                        // Check if table name matches pattern
                        if (matchesPattern(tableName, synonym.getJsonArray("patterns"))) {
                            results.add(new JsonObject()
                                .put("type", "table")
                                .put("name", tableName)
                                .put("confidence", synonym.getDouble("confidence"))
                                .put("reason", "Table name matches " + category + " pattern")
                            );
                        }

                        // Check columns
                        JsonArray columns = table.getJsonArray("columns");
                        if (columns != null) {
                            for (int j = 0; j < columns.size(); j++) {
                                JsonObject column = columns.getJsonObject(j);
                                String columnName = column.getString("name");

                                if (columnName != null && matchesPattern(columnName, synonym.getJsonArray("patterns"))) {
                                    results.add(new JsonObject()
                                        .put("type", "column")
                                        .put("table", tableName)
                                        .put("column", columnName)
                                        .put("confidence", synonym.getDouble("confidence") * 0.9)
                                        .put("reason", "Column name matches " + category + " pattern")
                                    );
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
     * Disambiguate a column name using context
     */
    public JsonArray disambiguateColumn(String columnName, JsonObject context) {
        if (!graphBuilt) {
            throw new IllegalStateException("Knowledge graph not built yet");
        }

        JsonArray results = new JsonArray();
        String upperColumn = columnName.toUpperCase();

        JsonObject columnIndex = knowledgeGraph.getJsonObject("columnIndex");
        JsonArray tables = columnIndex != null ? columnIndex.getJsonArray(upperColumn) : null;
        if (tables == null || tables.isEmpty()) {
            return results;
        }

        // Get context hints
        JsonArray contextTables = context.getJsonArray("tables");
        String queryIntent = context.getString("intent", "");

        for (int i = 0; i < tables.size(); i++) {
            String tableName = tables.getString(i);
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

        // Sort by confidence using JsonArray
        // Create a list for sorting to avoid modifying during iteration
        List<JsonObject> toSort = new ArrayList<>();
        for (int i = 0; i < results.size(); i++) {
            toSort.add(results.getJsonObject(i));
        }

        // Sort with comparator
        toSort.sort((a, b) -> Double.compare(
            b.getDouble("confidence"),
            a.getDouble("confidence")
        ));

        // Create new sorted JsonArray
        JsonArray sorted = new JsonArray();
        for (JsonObject obj : toSort) {
            sorted.add(obj);
        }

        return sorted;
    }

    /**
     * Check if a name matches any pattern in the list
     */
    private boolean matchesPattern(String name, JsonArray patterns) {
        String lowerName = name.toLowerCase();

        for (int i = 0; i < patterns.size(); i++) {
            String pattern = patterns.getString(i).toLowerCase();
            if (lowerName.contains(pattern)) {
                return true;
            }
        }

        return false;
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
            JsonArray tables = graph.getJsonArray("tables");
            if (tables != null && !tables.isEmpty()) {
                // Collect all table names
                List<String> tableNames = new ArrayList<>();
                for (int i = 0; i < tables.size(); i++) {
                    JsonObject table = tables.getJsonObject(i);
                    String tableName = table.getString("tableName");
                    if (tableName != null) {
                        tableNames.add(tableName);
                    }
                }

                // Log table names
                vertx.eventBus().publish("log", String.format(
                    "[KnowledgeGraph] Tables in schema: %s,1,KnowledgeGraphBuilder,Graph,Info",
                    String.join(", ", tableNames)
                ));

                // Log detailed info for tables containing order-related terms
                for (int i = 0; i < tables.size(); i++) {
                    JsonObject table = tables.getJsonObject(i);
                    String tableName = table.getString("tableName", "").toLowerCase();
                    if (tableName.contains("order") || tableName.contains("purchase") ||
                        tableName.contains("sale") || tableName.contains("transaction")) {

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
                            table.getString("tableName"),
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
     * Translate enum values between code and description
     */
    public JsonObject translateEnum(String table, String column, JsonArray values, String direction) {
        if (!graphBuilt) {
            throw new IllegalStateException("Knowledge graph not built yet");
        }

        String upperTable = table.toUpperCase();
        JsonObject enumValuesIndex = knowledgeGraph.getJsonObject("enumValuesIndex");
        JsonObject tableValues = enumValuesIndex != null ? enumValuesIndex.getJsonObject(upperTable) : null;

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
     * Get enum metadata for tables and columns
     */
    public JsonObject getEnumMetadata(String table, String column) {
        if (!graphBuilt) {
            throw new IllegalStateException("Knowledge graph not built yet");
        }

        JsonObject result = new JsonObject();

        if (table != null) {
            // Get specific enum table metadata
            String upperTable = table.toUpperCase();
            JsonObject enumTableIndex = knowledgeGraph.getJsonObject("enumTableIndex");
            JsonObject enumTable = enumTableIndex != null ? enumTableIndex.getJsonObject(upperTable) : null;
            if (enumTable != null) {
                result.put("table", table);
                result.put("isEnum", true);
                result.put("idColumn", enumTable.getString("idColumn"));
                result.put("descColumn", enumTable.getString("descColumn"));

                // Include sample values
                JsonObject enumValuesIndex = knowledgeGraph.getJsonObject("enumValuesIndex");
                JsonObject values = enumValuesIndex != null ? enumValuesIndex.getJsonObject(upperTable) : null;
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
            JsonObject enumTableIndex = knowledgeGraph.getJsonObject("enumTableIndex");
            JsonObject enumValuesIndex = knowledgeGraph.getJsonObject("enumValuesIndex");
            if (enumTableIndex != null) {
                for (String enumTableName : enumTableIndex.fieldNames()) {
                    JsonObject enumTableMeta = enumTableIndex.getJsonObject(enumTableName);
                    JsonObject tableInfo = new JsonObject()
                        .put("tableName", enumTableName)
                        .put("metadata", enumTableMeta);

                    JsonObject values = enumValuesIndex != null ? enumValuesIndex.getJsonObject(enumTableName) : null;
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
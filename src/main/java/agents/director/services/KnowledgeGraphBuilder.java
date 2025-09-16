package agents.director.services;

import io.vertx.core.Vertx;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.CompositeFuture;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Knowledge Graph Builder for Oracle Schema Intelligence.
 * Builds an in-memory graph at startup containing:
 * - Complete schema metadata (tables, columns, types)
 * - Foreign key relationships for join path discovery
 * - Synonym mappings for business term resolution
 * - Column indexes for disambiguation
 *
 * Uses Vert.x async patterns for parallel loading and non-blocking operations.
 */
public class KnowledgeGraphBuilder {

    private static KnowledgeGraphBuilder instance;
    private volatile Vertx vertx;
    private OracleConnectionManager connectionManager;

    // The in-memory knowledge graph
    private volatile JsonObject knowledgeGraph;
    private volatile boolean graphBuilt = false;
    private volatile long graphBuildTimestamp = 0;

    // Graph components for fast lookup
    private final Map<String, JsonObject> tableIndex = new ConcurrentHashMap<>();
    private final Map<String, List<String>> columnToTablesIndex = new ConcurrentHashMap<>();
    private final Map<String, List<JsonObject>> synonymIndex = new ConcurrentHashMap<>();
    private final Map<String, List<JsonObject>> relationshipIndex = new ConcurrentHashMap<>();
    private final Map<String, JsonObject> enumTableIndex = new ConcurrentHashMap<>();
    private final Map<String, Map<String, String>> enumValuesIndex = new ConcurrentHashMap<>();

    // Common business term patterns (shared with BusinessMappingServer)
    // Using unmodifiable map for thread safety
    private static final Map<String, List<String>> COMMON_TERM_PATTERNS;

    static {
        Map<String, List<String>> patterns = new HashMap<>();
        patterns.put("customer", Arrays.asList("customer", "clients", "account", "user", "member"));
        patterns.put("order", Arrays.asList("order", "purchase", "transaction", "sale"));
        patterns.put("product", Arrays.asList("product", "item", "sku", "merchandise", "goods"));
        patterns.put("employee", Arrays.asList("employee", "staff", "worker", "personnel", "emp"));
        patterns.put("date", Arrays.asList("date", "datetime", "timestamp", "created", "modified"));
        patterns.put("status", Arrays.asList("status", "state", "flag", "active", "enabled"));
        patterns.put("amount", Arrays.asList("amount", "total", "sum", "price", "cost", "value"));
        patterns.put("name", Arrays.asList("name", "title", "description", "label"));
        patterns.put("location", Arrays.asList("state", "province", "region", "territory", "prov"));
        patterns.put("postal", Arrays.asList("zip", "zipcode", "postal", "postal_code", "postcode"));
        patterns.put("province", Arrays.asList("state", "province", "prov"));
        patterns.put("postal_code", Arrays.asList("zip", "zipcode", "postal_code"));
        patterns.put("phone", Arrays.asList("phone", "telephone", "tel", "mobile", "cell"));
        patterns.put("address", Arrays.asList("address", "addr", "street", "location"));
        COMMON_TERM_PATTERNS = Collections.unmodifiableMap(patterns);
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
        CompositeFuture composite = CompositeFuture.join(
            loadTablesAndColumns(),
            loadForeignKeys(),
            loadSynonyms(),
            loadEnumTablesAndValues()
        );

        return composite.map(ar -> {
            JsonArray tables = composite.resultAt(0);
            JsonArray relationships = composite.resultAt(1);
            JsonArray synonyms = composite.resultAt(2);
            JsonObject enumData = composite.resultAt(3);

            // Build indexes for fast lookup
            buildIndexes(tables, relationships, synonyms);

            // Assemble the final graph
            JsonObject graph = new JsonObject()
                .put("tables", tables)
                .put("relationships", relationships)
                .put("synonyms", synonyms)
                .put("columnIndex", createColumnIndex())
                .put("enumTables", enumData.getJsonObject("enumTables", new JsonObject()))
                .put("enumValues", enumData.getJsonObject("enumValues", new JsonObject()))
                .put("metadata", new JsonObject()
                    .put("buildTime", System.currentTimeMillis() - startTime)
                    .put("timestamp", System.currentTimeMillis())
                    .put("tableCount", tables.size())
                    .put("relationshipCount", relationships.size())
                    .put("synonymCount", synonyms.size())
                    .put("enumTableCount", enumData.getJsonObject("enumTables", new JsonObject()).size())
                    .put("schema", getDefaultSchema())
                );

            return graph;
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
                List<Future> metadataFutures = new ArrayList<>();

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
                return CompositeFuture.join(metadataFutures)
                    .map(composite -> {
                        JsonArray tables = new JsonArray();
                        for (int i = 0; i < metadataFutures.size(); i++) {
                            tables.add(metadataFutures.get(i).result());
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

        return vertx.<JsonArray>executeBlocking(promise -> {
            try {
                JsonArray relationships = connectionManager.executeWithConnection(conn -> {
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

                promise.complete(relationships);

            } catch (Exception e) {
                promise.fail(e);
            }
        }, false);
    }

    /**
     * Load enum tables and their values
     */
    private Future<JsonObject> loadEnumTablesAndValues() {
        vertx.eventBus().publish("log", "[KnowledgeGraphBuilder] Loading enum tables and values,2,KnowledgeGraphBuilder,Graph,Loading");

        return vertx.<JsonObject>executeBlocking(promise -> {
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
                            if (!isValidOracleIdentifier(tableName) ||
                                !isValidOracleIdentifier(idColumn) ||
                                !isValidOracleIdentifier(descColumn)) {
                                vertx.eventBus().publish("log",
                                    "[KnowledgeGraphBuilder] Skipping table with invalid identifiers: " + tableName +
                                    ",1,KnowledgeGraphBuilder,Graph,Warning");
                                continue;
                            }

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

                                if (tableValues.size() > 0) {
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
                        }
                    }
                }

                enumData.put("enumTables", enumTables);
                enumData.put("enumValues", enumValues);

                vertx.eventBus().publish("log",
                    "[KnowledgeGraphBuilder] Discovered " + enumTables.size() + " enum tables,2,KnowledgeGraphBuilder,Graph,Progress");

                promise.complete(enumData);

            } catch (Exception e) {
                vertx.eventBus().publish("log",
                    "[KnowledgeGraphBuilder] Failed to load enum data: " + e.getMessage() + ",1,KnowledgeGraphBuilder,Graph,Warning");
                // Return empty but don't fail the whole graph build
                promise.complete(new JsonObject()
                    .put("enumTables", new JsonObject())
                    .put("enumValues", new JsonObject()));
            }
        }, false);
    }

    /**
     * Load synonym mappings for business terms
     */
    private Future<JsonArray> loadSynonyms() {
        vertx.eventBus().publish("log", "[KnowledgeGraphBuilder] Building synonym mappings,2,KnowledgeGraphBuilder,Graph,Loading");

        return vertx.<JsonArray>executeBlocking(promise -> {
            try {
                JsonArray synonyms = new JsonArray();

                // Build synonyms from common patterns
                for (Map.Entry<String, List<String>> entry : COMMON_TERM_PATTERNS.entrySet()) {
                    String category = entry.getKey();
                    List<String> terms = entry.getValue();

                    for (String term : terms) {
                        JsonObject synonym = new JsonObject()
                            .put("term", term)
                            .put("category", category)
                            .put("patterns", new JsonArray(terms))
                            .put("confidence", 0.8);

                        synonyms.add(synonym);
                    }
                }

                vertx.eventBus().publish("log",
                    "[KnowledgeGraphBuilder] Built " + synonyms.size() + " synonym mappings,2,KnowledgeGraphBuilder,Graph,Progress");

                promise.complete(synonyms);

            } catch (Exception e) {
                promise.fail(e);
            }
        }, false);
    }

    /**
     * Build indexes for fast lookup
     */
    private void buildIndexes(JsonArray tables, JsonArray relationships, JsonArray synonyms) {
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
                            columnToTablesIndex.computeIfAbsent(columnName.toUpperCase(), k -> new ArrayList<>())
                                .add(tableName.toUpperCase());
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
                relationshipIndex.computeIfAbsent(fromTable.toUpperCase(), k -> new ArrayList<>()).add(rel);

                // Also index reverse relationship for bidirectional traversal
                // NOTE: Swap both tables AND columns for correct reverse relationship
                JsonObject reverseRel = rel.copy()
                    .put("type", "foreign_key_reverse")
                    .put("fromTable", toTable)
                    .put("toTable", fromTable)
                    .put("fromColumn", toColumn)  // Swap columns too
                    .put("toColumn", fromColumn);

                relationshipIndex.computeIfAbsent(toTable.toUpperCase(), k -> new ArrayList<>()).add(reverseRel);
            }
        }

        // Build synonym index
        for (int i = 0; i < synonyms.size(); i++) {
            JsonObject synonym = synonyms.getJsonObject(i);
            String term = synonym.getString("term").toLowerCase();

            synonymIndex.computeIfAbsent(term, k -> new ArrayList<>()).add(synonym);
        }

        // Build enum indexes from the graph if it's been built
        if (knowledgeGraph != null) {
            JsonObject enumTables = knowledgeGraph.getJsonObject("enumTables");
            JsonObject enumValues = knowledgeGraph.getJsonObject("enumValues");

            if (enumTables != null) {
                for (String tableName : enumTables.fieldNames()) {
                    enumTableIndex.put(tableName.toUpperCase(), enumTables.getJsonObject(tableName));
                }
            }

            if (enumValues != null) {
                for (String tableName : enumValues.fieldNames()) {
                    JsonObject tableValues = enumValues.getJsonObject(tableName);
                    Map<String, String> valueMap = new HashMap<>();
                    for (String code : tableValues.fieldNames()) {
                        // Code is already uppercase from loading phase
                        valueMap.put(code, tableValues.getString(code));
                    }
                    enumValuesIndex.put(tableName.toUpperCase(), valueMap);
                }
            }
        }
    }

    /**
     * Create column index for the graph
     */
    private JsonObject createColumnIndex() {
        JsonObject index = new JsonObject();

        for (Map.Entry<String, List<String>> entry : columnToTablesIndex.entrySet()) {
            index.put(entry.getKey(), new JsonArray(entry.getValue()));
        }

        return index;
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
        Queue<List<String>> queue = new LinkedList<>();
        Set<String> visited = new HashSet<>();

        queue.offer(Arrays.asList(fromTable));
        visited.add(fromTable);

        while (!queue.isEmpty()) {
            List<String> path = queue.poll();
            String currentTable = path.get(path.size() - 1);

            if (currentTable.equals(toTable)) {
                // Found path - convert to join information
                return buildJoinPath(path);
            }

            // Explore neighbors
            List<JsonObject> relationships = relationshipIndex.get(currentTable);
            if (relationships != null) {
                for (JsonObject rel : relationships) {
                    String nextTable = rel.getString("toTable");

                    if (!visited.contains(nextTable)) {
                        visited.add(nextTable);
                        List<String> newPath = new ArrayList<>(path);
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
    private JsonArray buildJoinPath(List<String> tablePath) {
        JsonArray joinPath = new JsonArray();

        for (int i = 0; i < tablePath.size() - 1; i++) {
            String fromTable = tablePath.get(i);
            String toTable = tablePath.get(i + 1);

            // Find the relationship (use uppercase for lookup)
            List<JsonObject> rels = relationshipIndex.get(fromTable.toUpperCase());
            if (rels != null) {
                for (JsonObject rel : rels) {
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
        List<JsonObject> synonyms = synonymIndex.get(lowerTerm);
        if (synonyms != null) {
            for (JsonObject synonym : synonyms) {
                String category = synonym.getString("category");

                // Find matching tables and columns
                for (Map.Entry<String, JsonObject> entry : tableIndex.entrySet()) {
                    String tableName = entry.getKey();
                    JsonObject table = entry.getValue();

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
                        for (int i = 0; i < columns.size(); i++) {
                            JsonObject column = columns.getJsonObject(i);
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

        List<String> tables = columnToTablesIndex.get(upperColumn);
        if (tables == null || tables.isEmpty()) {
            return results;
        }

        // Get context hints
        JsonArray contextTables = context.getJsonArray("tables");
        String queryIntent = context.getString("intent", "");

        for (String tableName : tables) {
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

        // Sort by confidence
        List<Object> sorted = results.stream()
            .sorted((a, b) -> {
                JsonObject ja = (JsonObject) a;
                JsonObject jb = (JsonObject) b;
                return Double.compare(
                    jb.getDouble("confidence"),
                    ja.getDouble("confidence")
                );
            })
            .collect(Collectors.toList());

        return new JsonArray(sorted);
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
        Map<String, String> tableValues = enumValuesIndex.get(upperTable);

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
                String description = tableValues.get(upperValue);
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
                for (Map.Entry<String, String> entry : tableValues.entrySet()) {
                    if (entry.getValue().equalsIgnoreCase(value)) {
                        code = entry.getKey();
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
                    translation.put("translated", tableValues.get(upperValue));
                    translation.put("found", true);
                    translation.put("detectedDirection", "code_to_description");
                } else {
                    // Try as description
                    String code = null;
                    for (Map.Entry<String, String> entry : tableValues.entrySet()) {
                        if (entry.getValue().equalsIgnoreCase(value)) {
                            code = entry.getKey();
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
            JsonObject enumTable = enumTableIndex.get(upperTable);
            if (enumTable != null) {
                result.put("table", table);
                result.put("isEnum", true);
                result.put("idColumn", enumTable.getString("idColumn"));
                result.put("descColumn", enumTable.getString("descColumn"));

                // Include sample values
                Map<String, String> values = enumValuesIndex.get(upperTable);
                if (values != null && !values.isEmpty()) {
                    JsonArray sampleValues = new JsonArray();
                    int count = 0;
                    for (Map.Entry<String, String> entry : values.entrySet()) {
                        if (count++ >= 5) break; // Only show first 5 as samples
                        sampleValues.add(new JsonObject()
                            .put("code", entry.getKey())
                            .put("description", entry.getValue()));
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
            for (Map.Entry<String, JsonObject> entry : enumTableIndex.entrySet()) {
                JsonObject tableInfo = new JsonObject()
                    .put("tableName", entry.getKey())
                    .put("metadata", entry.getValue());

                Map<String, String> values = enumValuesIndex.get(entry.getKey());
                if (values != null) {
                    tableInfo.put("valueCount", values.size());
                }
                enumTableList.add(tableInfo);
            }
            result.put("enumTables", enumTableList);
        }

        return result;
    }
}
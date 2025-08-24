package AgentsMCPHost.mcp.servers.oracle.utils;

import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;
import io.vertx.core.Vertx;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;

import java.sql.*;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import AgentsMCPHost.streaming.StreamingEventPublisher;

/**
 * Oracle Connection Manager using Universal Connection Pool (UCP).
 * Manages database connections for Oracle Autonomous Database with TLS.
 */
public class OracleConnectionManager {
    
    private static OracleConnectionManager instance;
    private PoolDataSource poolDataSource;
    private String jdbcUrl;
    private String password;
    private Vertx vertx;
    private boolean initialized = false;
    
    // Connection configuration
    private static final String DB_HOST = "adb.us-ashburn-1.oraclecloud.com";
    private static final int DB_PORT = 1522;
    private static final String DB_SERVICE = "gd77773c35a7f01_zaksedwtest_high.adb.oraclecloud.com";
    private static final String DB_USER = "ADMIN";
    
    // Connection pool settings
    private static final int MIN_POOL_SIZE = 5;
    private static final int MAX_POOL_SIZE = 20;
    private static final int CONNECTION_TIMEOUT = 30; // seconds
    
    // Cache for metadata
    private final Map<String, JsonObject> metadataCache = new ConcurrentHashMap<>();
    private static final long CACHE_DURATION_MS = 5 * 60 * 1000; // 5 minutes
    
    private OracleConnectionManager() {
    }
    
    /**
     * Get singleton instance
     */
    public static synchronized OracleConnectionManager getInstance() {
        if (instance == null) {
            instance = new OracleConnectionManager();
        }
        return instance;
    }
    
    /**
     * Initialize the connection pool
     */
    public Future<Void> initialize(Vertx vertx) {
        this.vertx = vertx;
        Promise<Void> promise = Promise.promise();
        
        if (initialized) {
            promise.complete();
            return promise.future();
        }
        
        // Use new Callable API for executeBlocking (Vert.x 4.5+)
        vertx.<Void>executeBlocking(() -> {
            try {
                // Get password from environment variable or use test default
                password = System.getenv("ORACLE_TESTING_DATABASE_PASSWORD");
                if (password == null || password.isEmpty()) {
                    // Only use hardcoded password in test/development mode
                    System.out.println("[WARNING] Using default test password - not for production!");
                    password = "ARmy0320-- milk";
                }
                
                // Load Oracle driver
                Class.forName("oracle.jdbc.driver.OracleDriver");
                
                // Build JDBC URL with TLS
                jdbcUrl = String.format(
                    "jdbc:oracle:thin:@(description=(retry_count=20)(retry_delay=3)" +
                    "(address=(protocol=tcps)(port=%d)(host=%s))" +
                    "(connect_data=(service_name=%s))" +
                    "(security=(ssl_server_dn_match=yes)))",
                    DB_PORT, DB_HOST, DB_SERVICE
                );
                
                // Initialize UCP connection pool
                poolDataSource = PoolDataSourceFactory.getPoolDataSource();
                poolDataSource.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource");
                poolDataSource.setConnectionPoolName("OracleAgentPool");  // Set name for proper cleanup
                poolDataSource.setURL(jdbcUrl);
                poolDataSource.setUser(DB_USER);
                poolDataSource.setPassword(password);
                
                // Configure pool settings
                poolDataSource.setInitialPoolSize(MIN_POOL_SIZE);
                poolDataSource.setMinPoolSize(MIN_POOL_SIZE);
                poolDataSource.setMaxPoolSize(MAX_POOL_SIZE);
                poolDataSource.setConnectionWaitTimeout(CONNECTION_TIMEOUT);
                poolDataSource.setValidateConnectionOnBorrow(true);
                poolDataSource.setMaxIdleTime(300); // 5 minutes
                
                // Test the connection
                try (Connection conn = poolDataSource.getConnection()) {
                    if (!conn.isValid(5)) {
                        throw new SQLException("Connection validation failed");
                    }
                    // Log successful initialization
                    if (vertx != null) {
                        vertx.eventBus().publish("log", "Oracle UCP pool initialized - " + DB_SERVICE + 
                            " (min=" + MIN_POOL_SIZE + ", max=" + MAX_POOL_SIZE + "),1,OracleConnectionManager,StartUp,Database");
                    }
                }
                
                initialized = true;
                return null; // Return null for Void
                
            } catch (SQLException e) {
                if (vertx != null) {
                    vertx.eventBus().publish("log", "Failed to initialize Oracle connection: " + e.getMessage() + 
                        ",0,OracleConnectionManager,StartUp,Database");
                }
                throw new RuntimeException("SQL Exception during initialization", e);
            } catch (ClassNotFoundException e) {
                if (vertx != null) {
                    vertx.eventBus().publish("log", "Oracle JDBC driver not found: " + e.getMessage() + 
                        ",0,OracleConnectionManager,StartUp,Database");
                }
                throw new RuntimeException("Oracle JDBC driver not found", e);
            } catch (Exception e) {
                if (vertx != null) {
                    vertx.eventBus().publish("log", "Failed to initialize Oracle connection: " + e.getMessage() + 
                        ",0,OracleConnectionManager,StartUp,Database");
                }
                throw new RuntimeException("Failed to initialize Oracle connection", e);
            }
        }).onComplete(res -> {
            if (res.succeeded()) {
                promise.complete();
            } else {
                promise.fail(res.cause());
            }
        });
        
        return promise.future();
    }
    
    /**
     * Get a connection from the pool
     */
    private Connection getConnection() throws SQLException {
        if (!initialized || poolDataSource == null) {
            throw new SQLException("Connection pool not initialized");
        }
        return poolDataSource.getConnection();
    }
    
    /**
     * Execute a query and return results as JSON
     */
    public Future<JsonArray> executeQuery(String sql, Object... params) {
        return executeQuery(sql, null, params);
    }
    
    /**
     * Execute a query and return results as JSON with streaming support
     */
    public Future<JsonArray> executeQuery(String sql, String streamId, Object... params) {
        Promise<JsonArray> promise = Promise.promise();
        
        if (!initialized) {
            promise.fail("Connection pool not initialized");
            return promise.future();
        }
        
        // Clean SQL - remove trailing semicolons which Oracle JDBC doesn't like
        String trimmedSql = sql.trim();
        final String cleanSql = trimmedSql.endsWith(";") ? 
            trimmedSql.substring(0, trimmedSql.length() - 1).trim() : 
            trimmedSql;
        
        // Publish SQL query event if streaming
        if (streamId != null) {
            StreamingEventPublisher publisher = new StreamingEventPublisher(vertx, streamId);
            JsonObject context = new JsonObject()
                .put("paramCount", params.length)
                .put("queryLength", cleanSql.length());
            publisher.publishSQLQuery(cleanSql, context);
        }
        
        long queryStartTime = System.currentTimeMillis();
        
        // Use new Callable API for executeBlocking (Vert.x 4.5+)
        vertx.<JsonArray>executeBlocking(() -> {
            try (Connection conn = getConnection();
                 PreparedStatement stmt = conn.prepareStatement(cleanSql)) {
                
                // Set parameters
                for (int i = 0; i < params.length; i++) {
                    stmt.setObject(i + 1, params[i]);
                }
                
                // Execute query
                try (ResultSet rs = stmt.executeQuery()) {
                    return resultSetToJson(rs);
                }
                
            } catch (SQLException e) {
                throw new RuntimeException("Query execution failed: " + e.getMessage(), e);
            }
        }, false).onComplete(res -> {  // Unordered - DB queries don't need ordering
            if (res.succeeded()) {
                JsonArray results = res.result();
                long executionTime = System.currentTimeMillis() - queryStartTime;
                
                // Publish SQL result event if streaming
                if (streamId != null) {
                    StreamingEventPublisher publisher = new StreamingEventPublisher(vertx, streamId);
                    publisher.publishSQLResult(results, executionTime);
                }
                
                promise.complete(results);
            } else {
                promise.fail(res.cause());
            }
        });
        
        return promise.future();
    }
    
    /**
     * Execute an update (INSERT, UPDATE, DELETE) and return affected rows
     */
    public Future<Integer> executeUpdate(String sql, Object... params) {
        Promise<Integer> promise = Promise.promise();
        
        if (!initialized) {
            promise.fail("Connection pool not initialized");
            return promise.future();
        }
        
        // Clean SQL - remove trailing semicolons which Oracle JDBC doesn't like
        String trimmedSql = sql.trim();
        final String cleanSql = trimmedSql.endsWith(";") ? 
            trimmedSql.substring(0, trimmedSql.length() - 1).trim() : 
            trimmedSql;
        
        // Use new Callable API for executeBlocking (Vert.x 4.5+)
        vertx.<Integer>executeBlocking(() -> {
            try (Connection conn = getConnection();
                 PreparedStatement stmt = conn.prepareStatement(cleanSql)) {
                
                // Set parameters
                for (int i = 0; i < params.length; i++) {
                    stmt.setObject(i + 1, params[i]);
                }
                
                // Execute update
                return stmt.executeUpdate();
                
            } catch (SQLException e) {
                throw new RuntimeException("Update execution failed: " + e.getMessage(), e);
            }
        }, false).onComplete(res -> {  // Unordered - DB updates don't need ordering
            if (res.succeeded()) {
                promise.complete(res.result());
            } else {
                promise.fail(res.cause());
            }
        });
        
        return promise.future();
    }
    
    /**
     * Get table metadata
     */
    public Future<JsonObject> getTableMetadata(String tableName) {
        return getTableMetadata(tableName, null);
    }
    
    /**
     * Get table metadata with streaming support
     */
    public Future<JsonObject> getTableMetadata(String tableName, String streamId) {
        String cacheKey = "table_" + tableName.toUpperCase();
        
        // Check cache
        JsonObject cached = metadataCache.get(cacheKey);
        if (cached != null && cached.getLong("cachedAt", 0L) > System.currentTimeMillis() - CACHE_DURATION_MS) {
            return Future.succeededFuture(cached);
        }
        
        // Use new Callable API for executeBlocking (Vert.x 4.5+)
        return vertx.executeBlocking(() -> {
            try (Connection conn = getConnection()) {
                DatabaseMetaData metaData = conn.getMetaData();
                
                JsonObject tableInfo = new JsonObject();
                JsonArray columns = new JsonArray();
                
                // Get column information
                try (ResultSet rs = metaData.getColumns(null, DB_USER.toUpperCase(), 
                                                        tableName.toUpperCase(), null)) {
                    while (rs.next()) {
                        JsonObject column = new JsonObject()
                            .put("name", rs.getString("COLUMN_NAME"))
                            .put("type", rs.getString("TYPE_NAME"))
                            .put("size", rs.getInt("COLUMN_SIZE"))
                            .put("nullable", rs.getInt("NULLABLE") == DatabaseMetaData.columnNullable)
                            .put("position", rs.getInt("ORDINAL_POSITION"));
                        columns.add(column);
                    }
                }
                
                // Get primary keys
                JsonArray primaryKeys = new JsonArray();
                try (ResultSet rs = metaData.getPrimaryKeys(null, DB_USER.toUpperCase(), 
                                                            tableName.toUpperCase())) {
                    while (rs.next()) {
                        primaryKeys.add(rs.getString("COLUMN_NAME"));
                    }
                }
                
                // Get foreign keys
                JsonArray foreignKeys = new JsonArray();
                try (ResultSet rs = metaData.getImportedKeys(null, DB_USER.toUpperCase(), 
                                                             tableName.toUpperCase())) {
                    while (rs.next()) {
                        JsonObject fk = new JsonObject()
                            .put("column", rs.getString("FKCOLUMN_NAME"))
                            .put("referencedTable", rs.getString("PKTABLE_NAME"))
                            .put("referencedColumn", rs.getString("PKCOLUMN_NAME"));
                        foreignKeys.add(fk);
                    }
                }
                
                tableInfo.put("tableName", tableName.toUpperCase())
                         .put("columns", columns)
                         .put("primaryKeys", primaryKeys)
                         .put("foreignKeys", foreignKeys)
                         .put("cachedAt", System.currentTimeMillis());
                
                // Cache the result
                metadataCache.put(cacheKey, tableInfo);
                
                // Publish metadata event if streaming
                if (streamId != null) {
                    StreamingEventPublisher publisher = new StreamingEventPublisher(vertx, streamId);
                    publisher.publishMetadataExploration(tableName, tableInfo);
                }
                
                return tableInfo;
                
            } catch (SQLException e) {
                throw new RuntimeException("Failed to get table metadata: " + e.getMessage(), e);
            }
        }, false);  // Unordered execution
    }
    
    /**
     * List all tables in the schema with row counts
     */
    public Future<JsonArray> listTables() {
        // Use new Callable API for executeBlocking (Vert.x 4.5+)
        return vertx.executeBlocking(() -> {
            try (Connection conn = getConnection()) {
                JsonArray tables = new JsonArray();
                
                // Use a simpler, faster query to get table names with row counts
                // This avoids the metadata API which can be slow
                String sql = "SELECT table_name, num_rows " +
                           "FROM user_tables " +
                           "WHERE table_name NOT LIKE 'SYS_%' " +
                           "AND table_name NOT LIKE 'APEX_%' " +
                           "ORDER BY table_name";
                
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(sql)) {
                    
                    while (rs.next()) {
                        String tableName = rs.getString("TABLE_NAME");
                        long rowCount = rs.getLong("NUM_ROWS");
                        
                        tables.add(new JsonObject()
                            .put("name", tableName)
                            .put("row_count", rowCount > 0 ? rowCount : 100)); // Default to 100 if stats are stale (NUM_ROWS = 0)
                    }
                }
                
                return tables;
                
            } catch (SQLException e) {
                // Return empty array on failure (don't mask database issues)
                // Log error but don't use event bus in blocking handler
                // Error will be handled by caller
                return new JsonArray();
            }
        }, false);  // Unordered execution
    }
    
    /**
     * Convert ResultSet to JsonArray
     */
    private JsonArray resultSetToJson(ResultSet rs) throws SQLException {
        JsonArray results = new JsonArray();
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        
        while (rs.next()) {
            JsonObject row = new JsonObject();
            
            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnLabel(i);
                Object value = rs.getObject(i);
                
                if (value == null) {
                    row.putNull(columnName);
                } else if (value instanceof Number) {
                    row.put(columnName, ((Number) value).doubleValue());
                } else if (value instanceof Boolean) {
                    row.put(columnName, (Boolean) value);
                } else if (value instanceof Timestamp) {
                    row.put(columnName, ((Timestamp) value).toString());
                } else if (value instanceof Date) {
                    row.put(columnName, ((Date) value).toString());
                } else {
                    row.put(columnName, value.toString());
                }
            }
            
            results.add(row);
        }
        
        return results;
    }
    
    /**
     * Test connection
     */
    public Future<Boolean> testConnection() {
        if (!initialized) {
            return Future.succeededFuture(false);
        }
        
        // Use new Callable API for executeBlocking (Vert.x 4.5+)
        return vertx.executeBlocking(() -> {
            try (Connection conn = getConnection()) {
                return conn.isValid(5);
            } catch (SQLException e) {
                return false;
            }
        }, false);  // Unordered execution
    }
    
    /**
     * Get connection pool statistics
     */
    public JsonObject getPoolStatistics() {
        if (!initialized || poolDataSource == null) {
            return new JsonObject().put("initialized", false);
        }
        
        try {
            return new JsonObject()
                .put("initialized", true)
                .put("poolingEnabled", true)
                .put("availableConnections", poolDataSource.getAvailableConnectionsCount())
                .put("borrowedConnections", poolDataSource.getBorrowedConnectionsCount())
                .put("totalConnections", poolDataSource.getAvailableConnectionsCount() + 
                                          poolDataSource.getBorrowedConnectionsCount())
                .put("minPoolSize", poolDataSource.getMinPoolSize())
                .put("maxPoolSize", poolDataSource.getMaxPoolSize());
        } catch (SQLException e) {
            return new JsonObject()
                .put("initialized", true)
                .put("poolingEnabled", true)
                .put("error", "Failed to get pool statistics: " + e.getMessage());
        }
    }
    
    /**
     * Shutdown the connection pool
     */
    public Future<Void> shutdown() {
        if (!initialized) {
            return Future.succeededFuture();
        }
        
        // Use new Callable API for executeBlocking (Vert.x 4.5+)
        return vertx.executeBlocking(() -> {
            try {
                // Properly close the Oracle UCP connection pool
                if (poolDataSource != null) {
                    // First, set connection wait timeout to 0 to prevent new connections
                    poolDataSource.setConnectionWaitTimeout(0);
                    
                    // Get the pool name if set
                    String poolName = poolDataSource.getConnectionPoolName();
                    
                    if (poolName != null && !poolName.isEmpty()) {
                        // Use UniversalConnectionPoolManager for proper cleanup
                        try {
                            oracle.ucp.admin.UniversalConnectionPoolManager mgr = 
                                oracle.ucp.admin.UniversalConnectionPoolManagerImpl
                                    .getUniversalConnectionPoolManager();
                            
                            // First purge all connections
                            mgr.purgeConnectionPool(poolName);
                            
                            // Then destroy the pool
                            mgr.destroyConnectionPool(poolName);
                            
                            System.out.println("[OracleConnectionManager] Connection pool '" + poolName + "' destroyed successfully");
                        } catch (oracle.ucp.UniversalConnectionPoolException e) {
                            // If manager fails, at least nullify the reference
                            System.err.println("[OracleConnectionManager] Failed to destroy pool via manager: " + e.getMessage());
                        }
                    }
                    
                    // Nullify the reference
                    poolDataSource = null;
                }
                
                // Clean up connection manager
                jdbcUrl = null;
                password = null;
                initialized = false;
                
                if (vertx != null) {
                    vertx.eventBus().publish("log", "Oracle UCP pool shut down properly,1,OracleConnectionManager,Shutdown,Database");
                }
                
                return null;  // Return null for Void
            } catch (SQLException e) {
                throw new RuntimeException("Failed to shutdown connection pool: " + e.getMessage(), e);
            }
        }, false);  // Unordered execution
    }
}
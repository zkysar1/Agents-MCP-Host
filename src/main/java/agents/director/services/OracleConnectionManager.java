package agents.director.services;

import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;
import oracle.ucp.admin.UniversalConnectionPoolManager;
import oracle.ucp.admin.UniversalConnectionPoolManagerImpl;
import oracle.ucp.UniversalConnectionPoolException;
import io.vertx.core.Vertx;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;

import java.sql.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.time.Instant;

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
    private String lastConnectionError = null;
    private long lastConnectionAttempt = 0;
    private static final long RETRY_INTERVAL_MS = 5000; // 5 seconds between retries
    private int retryCount = 0;
    private static final int MAX_RETRY_COUNT = 3;
    
    // Connection configuration
    private static final String DB_HOST = "adb.us-ashburn-1.oraclecloud.com";
    private static final int DB_PORT = 1522;
    private static final String DB_SERVICE = "gd77773c35a7f01_zaksedwtest_high.adb.oraclecloud.com";
    private static final String DB_USER = "ADMIN";
    
    // Connection pool settings - optimized for development/testing
    private static final int MIN_POOL_SIZE = 2;  // Reduced from 5
    private static final int MAX_POOL_SIZE = 10; // Reduced from 20
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
     * Publish critical error to event bus for system-wide notification
     */
    private void publishCriticalError(String error, String operation) {
        if (vertx != null) {
            JsonObject criticalError = new JsonObject()
                .put("eventType", "critical.error")
                .put("component", "OracleConnectionManager")
                .put("operation", operation)
                .put("error", error)
                .put("severity", "CRITICAL")
                .put("timestamp", Instant.now().toString())
                .put("requiresRestart", true)
                .put("userMessage", "Database connection critical failure. The system cannot access the database. Please contact support or restart the system.");
            
            vertx.eventBus().publish("critical.error", criticalError);
            
            // Also log for persistence
            vertx.eventBus().publish("log",
                "CRITICAL: " + error + ",0,OracleConnectionManager," + operation + ",Database");
        }
    }
    
    /**
     * Initialize the connection pool
     */
    public Future<Void> initialize(Vertx vertx) {
        this.vertx = vertx;
        Promise<Void> promise = Promise.<Void>promise();
        
        System.out.println("[OracleConnectionManager] Initialize called from: " + Thread.currentThread().getName());
        
        // Check if we should skip Oracle initialization
        if ("true" == "true") {
            System.out.println("[OracleConnectionManager] SKIP_ORACLE=true, skipping Oracle initialization");
            initialized = false;
            promise.complete();
            return promise.future();
        }
        
        // Quick check if already initialized and healthy
        if (initialized && isConnectionHealthy()) {
            System.out.println("[OracleConnectionManager] Already initialized and healthy, skipping initialization");
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
                    password = "Violet2.Barnstorm_A9";
                }
                
                // Build JDBC URL with TLS
                jdbcUrl = String.format(
                    "jdbc:oracle:thin:@(description=(retry_count=20)(retry_delay=3)" +
                    "(address=(protocol=tcps)(port=%d)(host=%s))" +
                    "(connect_data=(service_name=%s))" +
                    "(security=(ssl_server_dn_match=yes)))",
                    DB_PORT, DB_HOST, DB_SERVICE
                );
                
                // Get the UCP manager singleton
                UniversalConnectionPoolManager mgr = 
                    UniversalConnectionPoolManagerImpl.getUniversalConnectionPoolManager();
                
                // Check if our pool already exists
                String[] existingPools = mgr.getConnectionPoolNames();
                boolean poolExists = false;
                
                System.out.println("[OracleConnectionManager] Found " + existingPools.length + " existing pools");
                for (String poolName : existingPools) {
                    System.out.println("[OracleConnectionManager] - Pool: " + poolName);
                    if ("OracleAgentPool".equals(poolName)) {
                        poolExists = true;
                        break;
                    }
                }
                
                if (poolExists) {
                    // Pool exists - try to reuse it
                    try {
                        vertx.eventBus().publish("log", "[OracleConnectionManager] Found existing pool 'OracleAgentPool', attempting to reuse..." + ",2,OracleConnectionManager,Database,System");
                        
                        // Get existing pool reference
                        // Note: getConnectionPool returns UniversalConnectionPoolAdapter which implements PoolDataSource
                        poolDataSource = (PoolDataSource) mgr.getConnectionPool("OracleAgentPool");
                        
                        // Test if it's healthy
                        try (Connection conn = poolDataSource.getConnection()) {
                            if (conn.isValid(5)) {
                                // Existing pool is healthy - reuse it!
                                initialized = true;
                                lastConnectionError = null;
                                
                                if (vertx != null) {
                                    vertx.eventBus().publish("log", "Reusing existing healthy Oracle connection pool,1,OracleConnectionManager,StartUp,Database");
                                }
                                
                                vertx.eventBus().publish("log", "[OracleConnectionManager] Successfully reusing existing healthy connection pool" + ",2,OracleConnectionManager,Database,System");
                                return null; // Success - reused existing pool
                            }
                        }
                    } catch (Exception e) {
                        // Existing pool is unhealthy - need to destroy and recreate
                        vertx.eventBus().publish("log", "[OracleConnectionManager] Existing pool unhealthy: " + e.getMessage() + ",2,OracleConnectionManager,Database,System");
                        vertx.eventBus().publish("log", "[OracleConnectionManager] Destroying unhealthy pool and creating new one..." + ",2,OracleConnectionManager,Database,System");
                        
                        try {
                            mgr.destroyConnectionPool("OracleAgentPool");
                        } catch (UniversalConnectionPoolException uce) {
                            // Ignore - pool might already be in bad state
                            vertx.eventBus().publish("log", "[OracleConnectionManager] Warning during pool destroy: " + uce.getMessage() + ",1,OracleConnectionManager,Database,System");
                        }
                        
                        poolExists = false; // Will create new one below
                    }
                }
                
                // Create new pool only if needed
                if (!poolExists) {
                    System.out.println("[OracleConnectionManager] Creating new UCP pool with min=" + MIN_POOL_SIZE + " max=" + MAX_POOL_SIZE);
                    vertx.eventBus().publish("log", "[OracleConnectionManager] Creating new connection pool..." + ",2,OracleConnectionManager,Database,System");
                    
                    // Load Oracle driver
                    Class.forName("oracle.jdbc.driver.OracleDriver");
                    
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
                    
                    // Additional tuning for cloud connections
                    poolDataSource.setConnectionWaitTimeout(10); // Reduce to 10 seconds for faster failure
                    poolDataSource.setInactiveConnectionTimeout(120); // 2 minutes
                    poolDataSource.setAbandonedConnectionTimeout(300); // 5 minutes
                    poolDataSource.setTimeToLiveConnectionTimeout(600); // 10 minutes
                    
                    // Disable features known to cause memory issues
                    poolDataSource.setFastConnectionFailoverEnabled(false); // Disable FCF
                    poolDataSource.setONSConfiguration(""); // Disable Oracle Notification Service
                    poolDataSource.setSecondsToTrustIdleConnection(0); // Disable trust for idle connections
                    poolDataSource.setSQLForValidateConnection("SELECT 1 FROM DUAL"); // Simple validation query
                    
                    // Test the connection
                    try (Connection conn = poolDataSource.getConnection()) {
                        if (!conn.isValid(5)) {
                            throw new SQLException("Connection validation failed");
                        }
                        
                        // Only set initialized to true if connection actually works
                        initialized = true;
                        lastConnectionError = null;
                        
                        // Log successful initialization
                        if (vertx != null) {
                            vertx.eventBus().publish("log", "Oracle UCP pool initialized - " + DB_SERVICE + 
                                " (min=" + MIN_POOL_SIZE + ", max=" + MAX_POOL_SIZE + "),1,OracleConnectionManager,StartUp,Database");
                        }
                        
                        vertx.eventBus().publish("log", "[OracleConnectionManager] Successfully created and tested new connection pool" + ",2,OracleConnectionManager,Database,System");
                        return null; // Return null for Void
                    }
                }
                
                // Should not reach here, but return null for completeness
                return null;
                
            } catch (SQLException e) {
                lastConnectionError = "SQL Exception: " + e.getMessage();
                lastConnectionAttempt = System.currentTimeMillis();
                
                // Publish critical error for SQL exceptions
                publishCriticalError(e.getMessage(), "Initialization");
                
                if (vertx != null) {
                    vertx.eventBus().publish("log", "Failed to initialize Oracle connection: " + e.getMessage() + 
                        ",0,OracleConnectionManager,StartUp,Database");
                }
                throw new RuntimeException("SQL Exception during initialization", e);
            } catch (ClassNotFoundException e) {
                lastConnectionError = "Oracle JDBC driver not found: " + e.getMessage();
                lastConnectionAttempt = System.currentTimeMillis();
                
                // Publish critical error for missing driver
                publishCriticalError("Oracle JDBC driver not found: " + e.getMessage(), "Initialization");
                
                if (vertx != null) {
                    vertx.eventBus().publish("log", "Oracle JDBC driver not found: " + e.getMessage() + 
                        ",0,OracleConnectionManager,StartUp,Database");
                }
                throw new RuntimeException("Oracle JDBC driver not found", e);
            } catch (Exception e) {
                lastConnectionError = "Connection failed: " + e.getMessage();
                lastConnectionAttempt = System.currentTimeMillis();
                
                // Publish critical error for any other initialization failure
                publishCriticalError("Connection failed: " + e.getMessage(), "Initialization");
                
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
        Promise<JsonArray> promise = Promise.<JsonArray>promise();
        
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
        Promise<Integer> promise = Promise.<Integer>promise();
        
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
                if (poolDataSource != null) {
                    String poolName = poolDataSource.getConnectionPoolName();
                    
                    // Mark as not initialized first
                    initialized = false;
                    
                    // IMPORTANT: Actually destroy the pool to prevent memory leaks
                    try {
                        UniversalConnectionPoolManager mgr = 
                            UniversalConnectionPoolManagerImpl.getUniversalConnectionPoolManager();
                        mgr.destroyConnectionPool(poolName);
                        System.out.println("[OracleConnectionManager] Destroyed pool: " + poolName);
                        
                        vertx.eventBus().publish("log", "[OracleConnectionManager] Destroyed connection pool: " + poolName + ",2,OracleConnectionManager,Database,System");
                    } catch (Exception e) {
                        System.err.println("[OracleConnectionManager] Error destroying pool: " + e.getMessage());
                        vertx.eventBus().publish("log", "[OracleConnectionManager] Error destroying pool: " + e.getMessage() + ",0,OracleConnectionManager,Database,System");
                    }
                    
                    poolDataSource = null;
                }
                
                // Clean up other references
                jdbcUrl = null;
                password = null;
                
                return null;  // Return null for Void
            } catch (Exception e) {
                throw new RuntimeException("Shutdown error: " + e.getMessage(), e);
            }
        }, false);  // Unordered execution
    }
    
    /**
     * Check if connection is healthy and available
     */
    public boolean isConnectionHealthy() {
        if (!initialized || poolDataSource == null) {
            return false;
        }
        
        try {
            // Quick check of pool statistics first
            int available = poolDataSource.getAvailableConnectionsCount();
            int borrowed = poolDataSource.getBorrowedConnectionsCount();
            
            // If pool is maxed out, we can't get a connection
            if (borrowed >= poolDataSource.getMaxPoolSize() && available == 0) {
                lastConnectionError = "Connection pool exhausted";
                return false;
            }
            
            // Actually try to get a connection to verify it works
            // Use a very short timeout to avoid blocking
            int originalTimeout = poolDataSource.getConnectionWaitTimeout();
            poolDataSource.setConnectionWaitTimeout(2); // 2 seconds
            
            try (Connection conn = poolDataSource.getConnection()) {
                boolean isValid = conn.isValid(1); // 1 second validation timeout
                if (!isValid) {
                    lastConnectionError = "Connection validation failed";
                    lastConnectionAttempt = System.currentTimeMillis();
                }
                return isValid;
            } catch (SQLException e) {
                lastConnectionError = "Failed to get connection: " + e.getMessage();
                lastConnectionAttempt = System.currentTimeMillis();
                return false;
            } finally {
                // Restore original timeout
                poolDataSource.setConnectionWaitTimeout(originalTimeout);
            }
        } catch (SQLException e) {
            lastConnectionError = "Pool error: " + e.getMessage();
            lastConnectionAttempt = System.currentTimeMillis();
            return false;
        }
    }
    
    /**
     * Get last connection error
     */
    public String getLastConnectionError() {
        if (lastConnectionError == null) {
            return null;
        }
        
        // Add time since last attempt
        if (lastConnectionAttempt > 0) {
            long timeSince = System.currentTimeMillis() - lastConnectionAttempt;
            return lastConnectionError + " (occurred " + (timeSince / 1000) + " seconds ago)";
        }
        
        return lastConnectionError;
    }
    
    /**
     * Get connection status for tools to check
     */
    public JsonObject getConnectionStatus() {
        JsonObject status = new JsonObject()
            .put("healthy", isConnectionHealthy())
            .put("initialized", initialized)
            .put("retryCount", retryCount);
            
        if (lastConnectionError != null) {
            status.put("lastError", getLastConnectionError());
        }
        
        if (initialized && poolDataSource != null) {
            try {
                status.put("availableConnections", poolDataSource.getAvailableConnectionsCount())
                      .put("borrowedConnections", poolDataSource.getBorrowedConnectionsCount())
                      .put("maxConnections", poolDataSource.getMaxPoolSize());
            } catch (SQLException e) {
                status.put("poolError", e.getMessage());
            }
        }
        
        return status;
    }
    
    /**
     * Attempt to reconnect if connection has failed
     */
    public Future<Void> attemptReconnection() {
        if (initialized && isConnectionHealthy()) {
            // Already connected
            return Future.succeededFuture();
        }
        
        // Check if we should retry
        long timeSinceLastAttempt = System.currentTimeMillis() - lastConnectionAttempt;
        long backoffTime = RETRY_INTERVAL_MS * (1L << Math.min(retryCount, 4)); // Exponential backoff, max 80 seconds
        
        if (timeSinceLastAttempt < backoffTime) {
            return Future.failedFuture("Retry backoff period not elapsed. Wait " + 
                (backoffTime - timeSinceLastAttempt) / 1000 + " more seconds");
        }
        
        if (retryCount >= MAX_RETRY_COUNT) {
            return Future.failedFuture("Maximum retry attempts (" + MAX_RETRY_COUNT + ") exceeded");
        }
        
        retryCount++;
        lastConnectionAttempt = System.currentTimeMillis();
        
        vertx.eventBus().publish("log", "[OracleConnectionManager] Attempting reconnection (attempt " + retryCount + "/" + MAX_RETRY_COUNT + ")" + ",1,OracleConnectionManager,Database,System");
        
        // If already initialized but not healthy, try to reset
        if (initialized) {
            return shutdown()
                .compose(v -> initialize(vertx))
                .onSuccess(v -> {
                    retryCount = 0; // Reset on success
                    vertx.eventBus().publish("log", "[OracleConnectionManager] Reconnection successful" + ",2,OracleConnectionManager,Database,System");
                })
                .onFailure(err -> {
                    vertx.eventBus().publish("log", "[OracleConnectionManager] Reconnection failed: " + err.getMessage() + ",0,OracleConnectionManager,Database,System");
                });
        } else {
            // Not initialized, just try to initialize
            return initialize(vertx)
                .onSuccess(v -> {
                    retryCount = 0; // Reset on success
                    vertx.eventBus().publish("log", "[OracleConnectionManager] Connection successful" + ",2,OracleConnectionManager,Database,System");
                })
                .onFailure(err -> {
                    vertx.eventBus().publish("log", "[OracleConnectionManager] Connection failed: " + err.getMessage() + ",0,OracleConnectionManager,Database,System");
                });
        }
    }
    
    /**
     * Execute a custom database operation with a connection from the pool.
     * The operation is executed in a blocking handler and the connection is
     * automatically returned to the pool after use.
     * 
     * @param operation A function that takes a Connection and returns a result
     * @return Future containing the result of the operation
     */
    public <T> Future<T> executeWithConnection(java.util.function.Function<Connection, T> operation) {
        if (!initialized) {
            return Future.failedFuture("Connection pool not initialized");
        }
        
        return vertx.executeBlocking(() -> {
            try (Connection conn = getConnection()) {
                return operation.apply(conn);
            } catch (SQLException e) {
                throw new RuntimeException("Database operation failed: " + e.getMessage(), e);
            }
        }, false);  // Unordered execution
    }
}
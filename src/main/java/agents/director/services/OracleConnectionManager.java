package agents.director.services;

import io.vertx.core.Vertx;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;

import java.sql.*;

/**
 * Simplified Oracle Connection Manager using direct JDBC connections.
 * No pooling - creates a new connection for each operation.
 */
public class OracleConnectionManager {
    
    private static OracleConnectionManager instance;
    private volatile Vertx vertx;
    
    // Connection details - can be overridden by environment variables
    private static final String DB_HOST = System.getenv("ORACLE_HOST") != null ? 
        System.getenv("ORACLE_HOST") : "adb.us-ashburn-1.oraclecloud.com";
    
    private static final String DB_PORT = System.getenv("ORACLE_PORT") != null ? 
        System.getenv("ORACLE_PORT") : "1522";
    
    private static final String DB_SERVICE = System.getenv("ORACLE_SERVICE") != null ? 
        System.getenv("ORACLE_SERVICE") : "gd77773c35a7f01_zaksedwtest_high.adb.oraclecloud.com";
    
    private static final String DB_USER = System.getenv("ORACLE_USER") != null ? 
        System.getenv("ORACLE_USER") : "ADMIN";
    
    private static final String DB_PASSWORD = System.getenv("ORACLE_PASSWORD") != null ? 
        System.getenv("ORACLE_PASSWORD") : "Violet2.Barnstorm_A9";
    
    private static final String JDBC_URL = String.format(
        "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=%s)(PORT=%s))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=%s)))",
        DB_HOST, DB_PORT, DB_SERVICE
    );
    
    private OracleConnectionManager() {
        // Load Oracle JDBC driver
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Oracle JDBC driver not found", e);
        }
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
     * Initialize - just saves Vertx instance
     */
    public Future<Void> initialize(Vertx vertx) {
        this.vertx = vertx;
        System.out.println("[OracleConnectionManager] Simplified Oracle Connection Manager initialized");
        return Future.succeededFuture();
    }
    
    /**
     * Get a new database connection
     */
    private Connection getConnection() throws SQLException {
        try {
            System.out.println("[OracleConnectionManager] Attempting connection to: " + DB_HOST + ":" + DB_PORT + "/" + DB_SERVICE);
            Connection conn = DriverManager.getConnection(JDBC_URL, DB_USER, DB_PASSWORD);
            System.out.println("[OracleConnectionManager] Connection successful");
            return conn;
        } catch (SQLException e) {
            System.err.println("[OracleConnectionManager] Connection failed!");
            System.err.println("  JDBC URL: " + JDBC_URL);
            System.err.println("  User: " + DB_USER);
            System.err.println("  Error: " + e.getMessage());
            throw e;
        }
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
        if (vertx == null) {
            return Future.failedFuture("OracleConnectionManager not initialized. Call initialize() first.");
        }
        
        // Clean SQL - remove trailing semicolons which Oracle JDBC doesn't like
        String trimmedSql = sql.trim();
        final String cleanSql = trimmedSql.endsWith(";") ? 
            trimmedSql.substring(0, trimmedSql.length() - 1).trim() : 
            trimmedSql;
        
        // Note: streamId parameter is ignored in simplified version
        
        return vertx.<JsonArray>executeBlocking(() -> {
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
        }, false);
    }
    
    /**
     * Execute an update (INSERT, UPDATE, DELETE) and return affected rows
     */
    public Future<Integer> executeUpdate(String sql, Object... params) {
        if (vertx == null) {
            return Future.failedFuture("OracleConnectionManager not initialized. Call initialize() first.");
        }
        
        // Clean SQL - remove trailing semicolons which Oracle JDBC doesn't like
        String trimmedSql = sql.trim();
        final String cleanSql = trimmedSql.endsWith(";") ? 
            trimmedSql.substring(0, trimmedSql.length() - 1).trim() : 
            trimmedSql;
        
        return vertx.<Integer>executeBlocking(() -> {
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
        }, false);
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
        if (vertx == null) {
            return Future.failedFuture("OracleConnectionManager not initialized. Call initialize() first.");
        }
        
        // Note: streamId parameter is ignored in simplified version
        
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
                         .put("foreignKeys", foreignKeys);
                
                return tableInfo;
                
            } catch (SQLException e) {
                throw new RuntimeException("Failed to get table metadata: " + e.getMessage(), e);
            }
        }, false);
    }
    
    /**
     * List all tables in the schema with row counts
     */
    public Future<JsonArray> listTables() {
        if (vertx == null) {
            return Future.failedFuture("OracleConnectionManager not initialized. Call initialize() first.");
        }
        
        return vertx.executeBlocking(() -> {
            try (Connection conn = getConnection()) {
                JsonArray tables = new JsonArray();
                
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
                            .put("row_count", rowCount > 0 ? rowCount : 100));
                    }
                }
                
                return tables;
                
            } catch (SQLException e) {
                throw new RuntimeException("Failed to list tables: " + e.getMessage(), e);
            }
        }, false);
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
        if (vertx == null) {
            return Future.failedFuture("OracleConnectionManager not initialized. Call initialize() first.");
        }
        
        return vertx.executeBlocking(() -> {
            try (Connection conn = getConnection()) {
                return conn.isValid(5);
            } catch (SQLException e) {
                return false;
            }
        }, false);
    }
    
    /**
     * Get connection pool statistics
     */
    public JsonObject getPoolStatistics() {
        // No pooling in simplified version
        return new JsonObject()
            .put("initialized", true)
            .put("poolingEnabled", false)
            .put("message", "Direct connections only - no pooling");
    }
    
    /**
     * Shutdown - nothing to do in simplified version
     */
    public Future<Void> shutdown() {
        System.out.println("[OracleConnectionManager] Shutdown called - no cleanup needed");
        return Future.succeededFuture();
    }
    
    /**
     * Check if connection is healthy and available
     */
    public boolean isConnectionHealthy() {
        // Simplified check - just verify we can load the driver
        // Actual connection test would be too expensive to do synchronously
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            return vertx != null; // Also check initialization
        } catch (ClassNotFoundException e) {
            return false;
        }
    }
    
    /**
     * Get connection status for tools to check
     */
    public JsonObject getConnectionStatus() {
        return new JsonObject()
            .put("healthy", isConnectionHealthy())
            .put("initialized", true)
            .put("poolingEnabled", false);
    }
    
    /**
     * No reconnection logic in simplified version
     */
    public Future<Void> attemptReconnection() {
        return Future.succeededFuture();
    }
    
    /**
     * No last connection error tracking in simplified version
     */
    public String getLastConnectionError() {
        return null;
    }
    
    /**
     * Execute a custom database operation with a connection.
     * The connection is automatically closed after use.
     * 
     * @param operation A function that takes a Connection and returns a result
     * @return Future containing the result of the operation
     */
    public <T> Future<T> executeWithConnection(java.util.function.Function<Connection, T> operation) {
        if (vertx == null) {
            return Future.failedFuture("OracleConnectionManager not initialized. Call initialize() first.");
        }
        
        return vertx.executeBlocking(() -> {
            try (Connection conn = getConnection()) {
                return operation.apply(conn);
            } catch (SQLException e) {
                throw new RuntimeException("Database operation failed: " + e.getMessage(), e);
            }
        }, false);
    }
}
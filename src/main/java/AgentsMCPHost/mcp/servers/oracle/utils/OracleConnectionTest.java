package AgentsMCPHost.mcp.servers.oracle.utils;

import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;
import java.sql.*;
import java.util.Properties;

/**
 * Standalone Oracle connection tester to diagnose connection issues.
 * Tests both direct JDBC and UCP pool connections.
 */
public class OracleConnectionTest {
    
    // Connection configuration
    private static final String DB_HOST = "adb.us-ashburn-1.oraclecloud.com";
    private static final int DB_PORT = 1522;
    private static final String DB_SERVICE = "gd77773c35a7f01_zaksedwtest_high.adb.oraclecloud.com";
    private static final String DB_USER = "ADMIN";
    
    public static void main(String[] args) {
        System.out.println("=== Oracle Connection Test ===");
        System.out.println("Host: " + DB_HOST);
        System.out.println("Port: " + DB_PORT);
        System.out.println("Service: " + DB_SERVICE);
        System.out.println("User: " + DB_USER);
        System.out.println();
        
        // Get password
        String password = System.getenv("ORACLE_TESTING_DATABASE_PASSWORD");
        if (password == null || password.isEmpty()) {
            System.out.println("[WARNING] Using default test password - not for production!");
            password = "ARmy0320-- milk";
        }
        
        // Test 1: Network connectivity
        testNetworkConnectivity();
        
        // Test 2: Direct JDBC connection
        testDirectJdbcConnection(password);
        
        // Test 3: UCP pool connection
        testUcpPoolConnection(password);
    }
    
    private static void testNetworkConnectivity() {
        System.out.println("=== Test 1: Network Connectivity ===");
        try {
            java.net.InetAddress address = java.net.InetAddress.getByName(DB_HOST);
            System.out.println("✓ DNS resolution successful: " + address.getHostAddress());
            
            // Test port connectivity
            try (java.net.Socket socket = new java.net.Socket()) {
                socket.connect(new java.net.InetSocketAddress(DB_HOST, DB_PORT), 5000);
                System.out.println("✓ Port " + DB_PORT + " is reachable");
            }
        } catch (Exception e) {
            System.err.println("✗ Network connectivity failed: " + e.getMessage());
            e.printStackTrace();
        }
        System.out.println();
    }
    
    private static void testDirectJdbcConnection(String password) {
        System.out.println("=== Test 2: Direct JDBC Connection ===");
        
        String jdbcUrl = String.format(
            "jdbc:oracle:thin:@(description=(retry_count=20)(retry_delay=3)" +
            "(address=(protocol=tcps)(port=%d)(host=%s))" +
            "(connect_data=(service_name=%s))" +
            "(security=(ssl_server_dn_match=yes)))",
            DB_PORT, DB_HOST, DB_SERVICE
        );
        
        System.out.println("JDBC URL: " + jdbcUrl);
        
        try {
            // Load driver
            Class.forName("oracle.jdbc.driver.OracleDriver");
            System.out.println("✓ Oracle JDBC driver loaded");
            
            // Set connection properties
            Properties props = new Properties();
            props.setProperty("user", DB_USER);
            props.setProperty("password", password);
            props.setProperty("oracle.net.ssl_server_dn_match", "true");
            props.setProperty("javax.net.ssl.trustStore", "truststore.jks");
            props.setProperty("javax.net.ssl.trustStoreType", "JKS");
            props.setProperty("oracle.net.authentication_services", "(TCPS)");
            
            // Try to connect
            System.out.println("Attempting connection...");
            long startTime = System.currentTimeMillis();
            
            try (Connection conn = DriverManager.getConnection(jdbcUrl, props)) {
                long connectTime = System.currentTimeMillis() - startTime;
                System.out.println("✓ Connection successful in " + connectTime + "ms");
                
                // Test query
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT 'Connected' as status FROM DUAL")) {
                    if (rs.next()) {
                        System.out.println("✓ Query successful: " + rs.getString("status"));
                    }
                }
                
                // Get connection metadata
                DatabaseMetaData meta = conn.getMetaData();
                System.out.println("Database Product: " + meta.getDatabaseProductName());
                System.out.println("Database Version: " + meta.getDatabaseProductVersion());
                System.out.println("Driver Version: " + meta.getDriverVersion());
                
            }
        } catch (ClassNotFoundException e) {
            System.err.println("✗ Oracle JDBC driver not found: " + e.getMessage());
        } catch (SQLException e) {
            System.err.println("✗ Connection failed: " + e.getMessage());
            System.err.println("SQL State: " + e.getSQLState());
            System.err.println("Error Code: " + e.getErrorCode());
            e.printStackTrace();
            
            // Check for specific error codes
            if (e.getMessage().contains("ORA-12506")) {
                System.err.println("\nPossible fix: Enable TLS authentication in Oracle Cloud Console");
            } else if (e.getMessage().contains("ORA-01017")) {
                System.err.println("\nPossible fix: Check username/password");
            } else if (e.getMessage().contains("IO Error")) {
                System.err.println("\nPossible fix: Check network/firewall settings");
            }
        }
        System.out.println();
    }
    
    private static void testUcpPoolConnection(String password) {
        System.out.println("=== Test 3: UCP Pool Connection ===");
        
        PoolDataSource poolDataSource = null;
        try {
            // Create pool
            poolDataSource = PoolDataSourceFactory.getPoolDataSource();
            System.out.println("✓ Pool data source created");
            
            // Configure pool
            poolDataSource.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource");
            poolDataSource.setConnectionPoolName("TestPool");
            
            String jdbcUrl = String.format(
                "jdbc:oracle:thin:@(description=(retry_count=20)(retry_delay=3)" +
                "(address=(protocol=tcps)(port=%d)(host=%s))" +
                "(connect_data=(service_name=%s))" +
                "(security=(ssl_server_dn_match=yes)))",
                DB_PORT, DB_HOST, DB_SERVICE
            );
            
            poolDataSource.setURL(jdbcUrl);
            poolDataSource.setUser(DB_USER);
            poolDataSource.setPassword(password);
            
            // Pool settings
            poolDataSource.setInitialPoolSize(1);
            poolDataSource.setMinPoolSize(1);
            poolDataSource.setMaxPoolSize(5);
            poolDataSource.setConnectionWaitTimeout(30);
            poolDataSource.setValidateConnectionOnBorrow(true);
            
            System.out.println("Pool configuration:");
            System.out.println("  Initial size: " + poolDataSource.getInitialPoolSize());
            System.out.println("  Min size: " + poolDataSource.getMinPoolSize());
            System.out.println("  Max size: " + poolDataSource.getMaxPoolSize());
            System.out.println("  Wait timeout: " + poolDataSource.getConnectionWaitTimeout() + "s");
            
            // Try to get connection
            System.out.println("\nGetting connection from pool...");
            long startTime = System.currentTimeMillis();
            
            try (Connection conn = poolDataSource.getConnection()) {
                long connectTime = System.currentTimeMillis() - startTime;
                System.out.println("✓ Got connection from pool in " + connectTime + "ms");
                
                if (conn.isValid(5)) {
                    System.out.println("✓ Connection is valid");
                }
                
                // Pool statistics
                System.out.println("\nPool statistics:");
                System.out.println("  Available connections: " + poolDataSource.getAvailableConnectionsCount());
                System.out.println("  Borrowed connections: " + poolDataSource.getBorrowedConnectionsCount());
                
            }
            
        } catch (SQLException e) {
            System.err.println("✗ UCP pool connection failed: " + e.getMessage());
            System.err.println("SQL State: " + e.getSQLState());
            System.err.println("Error Code: " + e.getErrorCode());
            e.printStackTrace();
            
            // UCP-specific error handling
            if (e.getMessage().contains("UCP-0")) {
                System.err.println("\nUCP-specific error detected. Possible causes:");
                System.err.println("- Missing Oracle UCP libraries");
                System.err.println("- Incorrect pool configuration");
                System.err.println("- Network/firewall issues");
                System.err.println("- Oracle Cloud ACL configuration");
            }
        } finally {
            // Clean up pool
            if (poolDataSource != null) {
                try {
                    oracle.ucp.admin.UniversalConnectionPoolManager mgr = 
                        oracle.ucp.admin.UniversalConnectionPoolManagerImpl
                            .getUniversalConnectionPoolManager();
                    
                    String poolName = poolDataSource.getConnectionPoolName();
                    if (poolName != null) {
                        mgr.destroyConnectionPool(poolName);
                        System.out.println("\n✓ Pool cleaned up");
                    }
                } catch (Exception e) {
                    System.err.println("Failed to clean up pool: " + e.getMessage());
                }
            }
        }
    }
}
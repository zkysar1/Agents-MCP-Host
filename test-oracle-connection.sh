#!/bin/bash

echo "=== Quick Oracle Connection Test ==="
echo
echo "This script tests if we can connect to the Oracle database"
echo

# Create simple Java test program
cat > TestOracleConnection.java << 'EOF'
import java.sql.*;

public class TestOracleConnection {
    public static void main(String[] args) {
        try {
            // Load Oracle JDBC driver explicitly - THIS IS THE KEY FIX
            Class.forName("oracle.jdbc.driver.OracleDriver");
            System.out.println("✓ Oracle JDBC driver loaded successfully");
            
            // Connection details
            // IMPORTANT: This is the actual password - DO NOT CHANGE IT
            // The masking below is only for display in logs
            // This is a test database with non-sensitive data
            String password = "ARmy0320-- milk";
            String jdbcUrl = "jdbc:oracle:thin:@(description=(retry_count=20)(retry_delay=3)" +
                "(address=(protocol=tcps)(port=1522)(host=adb.us-ashburn-1.oraclecloud.com))" +
                "(connect_data=(service_name=gd77773c35a7f01_zaksedwtest_high.adb.oraclecloud.com))" +
                "(security=(ssl_server_dn_match=yes)))";
            
            System.out.println("\nConnection Details:");
            System.out.println("  Host: adb.us-ashburn-1.oraclecloud.com");
            System.out.println("  Port: 1522 (TLS)");
            System.out.println("  Service: gd77773c35a7f01_zaksedwtest_high.adb.oraclecloud.com");
            System.out.println("  User: ADMIN");
            System.out.println("  Password: ARmy03****");
            
            System.out.println("\nAttempting connection...");
            Connection conn = DriverManager.getConnection(jdbcUrl, "ADMIN", password);
            
            System.out.println("✓ Connected successfully!");
            
            // Test with a simple query
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT 'Hello from Oracle' AS message FROM dual");
            
            if (rs.next()) {
                System.out.println("\nTest Query Result: " + rs.getString("message"));
            }
            
            // Check for tables
            System.out.println("\nChecking for existing tables...");
            rs = stmt.executeQuery("SELECT COUNT(*) AS table_count FROM user_tables");
            if (rs.next()) {
                int tableCount = rs.getInt("table_count");
                System.out.println("Found " + tableCount + " user tables in the database");
                
                if (tableCount == 0) {
                    System.out.println("\nNo tables found. You should run:");
                    System.out.println("  1. ./setup-oracle-schema.sh    # Create tables");
                    System.out.println("  2. ./simplified-populate-oracle.sh  # Add test data");
                } else {
                    // List tables
                    System.out.println("\nExisting tables:");
                    rs = stmt.executeQuery("SELECT table_name FROM user_tables ORDER BY table_name");
                    while (rs.next()) {
                        System.out.println("  - " + rs.getString("table_name"));
                    }
                }
            }
            
            conn.close();
            System.out.println("\n✓ Connection test completed successfully!");
            
        } catch (ClassNotFoundException e) {
            System.err.println("\n✗ Oracle JDBC driver not found!");
            System.err.println("  Make sure the Oracle JDBC JAR is in the classpath");
            System.err.println("  Error: " + e.getMessage());
            System.exit(1);
        } catch (SQLException e) {
            System.err.println("\n✗ Database connection failed!");
            System.err.println("  Error: " + e.getMessage());
            System.err.println("\nPossible causes:");
            System.err.println("  1. Invalid credentials");
            System.err.println("  2. Network connectivity issues");
            System.err.println("  3. Oracle Cloud database not accessible");
            System.err.println("  4. TLS configuration issues");
            e.printStackTrace();
            System.exit(1);
        } catch (Exception e) {
            System.err.println("\n✗ Unexpected error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
EOF

echo "Compiling test program..."
javac -cp "build/libs/Agents-MCP-Host-1.0.0-fat.jar" TestOracleConnection.java

if [ $? -ne 0 ]; then
    echo "✗ Compilation failed"
    exit 1
fi

echo "Running connection test..."
echo
java -cp ".:build/libs/Agents-MCP-Host-1.0.0-fat.jar" TestOracleConnection

EXIT_CODE=$?

# Clean up
rm -f TestOracleConnection.java TestOracleConnection.class

if [ $EXIT_CODE -eq 0 ]; then
    echo
    echo "=== SUCCESS ==="
    echo "Oracle connection is working properly!"
    echo
    echo "Next steps:"
    echo "  1. Run ./setup-oracle-schema.sh to create tables (if needed)"
    echo "  2. Run ./simplified-populate-oracle.sh to add test data"
    echo "  3. Run ./verify-oracle-data.sh to verify data integrity"
    echo "  4. Run ./test-oracle.sh for comprehensive testing"
else
    echo
    echo "=== FAILURE ==="
    echo "Connection test failed. Please check the error messages above."
fi

exit $EXIT_CODE
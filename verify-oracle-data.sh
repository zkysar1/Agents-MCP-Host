#!/bin/bash

echo "=== Oracle Database Verification ==="
echo "Checking data integrity and record counts"
echo

# Set password directly
export ORACLE_TESTING_DATABASE_PASSWORD="ARmy0320-- milk"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "✓ Using configured Oracle password"
echo

# Create Java verification program
cat > VerifyOracleData.java << 'EOF'
import java.sql.*;
import java.io.*;
import java.nio.file.*;
import java.util.*;

public class VerifyOracleData {
    private static Connection conn = null;
    private static Statement stmt = null;
    private static Map<String, Integer> baseline = new HashMap<>();
    private static Map<String, Integer> current = new HashMap<>();
    private static boolean hasIssues = false;
    
    public static void main(String[] args) {
        try {
            // Load Oracle JDBC driver explicitly
            Class.forName("oracle.jdbc.driver.OracleDriver");
            System.out.println("✓ Oracle JDBC driver loaded");
            
            String password = "ARmy0320-- milk";
            String jdbcUrl = "jdbc:oracle:thin:@(description=(retry_count=20)(retry_delay=3)" +
                "(address=(protocol=tcps)(port=1522)(host=adb.us-ashburn-1.oraclecloud.com))" +
                "(connect_data=(service_name=gd77773c35a7f01_zaksedwtest_high.adb.oraclecloud.com))" +
                "(security=(ssl_server_dn_match=yes)))";
            
            System.out.println("Connecting to Oracle database...");
            conn = DriverManager.getConnection(jdbcUrl, "ADMIN", password);
            stmt = conn.createStatement();
            System.out.println("✓ Connected successfully\n");
            
            // Check current counts
            checkTableCounts();
            
            // Verify relationships
            verifyRelationships();
            
            // Check for low stock products
            checkLowStock();
            
            // Check enumeration usage
            checkEnumerationUsage();
            
            // Save or compare baseline
            handleBaseline();
            
            conn.close();
            
            if (hasIssues) {
                System.out.println("\n✗ Verification found issues!");
                System.exit(1);
            } else {
                System.out.println("\n✓ All verification checks passed!");
            }
            
        } catch (Exception e) {
            System.err.println("ERROR: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    private static void checkTableCounts() throws SQLException {
        System.out.println("=== Table Record Counts ===");
        
        String[] tables = {
            "country_enum",
            "order_status_enum", 
            "product_category_enum",
            "customers",
            "products",
            "orders",
            "order_details"
        };
        
        for (String table : tables) {
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table);
            rs.next();
            int count = rs.getInt(1);
            current.put(table, count);
            System.out.printf("%-25s: %d records\n", table, count);
        }
        
        // Check expected minimums
        System.out.println("\n=== Checking Expected Minimums ===");
        
        checkMinimum("country_enum", 3);
        checkMinimum("order_status_enum", 5);
        checkMinimum("product_category_enum", 3);
        checkMinimum("customers", 10);
        checkMinimum("products", 20);
        checkMinimum("orders", 25);  // Updated to match simplified dataset
        checkMinimum("order_details", 45);  // Updated to match expected range (25-75 details)
    }
    
    private static void checkMinimum(String table, int minimum) {
        int count = current.get(table);
        if (count >= minimum) {
            System.out.printf("✓ %-25s: %d (minimum: %d)\n", table, count, minimum);
        } else if (count == 0) {
            System.out.printf("✗ %-25s: EMPTY (minimum: %d)\n", table, minimum);
            hasIssues = true;
        } else {
            System.out.printf("⚠ %-25s: %d (expected minimum: %d)\n", table, count, minimum);
        }
    }
    
    private static void verifyRelationships() throws SQLException {
        System.out.println("\n=== Verifying Relationships ===");
        
        // Check orphaned orders
        ResultSet rs = stmt.executeQuery(
            "SELECT COUNT(*) FROM orders o WHERE NOT EXISTS " +
            "(SELECT 1 FROM customers c WHERE c.customer_id = o.customer_id)"
        );
        rs.next();
        int orphanedOrders = rs.getInt(1);
        if (orphanedOrders == 0) {
            System.out.println("✓ No orphaned orders");
        } else {
            System.out.println("✗ Found " + orphanedOrders + " orphaned orders");
            hasIssues = true;
        }
        
        // Check orders without details
        rs = stmt.executeQuery(
            "SELECT COUNT(*) FROM orders o WHERE NOT EXISTS " +
            "(SELECT 1 FROM order_details od WHERE od.order_id = o.order_id)"
        );
        rs.next();
        int ordersWithoutDetails = rs.getInt(1);
        if (ordersWithoutDetails == 0) {
            System.out.println("✓ All orders have details");
        } else {
            System.out.println("⚠ Found " + ordersWithoutDetails + " orders without details");
        }
        
        // Check invalid product references
        rs = stmt.executeQuery(
            "SELECT COUNT(*) FROM order_details od WHERE NOT EXISTS " +
            "(SELECT 1 FROM products p WHERE p.product_id = od.product_id)"
        );
        rs.next();
        int invalidProducts = rs.getInt(1);
        if (invalidProducts == 0) {
            System.out.println("✓ All order details reference valid products");
        } else {
            System.out.println("✗ Found " + invalidProducts + " order details with invalid products");
            hasIssues = true;
        }
        
        // Check invalid status references
        rs = stmt.executeQuery(
            "SELECT COUNT(*) FROM orders o WHERE NOT EXISTS " +
            "(SELECT 1 FROM order_status_enum os WHERE os.status_id = o.status_id)"
        );
        rs.next();
        int invalidStatuses = rs.getInt(1);
        if (invalidStatuses == 0) {
            System.out.println("✓ All orders have valid status");
        } else {
            System.out.println("✗ Found " + invalidStatuses + " orders with invalid status");
            hasIssues = true;
        }
    }
    
    private static void checkLowStock() throws SQLException {
        System.out.println("\n=== Low Stock Products ===");
        
        ResultSet rs = stmt.executeQuery(
            "SELECT product_code, product_name, units_in_stock, reorder_level " +
            "FROM products " +
            "WHERE units_in_stock <= reorder_level " +
            "ORDER BY units_in_stock"
        );
        
        int lowStockCount = 0;
        while (rs.next()) {
            lowStockCount++;
            System.out.printf("⚠ %s - %s: %d units (reorder at %d)\n",
                rs.getString(1), rs.getString(2), rs.getInt(3), rs.getInt(4));
        }
        
        if (lowStockCount == 0) {
            System.out.println("ℹ No products are low in stock");
        } else {
            System.out.println("Found " + lowStockCount + " products low in stock");
        }
    }
    
    private static void checkEnumerationUsage() throws SQLException {
        System.out.println("\n=== Enumeration Usage ===");
        
        // Check order status distribution
        System.out.println("\nOrder Status Distribution:");
        ResultSet rs = stmt.executeQuery(
            "SELECT os.status_code, COUNT(o.order_id) AS cnt " +
            "FROM order_status_enum os " +
            "LEFT JOIN orders o ON os.status_id = o.status_id " +
            "GROUP BY os.status_code, os.status_order " +
            "ORDER BY os.status_order"
        );
        
        int unusedStatuses = 0;
        while (rs.next()) {
            int count = rs.getInt(2);
            System.out.printf("  %-15s: %d orders\n", rs.getString(1), count);
            if (count == 0) unusedStatuses++;
        }
        
        if (unusedStatuses > 0) {
            System.out.println("ℹ " + unusedStatuses + " status values are unused");
        }
        
        // Check category distribution
        System.out.println("\nProduct Category Distribution:");
        rs = stmt.executeQuery(
            "SELECT pc.category_name, COUNT(p.product_id) AS cnt " +
            "FROM product_category_enum pc " +
            "LEFT JOIN products p ON pc.category_id = p.category_id " +
            "GROUP BY pc.category_name " +
            "ORDER BY cnt DESC"
        );
        
        int unusedCategories = 0;
        while (rs.next()) {
            int count = rs.getInt(2);
            System.out.printf("  %-20s: %d products\n", rs.getString(1), count);
            if (count == 0) unusedCategories++;
        }
        
        if (unusedCategories > 0) {
            System.out.println("ℹ " + unusedCategories + " categories are unused");
        }
        
        // Check country distribution
        System.out.println("\nCountry Distribution:");
        rs = stmt.executeQuery(
            "SELECT cn.country_name, COUNT(c.customer_id) AS cnt " +
            "FROM country_enum cn " +
            "LEFT JOIN customers c ON cn.country_id = c.country_id " +
            "GROUP BY cn.country_name " +
            "ORDER BY cnt DESC"
        );
        
        int unusedCountries = 0;
        while (rs.next()) {
            int count = rs.getInt(2);
            System.out.printf("  %-20s: %d customers\n", rs.getString(1), count);
            if (count == 0) unusedCountries++;
        }
        
        if (unusedCountries > 0) {
            System.out.println("ℹ " + unusedCountries + " countries are unused");
        }
    }
    
    private static void handleBaseline() throws IOException, SQLException {
        System.out.println("\n=== Baseline Comparison ===");
        
        File baselineFile = new File(".oracle-baseline.txt");
        
        if (baselineFile.exists()) {
            // Load and compare baseline
            List<String> lines = Files.readAllLines(baselineFile.toPath());
            for (String line : lines) {
                String[] parts = line.split("=");
                if (parts.length == 2) {
                    baseline.put(parts[0], Integer.parseInt(parts[1]));
                }
            }
            
            System.out.println("Comparing with baseline...");
            boolean dataChanged = false;
            
            for (String table : current.keySet()) {
                int currentCount = current.get(table);
                int baselineCount = baseline.getOrDefault(table, -1);
                
                if (baselineCount == -1) {
                    System.out.printf("  ? %-25s: %d (no baseline)\n", table, currentCount);
                } else if (currentCount == baselineCount) {
                    System.out.printf("  ✓ %-25s: %d (unchanged)\n", table, currentCount);
                } else if (currentCount > baselineCount) {
                    System.out.printf("  ↑ %-25s: %d (was %d, +%d)\n", 
                        table, currentCount, baselineCount, currentCount - baselineCount);
                    dataChanged = true;
                } else {
                    System.out.printf("  ↓ %-25s: %d (was %d, %d)\n", 
                        table, currentCount, baselineCount, currentCount - baselineCount);
                    dataChanged = true;
                }
            }
            
            if (dataChanged) {
                System.out.println("\n⚠ Data has changed since baseline!");
                System.out.println("  This might indicate the database is not read-only during operations.");
            } else {
                System.out.println("\n✓ Data matches baseline - no changes detected");
            }
            
        } else {
            // Save baseline
            System.out.println("Creating baseline file...");
            PrintWriter writer = new PrintWriter(baselineFile);
            for (String table : current.keySet()) {
                writer.println(table + "=" + current.get(table));
            }
            writer.close();
            System.out.println("✓ Baseline saved to .oracle-baseline.txt");
            System.out.println("  Run this script again to check for data changes");
        }
    }
}
EOF

echo "Compiling verification program..."
javac -cp "build/libs/Agents-MCP-Host-1.0.0-fat.jar" VerifyOracleData.java

if [ $? -ne 0 ]; then
    echo -e "${RED}✗ Compilation failed${NC}"
    exit 1
fi

echo "Running verification..."
echo
java -cp ".:build/libs/Agents-MCP-Host-1.0.0-fat.jar" VerifyOracleData

EXIT_CODE=$?

# Clean up
rm -f VerifyOracleData.java VerifyOracleData.class

if [ $EXIT_CODE -ne 0 ]; then
    echo
    echo -e "${RED}✗ Verification failed${NC}"
    exit 1
else
    echo
    echo -e "${GREEN}✓ Verification complete!${NC}"
    echo
    echo "Tips:"
    echo "  • Run this script after populating data to create a baseline"
    echo "  • Run it again later to check if data has changed"
    echo "  • The baseline is stored in .oracle-baseline.txt"
    echo "  • Delete the baseline file to create a new one"
fi
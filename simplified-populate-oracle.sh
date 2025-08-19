#!/bin/bash

echo "=== Simplified Oracle Database Population ==="
echo "Creating minimal, static test data for reliable testing"
echo

# Set password directly
export ORACLE_TESTING_DATABASE_PASSWORD="ARmy0320-- milk"

echo "✓ Using configured Oracle password"
echo

# Create Java program with smaller, static dataset
cat > SimplifiedOraclePopulator.java << 'EOF'
import java.sql.*;

public class SimplifiedOraclePopulator {
    private static Connection conn = null;
    private static Statement stmt = null;
    private static int totalInserted = 0;
    
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
            conn.setAutoCommit(false);
            stmt = conn.createStatement();
            System.out.println("✓ Connected successfully\n");
            
            // Clear existing data first
            clearExistingData();
            
            // Insert data in phases with progress reporting
            insertEnumerations();
            insertCustomers();
            insertProducts();
            insertOrders();
            
            // Commit all changes
            conn.commit();
            System.out.println("\n✓ All data committed successfully!");
            
            // Show final statistics
            showStatistics();
            
            conn.close();
            System.out.println("\n✓ Population complete! Total records inserted: " + totalInserted);
            
        } catch (Exception e) {
            System.err.println("ERROR: " + e.getMessage());
            e.printStackTrace();
            try {
                if (conn != null) conn.rollback();
            } catch (SQLException se) {
                se.printStackTrace();
            }
            System.exit(1);
        }
    }
    
    private static void clearExistingData() throws SQLException {
        System.out.println("Clearing existing data...");
        stmt.execute("DELETE FROM order_details");
        stmt.execute("DELETE FROM orders");
        stmt.execute("DELETE FROM products");
        stmt.execute("DELETE FROM customers");
        stmt.execute("DELETE FROM product_category_enum");
        stmt.execute("DELETE FROM order_status_enum");
        stmt.execute("DELETE FROM country_enum");
        System.out.println("✓ Existing data cleared\n");
    }
    
    private static void insertEnumerations() throws SQLException {
        System.out.println("Phase 1: Inserting enumeration data...");
        
        // Order Status - Static, deterministic values
        String[] orderStatuses = {
            "INSERT INTO order_status_enum (status_code, status_description, status_order) VALUES ('PENDING', 'Order pending confirmation', 1)",
            "INSERT INTO order_status_enum (status_code, status_description, status_order) VALUES ('CONFIRMED', 'Order confirmed', 2)",
            "INSERT INTO order_status_enum (status_code, status_description, status_order) VALUES ('SHIPPED', 'Order shipped to customer', 3)",
            "INSERT INTO order_status_enum (status_code, status_description, status_order) VALUES ('DELIVERED', 'Order delivered successfully', 4)",
            "INSERT INTO order_status_enum (status_code, status_description, status_order) VALUES ('CANCELLED', 'Order cancelled', 5)"
        };
        
        for (String sql : orderStatuses) {
            stmt.execute(sql);
            totalInserted++;
        }
        System.out.println("  ✓ Inserted " + orderStatuses.length + " order statuses");
        
        // Product Categories - Static, deterministic values
        String[] categories = {
            "INSERT INTO product_category_enum (category_code, category_name, category_description) VALUES ('ELECTRONICS', 'Electronics', 'Electronic devices')",
            "INSERT INTO product_category_enum (category_code, category_name, category_description) VALUES ('SOFTWARE', 'Software', 'Software products')",
            "INSERT INTO product_category_enum (category_code, category_name, category_description) VALUES ('OFFICE', 'Office Supplies', 'Office supplies')"
        };
        
        for (String sql : categories) {
            stmt.execute(sql);
            totalInserted++;
        }
        System.out.println("  ✓ Inserted " + categories.length + " product categories");
        
        // Countries - Static, deterministic values
        String[] countries = {
            "INSERT INTO country_enum (country_code, country_name, region) VALUES ('USA', 'United States', 'North America')",
            "INSERT INTO country_enum (country_code, country_name, region) VALUES ('CAN', 'Canada', 'North America')",
            "INSERT INTO country_enum (country_code, country_name, region) VALUES ('GBR', 'United Kingdom', 'Europe')"
        };
        
        for (String sql : countries) {
            stmt.execute(sql);
            totalInserted++;
        }
        System.out.println("  ✓ Inserted " + countries.length + " countries");
        System.out.println("  Phase 1 complete!\n");
    }
    
    private static void insertCustomers() throws SQLException {
        System.out.println("Phase 2: Inserting customers...");
        
        // Get country IDs
        ResultSet rs = stmt.executeQuery("SELECT country_id FROM country_enum WHERE country_code = 'USA'");
        rs.next();
        int usaId = rs.getInt(1);
        
        rs = stmt.executeQuery("SELECT country_id FROM country_enum WHERE country_code = 'CAN'");
        rs.next();
        int canId = rs.getInt(1);
        
        rs = stmt.executeQuery("SELECT country_id FROM country_enum WHERE country_code = 'GBR'");
        rs.next();
        int gbrId = rs.getInt(1);
        
        // Insert exactly 10 static customers
        String[] customers = {
            String.format("INSERT INTO customers (company_name, contact_name, address, city, postal_code, country_id, phone, email, credit_limit) VALUES ('Acme Corp', 'John Smith', '123 Main St', 'New York', '10001', %d, '212-555-0001', 'john@acme.com', 50000)", usaId),
            String.format("INSERT INTO customers (company_name, contact_name, address, city, postal_code, country_id, phone, email, credit_limit) VALUES ('TechStart Inc', 'Sarah Johnson', '456 Tech Ave', 'San Francisco', '94105', %d, '415-555-0002', 'sarah@techstart.com', 75000)", usaId),
            String.format("INSERT INTO customers (company_name, contact_name, address, city, postal_code, country_id, phone, email, credit_limit) VALUES ('Global Trade', 'James Wilson', '789 Trade St', 'Los Angeles', '90001', %d, '213-555-0003', 'james@global.com', 60000)", usaId),
            String.format("INSERT INTO customers (company_name, contact_name, address, city, postal_code, country_id, phone, email, credit_limit) VALUES ('Maple Ltd', 'Emily Brown', '321 Maple Ave', 'Toronto', 'M5H2N2', %d, '416-555-0004', 'emily@maple.ca', 45000)", canId),
            String.format("INSERT INTO customers (company_name, contact_name, address, city, postal_code, country_id, phone, email, credit_limit) VALUES ('Northern Supply', 'Michael Davis', '654 North Rd', 'Vancouver', 'V6B1A1', %d, '604-555-0005', 'michael@northern.ca', 55000)", canId),
            String.format("INSERT INTO customers (company_name, contact_name, address, city, postal_code, country_id, phone, email, credit_limit) VALUES ('UK Trading', 'Oliver Taylor', '987 London Rd', 'London', 'EC1A1BB', %d, '020-7123-0006', 'oliver@uktrading.co.uk', 80000)", gbrId),
            String.format("INSERT INTO customers (company_name, contact_name, address, city, postal_code, country_id, phone, email, credit_limit) VALUES ('British Goods', 'Emma Wilson', '246 Oxford St', 'Manchester', 'M11AE', %d, '0161-123-0007', 'emma@britishgoods.co.uk', 70000)", gbrId),
            String.format("INSERT INTO customers (company_name, contact_name, address, city, postal_code, country_id, phone, email, credit_limit) VALUES ('West Coast Co', 'David Lee', '135 Pacific Blvd', 'Seattle', '98101', %d, '206-555-0008', 'david@westcoast.com', 65000)", usaId),
            String.format("INSERT INTO customers (company_name, contact_name, address, city, postal_code, country_id, phone, email, credit_limit) VALUES ('East Enterprises', 'Lisa Chen', '357 Atlantic Ave', 'Boston', '02110', %d, '617-555-0009', 'lisa@eastent.com', 40000)", usaId),
            String.format("INSERT INTO customers (company_name, contact_name, address, city, postal_code, country_id, phone, email, credit_limit) VALUES ('Central Supply', 'Robert King', '468 Michigan Ave', 'Chicago', '60601', %d, '312-555-0010', 'robert@central.com', 55000)", usaId)
        };
        
        for (String sql : customers) {
            stmt.execute(sql);
            totalInserted++;
        }
        System.out.println("  ✓ Inserted " + customers.length + " customers");
        System.out.println("  Phase 2 complete!\n");
    }
    
    private static void insertProducts() throws SQLException {
        System.out.println("Phase 3: Inserting products...");
        
        // Get category IDs
        ResultSet rs = stmt.executeQuery("SELECT category_id FROM product_category_enum WHERE category_code = 'ELECTRONICS'");
        rs.next();
        int elecId = rs.getInt(1);
        
        rs = stmt.executeQuery("SELECT category_id FROM product_category_enum WHERE category_code = 'SOFTWARE'");
        rs.next();
        int softId = rs.getInt(1);
        
        rs = stmt.executeQuery("SELECT category_id FROM product_category_enum WHERE category_code = 'OFFICE'");
        rs.next();
        int officeId = rs.getInt(1);
        
        // Insert exactly 20 static products
        String[] products = {
            // Electronics (7 products)
            String.format("INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level) VALUES ('Laptop Pro', 'ELEC001', %d, 'TechSupplier', 1299.99, 15, 5)", elecId),
            String.format("INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level) VALUES ('Wireless Mouse', 'ELEC002', %d, 'TechSupplier', 29.99, 50, 20)", elecId),
            String.format("INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level) VALUES ('USB Hub', 'ELEC003', %d, 'TechSupplier', 19.99, 75, 25)", elecId),
            String.format("INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level) VALUES ('Monitor 27inch', 'ELEC004', %d, 'DisplayCo', 399.99, 10, 5)", elecId),
            String.format("INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level) VALUES ('Keyboard Mechanical', 'ELEC005', %d, 'TechSupplier', 89.99, 25, 10)", elecId),
            String.format("INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level) VALUES ('Webcam HD', 'ELEC006', %d, 'CameraCo', 59.99, 30, 15)", elecId),
            String.format("INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level) VALUES ('Headphones', 'ELEC007', %d, 'AudioCo', 149.99, 20, 8)", elecId),
            
            // Software (7 products)
            String.format("INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level) VALUES ('Office Suite', 'SOFT001', %d, 'SoftCorp', 199.99, 100, 20)", softId),
            String.format("INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level) VALUES ('Antivirus Pro', 'SOFT002', %d, 'SecureSoft', 49.99, 150, 30)", softId),
            String.format("INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level) VALUES ('Photo Editor', 'SOFT003', %d, 'CreativeTools', 79.99, 80, 20)", softId),
            String.format("INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level) VALUES ('Video Editor', 'SOFT004', %d, 'CreativeTools', 149.99, 40, 10)", softId),
            String.format("INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level) VALUES ('Cloud Backup', 'SOFT005', %d, 'CloudCo', 9.99, 200, 50)", softId),
            String.format("INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level) VALUES ('Dev Tools Pro', 'SOFT006', %d, 'DevSoft', 299.99, 60, 15)", softId),
            String.format("INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level) VALUES ('Database Manager', 'SOFT007', %d, 'DataCorp', 499.99, 25, 5)", softId),
            
            // Office Supplies (6 products)
            String.format("INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level) VALUES ('Paper A4 500pk', 'OFFC001', %d, 'PaperCo', 12.99, 200, 50)", officeId),
            String.format("INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level) VALUES ('Pens Blue 12pk', 'OFFC002', %d, 'StationeryCo', 4.99, 300, 100)", officeId),
            String.format("INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level) VALUES ('Stapler', 'OFFC003', %d, 'OfficePro', 8.99, 80, 20)", officeId),
            String.format("INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level) VALUES ('Folders 10pk', 'OFFC004', %d, 'StationeryCo', 6.99, 150, 40)", officeId),
            String.format("INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level) VALUES ('Notebook A5', 'OFFC005', %d, 'PaperCo', 3.99, 250, 75)", officeId),
            String.format("INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level) VALUES ('Desk Organizer', 'OFFC006', %d, 'OfficePro', 24.99, 45, 15)", officeId)
        };
        
        for (String sql : products) {
            stmt.execute(sql);
            totalInserted++;
        }
        
        // Commit inserts before updating
        conn.commit();
        
        // Set 3 products to low stock for testing (after commit to avoid parallel modification issue)
        stmt.execute("UPDATE products SET units_in_stock = 2 WHERE product_code IN ('ELEC001', 'SOFT007', 'OFFC003')");
        conn.commit();
        
        System.out.println("  ✓ Inserted " + products.length + " products");
        System.out.println("  ✓ Set 3 products to low stock for testing");
        System.out.println("  Phase 3 complete!\n");
    }
    
    private static void insertOrders() throws SQLException {
        System.out.println("Phase 4: Inserting orders and order details...");
        
        // Get IDs for static references
        ResultSet rs = stmt.executeQuery("SELECT MIN(customer_id) FROM customers");
        rs.next();
        int firstCustomerId = rs.getInt(1);
        
        rs = stmt.executeQuery("SELECT MIN(product_id) FROM products");
        rs.next();
        int firstProductId = rs.getInt(1);
        
        rs = stmt.executeQuery("SELECT status_id FROM order_status_enum WHERE status_code = 'PENDING'");
        rs.next();
        int pendingId = rs.getInt(1);
        
        rs = stmt.executeQuery("SELECT status_id FROM order_status_enum WHERE status_code = 'CONFIRMED'");
        rs.next();
        int confirmedId = rs.getInt(1);
        
        rs = stmt.executeQuery("SELECT status_id FROM order_status_enum WHERE status_code = 'SHIPPED'");
        rs.next();
        int shippedId = rs.getInt(1);
        
        rs = stmt.executeQuery("SELECT status_id FROM order_status_enum WHERE status_code = 'DELIVERED'");
        rs.next();
        int deliveredId = rs.getInt(1);
        
        rs = stmt.executeQuery("SELECT status_id FROM order_status_enum WHERE status_code = 'CANCELLED'");
        rs.next();
        int cancelledId = rs.getInt(1);
        
        // Insert exactly 25 orders with deterministic data (reduced for faster execution)
        int ordersInserted = 0;
        int detailsInserted = 0;
        
        for (int i = 0; i < 25; i++) {
            int customerId = firstCustomerId + (i % 10); // Cycle through 10 customers
            int statusId;
            
            // Deterministic status distribution (adjusted for 25 orders)
            if (i < 5) statusId = pendingId;
            else if (i < 10) statusId = confirmedId;
            else if (i < 15) statusId = shippedId;
            else if (i < 20) statusId = deliveredId;
            else statusId = cancelledId;
            
            String orderNum = String.format("ORD-2025-%06d", 10000 + i);
            
            String orderSql = String.format(
                "INSERT INTO orders (order_number, customer_id, order_date, status_id, freight_cost, payment_method) " +
                "VALUES ('%s', %d, SYSDATE - %d, %d, %.2f, '%s')",
                orderNum, customerId, (50 - i), statusId, 10.00 + (i * 0.5),
                i % 3 == 0 ? "Credit Card" : i % 3 == 1 ? "PayPal" : "Bank Transfer"
            );
            
            stmt.execute(orderSql);
            ordersInserted++;
            totalInserted++;
            
            // Get the order ID (using PreparedStatement to avoid SQL injection)
            int orderId;
            try (PreparedStatement ps = conn.prepareStatement("SELECT order_id FROM orders WHERE order_number = ?")) {
                ps.setString(1, orderNum);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        orderId = rs.getInt(1);
                    } else {
                        throw new SQLException("Order not found: " + orderNum);
                    }
                }
            }
            
            // Add 1-3 order details per order (deterministic)
            int numDetails = (i % 3) + 1;
            for (int j = 0; j < numDetails; j++) {
                int productId = firstProductId + ((i + j) % 20); // Cycle through 20 products
                
                // Get product price (using PreparedStatement)
                double unitPrice;
                try (PreparedStatement ps = conn.prepareStatement("SELECT unit_price FROM products WHERE product_id = ?")) {
                    ps.setInt(1, productId);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            unitPrice = rs.getDouble(1);
                        } else {
                            throw new SQLException("Product not found: " + productId);
                        }
                    }
                }
                
                String detailSql = String.format(
                    "INSERT INTO order_details (order_id, product_id, quantity, unit_price, discount) " +
                    "VALUES (%d, %d, %d, %.2f, %.2f)",
                    orderId, productId, (j + 1), unitPrice, j == 2 ? 0.1 : 0.0
                );
                
                try {
                    stmt.execute(detailSql);
                    detailsInserted++;
                    totalInserted++;
                } catch (SQLException e) {
                    // Skip if duplicate product in same order
                }
            }
            
            // Progress indicator and commit every 10 orders to reduce round trips
            if ((i + 1) % 10 == 0) {
                conn.commit(); // Commit batch to reduce network overhead
                System.out.println("  ... " + (i + 1) + " orders processed and committed");
            }
        }
        
        // Final commit for any remaining orders
        conn.commit();
        
        System.out.println("  ✓ Inserted " + ordersInserted + " orders");
        System.out.println("  ✓ Inserted " + detailsInserted + " order details");
        System.out.println("  Phase 4 complete!\n");
    }
    
    private static void showStatistics() throws SQLException {
        System.out.println("\n=== Final Statistics ===");
        
        ResultSet rs = stmt.executeQuery(
            "SELECT " +
            "(SELECT COUNT(*) FROM customers) AS customers, " +
            "(SELECT COUNT(*) FROM products) AS products, " +
            "(SELECT COUNT(*) FROM orders) AS orders, " +
            "(SELECT COUNT(*) FROM order_details) AS order_lines " +
            "FROM dual"
        );
        
        if (rs.next()) {
            System.out.println("Customers: " + rs.getInt("customers"));
            System.out.println("Products: " + rs.getInt("products"));
            System.out.println("Orders: " + rs.getInt("orders"));
            System.out.println("Order Lines: " + rs.getInt("order_lines"));
        }
        
        System.out.println("\nOrder Status Distribution:");
        rs = stmt.executeQuery(
            "SELECT os.status_code, COUNT(o.order_id) AS cnt " +
            "FROM order_status_enum os " +
            "LEFT JOIN orders o ON os.status_id = o.status_id " +
            "GROUP BY os.status_code " +
            "ORDER BY cnt DESC"
        );
        
        while (rs.next()) {
            System.out.println("  " + rs.getString(1) + ": " + rs.getInt(2));
        }
        
        System.out.println("\nProducts with Low Stock:");
        rs = stmt.executeQuery(
            "SELECT product_code, product_name, units_in_stock " +
            "FROM products " +
            "WHERE units_in_stock <= reorder_level " +
            "ORDER BY units_in_stock"
        );
        
        while (rs.next()) {
            System.out.println("  " + rs.getString(1) + " - " + rs.getString(2) + ": " + rs.getInt(3) + " units");
        }
    }
}
EOF

echo "Compiling Java program..."
javac -cp "build/libs/Agents-MCP-Host-1.0.0-fat.jar" SimplifiedOraclePopulator.java

if [ $? -ne 0 ]; then
    echo "✗ Compilation failed"
    exit 1
fi

echo "Running population script (may take up to 90 seconds due to network latency)..."
echo
timeout 90 java -cp ".:build/libs/Agents-MCP-Host-1.0.0-fat.jar" SimplifiedOraclePopulator

EXIT_CODE=$?

# Clean up
rm -f SimplifiedOraclePopulator.java SimplifiedOraclePopulator.class

if [ $EXIT_CODE -eq 124 ]; then
    echo
    echo "✗ Population script timed out after 90 seconds"
    echo "  This might indicate a connection issue or slow network"
    exit 1
elif [ $EXIT_CODE -ne 0 ]; then
    echo
    echo "✗ Population script failed with exit code $EXIT_CODE"
    exit 1
else
    echo
    echo "✓ Simplified population complete!"
    echo
    echo "This script created a minimal, static dataset:"
    echo "  • 10 customers (from USA, Canada, UK)"
    echo "  • 20 products (Electronics, Software, Office)"
    echo "  • 25 orders with deterministic status distribution"
    echo "  • ~75 order detail lines"
    echo
    echo "The data is now static and suitable for reliable testing."
fi
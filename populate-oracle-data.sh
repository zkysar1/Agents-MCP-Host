#!/bin/bash

echo "=== Populating Oracle Database with Sample Data ==="
echo

# Set password directly
export ORACLE_TESTING_DATABASE_PASSWORD="ARmy0320-- milk"

echo "✓ Using configured Oracle password"
echo

# Create Java program to populate data
cat > PopulateOracleData.java << 'EOF'
import java.sql.*;
import java.util.Random;

public class PopulateOracleData {
    public static void main(String[] args) throws Exception {
        // Load Oracle JDBC driver explicitly
        Class.forName("oracle.jdbc.driver.OracleDriver");
        System.out.println("✓ Oracle JDBC driver loaded");
        
        String password = "ARmy0320-- milk";
        String jdbcUrl = "jdbc:oracle:thin:@(description=(retry_count=20)(retry_delay=3)" +
            "(address=(protocol=tcps)(port=1522)(host=adb.us-ashburn-1.oraclecloud.com))" +
            "(connect_data=(service_name=gd77773c35a7f01_zaksedwtest_high.adb.oraclecloud.com))" +
            "(security=(ssl_server_dn_match=yes)))";
        
        System.out.println("Connecting to Oracle database...");
        Connection conn = DriverManager.getConnection(jdbcUrl, "ADMIN", password);
        conn.setAutoCommit(false);
        Statement stmt = conn.createStatement();
        Random rand = new Random();
        
        System.out.println("Populating enumeration tables...\n");
        
        // Insert Order Status Enumerations
        String[] statusData = {
            "('PENDING', 'Order pending confirmation', 1)",
            "('CONFIRMED', 'Order confirmed', 2)",
            "('PROCESSING', 'Order being processed', 3)",
            "('PACKED', 'Order packed and ready', 4)",
            "('SHIPPED', 'Order shipped to customer', 5)",
            "('DELIVERED', 'Order delivered successfully', 6)",
            "('CANCELLED', 'Order cancelled', 7)",
            "('RETURNED', 'Order returned by customer', 8)",
            "('REFUNDED', 'Order refunded', 9)"
        };
        
        for (String status : statusData) {
            stmt.execute("INSERT INTO order_status_enum (status_code, status_description, status_order) VALUES " + status);
        }
        System.out.println("✓ Inserted " + statusData.length + " order statuses");
        
        // Insert Product Categories
        String[] categoryData = {
            "('ELECTRONICS', 'Electronics', 'Electronic devices and accessories')",
            "('COMPUTERS', 'Computers', 'Desktop and laptop computers')",
            "('SOFTWARE', 'Software', 'Software applications and licenses')",
            "('OFFICE', 'Office Supplies', 'Office equipment and supplies')",
            "('FURNITURE', 'Furniture', 'Office and home furniture')",
            "('BOOKS', 'Books', 'Books and publications')",
            "('FOOD', 'Food & Beverages', 'Food and beverage products')",
            "('CLOTHING', 'Clothing', 'Apparel and accessories')",
            "('TOYS', 'Toys & Games', 'Toys and gaming products')",
            "('SPORTS', 'Sports Equipment', 'Sports and fitness equipment')"
        };
        
        for (String cat : categoryData) {
            stmt.execute("INSERT INTO product_category_enum (category_code, category_name, category_description) VALUES " + cat);
        }
        System.out.println("✓ Inserted " + categoryData.length + " product categories");
        
        // Insert Countries
        String[] countryData = {
            "('USA', 'United States', 'North America')",
            "('CAN', 'Canada', 'North America')",
            "('GBR', 'United Kingdom', 'Europe')",
            "('DEU', 'Germany', 'Europe')",
            "('FRA', 'France', 'Europe')",
            "('JPN', 'Japan', 'Asia')",
            "('CHN', 'China', 'Asia')",
            "('AUS', 'Australia', 'Oceania')",
            "('BRA', 'Brazil', 'South America')",
            "('IND', 'India', 'Asia')"
        };
        
        for (String country : countryData) {
            stmt.execute("INSERT INTO country_enum (country_code, country_name, region) VALUES " + country);
        }
        System.out.println("✓ Inserted " + countryData.length + " countries");
        
        System.out.println("\nPopulating customers...");
        
        // Insert specific customers
        String[] customerInserts = {
            "INSERT INTO customers (company_name, contact_name, contact_title, address, city, postal_code, country_id, phone, email, credit_limit) " +
            "VALUES ('Acme Corporation', 'John Smith', 'CEO', '123 Main St', 'New York', '10001', " +
            "(SELECT country_id FROM country_enum WHERE country_code = 'USA'), '212-555-0100', 'john@acme.com', 50000)",
            
            "INSERT INTO customers (company_name, contact_name, contact_title, address, city, postal_code, country_id, phone, email, credit_limit) " +
            "VALUES ('TechStart Inc', 'Sarah Johnson', 'CTO', '456 Tech Ave', 'San Francisco', '94105', " +
            "(SELECT country_id FROM country_enum WHERE country_code = 'USA'), '415-555-0200', 'sarah@techstart.com', 75000)",
            
            "INSERT INTO customers (company_name, contact_name, contact_title, address, city, postal_code, country_id, phone, email, credit_limit) " +
            "VALUES ('Global Traders Ltd', 'James Wilson', 'Purchasing Manager', '789 Trade St', 'London', 'EC1A 1BB', " +
            "(SELECT country_id FROM country_enum WHERE country_code = 'GBR'), '020-7123-4567', 'james@globaltraders.co.uk', 100000)",
            
            "INSERT INTO customers (company_name, contact_name, contact_title, address, city, postal_code, country_id, phone, email, credit_limit) " +
            "VALUES ('Maple Enterprises', 'Emily Brown', 'Director', '321 Maple Ave', 'Toronto', 'M5H 2N2', " +
            "(SELECT country_id FROM country_enum WHERE country_code = 'CAN'), '416-555-0300', 'emily@maple.ca', 60000)",
            
            "INSERT INTO customers (company_name, contact_name, contact_title, address, city, postal_code, country_id, phone, email, credit_limit) " +
            "VALUES ('Deutsche Handel GmbH', 'Hans Mueller', 'Manager', 'Hauptstrasse 100', 'Berlin', '10115', " +
            "(SELECT country_id FROM country_enum WHERE country_code = 'DEU'), '030-123-4567', 'hans@deutschehandel.de', 80000)"
        };
        
        for (String insert : customerInserts) {
            stmt.execute(insert);
        }
        
        // Generate more customers
        ResultSet countryIds = stmt.executeQuery("SELECT country_id FROM country_enum");
        int[] countries = new int[10];
        int idx = 0;
        while (countryIds.next()) {
            countries[idx++] = countryIds.getInt(1);
        }
        
        for (int i = 6; i <= 50; i++) {
            int countryId = countries[rand.nextInt(10)];
            String insert = String.format(
                "INSERT INTO customers (company_name, contact_name, address, city, postal_code, country_id, phone, email, credit_limit) " +
                "VALUES ('Company %d Inc', 'Contact %d', '%d Business Ave', 'City %d', '10%03d', %d, '555-%04d', 'contact%d@company%d.com', %d)",
                i, i, i, i, i, countryId, i * 100, i, i, (rand.nextInt(10) + 1) * 10000
            );
            stmt.execute(insert);
        }
        System.out.println("✓ Inserted 50 customers");
        
        System.out.println("\nPopulating products...");
        
        // Get category IDs
        ResultSet catResult = stmt.executeQuery("SELECT category_id, category_code FROM product_category_enum");
        int[] categoryIds = new int[10];
        idx = 0;
        while (catResult.next()) {
            categoryIds[idx++] = catResult.getInt(1);
        }
        
        // Insert specific products
        String[] productInserts = {
            "INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level) " +
            "VALUES ('4K Smart TV 55\"', 'ELEC001', " + categoryIds[0] + ", 'Samsung', 899.99, 25, 10)",
            
            "INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level) " +
            "VALUES ('Wireless Headphones', 'ELEC002', " + categoryIds[0] + ", 'Sony', 299.99, 50, 20)",
            
            "INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level) " +
            "VALUES ('Gaming Laptop', 'COMP001', " + categoryIds[1] + ", 'ASUS', 1599.99, 12, 5)",
            
            "INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level) " +
            "VALUES ('Office Suite Pro', 'SOFT001', " + categoryIds[2] + ", 'Microsoft', 299.99, 100, 20)",
            
            "INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level) " +
            "VALUES ('Printer Paper A4', 'OFFC001', " + categoryIds[3] + ", 'Staples', 29.99, 200, 50)"
        };
        
        for (String insert : productInserts) {
            stmt.execute(insert);
        }
        
        // Generate more products
        for (int i = 6; i <= 100; i++) {
            int categoryId = categoryIds[rand.nextInt(10)];
            String insert = String.format(
                "INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level) " +
                "VALUES ('Product %d', 'PROD%03d', %d, 'Supplier %d', %.2f, %d, %d)",
                i, i, categoryId, rand.nextInt(5) + 1, 
                10 + rand.nextDouble() * 490, 
                rand.nextInt(100), 
                5 + rand.nextInt(15)
            );
            stmt.execute(insert);
        }
        System.out.println("✓ Inserted 100 products");
        
        System.out.println("\nPopulating orders...");
        
        // Get customer and status IDs
        ResultSet custResult = stmt.executeQuery("SELECT customer_id FROM customers");
        int[] customerIds = new int[50];
        idx = 0;
        while (custResult.next() && idx < 50) {
            customerIds[idx++] = custResult.getInt(1);
        }
        
        ResultSet statusResult = stmt.executeQuery("SELECT status_id FROM order_status_enum");
        int[] statusIds = new int[9];
        idx = 0;
        while (statusResult.next()) {
            statusIds[idx++] = statusResult.getInt(1);
        }
        
        // Generate orders
        for (int i = 1; i <= 200; i++) {
            int customerId = customerIds[rand.nextInt(50)];
            int statusId = statusIds[rand.nextInt(9)];
            
            // Get customer's city and country for shipping
            String insert = String.format(
                "INSERT INTO orders (order_number, customer_id, order_date, status_id, " +
                "shipping_city, shipping_country_id, freight_cost, payment_method) " +
                "SELECT 'ORD-2025-%06d', %d, SYSDATE - %d, %d, " +
                "city, country_id, %.2f, '%s' " +
                "FROM customers WHERE customer_id = %d",
                10000 + i, customerId, rand.nextInt(180), statusId,
                5 + rand.nextDouble() * 45,
                rand.nextInt(3) == 0 ? "Credit Card" : rand.nextInt(2) == 0 ? "PayPal" : "Bank Transfer",
                customerId
            );
            stmt.execute(insert);
        }
        System.out.println("✓ Inserted 200 orders");
        
        System.out.println("\nPopulating order details...");
        
        // Get product IDs
        ResultSet prodResult = stmt.executeQuery("SELECT product_id, unit_price FROM products");
        int[][] products = new int[100][2];
        idx = 0;
        while (prodResult.next() && idx < 100) {
            products[idx][0] = prodResult.getInt(1);
            products[idx][1] = (int) prodResult.getDouble(2);
            idx++;
        }
        
        // Add order details
        ResultSet orderResult = stmt.executeQuery("SELECT order_id FROM orders");
        int detailCount = 0;
        while (orderResult.next()) {
            int orderId = orderResult.getInt(1);
            int itemCount = 1 + rand.nextInt(5);
            
            for (int j = 0; j < itemCount; j++) {
                int prodIdx = rand.nextInt(100);
                int productId = products[prodIdx][0];
                double unitPrice = products[prodIdx][1];
                
                try {
                    String insert = String.format(
                        "INSERT INTO order_details (order_id, product_id, quantity, unit_price, discount) " +
                        "VALUES (%d, %d, %d, %.2f, %.2f)",
                        orderId, productId, 1 + rand.nextInt(10), unitPrice,
                        rand.nextDouble() < 0.8 ? 0 : rand.nextDouble() < 0.5 ? 0.1 : 0.2
                    );
                    stmt.execute(insert);
                    detailCount++;
                } catch (SQLException e) {
                    // Ignore duplicate product in same order
                }
            }
        }
        System.out.println("✓ Inserted " + detailCount + " order details");
        
        // Update some products to be low in stock
        stmt.execute("UPDATE products SET units_in_stock = 3 WHERE product_id IN (SELECT product_id FROM products WHERE ROWNUM <= 10)");
        System.out.println("✓ Set 10 products to low stock");
        
        conn.commit();
        
        System.out.println("\n=== Data Population Summary ===");
        
        // Show statistics
        ResultSet stats = stmt.executeQuery(
            "SELECT " +
            "(SELECT COUNT(*) FROM customers) AS customers, " +
            "(SELECT COUNT(*) FROM products) AS products, " +
            "(SELECT COUNT(*) FROM orders) AS orders, " +
            "(SELECT COUNT(*) FROM order_details) AS order_lines, " +
            "(SELECT SUM(total_amount) FROM orders) AS total_revenue " +
            "FROM dual"
        );
        
        if (stats.next()) {
            System.out.println("Total customers: " + stats.getInt("customers"));
            System.out.println("Total products: " + stats.getInt("products"));
            System.out.println("Total orders: " + stats.getInt("orders"));
            System.out.println("Total order lines: " + stats.getInt("order_lines"));
        }
        
        // Show order status distribution
        System.out.println("\nOrder Status Distribution:");
        ResultSet statusDist = stmt.executeQuery(
            "SELECT os.status_code, COUNT(o.order_id) AS order_count " +
            "FROM order_status_enum os " +
            "LEFT JOIN orders o ON os.status_id = o.status_id " +
            "GROUP BY os.status_code, os.status_order " +
            "ORDER BY os.status_order"
        );
        
        while (statusDist.next()) {
            System.out.println("  " + statusDist.getString(1) + ": " + statusDist.getInt(2));
        }
        
        conn.close();
        System.out.println("\n✓ Data population complete!");
    }
}
EOF

echo "Compiling population program..."
javac -cp "build/libs/Agents-MCP-Host-1.0.0-fat.jar" PopulateOracleData.java

if [ $? -ne 0 ]; then
    echo "✗ Compilation failed"
    exit 1
fi

echo "Running population program (timeout: 90 seconds)..."
echo "Note: This creates a larger dataset (200+ orders). For faster testing, use simplified-populate-oracle.sh"
echo
timeout 90 java -cp ".:build/libs/Agents-MCP-Host-1.0.0-fat.jar" PopulateOracleData

EXIT_CODE=$?

rm -f PopulateOracleData.java PopulateOracleData.class

if [ $EXIT_CODE -eq 124 ]; then
    echo
    echo "✗ Population script timed out after 90 seconds"
    echo "  Try using simplified-populate-oracle.sh for a smaller dataset"
    exit 1
elif [ $EXIT_CODE -ne 0 ]; then
    echo
    echo "✗ Population script failed with exit code $EXIT_CODE"
    exit 1
fi

echo
echo "✓ Sample data population complete!"
-- Sample Data for Oracle MCP Agent Testing
-- This script populates the tables with test data including enumeration values

-- Clear existing data (in reverse order due to foreign keys)
DELETE FROM order_details;
DELETE FROM orders;
DELETE FROM products;
DELETE FROM customers;
DELETE FROM product_category_enum;
DELETE FROM order_status_enum;
DELETE FROM country_enum;

-- Reset sequences
ALTER SEQUENCE order_number_seq RESTART START WITH 10000;

-- Insert Order Status Enumerations
INSERT INTO order_status_enum (status_code, status_description, status_order) VALUES 
('PENDING', 'Order pending confirmation', 1);
INSERT INTO order_status_enum (status_code, status_description, status_order) VALUES 
('CONFIRMED', 'Order confirmed', 2);
INSERT INTO order_status_enum (status_code, status_description, status_order) VALUES 
('PROCESSING', 'Order being processed', 3);
INSERT INTO order_status_enum (status_code, status_description, status_order) VALUES 
('PACKED', 'Order packed and ready', 4);
INSERT INTO order_status_enum (status_code, status_description, status_order) VALUES 
('SHIPPED', 'Order shipped to customer', 5);
INSERT INTO order_status_enum (status_code, status_description, status_order) VALUES 
('DELIVERED', 'Order delivered successfully', 6);
INSERT INTO order_status_enum (status_code, status_description, status_order) VALUES 
('CANCELLED', 'Order cancelled', 7);
INSERT INTO order_status_enum (status_code, status_description, status_order) VALUES 
('RETURNED', 'Order returned by customer', 8);
INSERT INTO order_status_enum (status_code, status_description, status_order) VALUES 
('REFUNDED', 'Order refunded', 9);

-- Insert Product Category Enumerations
INSERT INTO product_category_enum (category_code, category_name, category_description) VALUES 
('ELECTRONICS', 'Electronics', 'Electronic devices and accessories');
INSERT INTO product_category_enum (category_code, category_name, category_description) VALUES 
('COMPUTERS', 'Computers', 'Desktop and laptop computers');
INSERT INTO product_category_enum (category_code, category_name, category_description) VALUES 
('SOFTWARE', 'Software', 'Software applications and licenses');
INSERT INTO product_category_enum (category_code, category_name, category_description) VALUES 
('OFFICE', 'Office Supplies', 'Office equipment and supplies');
INSERT INTO product_category_enum (category_code, category_name, category_description) VALUES 
('FURNITURE', 'Furniture', 'Office and home furniture');
INSERT INTO product_category_enum (category_code, category_name, category_description) VALUES 
('BOOKS', 'Books', 'Books and publications');
INSERT INTO product_category_enum (category_code, category_name, category_description) VALUES 
('FOOD', 'Food & Beverages', 'Food and beverage products');
INSERT INTO product_category_enum (category_code, category_name, category_description) VALUES 
('CLOTHING', 'Clothing', 'Apparel and accessories');
INSERT INTO product_category_enum (category_code, category_name, category_description) VALUES 
('TOYS', 'Toys & Games', 'Toys and gaming products');
INSERT INTO product_category_enum (category_code, category_name, category_description) VALUES 
('SPORTS', 'Sports Equipment', 'Sports and fitness equipment');

-- Insert Country Enumerations
INSERT INTO country_enum (country_code, country_name, region) VALUES 
('USA', 'United States', 'North America');
INSERT INTO country_enum (country_code, country_name, region) VALUES 
('CAN', 'Canada', 'North America');
INSERT INTO country_enum (country_code, country_name, region) VALUES 
('GBR', 'United Kingdom', 'Europe');
INSERT INTO country_enum (country_code, country_name, region) VALUES 
('DEU', 'Germany', 'Europe');
INSERT INTO country_enum (country_code, country_name, region) VALUES 
('FRA', 'France', 'Europe');
INSERT INTO country_enum (country_code, country_name, region) VALUES 
('JPN', 'Japan', 'Asia');
INSERT INTO country_enum (country_code, country_name, region) VALUES 
('CHN', 'China', 'Asia');
INSERT INTO country_enum (country_code, country_name, region) VALUES 
('AUS', 'Australia', 'Oceania');
INSERT INTO country_enum (country_code, country_name, region) VALUES 
('BRA', 'Brazil', 'South America');
INSERT INTO country_enum (country_code, country_name, region) VALUES 
('IND', 'India', 'Asia');

-- Insert Customers (50+ records)
INSERT INTO customers (company_name, contact_name, contact_title, address, city, postal_code, country_id, phone, email, credit_limit) 
SELECT 'Acme Corporation', 'John Smith', 'CEO', '123 Main St', 'New York', '10001', 
    (SELECT country_id FROM country_enum WHERE country_code = 'USA'), '212-555-0100', 'john@acme.com', 50000 FROM dual;

INSERT INTO customers (company_name, contact_name, contact_title, address, city, postal_code, country_id, phone, email, credit_limit) 
SELECT 'TechStart Inc', 'Sarah Johnson', 'CTO', '456 Tech Ave', 'San Francisco', '94105', 
    (SELECT country_id FROM country_enum WHERE country_code = 'USA'), '415-555-0200', 'sarah@techstart.com', 75000 FROM dual;

INSERT INTO customers (company_name, contact_name, contact_title, address, city, postal_code, country_id, phone, email, credit_limit) 
SELECT 'Global Traders Ltd', 'James Wilson', 'Purchasing Manager', '789 Trade St', 'London', 'EC1A 1BB', 
    (SELECT country_id FROM country_enum WHERE country_code = 'GBR'), '020-7123-4567', 'james@globaltraders.co.uk', 100000 FROM dual;

INSERT INTO customers (company_name, contact_name, contact_title, address, city, postal_code, country_id, phone, email, credit_limit) 
SELECT 'Maple Enterprises', 'Emily Brown', 'Director', '321 Maple Ave', 'Toronto', 'M5H 2N2', 
    (SELECT country_id FROM country_enum WHERE country_code = 'CAN'), '416-555-0300', 'emily@maple.ca', 60000 FROM dual;

INSERT INTO customers (company_name, contact_name, contact_title, address, city, postal_code, country_id, phone, email, credit_limit) 
SELECT 'Deutsche Handel GmbH', 'Hans Mueller', 'Geschäftsführer', 'Hauptstraße 100', 'Berlin', '10115', 
    (SELECT country_id FROM country_enum WHERE country_code = 'DEU'), '030-123-4567', 'hans@deutschehandel.de', 80000 FROM dual;

INSERT INTO customers (company_name, contact_name, contact_title, address, city, postal_code, country_id, phone, email, credit_limit) 
SELECT 'Tokyo Electronics', 'Yamada Taro', 'Manager', '1-1-1 Shibuya', 'Tokyo', '150-0002', 
    (SELECT country_id FROM country_enum WHERE country_code = 'JPN'), '03-1234-5678', 'yamada@tokyoelec.jp', 90000 FROM dual;

INSERT INTO customers (company_name, contact_name, contact_title, address, city, postal_code, country_id, phone, email, credit_limit) 
SELECT 'Sydney Supplies', 'Michael Clarke', 'Operations Manager', '100 Harbor St', 'Sydney', '2000', 
    (SELECT country_id FROM country_enum WHERE country_code = 'AUS'), '02-9123-4567', 'michael@sydsupplies.com.au', 55000 FROM dual;

INSERT INTO customers (company_name, contact_name, contact_title, address, city, postal_code, country_id, phone, email, credit_limit) 
SELECT 'Brazil Trading Co', 'Carlos Silva', 'Director', 'Av Paulista 1000', 'São Paulo', '01310-100', 
    (SELECT country_id FROM country_enum WHERE country_code = 'BRA'), '11-3123-4567', 'carlos@braziltrading.com.br', 45000 FROM dual;

INSERT INTO customers (company_name, contact_name, contact_title, address, city, postal_code, country_id, phone, email, credit_limit) 
SELECT 'India Tech Solutions', 'Raj Patel', 'CEO', 'MG Road 500', 'Mumbai', '400001', 
    (SELECT country_id FROM country_enum WHERE country_code = 'IND'), '22-2123-4567', 'raj@indiatech.in', 70000 FROM dual;

INSERT INTO customers (company_name, contact_name, contact_title, address, city, postal_code, country_id, phone, email, credit_limit) 
SELECT 'China Manufacturing', 'Li Wei', 'General Manager', '888 Beijing Rd', 'Shanghai', '200001', 
    (SELECT country_id FROM country_enum WHERE country_code = 'CHN'), '21-6123-4567', 'liwei@chinamfg.cn', 120000 FROM dual;

-- Generate more customers
DECLARE
    v_counter NUMBER := 11;
    v_country_id NUMBER;
BEGIN
    FOR i IN 1..40 LOOP
        SELECT country_id INTO v_country_id FROM 
            (SELECT country_id FROM country_enum ORDER BY DBMS_RANDOM.VALUE) 
            WHERE ROWNUM = 1;
        
        INSERT INTO customers (company_name, contact_name, address, city, postal_code, country_id, phone, email, credit_limit)
        VALUES (
            'Company ' || v_counter || ' Inc',
            'Contact ' || v_counter,
            v_counter || ' Business Ave',
            'City ' || v_counter,
            '10' || LPAD(v_counter, 3, '0'),
            v_country_id,
            '555-' || LPAD(v_counter * 100, 4, '0'),
            'contact' || v_counter || '@company' || v_counter || '.com',
            ROUND(DBMS_RANDOM.VALUE(10000, 100000), -3)
        );
        v_counter := v_counter + 1;
    END LOOP;
END;
/

-- Insert Products (100+ records)
DECLARE
    v_cat_electronics NUMBER;
    v_cat_computers NUMBER;
    v_cat_software NUMBER;
    v_cat_office NUMBER;
    v_cat_furniture NUMBER;
    v_cat_books NUMBER;
    v_cat_food NUMBER;
    v_cat_clothing NUMBER;
    v_cat_toys NUMBER;
    v_cat_sports NUMBER;
BEGIN
    SELECT category_id INTO v_cat_electronics FROM product_category_enum WHERE category_code = 'ELECTRONICS';
    SELECT category_id INTO v_cat_computers FROM product_category_enum WHERE category_code = 'COMPUTERS';
    SELECT category_id INTO v_cat_software FROM product_category_enum WHERE category_code = 'SOFTWARE';
    SELECT category_id INTO v_cat_office FROM product_category_enum WHERE category_code = 'OFFICE';
    SELECT category_id INTO v_cat_furniture FROM product_category_enum WHERE category_code = 'FURNITURE';
    SELECT category_id INTO v_cat_books FROM product_category_enum WHERE category_code = 'BOOKS';
    SELECT category_id INTO v_cat_food FROM product_category_enum WHERE category_code = 'FOOD';
    SELECT category_id INTO v_cat_clothing FROM product_category_enum WHERE category_code = 'CLOTHING';
    SELECT category_id INTO v_cat_toys FROM product_category_enum WHERE category_code = 'TOYS';
    SELECT category_id INTO v_cat_sports FROM product_category_enum WHERE category_code = 'SPORTS';
    
    -- Electronics
    INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level)
    VALUES ('4K Smart TV 55"', 'ELEC001', v_cat_electronics, 'Samsung', 899.99, 25, 10);
    INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level)
    VALUES ('Wireless Headphones', 'ELEC002', v_cat_electronics, 'Sony', 299.99, 50, 20);
    INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level)
    VALUES ('Smartphone Pro Max', 'ELEC003', v_cat_electronics, 'Apple', 1299.99, 15, 5);
    INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level)
    VALUES ('Bluetooth Speaker', 'ELEC004', v_cat_electronics, 'JBL', 149.99, 40, 15);
    INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level)
    VALUES ('Digital Camera', 'ELEC005', v_cat_electronics, 'Canon', 799.99, 8, 5);
    
    -- Computers
    INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level)
    VALUES ('Gaming Laptop', 'COMP001', v_cat_computers, 'ASUS', 1599.99, 12, 5);
    INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level)
    VALUES ('Desktop PC Pro', 'COMP002', v_cat_computers, 'Dell', 1299.99, 10, 5);
    INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level)
    VALUES ('Ultrabook 14"', 'COMP003', v_cat_computers, 'HP', 999.99, 20, 8);
    INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level)
    VALUES ('Gaming Mouse', 'COMP004', v_cat_computers, 'Logitech', 79.99, 60, 20);
    INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level)
    VALUES ('Mechanical Keyboard', 'COMP005', v_cat_computers, 'Corsair', 149.99, 35, 15);
    
    -- Software
    INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level)
    VALUES ('Office Suite Pro', 'SOFT001', v_cat_software, 'Microsoft', 299.99, 100, 20);
    INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level)
    VALUES ('Antivirus Premium', 'SOFT002', v_cat_software, 'Norton', 89.99, 150, 30);
    INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level)
    VALUES ('Photo Editor Pro', 'SOFT003', v_cat_software, 'Adobe', 199.99, 75, 15);
    INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level)
    VALUES ('Database Manager', 'SOFT004', v_cat_software, 'Oracle', 599.99, 25, 5);
    INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level)
    VALUES ('Development IDE', 'SOFT005', v_cat_software, 'JetBrains', 199.99, 50, 10);
    
    -- Office Supplies
    INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level)
    VALUES ('Printer Paper A4', 'OFFC001', v_cat_office, 'Staples', 29.99, 200, 50);
    INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level)
    VALUES ('Ballpoint Pens (50pk)', 'OFFC002', v_cat_office, 'BIC', 12.99, 300, 100);
    INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level)
    VALUES ('Stapler Heavy Duty', 'OFFC003', v_cat_office, 'Swingline', 24.99, 80, 20);
    INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level)
    VALUES ('File Folders (100pk)', 'OFFC004', v_cat_office, '3M', 19.99, 150, 40);
    INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level)
    VALUES ('Whiteboard Markers', 'OFFC005', v_cat_office, 'Expo', 15.99, 120, 30);
    
    -- Generate more products
    FOR i IN 21..100 LOOP
        INSERT INTO products (product_name, product_code, category_id, supplier_name, unit_price, units_in_stock, reorder_level)
        VALUES (
            'Product ' || i,
            'PROD' || LPAD(i, 3, '0'),
            CASE MOD(i, 10)
                WHEN 0 THEN v_cat_electronics
                WHEN 1 THEN v_cat_computers
                WHEN 2 THEN v_cat_software
                WHEN 3 THEN v_cat_office
                WHEN 4 THEN v_cat_furniture
                WHEN 5 THEN v_cat_books
                WHEN 6 THEN v_cat_food
                WHEN 7 THEN v_cat_clothing
                WHEN 8 THEN v_cat_toys
                ELSE v_cat_sports
            END,
            'Supplier ' || MOD(i, 5),
            ROUND(DBMS_RANDOM.VALUE(10, 500), 2),
            ROUND(DBMS_RANDOM.VALUE(0, 100)),
            ROUND(DBMS_RANDOM.VALUE(5, 20))
        );
    END LOOP;
END;
/

-- Insert Orders (500+ records with various statuses)
DECLARE
    v_customer_id NUMBER;
    v_status_id NUMBER;
    v_country_id NUMBER;
    v_order_id NUMBER;
    v_product_id NUMBER;
    v_order_date DATE;
BEGIN
    -- Generate orders for the past 6 months
    FOR i IN 1..500 LOOP
        -- Random customer
        SELECT customer_id, country_id INTO v_customer_id, v_country_id FROM 
            (SELECT c.customer_id, c.country_id FROM customers c ORDER BY DBMS_RANDOM.VALUE) 
            WHERE ROWNUM = 1;
        
        -- Random status (weighted distribution)
        SELECT status_id INTO v_status_id FROM (
            SELECT status_id FROM order_status_enum 
            WHERE status_code IN (
                CASE 
                    WHEN DBMS_RANDOM.VALUE < 0.4 THEN 'DELIVERED'
                    WHEN DBMS_RANDOM.VALUE < 0.6 THEN 'SHIPPED'
                    WHEN DBMS_RANDOM.VALUE < 0.7 THEN 'PROCESSING'
                    WHEN DBMS_RANDOM.VALUE < 0.8 THEN 'CONFIRMED'
                    WHEN DBMS_RANDOM.VALUE < 0.85 THEN 'PENDING'
                    WHEN DBMS_RANDOM.VALUE < 0.95 THEN 'CANCELLED'
                    ELSE 'RETURNED'
                END
            )
        ) WHERE ROWNUM = 1;
        
        -- Random order date within last 6 months
        v_order_date := SYSDATE - DBMS_RANDOM.VALUE(0, 180);
        
        -- Insert order
        INSERT INTO orders (
            order_number, customer_id, order_date, required_date, shipped_date, 
            status_id, shipping_country_id, freight_cost, payment_method
        ) VALUES (
            NULL, -- Will be auto-generated by trigger
            v_customer_id,
            v_order_date,
            v_order_date + DBMS_RANDOM.VALUE(3, 14),
            CASE 
                WHEN v_status_id IN (SELECT status_id FROM order_status_enum WHERE status_code IN ('SHIPPED', 'DELIVERED'))
                THEN v_order_date + DBMS_RANDOM.VALUE(1, 7)
                ELSE NULL
            END,
            v_status_id,
            v_country_id,
            ROUND(DBMS_RANDOM.VALUE(5, 50), 2),
            CASE MOD(i, 3)
                WHEN 0 THEN 'Credit Card'
                WHEN 1 THEN 'PayPal'
                ELSE 'Bank Transfer'
            END
        ) RETURNING order_id INTO v_order_id;
        
        -- Add order details (1-5 items per order)
        FOR j IN 1..ROUND(DBMS_RANDOM.VALUE(1, 5)) LOOP
            -- Random product
            SELECT product_id INTO v_product_id FROM 
                (SELECT product_id FROM products ORDER BY DBMS_RANDOM.VALUE) 
                WHERE ROWNUM = 1;
            
            -- Insert order detail (ignore duplicates)
            BEGIN
                INSERT INTO order_details (order_id, product_id, quantity, unit_price, discount)
                SELECT 
                    v_order_id,
                    v_product_id,
                    ROUND(DBMS_RANDOM.VALUE(1, 10)),
                    unit_price,
                    CASE 
                        WHEN DBMS_RANDOM.VALUE < 0.8 THEN 0
                        WHEN DBMS_RANDOM.VALUE < 0.95 THEN 0.1
                        ELSE 0.2
                    END
                FROM products WHERE product_id = v_product_id;
            EXCEPTION
                WHEN DUP_VAL_ON_INDEX THEN
                    NULL; -- Skip duplicate product in same order
            END;
        END LOOP;
    END LOOP;
END;
/

-- Update some products to be low in stock (for testing)
UPDATE products 
SET units_in_stock = ROUND(DBMS_RANDOM.VALUE(0, 5))
WHERE product_id IN (
    SELECT product_id FROM 
    (SELECT product_id FROM products ORDER BY DBMS_RANDOM.VALUE)
    WHERE ROWNUM <= 10
);

-- Mark some products as discontinued
UPDATE products 
SET discontinued = 1
WHERE product_id IN (
    SELECT product_id FROM 
    (SELECT product_id FROM products ORDER BY DBMS_RANDOM.VALUE)
    WHERE ROWNUM <= 5
);

-- Commit all changes
COMMIT;

-- Display summary statistics
SELECT 'Order Status Distribution' AS report FROM dual;
SELECT os.status_code, os.status_description, COUNT(o.order_id) AS order_count
FROM order_status_enum os
LEFT JOIN orders o ON os.status_id = o.status_id
GROUP BY os.status_code, os.status_description, os.status_order
ORDER BY os.status_order;

SELECT 'Country Distribution' AS report FROM dual;
SELECT cn.country_name, cn.region, COUNT(c.customer_id) AS customer_count
FROM country_enum cn
LEFT JOIN customers c ON cn.country_id = c.country_id
GROUP BY cn.country_name, cn.region
ORDER BY customer_count DESC;

SELECT 'Product Category Distribution' AS report FROM dual;
SELECT pc.category_name, COUNT(p.product_id) AS product_count,
       SUM(p.units_in_stock) AS total_stock,
       AVG(p.unit_price) AS avg_price
FROM product_category_enum pc
LEFT JOIN products p ON pc.category_id = p.category_id
GROUP BY pc.category_name
ORDER BY product_count DESC;

SELECT 'Database Summary' AS report FROM dual;
SELECT 
    (SELECT COUNT(*) FROM customers) AS total_customers,
    (SELECT COUNT(*) FROM products) AS total_products,
    (SELECT COUNT(*) FROM orders) AS total_orders,
    (SELECT COUNT(*) FROM order_details) AS total_order_lines,
    (SELECT SUM(total_amount) FROM orders) AS total_revenue
FROM dual;
-- Insert data into dim_product
INSERT INTO products.dim_product (product_id, product_name, category, brand, unit_price,last_updt_col) VALUES
('SKU021', 'Smartwatch X2', 'Electronics', 'TechCo', 19.99, '2025-11-20 10:30:00'), -- PK=1
('SKU022', 'Organic Coffee Beans2', 'Food', 'JavaHaus', 12.50, '2025-11-20 10:30:00'),  -- PK=2
('SKU023', 'T-Shirt, Blue2', 'Apparel', 'StyleFit', 25.00, '2025-11-20 10:30:00');    -- PK=3

-- Insert data into dim_customer
INSERT INTO products.dim_customer (customer_id, full_name, city, is_loyalty_member,last_updt_col) VALUES
('CUST021', 'jack Johnson', 'Springfield', TRUE, '2025-11-20 10:30:00'),  -- PK=1
('CUST022', 'Bob Wire', 'Shelbyville', FALSE, '2025-11-20 10:30:00'), -- PK=2
('CUST023', 'Charlie White', 'Springfield', TRUE, '2025-11-20 10:30:00');  -- PK=3


UPDATE products.dim_product
SET unit_price = 9.99 , last_updt_col = '2025-11-20 10:30:00'
where product_id ='"SKU004"'

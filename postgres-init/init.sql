-- Create a new schema
CREATE SCHEMA IF NOT EXISTS products;

-- Create a sample table for the claims model
-- DDL for dim_product
CREATE TABLE dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(20) NOT NULL UNIQUE,
    product_name VARCHAR(100) NOT NULL,
    category VARCHAR(50) NOT NULL,
    brand VARCHAR(50),
    unit_price NUMERIC(10, 2) NOT NULL
);

-- DDL for dim_customer
CREATE TABLE dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL UNIQUE,
    full_name VARCHAR(100),
    city VARCHAR(50),
    is_loyalty_member BOOLEAN NOT NULL
);

-- DDL for dim_store
CREATE TABLE dim_store (
    store_key SERIAL PRIMARY KEY,
    store_id VARCHAR(10) NOT NULL UNIQUE,
    store_name VARCHAR(100) NOT NULL,
    region VARCHAR(50),
    store_manager VARCHAR(100)
);
-- Insert data into dim_product
INSERT INTO dim_product (product_id, product_name, category, brand, unit_price) VALUES
('SKU001', 'Smartwatch X', 'Electronics', 'TechCo', 199.99), -- PK=1
('SKU002', 'Organic Coffee Beans', 'Food', 'JavaHaus', 12.50),  -- PK=2
('SKU003', 'T-Shirt, Blue', 'Apparel', 'StyleFit', 25.00);    -- PK=3

-- Insert data into dim_customer
INSERT INTO dim_customer (customer_id, full_name, city, is_loyalty_member) VALUES
('CUST001', 'Alice Johnson', 'Springfield', TRUE),  -- PK=1
('CUST002', 'Bob Williams', 'Shelbyville', FALSE), -- PK=2
('CUST003', 'Charlie Brown', 'Springfield', TRUE);  -- PK=3

-- Insert data into dim_store
INSERT INTO dim_store (store_id, store_name, region, store_manager) VALUES
('S100', 'Downtown Plaza', 'North', 'Maria Lee'),  -- PK=1
('S101', 'West Side Mall', 'South', 'Tom Hardy'),    -- PK=2
('S102', 'East End Outlet', 'North', 'Jane Doe');   -- PK=3

-- DDL for fact_sales
CREATE TABLE fact_sales (
    sales_key BIGSERIAL PRIMARY KEY,
    sale_datetime TIMESTAMP NOT NULL,
    quantity_sold INTEGER NOT NULL,
    sale_amount_usd NUMERIC(10, 2) NOT NULL,
    cost_amount_usd NUMERIC(10, 2) NOT NULL,

    -- Foreign Key Constraints
    product_key INTEGER NOT NULL REFERENCES dim_product(product_key),
    customer_key INTEGER NOT NULL REFERENCES dim_customer(customer_key),
    store_key INTEGER NOT NULL REFERENCES dim_store(store_key)
);

-- Insert data into fact_sales
INSERT INTO fact_sales (
    sale_datetime, quantity_sold, sale_amount_usd, cost_amount_usd,
    product_key, customer_key, store_key
) VALUES
-- Sale 1: Alice (1) buys Smartwatch (1) at Downtown (1)
('2025-11-14 10:30:00', 1, 199.99, 120.00, 1, 1, 1),
-- Sale 2: Bob (2) buys Coffee (2) at West Side (2)
('2025-11-14 11:45:00', 2, 25.00, 15.00, 2, 2, 2),
-- Sale 3: Charlie (3) buys T-Shirt (3) at East End (3)
('2025-11-14 14:00:00', 3, 75.00, 40.00, 3, 3, 3),
-- Sale 4: Alice (1) buys Coffee (2) at Downtown (1)
('2025-11-14 16:15:00', 4, 50.00, 30.00, 2, 1, 1),
-- Sale 5: Bob (2) buys T-Shirt (3) at West Side (2)
('2025-11-15 09:00:00', 1, 25.00, 13.00, 3, 2, 2),
-- Sale 6: Charlie (3) buys Smartwatch (1) at East End (3)
('2025-11-15 12:30:00', 1, 199.99, 120.00, 1, 3, 3),
-- Sale 7: Alice (1) buys another T-Shirt (3) at Downtown (1)
('2025-11-15 15:45:00', 2, 50.00, 26.00, 3, 1, 1);


-- Create a new schema
CREATE SCHEMA IF NOT EXISTS products;

-- Create a sample table for the claims model
-- DDL for dim_product
CREATE TABLE products.dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(20) NOT NULL UNIQUE,
    product_name VARCHAR(100) NOT NULL,
    category VARCHAR(50) NOT NULL,
    brand VARCHAR(50),
    unit_price NUMERIC(10, 2) NOT NULL,
    last_updt_col TIMESTAMP NOT NULL
);

CREATE TABLE products.dim_product_audit (
    -- Columns from the original table
    product_key INT,
    product_id VARCHAR(20),
    product_name VARCHAR(100),
    category VARCHAR(50),
    brand VARCHAR(50),
    unit_price NUMERIC(10, 2),
    last_updt_col TIMESTAMP,

    -- Audit Metadata Columns
    audit_id BIGSERIAL PRIMARY KEY,      -- Unique ID for the audit record
    operation_type CHAR(1) NOT NULL,     -- 'I' (Insert), 'U' (Update), 'D' (Delete)
    operation_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- When the change happened
    operation_user TEXT DEFAULT current_user, -- Who made the change (PostgreSQL user)
    old_data JSONB,                      -- Stores the full row before an UPDATE/DELETE (optional but useful)
    new_data JSONB                       -- Stores the full row after an INSERT/UPDATE (optional but useful)
);

-- Recommended: Index the foreign key and operation type for fast lookups
CREATE INDEX idx_product_key_audit ON products.dim_product_audit (product_key);
CREATE INDEX idx_product_operation_type ON products.dim_product_audit (operation_type);

-- DDL for dim_customer
CREATE TABLE products.dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL UNIQUE,
    full_name VARCHAR(100),
    city VARCHAR(50),
    is_loyalty_member BOOLEAN NOT NULL,
    last_updt_col TIMESTAMP NOT NULL
);

CREATE TABLE products.dim_customer_audit (

    customer_key INT,
    customer_id VARCHAR(20) ,
    full_name VARCHAR(100),
    city VARCHAR(50),
    is_loyalty_member BOOLEAN ,
    last_updt_col TIMESTAMP ,

    -- Audit Metadata Columns
    audit_id BIGSERIAL PRIMARY KEY,      -- Unique ID for the audit record
    operation_type CHAR(1) NOT NULL,     -- 'I' (Insert), 'U' (Update), 'D' (Delete)
    operation_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- When the change happened
    operation_user TEXT DEFAULT current_user, -- Who made the change (PostgreSQL user)

    -- Full Row Data Capture
    -- JSONB stores the state of the row before and after the change
    old_data JSONB,
    new_data JSONB

);

-- Index the key for faster lookups when auditing a specific customer's history
CREATE INDEX idx_customer_key_audit ON products.dim_customer_audit (customer_key);
CREATE INDEX idx_customer_operation_type ON products.dim_customer_audit (operation_type);

-- DDL for dim_store
CREATE TABLE products.dim_store (
    store_key SERIAL PRIMARY KEY,
    store_id VARCHAR(10) NOT NULL UNIQUE,
    store_name VARCHAR(100) NOT NULL,
    region VARCHAR(50),
    store_manager VARCHAR(100),
    last_updt_col TIMESTAMP NOT NULL
);

-- DDL for fact_sales
CREATE TABLE products.fact_sales (
    sales_key SERIAL PRIMARY KEY,
    sale_datetime TIMESTAMP NOT NULL,
    quantity_sold INTEGER NOT NULL,
    sale_amount_usd NUMERIC(10, 2) NOT NULL,
    cost_amount_usd NUMERIC(10, 2) NOT NULL,

    -- Foreign Key Constraints
    product_key INTEGER NOT NULL REFERENCES products.dim_product(product_key),
    customer_key INTEGER NOT NULL REFERENCES products.dim_customer(customer_key),
    store_key INTEGER NOT NULL REFERENCES products.dim_store(store_key)
);

-- DDL for tracking table
CREATE TABLE products.version_tracking_ivt (
    delta_table_name VARCHAR(50) PRIMARY KEY,
    last_version_run INT NOT NULL,
    skip_versions VARCHAR(100),
    last_run_timestamp TIMESTAMP NOT NULL
);


CREATE OR REPLACE FUNCTION products.log_product_changes()
RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'INSERT') THEN
        INSERT INTO products.dim_product_audit (
            product_key, product_id, product_name, category, brand, unit_price, last_updt_col,
            operation_type, new_data
        ) VALUES (
            NEW.product_key, NEW.product_id, NEW.product_name, NEW.category, NEW.brand, NEW.unit_price, NEW.last_updt_col,
            'I', to_jsonb(NEW)
        );
        RETURN NEW;

    ELSIF (TG_OP = 'UPDATE') THEN
        INSERT INTO products.dim_product_audit (
            product_key, product_id, product_name, category, brand, unit_price, last_updt_col,
            operation_type, old_data, new_data
        ) VALUES (
            NEW.product_key, NEW.product_id, NEW.product_name, NEW.category, NEW.brand, NEW.unit_price, NEW.last_updt_col,
            'U', to_jsonb(OLD), to_jsonb(NEW)
        );
        RETURN NEW;

    ELSIF (TG_OP = 'DELETE') THEN
        INSERT INTO products.dim_product_audit (
            product_key, product_id, product_name, category, brand, unit_price, last_updt_col,
            operation_type, old_data
        ) VALUES (
            OLD.product_key, OLD.product_id, OLD.product_name, OLD.category, OLD.brand, OLD.unit_price, OLD.last_updt_col,
            'D', to_jsonb(OLD)
        );
        RETURN OLD;
    END IF;
    RETURN NULL; -- Should never happen
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION products.log_customer_changes()
RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'INSERT') THEN
        INSERT INTO products.dim_customer_audit (
            customer_key,
            customer_id ,
            full_name ,
            city ,
            is_loyalty_member ,
            last_updt_col,
            operation_type,
            new_data
        ) VALUES (
            NEW.customer_key,
            NEW.customer_id ,
            NEW.full_name ,
            NEW.city ,
            NEW.is_loyalty_member ,
            NEW.last_updt_col,
            'I',
            to_jsonb(NEW) -- Capture the inserted row
        );
        RETURN NEW;

    ELSIF (TG_OP = 'UPDATE') THEN
        INSERT INTO products.dim_customer_audit (
            customer_key,
            customer_id ,
            full_name ,
            city ,
            is_loyalty_member ,
            last_updt_col,
            operation_type,
            old_data,
            new_data
        ) VALUES (
            NEW.customer_key,
            NEW.customer_id ,
            NEW.full_name ,
            NEW.city ,
            NEW.is_loyalty_member ,
            NEW.last_updt_col,
            'U',
            to_jsonb(OLD), -- Capture the original row state
            to_jsonb(NEW)  -- Capture the new row state
        );
        RETURN NEW;

    ELSIF (TG_OP = 'DELETE') THEN
        INSERT INTO products.dim_customer_audit (
            customer_key,
            customer_id ,
            full_name ,
            city ,
            is_loyalty_member ,
            last_updt_col,
            operation_type,
            old_data
        ) VALUES (
            OLD.customer_key,
            OLD.customer_id ,
            OLD.full_name ,
            OLD.city ,
            OLD.is_loyalty_member ,
            OLD.last_updt_col,
            'D',
            to_jsonb(OLD) -- Capture the deleted row state
        );
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER product_audit_trigger
AFTER INSERT OR UPDATE OR DELETE ON products.dim_product
FOR EACH ROW
EXECUTE FUNCTION products.log_product_changes();

CREATE TRIGGER customer_audit_trigger
AFTER INSERT OR UPDATE OR DELETE ON products.dim_customer
FOR EACH ROW
EXECUTE FUNCTION products.log_customer_changes();

-- Insert data into dim_product
INSERT INTO products.dim_product (product_id, product_name, category, brand, unit_price,last_updt_col) VALUES
('SKU001', 'Smartwatch X', 'Electronics', 'TechCo', 199.99, '2025-11-14 10:30:00'), -- PK=1
('SKU002', 'Organic Coffee Beans', 'Food', 'JavaHaus', 12.50, '2025-11-14 10:30:00'),  -- PK=2
('SKU003', 'T-Shirt, Blue', 'Apparel', 'StyleFit', 25.00, '2025-11-14 10:30:00');    -- PK=3

-- Insert data into dim_customer
INSERT INTO products.dim_customer (customer_id, full_name, city, is_loyalty_member,last_updt_col) VALUES
('CUST001', 'Alice Johnson', 'Springfield', TRUE, '2025-11-14 10:30:00'),  -- PK=1
('CUST002', 'Bob Williams', 'Shelbyville', FALSE, '2025-11-14 10:30:00'), -- PK=2
('CUST003', 'Charlie Brown', 'Springfield', TRUE, '2025-11-14 10:30:00');  -- PK=3

-- Insert data into dim_store
INSERT INTO products.dim_store (store_id, store_name, region, store_manager,last_updt_col) VALUES
('S100', 'Downtown Plaza', 'North', 'Maria Lee', '2025-11-14 10:30:00'),  -- PK=1
('S101', 'West Side Mall', 'South', 'Tom Hardy', '2025-11-14 10:30:00'),    -- PK=2
('S102', 'East End Outlet', 'North', 'Jane Doe', '2025-11-14 10:30:00');   -- PK=3



-- Insert data into fact_sales
INSERT INTO products.fact_sales (
    sale_datetime, quantity_sold, sale_amount_usd, cost_amount_usd,
    product_key, customer_key, store_key
) VALUES
('2025-11-14 10:30:00', 1, 199.99, 120.00, 1, 1, 1),
('2025-11-14 11:45:00', 2, 25.00, 15.00, 2, 2, 2),
('2025-11-14 14:00:00', 3, 75.00, 40.00, 3, 3, 3),
('2025-11-14 16:15:00', 4, 50.00, 30.00, 2, 1, 1),
('2025-11-15 09:00:00', 1, 25.00, 13.00, 3, 2, 2),
('2025-11-15 12:30:00', 1, 199.99, 120.00, 1, 3, 3),
('2025-11-15 15:45:00', 2, 50.00, 26.00, 3, 1, 1);





-- Insert data into tracking table
INSERT INTO products.version_tracking_ivt (
    delta_table_name, last_version_run, skip_versions, last_run_timestamp
) VALUES
('dim_product', 1, ' ' , '2025-11-15 15:45:00'),
('dim_customer', 1, ' ' , '2025-11-15 15:45:00'),
('dim_store', 1,' ', '2025-11-15 15:45:00'),
('fact_sales', 1, ' ', '2025-11-15 15:45:00');

-- Bronze Layer Tables
-- Raw data ingestion tables

-- Create bronze sales transactions table
CREATE TABLE IF NOT EXISTS bronze.sales_transactions (
    transaction_id STRING,
    transaction_date TIMESTAMP,
    customer_id STRING,
    product_id STRING,
    quantity INT,
    unit_price DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    source_system STRING,
    ingestion_timestamp TIMESTAMP
) USING DELTA
PARTITIONED BY (DATE(transaction_date));

-- Create bronze customer data table
CREATE TABLE IF NOT EXISTS bronze.customers (
    customer_id STRING,
    customer_name STRING,
    email STRING,
    phone STRING,
    address STRING,
    city STRING,
    state STRING,
    country STRING,
    source_system STRING,
    ingestion_timestamp TIMESTAMP
) USING DELTA;

-- Silver Layer Tables
-- Cleaned and transformed data

-- Create silver sales table
CREATE TABLE IF NOT EXISTS silver.sales (
    transaction_id STRING,
    transaction_date DATE,
    transaction_time TIMESTAMP,
    customer_id STRING,
    product_id STRING,
    quantity INT,
    unit_price DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    is_valid BOOLEAN,
    processed_timestamp TIMESTAMP
) USING DELTA
PARTITIONED BY (transaction_date);

-- Create silver customers table
CREATE TABLE IF NOT EXISTS silver.customers (
    customer_id STRING,
    customer_name STRING,
    email STRING,
    phone STRING,
    full_address STRING,
    city STRING,
    state STRING,
    country STRING,
    is_active BOOLEAN,
    processed_timestamp TIMESTAMP
) USING DELTA;

-- Gold Layer Tables
-- Aggregated and analytics-ready data

-- Create gold daily sales summary
CREATE TABLE IF NOT EXISTS gold.daily_sales_summary (
    sale_date DATE,
    total_transactions INT,
    total_revenue DECIMAL(15,2),
    unique_customers INT,
    avg_transaction_value DECIMAL(10,2),
    created_timestamp TIMESTAMP
) USING DELTA
PARTITIONED BY (YEAR(sale_date), MONTH(sale_date));

-- Create gold customer metrics
CREATE TABLE IF NOT EXISTS gold.customer_metrics (
    customer_id STRING,
    total_transactions INT,
    total_spent DECIMAL(15,2),
    avg_transaction_value DECIMAL(10,2),
    first_purchase_date DATE,
    last_purchase_date DATE,
    customer_lifetime_days INT,
    is_vip BOOLEAN,
    created_timestamp TIMESTAMP
) USING DELTA;

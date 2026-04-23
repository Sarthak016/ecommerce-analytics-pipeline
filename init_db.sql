-- Create separate schema for our ecommerce data
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Staging tables (PySpark loads cleaned data here)
CREATE TABLE IF NOT EXISTS staging.orders (
    order_id VARCHAR,
    customer_id VARCHAR,
    product_id VARCHAR,
    order_date DATE,
    quantity INTEGER,
    unit_price NUMERIC(10,2),
    status VARCHAR,
    total_amount NUMERIC(10,2),
    ingested_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.customers (
    customer_id VARCHAR,
    name VARCHAR,
    email VARCHAR,
    city VARCHAR,
    state VARCHAR,
    signup_date DATE,
    segment VARCHAR,
    ingested_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.products (
    product_id VARCHAR,
    product_name VARCHAR,
    category VARCHAR,
    brand VARCHAR,
    cost_price NUMERIC(10,2),
    selling_price NUMERIC(10,2),
    stock_quantity INTEGER,
    ingested_at TIMESTAMP DEFAULT NOW()
);

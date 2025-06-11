/*
=========
Gold Layer Schema Definition
=========
Description:
This script creates the analytics-ready tables in the gold schema.
It includes both fact and dimension tables optimized for analytical queries.
*/

USE DataWarehouse;
GO

-- Dimension Tables
CREATE TABLE gold.dim_customer (
    customer_key INT IDENTITY(1,1) PRIMARY KEY,
    customer_id VARCHAR(50),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    marital_status VARCHAR(20),
    gender VARCHAR(20),
    create_date DATE,
    last_modified_date DATETIME DEFAULT GETDATE()
);

CREATE TABLE gold.dim_product (
    product_key INT IDENTITY(1,1) PRIMARY KEY,
    product_id VARCHAR(50),
    product_name VARCHAR(200),
    category VARCHAR(100),
    sub_category VARCHAR(100),
    unit_price DECIMAL(10,2),
    last_modified_date DATETIME DEFAULT GETDATE()
);

CREATE TABLE gold.dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE,
    year INT,
    quarter INT,
    month INT,
    month_name VARCHAR(20),
    day INT,
    day_of_week INT,
    day_name VARCHAR(20),
    is_weekend BIT
);

-- Fact Tables
CREATE TABLE gold.fact_sales (
    sale_key INT IDENTITY(1,1) PRIMARY KEY,
    customer_key INT FOREIGN KEY REFERENCES gold.dim_customer(customer_key),
    product_key INT FOREIGN KEY REFERENCES gold.dim_product(product_key),
    date_key INT FOREIGN KEY REFERENCES gold.dim_date(date_key),
    quantity INT,
    unit_price DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    created_date DATETIME DEFAULT GETDATE()
);

-- Create indexes for better query performance
CREATE NONCLUSTERED INDEX idx_fact_sales_customer 
ON gold.fact_sales(customer_key);

CREATE NONCLUSTERED INDEX idx_fact_sales_product 
ON gold.fact_sales(product_key);

CREATE NONCLUSTERED INDEX idx_fact_sales_date 
ON gold.fact_sales(date_key);

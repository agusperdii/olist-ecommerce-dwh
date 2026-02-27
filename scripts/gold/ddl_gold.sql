/*
============================================================
Gold Layer Table Initialization
============================================================
Script Purpose:
    - Create Star Schema for Analytics (Tableau/PowerBI).
    - Flatten normalized Silver tables into Dimensions and Facts.
    - Optimize data structure for high-performance reporting.

WARNING:
    This script contains DROP TABLE statements which will 
    PERMANENTLY DELETE all existing records and metadata 
    within the target tables.

    USE WITH EXTREME CAUTION. Ensure you have a valid backup 
    before execution. Do not run this script in a Production 
    environment unless a full data reset is intended.
============================================================
*/

-- 1. dim_customers
IF OBJECT_ID('gold.dim_customers', 'U') IS NOT NULL 
    DROP TABLE gold.dim_customers;
CREATE TABLE gold.dim_customers (
    customer_id             CHAR(32) PRIMARY KEY,
    customer_unique_id      CHAR(32),
    customer_city           NVARCHAR(50),
    customer_state          CHAR(2)
);

-- 2. dim_sellers
IF OBJECT_ID('gold.dim_sellers', 'U') IS NOT NULL 
    DROP TABLE gold.dim_sellers;
CREATE TABLE gold.dim_sellers (
    seller_id               CHAR(32) PRIMARY KEY,
    seller_city             NVARCHAR(50),
    seller_state            CHAR(2)
);

-- 3. dim_products
IF OBJECT_ID('gold.dim_products', 'U') IS NOT NULL 
    DROP TABLE gold.dim_products;
CREATE TABLE gold.dim_products (
    product_id                    CHAR(32) PRIMARY KEY,
    product_category_name_english NVARCHAR(60),
    product_photos_qty            TINYINT,
    product_weight_g              INT,
    product_length_cm             TINYINT,
    product_height_cm             TINYINT,
    product_width_cm              TINYINT
);

-- 4. fact_sales
IF OBJECT_ID('gold.fact_sales', 'U') IS NOT NULL 
    DROP TABLE gold.fact_sales;
CREATE TABLE gold.fact_sales (
    -- Keys
    order_id                      CHAR(32), 
    order_item_id                 INT, 
    product_id                    CHAR(32), 
    customer_id                   CHAR(32), 
    seller_id                     CHAR(32), 
    
    -- Dates
    order_purchase_date           DATETIME,
    order_approved_date           DATETIME,
    order_delivered_customer_date DATETIME,
    order_estimated_delivery_date DATETIME,
    
    -- Measures
    price                         DECIMAL(10, 2),
    freight_value                 DECIMAL(10, 2),
    total_order_value             DECIMAL(10, 2),
    
    -- Attributes
    order_status                  VARCHAR(15),
    delivery_days                 INT,
    delivery_status               VARCHAR(20),
    review_score                  TINYINT
);

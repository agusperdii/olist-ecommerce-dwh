/*
============================================================
Silver Layer Table Initialization
============================================================
Script Purpose:
    - Drop and Recreate tables within the 'silver' schema.
    - Define initial structures for Olist E-commerce data.
    - Serve as the landing zone for raw data ingestion.

WARNING:
    This script contains DROP TABLE statements which will 
    PERMANENTLY DELETE all existing records and metadata 
    within the target tables.

    USE WITH EXTREME CAUTION. Ensure you have a valid backup 
    before execution. Do not run this script in a Production 
    environment unless a full data reset is intended.
============================================================
*/

IF OBJECT_ID('silver.olist_customers', 'U') IS NOT NULL
    DROP TABLE silver.olist_customers;
CREATE TABLE silver.olist_customers (
    customer_id               CHAR(32),
    customer_unique_id        CHAR(32),
    customer_zip_code_prefix  CHAR(5),
    customer_city             NVARCHAR(32),
    customer_state            CHAR(2)
);

IF OBJECT_ID('silver.olist_order_items', 'U') IS NOT NULL
    DROP TABLE silver.olist_order_items;
CREATE TABLE silver.olist_order_items (
    order_id            CHAR(32),
    order_item_id       INT,
    product_id          CHAR(32),
    seller_id           CHAR(32),
    shipping_limit_date DATETIME,
    price               DECIMAL(10, 2),
    freight_value       DECIMAL(10, 2)
);

IF OBJECT_ID('silver.olist_order_payments', 'U') IS NOT NULL
    DROP TABLE silver.olist_order_payments;
CREATE TABLE silver.olist_order_payments (
    order_id             CHAR(32),
    payment_sequential   TINYINT,
    payment_type         VARCHAR(15),
    payment_installments TINYINT,
    payment_value        DECIMAL(15,2)
);

IF OBJECT_ID('silver.olist_order_reviews', 'U') IS NOT NULL
    DROP TABLE silver.olist_order_reviews;
CREATE TABLE silver.olist_order_reviews (
    review_id               CHAR(32),
    order_id                CHAR(32),
    review_score            TINYINT,
    review_comment_title    NVARCHAR(50),
    review_comment_message  NVARCHAR(300),
    review_creation_date    DATETIME,
    review_answer_timestamp DATETIME
);

IF OBJECT_ID('silver.olist_orders', 'U') IS NOT NULL
    DROP TABLE silver.olist_orders;
CREATE TABLE silver.olist_orders (
    order_id                        CHAR(32),
    customer_id                     CHAR(32),
    order_status                    VARCHAR(15),
    order_purchase_timestamp        DATETIME,
    order_approved_at               DATETIME,
    order_delivered_carrier_date    DATETIME,
    order_delivered_customer_date   DATETIME,
    order_estimated_delivery_date   DATETIME
);

IF OBJECT_ID('silver.olist_products', 'U') IS NOT NULL
    DROP TABLE silver.olist_products;
CREATE TABLE silver.olist_products (
    product_id                    CHAR(32) PRIMARY KEY,
    product_category_name         NVARCHAR(60),
    product_category_name_english NVARCHAR(60),
    product_name_length           TINYINT,
    product_description_length    SMALLINT,
    product_photos_qty            TINYINT,
    product_weight_g              INT,
    product_length_cm             TINYINT,
    product_height_cm             TINYINT,
    product_width_cm              TINYINT
);

IF OBJECT_ID('silver.olist_sellers', 'U') IS NOT NULL
    DROP TABLE silver.olist_sellers;
CREATE TABLE silver.olist_sellers (
    seller_id               CHAR(32),
    seller_zip_code_prefix  CHAR(5),
    seller_city             NVARCHAR(50),
    seller_state            CHAR(2)
);

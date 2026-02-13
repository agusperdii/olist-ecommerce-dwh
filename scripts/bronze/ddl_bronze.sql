/*
============================================================
Bronze Layer Table Initialization
============================================================
Script Purpose:
    - Drop and Recreate tables within the 'bronze' schema.
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

IF OBJECT_ID('bronze.olist_customers', 'U') IS NOT NULL
    DROP TABLE bronze.olist_customers;
CREATE TABLE bronze.olist_customers (
    customer_id               CHAR(32),
    customer_unique_id        CHAR(32),
    customer_zip_code_prefix  CHAR(5),
    customer_city             NVARCHAR(100),
    customer_state            CHAR(2)
);

IF OBJECT_ID('bronze.olist_geolocation', 'U') IS NOT NULL
    DROP TABLE bronze.olist_geolocation;
CREATE TABLE bronze.olist_geolocation (
    geolocation_zip_code_prefix CHAR(5),
    geolocation_lat             VARCHAR(30),
    geolocation_lng             VARCHAR(30),
    geolocation_city            NVARCHAR(100),
    geolocation_state           CHAR(2)
);

IF OBJECT_ID('bronze.olist_order_items', 'U') IS NOT NULL
    DROP TABLE bronze.olist_order_items;
CREATE TABLE bronze.olist_order_items (
    order_id            CHAR(32),
    order_item_id       INT,
    product_id          CHAR(32),
    seller_id           CHAR(32),
    shipping_limit_date VARCHAR(25),
    price               VARCHAR(20),
    freight_value       VARCHAR(20)
);

IF OBJECT_ID('bronze.olist_order_payments', 'U') IS NOT NULL
    DROP TABLE bronze.olist_order_payments;
CREATE TABLE bronze.olist_order_payments (
    order_id             CHAR(32),
    payment_sequential   INT,
    payment_type         NVARCHAR(30),
    payment_installments INT,
    payment_value        VARCHAR(20)
);

IF OBJECT_ID('bronze.olist_order_reviews', 'U') IS NOT NULL
    DROP TABLE bronze.olist_order_reviews;
CREATE TABLE bronze.olist_order_reviews (
    review_id               CHAR(32),
    order_id                CHAR(32),
    review_score            INT,
    review_comment_title    NVARCHAR(200),
    review_comment_message  NVARCHAR(1000),
    review_creation_date    DATETIME,
    review_answer_timestamp DATETIME
);

IF OBJECT_ID('bronze.olist_orders', 'U') IS NOT NULL
    DROP TABLE bronze.olist_orders;
CREATE TABLE bronze.olist_orders (
    order_id                        CHAR(32),
    customer_id                     CHAR(32),
    order_status                    NVARCHAR(20),
    order_purchase_timestamp        VARCHAR(25),
    order_approved_at               VARCHAR(25),
    order_delivered_carrier_date    VARCHAR(25),
    order_delivered_customer_date   VARCHAR(25),
    order_estimated_delivery_date   VARCHAR(25)
);

IF OBJECT_ID('bronze.olist_products', 'U') IS NOT NULL
    DROP TABLE bronze.olist_products;
CREATE TABLE bronze.olist_products (
    product_id                   CHAR(32),
    product_category_name        NVARCHAR(100),
    product_name_lenght          INT,
    product_description_lenght   INT,
    product_photos_qty           INT,
    product_weight_g             INT,
    product_length_cm            INT,
    product_height_cm            INT,
    product_width_cm             INT
);

IF OBJECT_ID('bronze.olist_sellers', 'U') IS NOT NULL
    DROP TABLE bronze.olist_sellers;
CREATE TABLE bronze.olist_sellers (
    seller_id               CHAR(32),
    seller_zip_code_prefix  CHAR(5),
    seller_city             NVARCHAR(100),
    seller_state            CHAR(2)
);

IF OBJECT_ID('bronze.product_category_name_translation', 'U') IS NOT NULL
    DROP TABLE bronze.product_category_name_translation;
CREATE TABLE bronze.product_category_name_translation (
    product_category_name           NVARCHAR(100),
    product_category_name_english   NVARCHAR(100)
);

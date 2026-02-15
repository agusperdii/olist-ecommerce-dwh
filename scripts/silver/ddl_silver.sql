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

/*
============================================================
Stored Procedure: gold.load_gold
============================================================
Script Purpose:
    - Truncates and reloads all tables in the 'gold' schema.
    - Populates the Star Schema (Dimensions & Fact tables).
    - Performs final transformations and business logic 
      from the 'silver' layer to the 'gold' layer.
    - Captures and logs load duration and error details.

Usage Example:
    EXEC gold.load_gold;

WARNING:
    This procedure uses TRUNCATE TABLE, which will 
    IMMEDIATELY and PERMANENTLY DELETE all existing data 
    in the gold tables before re-inserting records.
============================================================
*/

CREATE OR ALTER PROCEDURE gold.load_gold AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '==========================================================';
        PRINT 'LOADING GOLD LAYER (STAR SCHEMA)';
        PRINT 'Start Time: ' + CONVERT(VARCHAR, @batch_start_time, 120);
        PRINT '==========================================================';

        -- 1. Dim Customers
        SET @start_time = GETDATE();
        PRINT '----------------------------------------------------------';
        PRINT 'Table: [gold].[dim_customers]';
        PRINT '  >> Truncating table...';
        TRUNCATE TABLE gold.dim_customers;
        PRINT '  >> Inserting data from silver...';
        INSERT INTO gold.dim_customers (
            customer_id,
            customer_unique_id,
            customer_city,
            customer_state
        )
        SELECT 
            customer_id,
            customer_unique_id,
            customer_city,
            customer_state
        FROM silver.olist_customers;
        SET @end_time = GETDATE();
        PRINT '[Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds]';

        -- 2. Dim Sellers
        SET @start_time = GETDATE();
        PRINT '----------------------------------------------------------';
        PRINT 'Table: [gold].[dim_sellers]';
        PRINT '  >> Truncating table...';
        TRUNCATE TABLE gold.dim_sellers;
        PRINT '  >> Inserting data from silver...';
        INSERT INTO gold.dim_sellers (
            seller_id,
            seller_city,
            seller_state
        )
        SELECT 
            seller_id,
            seller_city,
            seller_state
        FROM silver.olist_sellers;
        SET @end_time = GETDATE();
        PRINT '[Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds]';

        -- 3. Dim Products
        SET @start_time = GETDATE();
        PRINT '----------------------------------------------------------';
        PRINT 'Table: [gold].[dim_products]';
        PRINT '  >> Truncating table...';
        TRUNCATE TABLE gold.dim_products;
        PRINT '  >> Inserting data from silver (with NULL handling)...';
        INSERT INTO gold.dim_products (
            product_id,
            product_category_name_english,
            product_photos_qty,
            product_weight_g,
            product_length_cm,
            product_height_cm,
            product_width_cm
        )
        SELECT 
            product_id,
            ISNULL(product_category_name_english, 'Other Categories') AS product_category_name_english,
            product_photos_qty,
            product_weight_g,
            product_length_cm,
            product_height_cm,
            product_width_cm
        FROM silver.olist_products;
        SET @end_time = GETDATE();
        PRINT '[Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds]';

        -- 4. Fact Sales
        SET @start_time = GETDATE();
        PRINT '----------------------------------------------------------';
        PRINT 'Table: [gold].[fact_sales]';
        PRINT '  >> Truncating table...';
        TRUNCATE TABLE gold.fact_sales;
        PRINT '  >> Inserting data (Fact calculations & joins)...';
        INSERT INTO gold.fact_sales (
            order_id,
            order_item_id,
            product_id,
            customer_id,
            seller_id,
            order_purchase_date,
            order_approved_date,
            order_delivered_customer_date,
            order_estimated_delivery_date,
            price,
            freight_value,
            total_order_value,
            order_status,
            delivery_days,
            delivery_status,
            review_score
        )
        SELECT 
            o.order_id,
            oi.order_item_id,
            oi.product_id,
            o.customer_id,
            oi.seller_id,
            o.order_purchase_timestamp,
            o.order_approved_at,
            o.order_delivered_customer_date,
            o.order_estimated_delivery_date,
            oi.price,
            oi.freight_value,
            (oi.price + oi.freight_value) AS total_order_value,
            o.order_status,
            DATEDIFF(day, o.order_purchase_timestamp, o.order_delivered_customer_date) AS delivery_days,
            CASE 
                WHEN o.order_delivered_customer_date IS NULL THEN 'Not Delivered'
                WHEN o.order_delivered_customer_date <= o.order_estimated_delivery_date THEN 'On Time'
                ELSE 'Late'
            END AS delivery_status,
            r.review_score
        FROM silver.olist_orders o
        JOIN silver.olist_order_items oi 
            ON o.order_id = oi.order_id
        LEFT JOIN (
            SELECT 
                order_id, 
                review_score,
                ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY review_creation_date DESC) as rn
            FROM silver.olist_order_reviews
        ) r ON o.order_id = r.order_id AND r.rn = 1;
        SET @end_time = GETDATE();
        PRINT '[Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds]';

        SET @batch_end_time = GETDATE();
        PRINT '==========================================================';
        PRINT 'SUCCESS: GOLD LAYER LOAD COMPLETE!';
        PRINT 'End Time: ' + CONVERT(VARCHAR, @batch_end_time, 120);
        PRINT 'Total Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '==========================================================';

    END TRY
    BEGIN CATCH
        PRINT '==========================================================';
        PRINT 'ERROR OCCURRED DURING GOLD LAYER LOAD';
        PRINT 'Error Number : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT '=========================================================='; 
    END CATCH
END

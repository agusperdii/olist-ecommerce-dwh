/*
============================================================
Stored Procedure: silver.load_silver
============================================================
Script Purpose:
    - Truncates and reloads all tables in the 'silver' schema.
    - Performs data transformation and casting from the 
      'bronze' layer to the 'silver' layer.
    - Captures and logs load duration and error details.

Usage Example:
    EXEC silver.load_silver;

WARNING:
    This procedure uses TRUNCATE TABLE, which will 
    IMMEDIATELY and PERMANENTLY DELETE all existing data 
    in the silver tables before re-inserting records.
============================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '==========================================================';
        PRINT 'LOADING SILVER LAYER';
        PRINT 'Start Time: ' + CONVERT(VARCHAR, @batch_start_time, 120);
        PRINT '==========================================================';

        -- 1. olist_customers
        SET @start_time = GETDATE();
        PRINT '----------------------------------------------------------';
        PRINT 'Table: [silver].[olist_customers]';
        PRINT '  >> Truncating table...';
        TRUNCATE TABLE silver.olist_customers;
        PRINT '  >> Inserting data from bronze...';
        INSERT INTO silver.olist_customers (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state)
        SELECT 
            customer_id,
            customer_unique_id,
            customer_zip_code_prefix,
            UPPER(customer_city),
            customer_state
        FROM bronze.olist_customers;
        SET @end_time = GETDATE();
        PRINT '[Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds]';

        -- 2. olist_sellers
        SET @start_time = GETDATE();
        PRINT '----------------------------------------------------------';
        PRINT 'Table: [silver].[olist_sellers]';
        PRINT '  >> Truncating table...';
        TRUNCATE TABLE silver.olist_sellers;
        PRINT '  >> Inserting data from bronze...';
        INSERT INTO silver.olist_sellers (seller_id, seller_zip_code_prefix, seller_city, seller_state)
        SELECT 
            seller_id,
            seller_zip_code_prefix,
            UPPER(seller_city),
            seller_state
        FROM bronze.olist_sellers;
        SET @end_time = GETDATE();
        PRINT '[Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds]';

        -- 3. olist_orders
        SET @start_time = GETDATE();
        PRINT '----------------------------------------------------------';
        PRINT 'Table: [silver].[olist_orders]';
        PRINT '  >> Truncating table...';
        TRUNCATE TABLE silver.olist_orders;
        PRINT '  >> Inserting data from bronze...';
        INSERT INTO silver.olist_orders (
            order_id, customer_id, order_status, 
            order_purchase_timestamp, order_approved_at, 
            order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date
        )
        SELECT 
            order_id,
            customer_id,
            order_status,
            CAST(order_purchase_timestamp AS DATETIME),
            CAST(order_approved_at AS DATETIME),
            CAST(order_delivered_carrier_date AS DATETIME),
            CAST(order_delivered_customer_date AS DATETIME),
            CAST(order_estimated_delivery_date AS DATETIME)
        FROM bronze.olist_orders;
        SET @end_time = GETDATE();
        PRINT '[Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds]';

        -- 4. olist_order_items
        SET @start_time = GETDATE();
        PRINT '----------------------------------------------------------';
        PRINT 'Table: [silver].[olist_order_items]';
        PRINT '  >> Truncating table...';
        TRUNCATE TABLE silver.olist_order_items;
        PRINT '  >> Inserting data from bronze...';
        INSERT INTO silver.olist_order_items (
            order_id, order_item_id, product_id, seller_id, 
            shipping_limit_date, price, freight_value
        )
        SELECT 
            order_id,
            CAST(order_item_id AS INT),
            product_id,
            seller_id,
            CAST(shipping_limit_date AS DATETIME),
            CAST(price AS DECIMAL(10, 2)),
            CAST(freight_value AS DECIMAL(10, 2))
        FROM bronze.olist_order_items;
        SET @end_time = GETDATE();
        PRINT '[Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds]';

        -- 5. olist_order_payments
        SET @start_time = GETDATE();
        PRINT '----------------------------------------------------------';
        PRINT 'Table: [silver].[olist_order_payments]';
        PRINT '  >> Truncating table...';
        TRUNCATE TABLE silver.olist_order_payments;
        PRINT '  >> Inserting data from bronze...';
        INSERT INTO silver.olist_order_payments (
            order_id, payment_sequential, payment_type, 
            payment_installments, payment_value
        )
        SELECT 
            order_id,
            CAST(payment_sequential AS TINYINT),
            payment_type,
            CAST(payment_installments AS TINYINT),
            CAST(payment_value AS DECIMAL(15, 2))
        FROM bronze.olist_order_payments;
        SET @end_time = GETDATE();
        PRINT '[Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds]';

        -- 6. olist_order_reviews
        SET @start_time = GETDATE();
        PRINT '----------------------------------------------------------';
        PRINT 'Table: [silver].[olist_order_reviews]';
        PRINT '  >> Truncating table...';
        TRUNCATE TABLE silver.olist_order_reviews;
        PRINT '  >> Inserting data from bronze...';
        INSERT INTO silver.olist_order_reviews (
            review_id, order_id, review_score, 
            review_comment_title, review_comment_message, 
            review_creation_date, review_answer_timestamp
        )
        SELECT 
            review_id,
            order_id,
            CAST(review_score AS TINYINT),
            review_comment_title,
            review_comment_message,
            CAST(review_creation_date AS DATETIME),
            CAST(review_answer_timestamp AS DATETIME)
        FROM bronze.olist_order_reviews;
        SET @end_time = GETDATE();
        PRINT '[Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds]';

        -- 7. olist_products
        SET @start_time = GETDATE();
        PRINT '----------------------------------------------------------';
        PRINT 'Table: [silver].[olist_products]';
        PRINT '  >> Truncating table...';
        TRUNCATE TABLE silver.olist_products;
        PRINT '  >> Inserting data from bronze (with Category Join)...';
        INSERT INTO silver.olist_products (
            product_id, product_category_name, product_category_name_english, 
            product_name_length, product_description_length, product_photos_qty, 
            product_weight_g, product_length_cm, product_height_cm, product_width_cm
        )
        SELECT 
            p.product_id,
            p.product_category_name,
            t.product_category_name_english,
            CAST(p.product_name_lenght AS TINYINT),
            CAST(p.product_description_lenght AS SMALLINT),
            CAST(p.product_photos_qty AS TINYINT),
            CAST(p.product_weight_g AS INT),
            CAST(p.product_length_cm AS TINYINT),
            CAST(p.product_height_cm AS TINYINT),
            CAST(p.product_width_cm AS TINYINT)
        FROM bronze.olist_products p
        LEFT JOIN bronze.product_category_name_translation t
            ON p.product_category_name = t.product_category_name;
        SET @end_time = GETDATE();
        PRINT '[Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds]';

        SET @batch_end_time = GETDATE();
        PRINT '==========================================================';
        PRINT 'SUCCESS: SILVER LAYER LOAD COMPLETE!';
        PRINT 'End Time: ' + CONVERT(VARCHAR, @batch_end_time, 120);
        PRINT 'Total Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '==========================================================';

    END TRY
    BEGIN CATCH
        PRINT '==========================================================';
        PRINT 'ERROR OCCURRED DURING LOAD';
        PRINT 'Error Number : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT '=========================================================='; 
    END CATCH
END

/*
============================================================
Stored Procedure: bronze.load_bronze
============================================================
Script Purpose:
    - Truncates and reloads all tables in the 'bronze' schema.
    - Performs Bulk Insert from CSV files located in the 
      specified filesystem path.
    - Captures and logs load duration and error details.

Usage Example:
    EXEC bronze.load_bronze;

WARNING:
    This procedure uses TRUNCATE TABLE, which will 
    IMMEDIATELY and PERMANENTLY DELETE all existing data 
    in the bronze tables before importing new records.

    Ensure that the source CSV files are present in the 
    specified directory and that the SQL Server service 
    account has the necessary read permissions.
============================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '==========================================================';
        PRINT 'LOADING BRONZE LAYER';
        PRINT 'Start Time: ' + CONVERT(VARCHAR, @batch_start_time, 120);
        PRINT '==========================================================';

        -- 1. olist_customers
        SET @start_time = GETDATE();
        PRINT '----------------------------------------------------------';
        PRINT 'Table: [bronze].[olist_customers]';
        PRINT '  >> Truncating table...';
        TRUNCATE TABLE bronze.olist_customers;
        PRINT '  >> Performing BULK INSERT...';
        BULK INSERT bronze.olist_customers
        FROM '/var/opt/mssql/data/olist_data/olist_customers_dataset.csv'
        WITH(
            FORMAT = 'CSV', 
            FIRSTROW = 2, 
            FIELDTERMINATOR = ',', 
            ROWTERMINATOR = '\n', 
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '[Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds]';

        -- 2. olist_geolocation
        SET @start_time = GETDATE();
        PRINT '----------------------------------------------------------';
        PRINT 'Table: [bronze].[olist_geolocation]';
        PRINT '  >> Truncating table...';
        TRUNCATE TABLE bronze.olist_geolocation;
        PRINT '  >> Performing BULK INSERT...';
        BULK INSERT bronze.olist_geolocation
        FROM '/var/opt/mssql/data/olist_data/olist_geolocation_dataset.csv'
        WITH(
            FORMAT = 'CSV', 
            FIRSTROW = 2, 
            FIELDTERMINATOR = ',', 
            ROWTERMINATOR = '\n', 
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '[Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds]';

        -- 3. olist_order_items
        SET @start_time = GETDATE();
        PRINT '----------------------------------------------------------';
        PRINT 'Table: [bronze].[olist_order_items]';
        PRINT '  >> Truncating table...';
        TRUNCATE TABLE bronze.olist_order_items;
        PRINT '  >> Performing BULK INSERT...';
        BULK INSERT bronze.olist_order_items
        FROM '/var/opt/mssql/data/olist_data/olist_order_items_dataset.csv'
        WITH(
            FORMAT = 'CSV', 
            FIRSTROW = 2, 
            FIELDTERMINATOR = ',', 
            ROWTERMINATOR = '\n', 
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '[Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds]';

        -- 4. olist_order_payments
        SET @start_time = GETDATE();
        PRINT '----------------------------------------------------------';
        PRINT 'Table: [bronze].[olist_order_payments]';
        PRINT '  >> Truncating table...';
        TRUNCATE TABLE bronze.olist_order_payments;
        PRINT '  >> Performing BULK INSERT...';
        BULK INSERT bronze.olist_order_payments
        FROM '/var/opt/mssql/data/olist_data/olist_order_payments_dataset.csv'
        WITH(
            FORMAT = 'CSV', 
            FIRSTROW = 2, 
            FIELDTERMINATOR = ',', 
            ROWTERMINATOR = '\n', 
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '[Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds]';

        -- 5. olist_order_reviews
        SET @start_time = GETDATE();
        PRINT '----------------------------------------------------------';
        PRINT 'Table: [bronze].[olist_order_reviews]';
        PRINT '  >> Truncating table...';
        TRUNCATE TABLE bronze.olist_order_reviews;
        PRINT '  >> Performing BULK INSERT...';
        BULK INSERT bronze.olist_order_reviews
        FROM '/var/opt/mssql/data/olist_data/olist_order_reviews_dataset.csv'
        WITH(
            FORMAT = 'CSV', 
            FIRSTROW = 2, 
            FIELDTERMINATOR = ',', 
            ROWTERMINATOR = '\r\n', 
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '[Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds]';

        -- 6. olist_orders
        SET @start_time = GETDATE();
        PRINT '----------------------------------------------------------';
        PRINT 'Table: [bronze].[olist_orders]';
        PRINT '  >> Truncating table...';
        TRUNCATE TABLE bronze.olist_orders;
        PRINT '  >> Performing BULK INSERT...';
        BULK INSERT bronze.olist_orders
        FROM '/var/opt/mssql/data/olist_data/olist_orders_dataset.csv'
        WITH(
            FORMAT = 'CSV', 
            FIRSTROW = 2, 
            FIELDTERMINATOR = ',', 
            ROWTERMINATOR = '\n', 
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '[Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds]';

        -- 7. olist_products
        SET @start_time = GETDATE();
        PRINT '----------------------------------------------------------';
        PRINT 'Table: [bronze].[olist_products]';
        PRINT '  >> Truncating table...';
        TRUNCATE TABLE bronze.olist_products;
        PRINT '  >> Performing BULK INSERT...';
        BULK INSERT bronze.olist_products
        FROM '/var/opt/mssql/data/olist_data/olist_products_dataset.csv'
        WITH(
            FORMAT = 'CSV', 
            FIRSTROW = 2, 
            FIELDTERMINATOR = ',', 
            ROWTERMINATOR = '\n', 
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '[Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds]';

        -- 8. olist_sellers
        SET @start_time = GETDATE();
        PRINT '----------------------------------------------------------';
        PRINT 'Table: [bronze].[olist_sellers]';
        PRINT '  >> Truncating table...';
        TRUNCATE TABLE bronze.olist_sellers;
        PRINT '  >> Performing BULK INSERT...';
        BULK INSERT bronze.olist_sellers
        FROM '/var/opt/mssql/data/olist_data/olist_sellers_dataset.csv'
        WITH(
            FORMAT = 'CSV', 
            FIRSTROW = 2, 
            FIELDTERMINATOR = ',', 
            ROWTERMINATOR = '\n', 
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '[Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds]';

        -- 9. product_category_name_translation
        SET @start_time = GETDATE();
        PRINT '----------------------------------------------------------';
        PRINT 'Table: [bronze].[product_category_name_translation]';
        PRINT '  >> Truncating table...';
        TRUNCATE TABLE bronze.product_category_name_translation;
        PRINT '  >> Performing BULK INSERT...';
        BULK INSERT bronze.product_category_name_translation
        FROM '/var/opt/mssql/data/olist_data/product_category_name_translation.csv'
        WITH(
            FORMAT = 'CSV', 
            FIRSTROW = 2, 
            FIELDTERMINATOR = ',', 
            ROWTERMINATOR = '\r\n', 
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '[Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds]';

        SET @batch_end_time = GETDATE();
        PRINT '==========================================================';
        PRINT 'SUCCESS: BRONZE LAYER LOAD COMPLETE!';
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

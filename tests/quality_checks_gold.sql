/*
============================================================
Data Quality Check: Gold Layer (Star Schema)
============================================================
Script Purpose:
    - Validates data integrity across Dimensions and Facts.
    - Checks for duplicate records and NULL values in PKs.
    - Verifies Referential Integrity (Fact to Dimensions).
    - Validates Business Logic (Price ranges, Date consistency).

Usage:
    Run this script after executing gold.load_gold to ensure 
    the reporting layer is reliable.
============================================================
*/

PRINT '==========================================================';
PRINT 'STARTING DATA QUALITY CHECK - GOLD LAYER';
PRINT '==========================================================';

-- ---------------------------------------------------------
-- 1. Uniqueness Check (Primary Keys)
-- ---------------------------------------------------------
PRINT '>> Checking Uniqueness of Primary Keys...';

SELECT 'Duplicate Customers' as issue, COUNT(*) as count FROM (SELECT customer_id FROM gold.dim_customers GROUP BY customer_id HAVING COUNT(*) > 1) t
UNION ALL
SELECT 'Duplicate Sellers', COUNT(*) FROM (SELECT seller_id FROM gold.dim_sellers GROUP BY seller_id HAVING COUNT(*) > 1) t
UNION ALL
SELECT 'Duplicate Products', COUNT(*) FROM (SELECT product_id FROM gold.dim_products GROUP BY product_id HAVING COUNT(*) > 1) t;


-- ---------------------------------------------------------
-- 2. Completeness Check (NULL Values in Critical Columns)
-- ---------------------------------------------------------
PRINT '>> Checking for NULL values in critical columns...';

SELECT 
    'Fact Sales NULL IDs' as issue, 
    COUNT(*) as record_count 
FROM gold.fact_sales 
WHERE order_id IS NULL OR product_id IS NULL OR customer_id IS NULL OR seller_id IS NULL

UNION ALL

SELECT 
    'Products NULL Category', 
    COUNT(*) 
FROM gold.dim_products 
WHERE product_category_name_english IS NULL;


-- ---------------------------------------------------------
-- 3. Referential Integrity Check (Fact -> Dims)
-- ---------------------------------------------------------
PRINT '>> Checking Referential Integrity (Orphaned Records)...';

SELECT 'Orphaned Customers in Fact' as issue, COUNT(*) as count
FROM gold.fact_sales f LEFT JOIN gold.dim_customers c ON f.customer_id = c.customer_id WHERE c.customer_id IS NULL
UNION ALL
SELECT 'Orphaned Products in Fact', COUNT(*)
FROM gold.fact_sales f LEFT JOIN gold.dim_products p ON f.product_id = p.product_id WHERE p.product_id IS NULL
UNION ALL
SELECT 'Orphaned Sellers in Fact', COUNT(*)
FROM gold.fact_sales f LEFT JOIN gold.dim_sellers s ON f.seller_id = s.seller_id WHERE s.seller_id IS NULL;


-- ---------------------------------------------------------
-- 4. Business Logic & Validity Check
-- ---------------------------------------------------------
PRINT '>> Checking Business Logic Consistency...';

SELECT 'Negative Price/Freight' as issue, COUNT(*) as count FROM gold.fact_sales WHERE price < 0 OR freight_value < 0
UNION ALL
SELECT 'Invalid Review Score (>5)', COUNT(*) FROM gold.fact_sales WHERE review_score > 5
UNION ALL
SELECT 'Delivery Date Before Purchase', COUNT(*) FROM gold.fact_sales WHERE order_delivered_customer_date < order_purchase_date
UNION ALL
SELECT 'Invalid Delivery Status', COUNT(*) FROM gold.fact_sales WHERE delivery_status NOT IN ('On Time', 'Late', 'Not Delivered');

PRINT '==========================================================';
PRINT 'DATA QUALITY CHECK COMPLETE';
PRINT '==========================================================';

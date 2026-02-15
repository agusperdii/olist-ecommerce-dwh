/*
============================================================
Silver Layer Data Quality Check Report
============================================================
Script Purpose:
    - Perform comprehensive data quality audits on 'silver' schema tables.
    - Validate dimensions: Completeness, Uniqueness, Validity, and Consistency.
    - generate a summary report to determine if data is ready for Gold Layer.

REPORT LEGEND:
    - PASSED : Data is clean and ready for promotion.
    - WARNING: Minor issues detected (e.g., small % of nulls) but acceptable.
    - FAILED : Critical issues detected (e.g., duplicates, negative prices).
               Immediate action required before proceeding to Gold Layer.
============================================================
*/

WITH DQ_Checks AS (
    -- 1. CHECK CUSTOMERS
    SELECT 
        'silver.olist_customers' AS Table_Name,
        'Uniqueness: Duplicate Customer IDs' AS DQ_Rule,
        COUNT(*) AS Total_Records,
        COUNT(customer_id) - COUNT(DISTINCT customer_id) AS Failed_Records
    FROM silver.olist_customers
    
    UNION ALL

    SELECT 
        'silver.olist_customers',
        'Completeness: Null Zip Code or City',
        COUNT(*),
        SUM(CASE WHEN customer_zip_code_prefix IS NULL OR customer_city IS NULL THEN 1 ELSE 0 END)
    FROM silver.olist_customers

    UNION ALL

    -- 2. CHECK ORDERS
    SELECT 
        'silver.olist_orders',
        'Uniqueness: Duplicate Order IDs',
        COUNT(*),
        COUNT(order_id) - COUNT(DISTINCT order_id)
    FROM silver.olist_orders
    
    UNION ALL

    SELECT 
        'silver.olist_orders',
        'Consistency: Delivered Date < Purchase Date (Time Travel)',
        COUNT(*),
        SUM(CASE 
            WHEN order_delivered_customer_date IS NOT NULL 
                 AND order_delivered_customer_date < order_purchase_timestamp 
            THEN 1 ELSE 0 
        END)
    FROM silver.olist_orders

    UNION ALL

    -- 3. CHECK ORDER ITEMS
    SELECT 
        'silver.olist_order_items',
        'Validity: Negative Price or Freight',
        COUNT(*),
        SUM(CASE WHEN price < 0 OR freight_value < 0 THEN 1 ELSE 0 END)
    FROM silver.olist_order_items

    UNION ALL
    
    SELECT 
        'silver.olist_order_items',
        'Integrity: Orphan Orders (Item exists but Order missing)',
        COUNT(*),
        SUM(CASE WHEN o.order_id IS NULL THEN 1 ELSE 0 END)
    FROM silver.olist_order_items oi
    LEFT JOIN silver.olist_orders o ON oi.order_id = o.order_id

    UNION ALL

    -- 4. CHECK PAYMENTS
    SELECT 
        'silver.olist_order_payments',
        'Completeness: Null Payment Type',
        COUNT(*),
        SUM(CASE WHEN payment_type IS NULL THEN 1 ELSE 0 END)
    FROM silver.olist_order_payments

    UNION ALL

    -- 5. CHECK REVIEWS
    SELECT 
        'silver.olist_order_reviews',
        'Validity: Score Out of Range (Not 1-5)',
        COUNT(*),
        SUM(CASE WHEN review_score < 1 OR review_score > 5 THEN 1 ELSE 0 END)
    FROM silver.olist_order_reviews

    UNION ALL

    -- 6. CHECK PRODUCTS
    SELECT 
        'silver.olist_products',
        'Completeness: Null Category (Critical for Analysis)',
        COUNT(*),
        SUM(CASE WHEN product_category_name IS NULL AND product_category_name_english IS NULL THEN 1 ELSE 0 END)
    FROM silver.olist_products
    
    UNION ALL

    SELECT 
        'silver.olist_products',
        'Validity: Zero or Negative Dimensions/Weight',
        COUNT(*),
        SUM(CASE 
            WHEN product_weight_g <= 0 
              OR product_length_cm <= 0 
              OR product_height_cm <= 0 
              OR product_width_cm <= 0 
            THEN 1 ELSE 0 
        END)
    FROM silver.olist_products
)

-- FINAL REPORT FORMAT
SELECT 
    Table_Name,
    DQ_Rule,
    Total_Records,
    Failed_Records,
    -- Hitung Persentase Kesehatan Data (Score)
    CAST((Total_Records - Failed_Records) * 100.0 / NULLIF(Total_Records, 0) AS DECIMAL(5,2)) AS Quality_Score_Pct,
    CASE 
        WHEN Failed_Records = 0 THEN 'PASSED'
        WHEN (Total_Records - Failed_Records) * 100.0 / NULLIF(Total_Records, 0) >= 99.0 THEN 'WARNING (Acceptable)'
        ELSE 'FAILED (Action Required)'
    END AS Status
FROM DQ_Checks
ORDER BY Quality_Score_Pct ASC;

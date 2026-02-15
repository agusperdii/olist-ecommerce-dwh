/*
Layer: Bronze
Purpose: Add missing translation data not available in original source
Reason: Required for downstream transformation to silver/gold
Author: Agus Perdiana
Date: 2026-02-15
*/

IF NOT EXISTS (
    SELECT 1 
    FROM bronze.product_category_name_translation
    WHERE product_category_name = 'pc_gamer'
)
BEGIN
    INSERT INTO bronze.product_category_name_translation 
    (product_category_name, product_category_name_english)
    VALUES ('pc_gamer', 'pc_gamer');
END;

IF NOT EXISTS (
    SELECT 1 
    FROM bronze.product_category_name_translation
    WHERE product_category_name = 'portateis_cozinha_e_preparadores_de_alimentos'
)
BEGIN
    INSERT INTO bronze.product_category_name_translation 
    (product_category_name, product_category_name_english)
    VALUES ('portateis_cozinha_e_preparadores_de_alimentos', 'kitchen_and_food_preparators');
END;

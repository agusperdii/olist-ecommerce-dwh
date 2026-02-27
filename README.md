# 🛒 Olist E-Commerce Data Warehouse (End-to-End)

![SQL Server](https://img.shields.io/badge/Database-SQL%20Server-CC2927?style=for-the-badge&logo=microsoft-sql-server&logoColor=white)
![ETL](https://img.shields.io/badge/Pipeline-ETL%20%2F%20ELT-blue?style=for-the-badge)
![Architecture](https://img.shields.io/badge/Architecture-Medallion%20(Bronze%2FSilver%2FGold)-orange?style=for-the-badge)

## 📌 Project Overview
This project demonstrates an end-to-end Data Engineering solution using the **Brazilian E-Commerce Public Dataset by Olist**. The goal is to build a **Data Warehouse (DWH)** optimized for Business Intelligence (Tableau/PowerBI) using the **Medallion Architecture**.

The pipeline transforms raw, unorganized CSV data into a clean, analytical **Star Schema**, handling complex data quality issues like duplicate reviews, missing categories, and Portuguese character encoding along the way.

---

## 🏗️ Architecture: The Medallion Layer
![High Level Architecture](docs/High%20Level%20Architecture.png)
The project is structured into three logical layers within Microsoft SQL Server:

### 1. 🥉 Bronze Layer
* **Purpose:** Landing zone for raw data ingestion.
* **Characteristics:**
    * Direct mapping from source CSVs.
    * No strict data types enforcement (mostly `VARCHAR`).
    * Preserves original state for audit purposes.

### 2. 🥈 Silver Layer (Clean & Enriched Zone)
* **Purpose:** Data cleaning, standardization, and normalization.
* **Key Transformations:**
    * Casting strings to `DATETIME`, `DECIMAL`, `INT` using safe casting (`TRY_CAST`).
    * Converting City names to `UPPER` case to handle casing inconsistencies.
    * Using `NVARCHAR` to support Brazilian Portuguese accents (e.g., `ã`, `ç`).
    * Joining Product tables with English Translation tables.
    * Using `COALESCE` to handle missing product categories (tagged as 'Other Categories').

### 3. 🥇 Gold Layer
* **Purpose:** Consumption-ready data optimized for BI tools.
* **Modeling Strategy:** **Star Schema**.
* **Business Logic Implemented:**
    * Logic to determine 'On Time' vs 'Late' deliveries.
    * Summing `Price` + `Freight` for GMV calculation.
    * Fetching only the *latest* review per order using Window Functions (`ROW_NUMBER`).

---

## 🗂️ Data Modeling

The Gold Layer is designed to simplify reporting.

### Fact Table: `fact_sales`
Central table containing transactional metrics (Granularity: per order item).
* **Measures:** `price`, `freight_value`, `total_order_value`.
* **Attributes:** `order_status`, `delivery_days`, `delivery_status`, `review_score`.
* **Foreign Keys:** Links to Customers, Products, and Sellers.

### Dimension Tables
* **`dim_products`**: Contains English category names and physical dimensions.
* **`dim_customers`**: Customer location (City, State).
* **`dim_sellers`**: Seller location (City, State).

---

## 🛠️ Key Engineering Challenges & Solutions

| Challenge | Solution Implemented |
| :--- | :--- |
| **Portuguese Characters** | Used `NVARCHAR` instead of `VARCHAR` to preserve diacritics (accents) in city names and categories. |
| **Missing Categories** | Identified categories like `pc_gamer` missing from translation. Updated dictionary and used `COALESCE(col, 'Other Categories')` in the final load. |
| **Duplicate Reviews** | A single order could have multiple reviews (updates). Implemented `ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY date DESC)` to strictly select the latest customer sentiment. |
| **Ghost Orders** | Handled orders with `invoiced` status but no delivery date by allowing `NULL` in date columns but flagging them as 'Not Delivered' in the status logic. |
| **Null Handling for BI** | Converted `NULL` dimensions to 'Unknown' or 'Other' to prevent blank bars in Tableau visualizations. |

---

## 🔍 Data Quality (DQ) Checks
A comprehensive SQL script was developed to audit the Silver Layer before promotion to Gold.
* Checks for critical NULLs in IDs and timestamps.
* Ensures Primary Keys (Order ID, Customer ID) are unique.
* "Time Travel" check (e.g., ensuring `Delivered Date` > `Purchase Date`).
* Checks for negative prices or freight values.

---

## 🚀 How to Run

1. Initialize DB: Run `scripts/00_init_database_and_schemas.sql`.
2. Bronze Layer: Execute `ddl_bronze.sql`, `procedure_load_bronze.sql`, and `seed_translation.sql` inside `scripts/bronze/`.
3. Silver Layer: Execute `ddl_silver.sql` and `procedure_load_silver.sql` inside `scripts/silver/`.
4. Quality Check: Run the validation scripts located in `tests/`.
5. Visualize: Connect your BI tool to the SQL Server database.

---

## 📈 Analytics Potential
With this DWH, the following questions can be answered immediately:
* What is the trend of **Late Deliveries** across different States?
* Which **Product Categories** have the highest Freight-to-Price ratio?
* Is there a correlation between **Delivery Time** and **Review Scores**?
* Who are the top Sellers based on **Total Order Value** (GMV)?

---

**Author:** Kadek Agus Perdiana  
**Tech Stack:** SQL Server (T-SQL), Data Warehousing Concepts, ETL.

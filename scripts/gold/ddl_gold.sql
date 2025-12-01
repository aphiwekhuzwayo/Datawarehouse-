/*
==========================================================================
DDL Script: Create Gold Views
==========================================================================
SCRIPT PURPOSE:
    This script creates the GOLD layer views for the Data Warehouse.
    These views represent the final analytical layer in the Medallion Architecture.

    1. gold.dim_customers
       - Integrates customer data from CRM and ERP systems.
       - Cleans gender field and enriches with demographic details.
       - Generates a surrogate key (customer_key) for the star schema.

    2. gold.dim_products
       - Combines product master data with product category information.
       - Filters out inactive/expired products.
       - Generates a product surrogate key for downstream fact tables.

    3. gold.fact_sales
       - Integrates transactional sales details with customer and product dimensions.
       - Applies business logic to structure data for reporting.
       - Aligns keys to the star schema by joining dim_products and dim_customers.

    These Gold layer views support analytics, dashboards, and business reporting.
*/

IF OBJECT_ID('gold.dim_customers','V') IS NOT NULL
	DROP VIEW gold.dim_customers

GO

CREATE VIEW gold.dim_customers AS
SELECT
	ROW_NUMBER() OVER(ORDER BY cust_id) AS customer_key ,
	ci.cust_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.CNTRY AS country,
	ci.cst_material_status AS material_status,
	CASE
		WHEN ci.cst_gndr != 'unkown' THEN ci.cst_gndr
		ELSE COALESCE(ca.GEN , 'unkown')
	END AS gender,
	ca.BDATE AS birth_date
FROM
	silver.crm_cust_info AS ci
LEFT JOIN
	silver.erp_CUST_AZ12 AS ca
ON 
	ci.cst_key = ca.CID
LEFT JOIN
	silver.erp_LOC_A10 AS la
ON 
	ci.cst_key = la.CID;


IF OBJECT_ID('gold.dim_products','V') IS NOT NULL
	DROP VIEW gold.dim_products

GO

CREATE VIEW gold.dim_products AS 
SELECT 
    ROW_NUMBER() OVER (ORDER BY prd_start_dt) AS product_key,
    pi.prd_id AS product_id,
    pi.prd_key AS product_number,
    pi.prd_nm AS product_name,
    pi.cat_id AS category_id,
    pc.CAT AS category,
    pc.SUBCAT AS sub_category,
    pc.MAINTANANCE,
    pi.prd_cost AS cost,
    pi.prd_line AS product_line,
    pi.prd_start_dt AS start_date
FROM 
    silver.crm_prd_info AS pi
LEFT JOIN
    silver.erp_PX_CAT_G1V2 AS pc
ON 
    pi.cat_id = pc.ID
WHERE
    pi.prd_end_dt IS NULL -- Filter out the old data
;

IF OBJECT_ID('gold.fact_sales','V') IS NOT NULL
	DROP VIEW gold.fact_sales

GO

CREATE VIEW gold.fact_sales AS
SELECT 
    sd.sales_ord_num AS order_number,
    dp.product_key,
    dc.customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS ship_date,
    sd.sls_due_dt AS due_date,
    sd.sls_sales AS sales_amount ,
    sd.sls_quantity AS quantity,
    sd.sls_price AS price
FROM 
    silver.crm_sales_details AS sd
LEFT JOIN 
    gold.dim_products AS dp
ON 
    sd.sls_prd_key = dp.product_number 
LEFT JOIN
    gold.dim_customers AS dc
ON 
    sd.sls_cust_id = dc.customer_id

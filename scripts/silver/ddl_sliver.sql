/*
==============================================
Creating tables for the silver layer 
=============================================
Purpose of this script:
	The purpose of this script is to create new empty tables where we will 
	perform a full load on them. The name of the tables for the crm source 
	are as follows :
	"silver.crm_cust_info", "silver.crm_prd_info","silver.crm_sales_details".
	The name of the tables for the erp source system are as follows:
	"silver.erp_CUST_AZ12", "silver.erp_LOC_A10", "silver.erp_PX_CAT_G1V2".

Warning:
	If any table already exists then the table will be dropped with 
	all of its information and be trucanted and be new. proceed with 
	caution.
*/

USE DataWarehouse;

	-- Creating tables for the crm source system 
IF OBJECT_ID ('silver.crm_cust_info','u') IS NOT NULL
	DROP TABLE silver.crm_cust_info;

GO

CREATE TABLE silver.crm_cust_info(
	cust_id int,
	cst_key	NVARCHAR(100),
	cst_firstname NVARCHAR(100),
	cst_lastname NVARCHAR(100),
	cst_material_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_createdate DATE,
);

GO

IF OBJECT_ID ('silver.crm_prd_info','u') IS NOT NULL
	DROP TABLE silver.crm_prd_info;

GO

CREATE TABLE silver.crm_prd_info(
	prd_id INT,
	cat_id NVARCHAR(100),
	prd_key NVARCHAR(100),
	prd_nm NVARCHAR(100),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
);

GO

IF OBJECT_ID ('silver.crm_sales_details','u') IS NOT NULL
	DROP TABLE silver.crm_sales_details;

GO

CREATE TABLE silver.crm_sales_details(
	sales_ord_num NVARCHAR(100),
	sls_prd_key NVARCHAR(100),
	sls_cust_id INT,
	sls_order_dt DATE,
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);

GO

	-- Creating tables for the erp source system 
IF OBJECT_ID ('silver.erp_CUST_AZ12','u') IS NOT NULL
	DROP TABLE silver.erp_CUST_AZ12;

GO

CREATE TABLE silver.erp_CUST_AZ12(
	CID NVARCHAR(100),
	BDATE DATE,
	GEN NVARCHAR(50)
);

GO

IF OBJECT_ID ('silver.erp_LOC_A10','u') IS NOT NULL
	DROP TABLE silver.erp_LOC_A10;

GO

CREATE TABLE silver.erp_LOC_A10(
	CID NVARCHAR(100),
	CNTRY NVARCHAR(100)
);

GO

IF OBJECT_ID ('silver.erp_PX_CAT_G1V2','u') IS NOT NULL
	DROP TABLE silver.erp_PX_CAT_G1V2

GO

CREATE TABLE silver.erp_PX_CAT_G1V2(
	ID NVARCHAR(50),
	CAT NVARCHAR(100),
	SUBCAT NVARCHAR(100),
	MAINTANANCE NVARCHAR(100)
);

GO

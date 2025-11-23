/*
==============================================
Creating tables 
=============================================
Purpose of this script:
	The purpose of this script is to create new empty tables where we will 
	perform a full load on them. The name of the tables for the crm source 
	are as follows :
	"bronze.crm_cust_info", "bronze.crm_prd_info","bronze.crm_sales_details".
	The name of the tables for the erp source system are as follows:
	"bronze.erp_CUST_AZ12", "bronze.erp_LOC_A10", "bronze.erp_PX_CAT_G1V2".

Warning:
	If any table already exists then the table will be dropped with 
	all of its information and be trucanted and be new. proceed with 
	caution.
*/

USE DataWarehouse;

	-- Craeting tables for the crm source system 
IF OBJECT_ID ('bronze.crm_cust_info','u') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;

GO

CREATE TABLE bronze.crm_cust_info(
	cust_id int,
	cst_key	NVARCHAR(100),
	cst_firstname NVARCHAR(100),
	cst_lastname NVARCHAR(100),
	cst_material_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_createdate DATE,
);

GO

IF OBJECT_ID ('bronze.crm_prd_info','u') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;

GO

CREATE TABLE bronze.crm_prd_info(
	prd_id INT,
	prd_key NVARCHAR(100),
	prd_nm NVARCHAR(100),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATETIME,
	prd_end_dt DATETIME,
);

GO

IF OBJECT_ID ('bronze.crm_sales_details','u') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;

GO

CREATE TABLE bronze.crm_sales_details(
	sales_ord_num NVARCHAR(100),
	sls_prd_key NVARCHAR(100),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);

GO

	-- Creating tables for the erp source system 
IF OBJECT_ID ('bronze.erp_CUST_AZ12','u') IS NOT NULL
	DROP TABLE bronze.erp_CUST_AZ12;

GO

CREATE TABLE bronze.erp_CUST_AZ12(
	CID NVARCHAR(100),
	BDATE DATE,
	GEN NVARCHAR(50)
);

GO

IF OBJECT_ID ('bronze.erp_LOC_A10','u') IS NOT NULL
	DROP TABLE bronze.erp_LOC_A10;

GO

CREATE TABLE bronze.erp_LOC_A10(
	CID NVARCHAR(100),
	CNTRY NVARCHAR(100)
);

GO

IF OBJECT_ID ('bronze.erp_PX_CAT_G1V2','u') IS NOT NULL
	DROP TABLE bronze.erp_PX_CAT_G1V2

GO

CREATE TABLE bronze.erp_PX_CAT_G1V2(
	ID NVARCHAR(50),
	CAT NVARCHAR(100),
	SUBCAT NVARCHAR(100),
	MAINTANANCE NVARCHAR(100)
);

GO


/*
======================================================================================
DDL Script: Bulk insert stored procedure
======================================================================================
Script purpose:
   This script is to create a stored procedure that will allow for the loading of the 
   data from the bronze layer to the silver layer . The code will truncate the tables before 
   bulk inserting data as this is a SCD0(no historazation) so proceed with caution and then
clean up the data before loading to the silver layer .
=======================================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN 
	 BEGIN TRY

        PRINT('========================================')
        PRINT('Loading the bronze layer')
        PRINT('========================================')

        PRINT('----------------------------------------')
        PRINT('Loading CRM tables')
        PRINT('----------------------------------------')

	PRINT('>>>>>>> Truncating the table')
	TRUNCATE TABLE silver.crm_cust_info;

	PRINT('----------------------------')
	PRINT('-- selecting the clean data that can be loaded in the silver layer ')
	INSERT INTO silver.crm_cust_info 
		SELECT
			cust_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE 
				WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
				ELSE  'Unknown'
			END AS cst_material_status, -- Normalize material status to readable format 
			CASE 
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE  'Unknown'
			END AS cst_gndr, -- Normalize gender values to readable format

				cst_createdate
		FROM(
				SELECT
					*,
					COUNT(*) OVER(PARTITION BY cust_id) AS COUNT,
					ROW_NUMBER() OVER( PARTITION BY cust_id ORDER BY cst_createdate DESC) AS ranking
				FROM
					bronze.crm_cust_info
				) table_1
		WHERE 
			ranking = 1 AND cust_id IS NOT NULL; -- Select the most recent record per customer

	PRINT('>>>TRUNCATING silver.crm_prd_info')
	TRUNCATE TABLE silver.crm_prd_info

	PRINT('----------------------------')
	PRINT('-- selecting the clean data that can be loaded in the silver layer ')
	INSERT INTO silver.crm_prd_info
		SELECT
			prd_id,
			REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id ,-- Extract category id
			SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,-- Extract product_key
			TRIM(prd_nm) AS prd_nm,
			ISNULL(prd_cost,0) AS prd_cost,
			CASE TRIM(UPPER(prd_line))
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Tourin'
				ELSE 'Unkown'
			END AS prd_line, -- Map product line codes to descriptive values
			CAST(prd_start_dt AS DATE) AS prd_start_dt,
			CAST(LEAD(prd_start_dt) OVER( PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
		FROM
			bronze.crm_prd_info;

	PRINT('>>> TRUNCATING TABLE silver.crm_sales_details')
	TRUNCATE TABLE silver.crm_sales_details

	PRINT('----------------------------')
	PRINT('-- selecting the clean data that can be loaded in the silver layer ')
	INSERT INTO silver.crm_sales_details
		SELECT 
			sales_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE 
				WHEN sls_order_dt IS NULL OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS NVARCHAR) AS DATE)
			END AS sls_order_date,
			CASE
				WHEN sls_ship_dt IS NULL OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS NVARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE
				WHEN sls_due_dt IS NULL OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS NVARCHAR) AS DATE)
			END AS sls_due_dt ,
			CASE
				WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * sls_price THEN ABS(sls_price) * sls_quantity
				ELSE sls_sales
			END AS sls_sales,
			sls_quantity,
			CASE 
				WHEN sls_price IS NULL OR sls_price = 0 THEN CAST(sls_sales AS FLOAT)/sls_quantity
				WHEN sls_price < 0 THEN ABS(sls_price)
				ELSE sls_price
			END AS sls_price
		  FROM 
			   bronze.crm_sales_details;
		

	PRINT('>>> TRUNCATING TABLE silver.erp_CUST_AZ12')
	TRUNCATE TABLE silver.erp_CUST_AZ12

	PRINT('----------------------------')
	PRINT('-- selecting the clean data that can be loaded in the silver layer ')
	INSERT INTO silver.erp_CUST_AZ12
		SELECT 
			CASE
				WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LEN(CID))
				ELSE CID
			END AS CID,
			CASE
				WHEN BDATE > GETDATE() THEN NULL 
				ELSE BDATE
			 END AS BDATE,
			CASE 
				WHEN UPPER(TRIM(GEN)) IN ('F','FEMALE') THEN 'FEMALE'
				WHEN UPPER(TRIM(GEN)) IN ('M','MALE') THEN 'MALE'
				ELSE NULL
			END AS GEN 
		  FROM bronze.erp_CUST_AZ12;

	  PRINT('>>> TRUNCATING TABLE silver.erp_LOC_A10')
	  TRUNCATE TABLE silver.erp_LOC_A10

	PRINT('----------------------------')
	PRINT('-- selecting the clean data that can be loaded in the silver layer ')
	INSERT INTO silver.erp_LOC_A10
		SELECT
			REPLACE(CID,'-','') AS CID,
			CASE 
				WHEN UPPER(TRIM(CNTRY)) = 'DE' OR CNTRY = 'Germany' THEN 'Germany'
				WHEN UPPER(TRIM(CNTRY)) = 'USA' OR UPPER(TRIM(CNTRY)) = 'US' OR CNTRY = 'United States' THEN 'United States of America'
				WHEN TRIM(CNTRY) = '' OR CNTRY IS NULL THEN 'Unkown'
				ELSE CNTRY
			END AS CNTRY 
		FROM
			bronze.erp_loc_a10;

	PRINT('>>> TRUNCATING TABLE silver.erp_PX_CAT_G1V2')
	TRUNCATE TABLE silver.erp_PX_CAT_G1V2

	PRINT('----------------------------')
	PRINT('-- selecting the clean data that can be loaded in the silver layer ')
	INSERT INTO silver.erp_PX_CAT_G1V2
		SELECT 
			   [ID]
			  ,[CAT]
			  ,[SUBCAT]
			  ,[MAINTANANCE]
		  FROM [DataWarehouse].[bronze].[erp_PX_CAT_G1V2]
	END TRY
	BEGIN CATCH 
	    print('An error has occured:');
		print('error message:' + '' + error_message());
		print('error line:' +' ' + cast(error_line() as NVARCHAR));
		print('error number:' + ' ' + + cast(error_number() as NVARCHAR))
	END CATCH
END

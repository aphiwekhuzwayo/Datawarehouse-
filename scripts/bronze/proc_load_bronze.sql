/*
======================================================================================
DDL Script: Bulk insert stored procedure
======================================================================================
Script purpose:
   This script is to create a stored procedure that will allow for the loading of the 
   data from the sources to the databse tables. The code will truncate the tables before 
   bulk inserting data as this is a SCD0(no historazation) so proceed with caution.
=======================================================================================
*/

CREATE OR ALTER PROCEDURE bronze.bulk_insert AS 
BEGIN 
    BEGIN TRY
        -- Declaring the variables
        DECLARE 
            @Starttime DATETIME, @Endtime DATETIME, @batch_start_time DATETIME, @batch_stop_time DATETIME
        SET @batch_start_time = GETDATE();
        PRINT('========================================')
        PRINT('Loading the bronze layer')
        PRINT('========================================')

        PRINT('----------------------------------------')
        PRINT('Loading CRM tables')
        PRINT('----------------------------------------')

        SET @Starttime = GETDATE();
        PRINT('>> Truncating table: bronze.crm_cust_info')
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT('>>Inserting table: bronze.crm_cust_info')
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\user\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        with(
                firstrow=2,
                fieldterminator=',',
                TABLOCK
            )

        SET @Endtime = GETDATE();
        PRINT('>>Loading time:' + CAST(DATEDIFF(SECOND,@Endtime,@Starttime) AS NVARCHAR))
        PRINT('---------------------------------------------------')

        SET @Starttime =GETDATE()
        PRINT('>>Truncating table: bronze.crm_prd_info')
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT('>>Inserting table: bronze.crm_prd_info')
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\user\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        with(
                firstrow=2,
                fieldterminator=',',
                TABLOCK
            );
        SET @Endtime = GETDATE();
        PRINT('>>Loading time:' + CAST(DATEDIFF(SECOND,@Endtime,@Starttime) AS NVARCHAR));
        PRINT('---------------------------------------------------')

        SET @Starttime = GETDATE();
        PRINT('>>Truncating table: bronze.crm_sales_details')
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT('>>Inserting table: bronze.crm_sales_details')
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\user\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        with(
                firstrow=2,
                fieldterminator=',',
                TABLOCK
            );
        SET @Endtime = GETDATE();
        PRINT('>>Loading time:' + CAST(DATEDIFF(SECOND,@Endtime,@Starttime) AS NVARCHAR))
        PRINT('---------------------------------------------------')

        PRINT('-----------------------------------------')
        PRINT('Loading ERP tables')
        PRINT('-----------------------------------------')

        SET @Starttime = GETDATE();
        PRINT('>>Trucating table: bronze.erp_CUST_AZ12')
        TRUNCATE TABLE bronze.erp_CUST_AZ12;

        PRINT('>>Inserting table: bronze.erp_CUST_AZ12')
        BULK INSERT bronze.erp_CUST_AZ12
        FROM 'C:\Users\user\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        with(
                firstrow=2,
                fieldterminator=',',
                TABLOCK
            );
        SET @Endtime = GETDATE();
        PRINT('>>Loading time:' + CAST(DATEDIFF(SECOND,@Endtime,@Starttime) AS NVARCHAR));
        PRINT('---------------------------------------------------')

        SET @Starttime = GETDATE();
        PRINT('>>Trucating table: bronze.erp_LOC_A_10')
        TRUNCATE TABLE bronze.erp_LOC_A10;

        PRINT('>>Inserting table: bronze.erp_LOC_A_10')
        BULK INSERT bronze.erp_LOC_A10
        FROM 'C:\Users\user\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        with(
                firstrow=2,
                fieldterminator=',',
                TABLOCK
            );
        SET @Endtime = GETDATE();
        PRINT('>>Loading time:' + CAST(DATEDIFF(SECOND,@Endtime,@Starttime) AS NVARCHAR))
        PRINT('---------------------------------------------------')

        SET @Starttime = GETDATE();
        PRINT('>>Trucating table: bronze.erp_PX_CAT_G1V2')
        TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;

        PRINT('>>Inserting table: bronze.erp_PX_CAT_G1V2')
        BULK INSERT bronze.erp_PX_CAT_G1V2
        FROM 'C:\Users\user\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        with(
                firstrow=2,
                fieldterminator=',',
                TABLOCK
            );
        SET @Endtime = GETDATE();
        PRINT('>>Loading time:' + CAST(DATEDIFF(SECOND,@Endtime,@Starttime) AS NVARCHAR));

        SET @batch_stop_time = GETDATE()
        PRINT('>>Loading time:' + CAST(DATEDIFF(SECOND,@batch_stop_time,@batch_start_time) AS NVARCHAR))
    END TRY
    BEGIN CATCH
        print('An error has occured:');
		print('error message:' + '' + error_message());
		print('error line:' +' ' + cast(error_line() as NVARCHAR));
		print('error number:' + ' ' + + cast(error_number() as NVARCHAR))
    END CATCH
END



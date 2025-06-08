/*
  ========================================
  Stored procedure : Load Bronze Layer
  ========================================
  This store procedure loads data into the 'bronze' schama from external csv files. 
  It performs the following actions : 
    1. Truncates the bronzes tables before loading data
    2. Uses the 'BULK INSERT' command to load data from csv files to bronze tables

  Parameters:
    None.

  Execution : 
    EXEC bronze.load_bronze_data;

*/


CREATE PROCEDURE bronze.load_bronze_data AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @start_batch_load_time DATETIME, @end_batch_load_time DATETIME
    BEGIN TRY
        PRINT '===============================================';
        PRINT 'Load Bronze layer';
        PRINT '===============================================';

        SET @start_batch_load_time = GETDATE();

        -- crm_sales_details
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table : bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Inserting data into : bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM '/var/opt/mssql/datasets/source_crm/sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';

        PRINT '------------------------------------------------------';

        -- crm_prd_info
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table : bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;
        
        PRINT '>> Inserting data into : bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM '/var/opt/mssql/datasets/source_crm/prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';

        PRINT '------------------------------------------------------';

        -- crm_cust_info
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table : bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> Inserting data into : bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM '/var/opt/mssql/datasets/source_crm/cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';

        PRINT '------------------------------------------------------';

        -- erp_cust_az12
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table : bronze.crm_prd_info';
        TRUNCATE TABLE bronze.erp_cust_az12;
        
        PRINT '>> Inserting data into : bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM '/var/opt/mssql/datasets/source_erp/cust_az12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';

        PRINT '------------------------------------------------------';


        -- erp.loc_A101
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table : bronze.erp_loc_A101';
        TRUNCATE TABLE bronze.erp_loc_A101;

        PRINT '>> Inserting data into : bronze.erp_loc_A101';
        BULK INSERT bronze.erp_loc_A101
        FROM '/var/opt/mssql/datasets/source_erp/loc_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';

        PRINT '------------------------------------------------------';

        -- erp_px_cat_g1v2
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table : bronze.erp_px_cat_g1v2';    
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> Inserting data into : bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM '/var/opt/mssql/datasets/source_erp/px_cat_g1v2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';

        SET @end_batch_load_time = GETDATE();

        PRINT '=======================================================';
        PRINT '- Load completing -';
        PRINT '>> Batch load duration : ' + CAST(DATEDIFF(second, @start_batch_load_time, @start_batch_load_time) AS NVARCHAR) + 'seconds';
        PRINT '=======================================================';

    END TRY
    BEGIN CATCH
        PRINT '=====================================';
        PRINT 'ERROR OCCURED DURING LOADING';
        PRINT '=====================================';
    END CATCH
END;
GO

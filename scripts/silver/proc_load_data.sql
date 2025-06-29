/*
  ========================================
  Stored procedure : Load Bronze Layer
  ========================================
  This store procedure loads data into the 'silver' schama from external bronze layer. 
  It performs few transformation (cleaning).

  Parameters:
    None.

  Execution : 
    EXEC silver.load_silver_data;

*/

CREATE OR ALTER PROCEDURE silver.load_silver_data AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @start_batch_load_time DATETIME, @end_batch_load_time DATETIME
    BEGIN TRY
            
        PRINT '============================================';
        PRINT 'LOADING SILVER LAYER after transformation';
        PRINT '============================================';

        /*
            Transform customer info
            - Fixing the spaces missing issues in first and last name
            - Clean the marital status. Adding a descriptive name for values
            - Clean the gender values too. Adding descriptive name for better description

        */
        SET @start_batch_load_time = GETDATE();

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table : silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;
        PRINT '>> Inserting data into : silver.crm_cust_info';
        INSERT INTO silver.crm_cust_info (
            cst_id, 
            cst_key, 
            cst_firstname,
            cst_lastname, 
            cst_marital_status, 
            cst_gndr, 
            cst_create_date)

        SELECT 
        cst_id, 
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname) AS cst_lastname,
        CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END cst_material_status,
        CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END cst_gndr,
        cst_create_date
        FROM (
            SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
            FROM bronze.crm_cust_info 
        ) t WHERE flag_last = 1 OR cst_id IS NOT NULL;
        SET @end_time = GETDATE();
        PRINT '>> Load crm customer info into silver layer duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
        PRINT '-------------------------------------------------';


        /*
            Transform product info
            - Replace format of category values
            - Adding description name for product line
            - Fix the date problem. 
        */
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table : silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;
        PRINT '>> Inserting data into : silver.crm_prd_info';
        INSERT INTO silver.crm_prd_info (
            prd_id,
            cat_id, 
            prd_key, 
            prd_nm, 
            prd_cost, 
            prd_line, 
            prd_start_dt, 
            prd_end_dt  
        )
        SELECT 
                prd_id,
                REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
                SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
                prd_nm,
                ISNULL(prd_cost, 0) AS prd_cost,
                CASE UPPER(TRIM(prd_line))
                    WHEN 'S' THEN 'Other Sales'
                    WHEN 'R' THEN 'Road'
                    WHEN 'M' THEN 'Mountain'
                    WHEN 'T' THEN 'Touring'
                    ELSE 'n/a'
                END AS prd_line,
                CAST (prd_start_dt AS DATE) AS prd_start_dt,
                CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
        FROM bronze.crm_prd_info;
        SET @end_time = GETDATE();
        PRINT '>> Load crm product info into silver layer duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
        PRINT '-------------------------------------------------';


        /*
            Transform sales details informations
            - Fix the sales order date, due date and ship date
            - Fix the inconherence in the relationship between sales, quantity and price sales
        */
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table : silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;
        PRINT '>> Inserting data into : silver.crm_sales_details';
        INSERT INTO silver.crm_sales_details (
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        )
        SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL 
            ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
        END AS sls_order_dt,

        CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL 
            ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
        END AS sls_ship_dt,

        CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL 
            ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
        END AS sls_due_dt,

        CASE WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != ABS(sls_price) * sls_quantity 
            THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
        END AS sls_sales,
        sls_quantity,

        CASE WHEN sls_price IS NULL OR sls_price <= 0
            THEN sls_sales / NULLIF(sls_quantity, 0)
            ELSE sls_price
        END AS sls_price
        FROM bronze.crm_sales_details
        SET @end_time = GETDATE();
        PRINT '>> Load crm sales details data into silver layer duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
        PRINT '-------------------------------------------------';


        /*
            Transform erp cust info
            - Transform the custumer info
            - Set future birthday to NULL
            - Set descriptive name for gender column
        */
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table : silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;
        PRINT '>> Inserting data into : silver.erp_cust_az12';
        INSERT INTO silver.erp_cust_az12 (
            cid, 
            bdate, 
            gen
        )
        SELECT DISTINCT
        CASE WHEN cid LIKE 'NA%' THEN SUBSTRING(cid, 4, LEN(cid))
            ELSE cid
        END AS cid, 

        CASE WHEN bdate > GETDATE() THEN NULL
            ELSE bdate
        END AS bdate,

        CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'n/a'
        END AS gen
        FROM bronze.erp_cust_az12;
        SET @end_time = GETDATE();
        PRINT '>> Load erp custumer info into silver layer duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
        PRINT '-------------------------------------------------';

        /*
            Transform erp location customer
            - Handle id format
            - Handle country name
        */
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table : silver.erp_loc_A101';
        TRUNCATE TABLE silver.erp_loc_A101;
        PRINT '>> Inserting data into : silver.erp_loc_A101';
        INSERT INTO silver.erp_loc_A101 (
            cid, 
            cntry
        )
        SELECT
        REPLACE(cid, '-', '') cid,
        CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(cntry)
        END AS cntry
        FROM bronze.erp_loc_A101;
        SET @end_time = GETDATE();
        PRINT '>> Load ERP location data into silver layer duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
        PRINT '-------------------------------------------------';


        /*
            No transformations needed.
            Data is clean
        */
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table : silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;
        PRINT '>> Inserting data into : silver.erp_px_cat_g1v2';
        INSERT INTO silver.erp_px_cat_g1v2 (
            id, 
            cat,
            subcat,
            maintenance
        )
        SELECT 
        id,
        cat,
        subcat,
        maintenance
        FROM bronze.erp_px_cat_g1v2
        SET @end_time = GETDATE();
        PRINT '>> Load ERP category into silver layer duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
        PRINT '-------------------------------------------------';

        SET @end_batch_load_time = GETDATE();
        PRINT '===============================================';
        PRINT '>> Total duration : ' + CAST(DATEDIFF(SECOND, @start_batch_load_time, @end_batch_load_time) AS NVARCHAR) + 'seconds';
        PRINT '===============================================';
    END TRY
    BEGIN CATCH
        PRINT '==========================================';
        PRINT '>> Error during loading bronze layer';
        PRINT '>> Error message : ' + ERROR_MESSAGE();
        PRINT '>> Error number : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT '>> Error state : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '==========================================';
    END CATCH
END

/*
  ========================================
  Stored procedure : Load Gold Layer
  ========================================
  This stored procedure loads data into the 'gold' schema from the silver layer.
  It performs dimensional modeling and creates analytics-ready datasets.

  Parameters:
    None.

  Execution : 
    EXEC gold.load_gold_data;
*/

CREATE OR ALTER PROCEDURE gold.load_gold_data AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @start_batch_load_time DATETIME, @end_batch_load_time DATETIME
    BEGIN TRY
        PRINT '============================================';
        PRINT 'LOADING GOLD LAYER - ANALYTICS DATA';
        PRINT '============================================';

        SET @start_batch_load_time = GETDATE();

        -- Load dim_customer
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table : gold.dim_customer';
        TRUNCATE TABLE gold.dim_customer;
        
        PRINT '>> Loading gold.dim_customer';
        INSERT INTO gold.dim_customer (
            customer_id,
            first_name,
            last_name,
            marital_status,
            gender,
            create_date
        )
        SELECT DISTINCT
            cst_id,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date
        FROM silver.crm_cust_info;

        -- Load dim_product
        PRINT '>> Truncating Table : gold.dim_product';
        TRUNCATE TABLE gold.dim_product;
        
        PRINT '>> Loading gold.dim_product';
        INSERT INTO gold.dim_product (
            product_id,
            product_name,
            category,
            sub_category,
            unit_price
        )
        SELECT DISTINCT
            p.prd_id,
            p.prd_name,
            pc.category_name,
            pc.sub_category,
            p.unit_price
        FROM silver.crm_prd_info p
        JOIN silver.erp_product_categories pc ON p.category_id = pc.category_id;

        -- Load dim_date
        PRINT '>> Loading gold.dim_date';
        -- Only if not already loaded (date dimension is usually loaded once)
        IF NOT EXISTS (SELECT 1 FROM gold.dim_date)
        BEGIN
            ;WITH DateCTE AS (
                SELECT CAST('2020-01-01' AS DATE) AS full_date
                UNION ALL
                SELECT DATEADD(DAY, 1, full_date)
                FROM DateCTE
                WHERE full_date < '2025-12-31'
            )
            INSERT INTO gold.dim_date (
                date_key,
                full_date,
                year,
                quarter,
                month,
                month_name,
                day,
                day_of_week,
                day_name,
                is_weekend
            )
            SELECT 
                CONVERT(INT, CONVERT(VARCHAR, full_date, 112)) as date_key,
                full_date,
                YEAR(full_date) as year,
                DATEPART(QUARTER, full_date) as quarter,
                MONTH(full_date) as month,
                DATENAME(MONTH, full_date) as month_name,
                DAY(full_date) as day,
                DATEPART(WEEKDAY, full_date) as day_of_week,
                DATENAME(WEEKDAY, full_date) as day_name,
                CASE WHEN DATEPART(WEEKDAY, full_date) IN (1, 7) THEN 1 ELSE 0 END as is_weekend
            FROM DateCTE
            OPTION (MAXRECURSION 3000);
        END

        -- Load fact_sales
        PRINT '>> Truncating Table : gold.fact_sales';
        TRUNCATE TABLE gold.fact_sales;
        
        PRINT '>> Loading gold.fact_sales';
        INSERT INTO gold.fact_sales (
            customer_key,
            product_key,
            date_key,
            quantity,
            unit_price,
            total_amount
        )
        SELECT 
            c.customer_key,
            p.product_key,
            CONVERT(INT, CONVERT(VARCHAR, s.sale_date, 112)) as date_key,
            s.quantity,
            s.unit_price,
            s.quantity * s.unit_price as total_amount
        FROM silver.crm_sales_details s
        JOIN gold.dim_customer c ON s.customer_id = c.customer_id
        JOIN gold.dim_product p ON s.product_id = p.product_id;

        SET @end_batch_load_time = GETDATE();
        PRINT '>> Total Load Duration: ' + CAST(DATEDIFF(second, @start_batch_load_time, @end_batch_load_time) AS NVARCHAR) + ' seconds';
        PRINT '============================================';
    END TRY
    BEGIN CATCH
        PRINT 'Error loading gold layer:';
        PRINT ERROR_MESSAGE();
        THROW;
    END CATCH;
END;

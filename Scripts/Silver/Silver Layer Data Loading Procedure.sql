
/*
	This script truncates tables in DataWarehouse silver layer and inserts data into them
	from bronze layer. The sources for the datasets are CRM and ERP from bronze layer.

	WARNING: This script does truncate the tables, therefore if any adjustments have been made
			 They will be lost.
*/
	
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN

	DECLARE @start_time DATETIME, @end_time DATETIME
	DECLARE @silver_start_time DATETIME, @silver_end_time DATETIME
	BEGIN TRY
	SET @silver_start_time = GETDATE();

	PRINT '======================================';
	PRINT 'Loading silver Layer';
	PRINT '======================================';

	PRINT '======================================';
	PRINT 'Loading CRM Tables';
	PRINT '======================================';
	SET @start_time = GETDATE();


	/*
		This SQL script inserts customer information into the silver.crm_cust_info table.
		It selects data from the bronze.crm_cust_info table, applying several transformations:
		Deduplication: It ensures only the most recent record for each cst_id is selected from the bronze layer, based on cst_create_date.
		Data Cleaning: It removes leading/trailing spaces from cst_firstname and cst_lastname.
		Standardization: It converts cst_marital_status and cst_gndr codes (e.g., 'S', 'M', 'F') into more descriptive text (e.g., 'Single', 'Married', 'Female').
	*/


	PRINT '>> Truncating Table: silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;
	PRINT '>> Inserting Data Into: silver.crm_cust_info';

	INSERT INTO silver.crm_cust_info(
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
		END cst_marital_status,
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			 ELSE 'n/a'
		END cst_gndr,
		cst_create_date
	FROM
		(

	SELECT 
		*,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM 
		bronze.crm_cust_info
		)t WHERE flag_last = 1;

	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '----------------------------'



	/*
		 This script cleans, transforms, and enriches product information by extracting category IDs, 
		 reformatting product keys, handling null costs, mapping product line codes to descriptive names, 
		 and calculating effective end dates for product validity periods.
	*/

	SET @start_time = GETDATE();
	PRINT '>> Truncating Table: silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;
	PRINT '>> Inserting Data Into: silver.crm_prd_info';

	INSERT INTO silver.crm_prd_info
	(
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
		REPLACE(SUBSTRING(prd_key, 1 ,5), '-','_') AS cat_id,
		SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
		prd_nm,
		ISNULL(prd_cost,0) AS prd_cost,
		CASE UPPER(TRIM(prd_line))
			 WHEN 'R' THEN 'Road'
			 WHEN 'M' THEN 'Mountain'
			 WHEN 'S' THEN 'Other Sales'
			 WHEN 'T' THEN 'Touring'
		ELSE 'N/A'
		END AS prd_line,
		CAST(prd_start_dt AS DATE) AS prd_start_dt ,
		CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS DATE) AS prd_end_dt
	FROM bronze.crm_prd_info

	SET @end_time = GETDATE();
	PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '---------------------------';

	/*
		This SQL script inserts and transforms data from the bronze.crm_sales_details table into the silver.crm_sales_details table. 
		It performs data cleaning and validation on date fields and ensures consistency and validity for sales, quantity, 
		and price values before insertion.
	*/

	SET @start_time = GETDATE();

	PRINT '>> Truncating Table: silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details
	PRINT '>> Inserting Data into: silver.crm_sales_details';

	INSERT INTO silver.crm_sales_details
	(
		sls__ord_num,
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
		sls__ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_order_dt AS VARCHAR)AS DATE)
		END AS sls_order_dt,
		CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_ship_dt AS VARCHAR)AS DATE)
		END AS sls_ship_dt,
		CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_due_dt AS VARCHAR)AS DATE)
		END AS sls_due_dt,
		CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
			 ELSE sls_sales 
		END AS sls_sales,
		CASE WHEN sls_price IS NULL OR sls_price <= 0
				THEN sls_sales / NULLIF(sls_quantity,0)
			ELSE sls_price
		END AS sls_price,
		sls_quantity
	FROM
		bronze.crm_sales_details;

	SET @end_time = GETDATE();
	PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '---------------------------';

	/*
		This SQL script populates the silver.erp_cust_az12 table with customer information. 
		It cleanses and transforms data from the bronze.erp_cust_az12 source, specifically:
		Removing 'NAS' prefix from cid values. Setting future bdate values to NULL.
		Standardizing gen values to 'Female', 'Male', or 'N/A'.	
	*/

	SET @start_time = GETDATE();

	PRINT '>> Truncating Table: silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12
	PRINT '>> Inserting Data into: silver.erp_cust_az12';

	INSERT INTO silver.erp_cust_az12
	(
		cid,
		bdate,
		gen
	)

	SELECT
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
			 ELSE cid
		END AS cid,
		CASE WHEN bdate > GETDATE() THEN NULL
			 ELSE bdate
		END AS bdate,
		CASE 
			WHEN  UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
			WHEN  UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
			ELSE 'N/A'
		END AS gen
	FROM
		bronze.erp_cust_az12

	SET @end_time = GETDATE();
	PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '---------------------------';

		/*
		This SQL script populates the silver.erp_loc_a101 table with customer location information. 
		It cleanses and transforms data from the bronze.erp_loc_a101 source by:
		Removing hyphens from cid values.
		Standardizing cntry values (e.g., 'USA'/'US' to 'United States', 'DE' to 'Germany', and empty/null to 'N/A').
	*/

	SET @start_time = GETDATE();

	PRINT '>> Truncating Table: silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101
	PRINT '>> Inserting Data into: silver.erp_loc_a101';
	INSERT INTO silver.erp_loc_a101
	(
		cid,
		cntry
	)

	SELECT
		REPLACE(cid,'-','') AS cid,
			CASE 
			WHEN TRIM(cntry) IN ('USA','US') THEN 'United States'
			WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
			ELSE TRIM(cntry)
		END AS cntry
	FROM
		bronze.erp_loc_a101

	SET @end_time = GETDATE();
	PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
	PRINT '---------------------------';


	/*
		This SQL script loads data from the table bronze.erp_px_cat_g1v2 into silver layer of the
		data warehouse. No transformations are applied as the data is already clean.
	*/

	SET @start_time = GETDATE();

	PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2
	PRINT '>> Inserting Data into: silver.erp_px_cat_g1v2';
	INSERT INTO silver.erp_px_cat_g1v2
	(
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
	FROM
		bronze.erp_px_cat_g1v2;

	SET @end_time = GETDATE();
	PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR);
	PRINT '---------------------------';

	SET @silver_end_time = GETDATE();
	PRINT '======================================';
	PRINT 'Silver Layer Load Duration: ' + CAST(DATEDIFF(second,@silver_start_time, @silver_end_time) AS NVARCHAR) + ' seconds';
	PRINT '======================================';

	END TRY

	BEGIN CATCH
		PRINT '======================================================';
		PRINT 'ERROR OCCURED DURING LOADING OF Silver LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '======================================================';
	END CATCH


END


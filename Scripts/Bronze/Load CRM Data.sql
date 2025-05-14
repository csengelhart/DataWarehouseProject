/*
	This script truncates tables in DataWarehouse and bulk inserts data into them
	from filepath. The sources for the datasets are CRM and ERP. The script measures and outputs the time
	it takes for each table to load data and also how much time it takes the bronze layer to load.
	The script uses a try-catch block to catch any error, if an error is caught the error message is 
	printed to the console.
	
	WARNING: This script does truncate the tables, therefore if any adjustments have been made
			 They will be lost.
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME
	DECLARE @bronze_start_time DATETIME, @bronze_end_time DATETIME
	BEGIN TRY
	SET @bronze_start_time = GETDATE();

	PRINT '======================================';
	PRINT 'Loading Bronze Layer';
	PRINT '======================================';

	PRINT '======================================';
	PRINT 'Loading CRM Tables';
	PRINT '======================================';
	 
	SET @start_time = GETDATE();
	PRINT ' >> Truncating Table: bronze.crm_cust_info';
	TRUNCATE TABLE bronze.crm_cust_info;

	PRINT '>> Inserting Data Into Table: bronze.crm_cust_info';
	-- Load data into crm_cust_info data table
	BULK INSERT bronze.crm_cust_info
	FROM 'C:\Temp\datasets\source_crm\cust_info.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '----------------------------'

	
	SET @start_time = GETDATE();
	PRINT ' >> Truncating Table: bronze.crm_prd_info';
	TRUNCATE TABLE bronze.crm_prd_info;

	PRINT '>> Inserting Data Into Table: bronze.crm_prd_info';
	-- Load data into crm_prd_info data table
	BULK INSERT bronze.crm_prd_info
	FROM 'C:\Temp\datasets\source_crm\prd_info.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR);
	PRINT '---------------------------';


	SET @start_time = GETDATE();
	PRINT ' >> Truncating Table: bronze.crm_sales_details';
	TRUNCATE TABLE bronze.crm_sales_details;

	PRINT '>> Inserting Data Into Table: bronze.crm_sales_details';
	-- Load data into crm_cust_sales_details table
	BULK INSERT bronze.crm_sales_details
	FROM 'C:\Temp\datasets\source_crm\sales_details.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '----------------------------'


	PRINT '======================================';
	PRINT 'Loading ERP Tables';
	PRINT '======================================';

	SET @start_time = GETDATE();
	PRINT ' >> Truncating Table: bronze.erp_cust_az12';
	TRUNCATE TABLE bronze.erp_cust_az12;

	PRINT '>> Inserting Data Into Table: bronze.erp_cust_az12';
	-- Load data in to erp_cust_az12 table
	BULK INSERT bronze.erp_cust_az12
	FROM 'C:\Temp\datasets\source_erp\CUST_AZ12.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR);
	PRINT '-------------------------------------';

	SET @start_time = GETDATE();
	PRINT ' >> Truncating Table: bronze.erp_loc_a101';
	TRUNCATE TABLE bronze.erp_loc_a101;

	PRINT '>> Inserting Data Into Table: bronze.erp_loc_a101';
	-- Load data into erp_loc_a101 table
	BULK INSERT bronze.erp_loc_a101
	FROM 'C:\Temp\datasets\source_erp\LOC_A101.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	SET @end_time = GETDATE();
	PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR);
	PRINT '---------------------------------------------';

	SET @start_time = GETDATE();
	PRINT ' >> Truncating Table: bronze.erp_px_cat_g1v2';
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;

	PRINT '>> Inserting Data Into Table: bronze.erp_px_cat_g1v2';
	-- Load data into erp_px_cat_g1v2 table
	BULK INSERT bronze.erp_px_cat_g1v2
	FROM 'C:\Temp\datasets\source_erp\PX_CAT_G1V2.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	
	SET @end_time = GETDATE();
	PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR)

	SET @bronze_end_time = GETDATE();
	PRINT '======================================';
	PRINT 'Bronze Layer Load Duration: ' + CAST(DATEDIFF(second,@bronze_start_time, @bronze_end_time) AS NVARCHAR) + ' seconds';
	PRINT '======================================';

	END TRY

	BEGIN CATCH
		PRINT '======================================================';
		PRINT 'ERROR OCCURED DURING LOADING OF BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '======================================================';
	END CATCH
END
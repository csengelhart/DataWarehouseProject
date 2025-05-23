/*
	
	This script performs a series of data quality checks on the silver.crm_cust_info table. It identifies potential issues by:
	Verifying Primary Key Integrity: 
	Checking for NULL values and duplicate entries in the cst_id column.
	Detecting Unwanted Spaces: Identifying cst_gndr values that still contain leading or trailing spaces after processing.
	Assessing Data Standardization and Consistency: Displaying distinct values for cst_gndr and cst_marital_status to ensure 
	data has been properly transformed and adheres to expected formats.

*/



-- Check for nulls and duplicates in primary key cst_id
SELECT
	cst_id,
	COUNT(*)
FROM
	silver.crm_cust_info
GROUP BY
	cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;


-- Check for unwanted spaces
SELECT 
	cst_gndr
FROM
	silver.crm_cust_info
WHERE
	cst_gndr!= TRIM(cst_gndr )


-- Data Standardization & Consistency
SELECT 
	DISTINCT cst_gndr
FROM
	silver.crm_cust_info;


SELECT 
	DISTINCT cst_marital_status
FROM
	silver.crm_cust_info;

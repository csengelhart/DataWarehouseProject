/*
	This SQL script focuses on data quality assessment for the silver.erp_cust_az12 table. It includes queries to:
	Retrieve all customer records.
	Identify and report out-of-range birth dates (those before 1924-01-01 or in the future).
	Verify the standardization and consistency of gender data.
*/

SELECT 
	*
FROM
	silver.erp_cust_az12


-- Identify out of range dates
-- bronze results: some bdates in future e.g. year 2050 , 9999 etc
SELECT DISTINCT
	bdate
FROM
	silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();


-- gender data standardization & consistency
SELECT
	DISTINCT gen
FROM
	silver.erp_cust_az12;


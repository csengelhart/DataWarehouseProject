/*
 This script performs a quality check on the Silver layer for table crm_prd_info.
 Checks include null values in primary key, unwanted spaces in prd_name, checking prod_cost for nulls
 or negative values, and checking for invalid start and end dates.
*/

SELECT
	*
FROM
	silver.crm_prd_info


-- Check For Nulls or Duplicates in Primary Key
SELECT
	prd_id,
	COUNT(*)
FROM
	silver.crm_prd_info
GROUP BY
	prd_id
HAVING
	COUNT(*) > 1 or prd_id IS NULL;


-- check for unwanted spaces in prd_nm
SELECT
	prd_nm
FROM 
	silver.crm_prd_info
WHERE
	prd_nm != TRIM(prd_nm);


-- check prd_cost for nulls and negative numbers
SELECT 
	prd_cost
FROM
	silver.crm_prd_info
WHERE
	prd_cost IS NULL OR prd_cost < 0;


-- check prd_line possible values
SELECT
	DISTINCT prd_line
FROM
	silver.crm_prd_info;

-- Check for invalid date orders
SELECT
	*
FROM
	silver.crm_prd_info
WHERE
	prd_start_dt > prd_end_dt
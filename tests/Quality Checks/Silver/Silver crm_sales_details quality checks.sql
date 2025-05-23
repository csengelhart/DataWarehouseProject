/*
This SQL script performs various data quality checks and validations on the silver.crm_sales_details table, 
as well as some checks against silver.crm_prd_info, silver.crm_cust_info, and silver.crm_sales_details.
it specifically looks for:
Unwanted spaces in sls_ord_num.
Product keys (sls_prd_key) that do not exist in the silver.crm_prd_info table.
Customer IDs (sls_cust_id) that do not exist in the silver.crm_cust_info table.
Invalid order dates where sls_order_dt is after sls_ship_dt or sls_due_dt.
Data consistency issues where sls_sales does not equal sls_price * sls_quantity, or where any of these columns are NULL or less than or equal to 0.
*/


SELECT
	*
FROM
	silver.crm_sales_details;

-- check for unwanted spaces in sls_ord_num
SELECT
	*
FROM
	silver.crm_sales_details
WHERE
	sls__ord_num != TRIM(sls__ord_num)

-- check for prd keys not in crm_prd_info table
-- result: all products in sales found in prd_info table
SELECT
	*
FROM
	silver.crm_sales_details
WHERE sls_prd_key NOT IN
(
SELECT 
	sls_prd_key
FROM
	silver.crm_prd_info
);


-- check for customer ids not in crm_cust_info table
-- result: all customer ids in sales found in cust_info_table
SELECT
	*
FROM
	silver.crm_sales_details
WHERE sls_cust_id NOT IN
(
SELECT 
	sls_cust_id
FROM
	silver.crm_cust_info
);

-- Check for invalid order dates
-- result : No records found 
SELECT
	*
FROM
	silver.crm_sales_details
WHERE
	sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt


-- Check for data consistency between sls_sales, sls_quantity, sls_price
-- sales = quantity * price
-- values cannot be null or less than or equal to 0
SELECT DISTINCT
	sls_sales,
	sls_quantity,
	sls_price
FROM
	silver.crm_sales_details
WHERE
	sls_sales != sls_price * sls_quantity
	OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
	OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
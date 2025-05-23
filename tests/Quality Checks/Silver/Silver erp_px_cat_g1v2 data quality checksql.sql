/*
	This script performs quality checks on silver.erp_px_cat_g1v2 table.
	Quality checks such as checking for unwanted spaces in the fields cat, subcat, and maintenance.
	Also checking data consistency and standardization in each field.
*/

SELECT
	id,
	cat,
	subcat,
	maintenance
FROM
	silver.erp_px_cat_g1v2;

-- Check for unwanted spaces in cat, subcat, maintenance field 
-- bronze result: no unwanted spaces in any fields
SELECT 
	*
FROM
	silver.erp_px_cat_g1v2
WHERE
	cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance);


-- Check data standardization & consistency
-- bronze result: standard and consistent data in both cat, subcat and maintenance fields 
SELECT 
	DISTINCT cat
FROM
	silver.erp_px_cat_g1v2;

SELECT
	DISTINCT subcat
FROM
	silver.erp_px_cat_g1v2;

SELECT
	DISTINCT maintenance
FROM
	silver.erp_px_cat_g1v2;

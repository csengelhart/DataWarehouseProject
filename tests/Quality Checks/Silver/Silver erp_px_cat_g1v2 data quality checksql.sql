SELECT
	id,
	cat,
	subcat,
	maintenance
FROM
	bronze.erp_px_cat_g1v2;

-- Check for unwanted spaces in cat, subcat, maintenance field 
-- bronze result: no unwanted spaces in any fields
SELECT 
	*
FROM
	bronze.erp_px_cat_g1v2
WHERE
	cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance);


-- Check data standardization & consistency
-- bronze result: standard and consistent data in both cat, subcat and maintenance fields 
SELECT 
	DISTINCT cat
FROM
	bronze.erp_px_cat_g1v2;

SELECT
	DISTINCT subcat
FROM
	bronze.erp_px_cat_g1v2;

SELECT
	DISTINCT maintenance
FROM
	bronze.erp_px_cat_g1v2;
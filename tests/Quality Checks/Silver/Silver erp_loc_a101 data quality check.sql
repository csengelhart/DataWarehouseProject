/*
	This script performs essential data quality verification on the silver.erp_loc_a101 table, 
	focusing on cid field integrity (no hyphens) and cntry field consistency (reformatted abbreviations and NULL/empty values standardized to 'N/A').
/*

SELECT
	*
FROM
	bronze.erp_loc_a101


-- ensure no cid values have "-"
SELECT
	cid 
FROM
	silver.erp_loc_a101
WHERE
	cid LIKE '%-%';
	

-- country field data consistency check
-- bronze result: DE, USA, US must be reformatted for higher cardinality
-- silver result: abbreviated countries reformatted and null or empty values updated to N/A
SELECT 
	DISTINCT cntry
FROM
	silver.erp_loc_a101
ORDER BY
	cntry
/*
	=======================================================================================================================
	gold.dim_product View Creation
	=======================================================================================================================
	
	
	This SQL script creates a gold.dim_products view, designed as a dimension table for product information.
	
	The view is constructed by:
	
	Selecting product details from silver.crm_prd_info.
	Generating a unique product_key using a ROW_NUMBER() window function, ordered by prd_start_dt and prd_key to ensure consistent numbering.
	Enriching product attributes by joining with silver.erp_px_cat_g1v2 to include category, subcategory, and maintenance information.
	Filtering out historical product data by excluding records where prd_end_dt is not NULL, retaining only current product information.
	Renaming columns for improved readability and alignment with the gold layer's naming conventions.
*/


CREATE VIEW gold.dim_products AS
SELECT
	ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS product_category,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance,
	pn.prd_cost AS cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date 
	
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL -- Filter out all historial data 


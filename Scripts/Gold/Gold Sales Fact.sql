/*	
	========================================================================================================
	gold.fact_sales view creation
	========================================================================================================
	
	This SQL script creates a gold.fact_sales view, establishing a fact table for sales transactions.

	This view:

	Selects detailed sales transaction data from silver.crm_sales_details.
	Integrates product_key from the gold.dim_products dimension table.
	Integrates customer_key from the gold.dim_customers dimension table.
	Renames various sales-related columns for clarity and consistency within the gold layer, making the data ready for analytical purposes.
*/

CREATE VIEW gold.fact_sales AS
SELECT
	sd.sls__ord_num AS order_number,
	pr.product_key,
	cu.customer_key,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS shipping_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id




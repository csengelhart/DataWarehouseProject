/*
	=============================================================================================================
	Gold.dim_customers View Creation
	=============================================================================================================

	This SQL script creates a gold.dim_customers view, which serves as a dimension table for customer information.
	
	The view is populated by:
	Selecting core customer data from silver.crm_cust_info.
	Generating a unique customer_key using a ROW_NUMBER() window function.
	Enriching the customer data by joining with silver.erp_cust_az12  and silver.erp_loc_a101.
	Implementing a data consolidation rule for gender, prioritizing information from crm_cust_info and falling back to erp_cust_az12 if the CRM value is 'n/a'.
	Renaming columns for clarity and consistency within the gold layer.
*/

CREATE VIEW gold.dim_customers AS
SELECT
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	cst_firstname AS first_name,
	cst_lastname AS last_name,
	la.cntry AS  country,
	cst_marital_status AS marital_status,
	case WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- crm is the master for gender info
		 ELSE COALESCE(ca.gen, 'n/a')
	END AS gender,
	ca.bdate AS birthdate,
	cst_create_date AS create_date
	
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid



/*
	=========================================================================
	DDL Script: Create Silver Tables for ERP Sources
	=========================================================================

	Script Purpose:
	This script creates tables in the 'silver' schema dropping existing tables
	if they already exist.
		Run this script to re-define the DDL structure of 'bronze' Tables

	==========================================================================
*/

IF OBJECT_ID ('silver.erp_loc_a101' , 'U') IS NOT NULL
	DROP TABLE silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101 (
	cid NVARCHAR(50),
	cntry NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
)

IF OBJECT_ID ('silver.erp_cust_az12' , 'U') IS NOT NULL
	DROP TABLE silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12 (
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);


IF OBJECT_ID ('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
	DROP TABLE silver.erp_px_cat_g1v2
CREATE TABLE silver.erp_px_cat_g1v2 (
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

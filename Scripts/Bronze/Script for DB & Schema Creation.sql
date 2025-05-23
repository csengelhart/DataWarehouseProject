
/*
	=========================================
	Create Database and Schema
	=========================================

	Script Purpose
	This script creates a new database named 'DataWarehouse' after checking if it already exists.
	If the database exists, it is dropped and recreated. Additionally the script creates three schemas within the
	database: bronze, silver, gold


	WARNING:
	Running this script will drop your existing 'DataWarehouse' database if it exists.
	All data in the database will be permanently deleted. Proceed with caution.
*/



USE master
GO

-- Drop and recreate the DataWarehouse database
IF EXISTS ( SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO
	
-- Create the DataWarehouse database
CREATE DATABASE DataWarehouse
GO

USE DataWarehouse

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO




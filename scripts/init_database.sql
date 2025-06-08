/*
=========
Create database ans schemas
=========
Description : 
  This script creates a new database named Datawarehouse after checking if it already exists. 
  If this database exists, it's dropped and recreated. Additionnaly, the script sets up three schemas within the database bronze, silver and gold.

Warning : 
  Proceed with caution and ensure you have proper backups before running this script because it'll dropped the entire database if it exists
*/

USE master;
GO 

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name= 'DataWarehouse')
BEGIN
  ALTER DATABASE Datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
  DROP DATABASE DataWarehouse;
END;
GO

-- create the database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO

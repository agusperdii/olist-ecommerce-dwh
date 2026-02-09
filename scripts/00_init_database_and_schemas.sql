/*
============================================================
Database and Schema Initialization
============================================================
Script Purpose:
    - Create the DataWarehouse database
    - Initialize the bronze, silver, and gold schemas
    - Intended for initial database setup or re-initialization

WARNING:
    This script will DROP and RECREATE the DataWarehouse database
    and initialize all required schemas.
    ALL existing data will be permanently deleted.

    DO NOT run this script on a production environment
    or on a database that already contains important data.
============================================================
*/

USE master;
GO

-- Drop the existing 'DataWarehouse' database if it exists
-- WARNING: This will permanently DROP the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

-- Use the 'DataWarehouse' database
USE DataWarehouse;
GO

-- Create Bronze schema
CREATE SCHEMA bronze;
GO

-- Create Silver schema
CREATE SCHEMA silver;
GO

-- Create Gold schema
CREATE SCHEMA gold;
GO

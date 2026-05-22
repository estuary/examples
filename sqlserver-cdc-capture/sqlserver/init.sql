-- Use master database to create a new database
USE master;
GO

-- Create a new database if it doesn't already exist
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'SampleDB')
BEGIN
    CREATE DATABASE SampleDB;
END
GO

-- Switch to the new database
USE SampleDB;
GO

-- Enable CDC for the database if it's not already enabled
IF NOT EXISTS (SELECT name FROM sys.databases WHERE is_cdc_enabled = 1 AND name = N'SampleDB')
BEGIN
    EXEC sys.sp_cdc_enable_db;
END
GO

-- Create login if it doesn't already exist
IF NOT EXISTS (SELECT name FROM sys.server_principals WHERE name = N'flow_capture')
BEGIN
    CREATE LOGIN flow_capture WITH PASSWORD = 'Secretsecret1';
END
GO

-- Create user if it doesn't already exist
IF NOT EXISTS (SELECT name FROM sys.database_principals WHERE name = N'flow_capture')
BEGIN
    CREATE USER flow_capture FOR LOGIN flow_capture;
END
GO

-- Grant the user permissions on the CDC schema and schemas with data
GRANT SELECT ON SCHEMA::dbo TO flow_capture;
GRANT SELECT ON SCHEMA::cdc TO flow_capture;
GRANT VIEW DATABASE STATE TO flow_capture;
GO

-- Create the sales table if it doesn't already exist
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'sales')
BEGIN
    CREATE TABLE dbo.sales (
        sale_id INT IDENTITY(1,1) PRIMARY KEY,
        product_id INT NOT NULL,
        customer_id INT NOT NULL,
        sale_date DATETIME NOT NULL,
        quantity INT NOT NULL,
        unit_price FLOAT NOT NULL,
        total_price FLOAT NOT NULL
    );
END
GO

-- Enable CDC on the sales table if it's not already enabled
IF NOT EXISTS (SELECT * FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('dbo.sales'))
BEGIN
    EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'sales', @role_name = 'flow_capture';
END
GO

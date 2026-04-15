-- ============================================================
-- EIA860_AlterTables.sql
-- Updates tables to match exact column names from 2023 files
-- Run this in SSMS before running the updated scripts
-- ============================================================
USE YOUR_DATABASE_NAME;
GO

-- ============================================================
-- Fix Utility Table - remove Phone column (does not exist in file)
-- ============================================================
IF EXISTS (SELECT * FROM sys.columns 
           WHERE object_id = OBJECT_ID('EIA.EIA860_UtilityData') AND name = 'Phone')
BEGIN
    ALTER TABLE EIA.EIA860_UtilityData DROP COLUMN Phone;
    PRINT 'Removed Phone column from EIA860_UtilityData';
END

IF EXISTS (SELECT * FROM sys.columns 
           WHERE object_id = OBJECT_ID('EIA.EIA860_UtilityData_Staging') AND name = 'Phone')
BEGIN
    ALTER TABLE EIA.EIA860_UtilityData_Staging DROP COLUMN Phone;
    PRINT 'Removed Phone column from EIA860_UtilityData_Staging';
END
GO

-- ============================================================
-- Fix Wind Table - remove columns that don't exist, add new ones
-- ============================================================
-- Remove TurbineRatedCapacityMW (does not exist in file)
IF EXISTS (SELECT * FROM sys.columns 
           WHERE object_id = OBJECT_ID('EIA.EIA860_WindData') AND name = 'TurbineRatedCapacityMW')
BEGIN
    ALTER TABLE EIA.EIA860_WindData DROP COLUMN TurbineRatedCapacityMW;
    PRINT 'Removed TurbineRatedCapacityMW from EIA860_WindData';
END

IF EXISTS (SELECT * FROM sys.columns 
           WHERE object_id = OBJECT_ID('EIA.EIA860_WindData_Staging') AND name = 'TurbineRatedCapacityMW')
BEGIN
    ALTER TABLE EIA.EIA860_WindData_Staging DROP COLUMN TurbineRatedCapacityMW;
    PRINT 'Removed TurbineRatedCapacityMW from EIA860_WindData_Staging';
END

-- Remove RotorDiameter (does not exist in file)
IF EXISTS (SELECT * FROM sys.columns 
           WHERE object_id = OBJECT_ID('EIA.EIA860_WindData') AND name = 'RotorDiameter')
BEGIN
    ALTER TABLE EIA.EIA860_WindData DROP COLUMN RotorDiameter;
    PRINT 'Removed RotorDiameter from EIA860_WindData';
END

IF EXISTS (SELECT * FROM sys.columns 
           WHERE object_id = OBJECT_ID('EIA.EIA860_WindData_Staging') AND name = 'RotorDiameter')
BEGIN
    ALTER TABLE EIA.EIA860_WindData_Staging DROP COLUMN RotorDiameter;
    PRINT 'Removed RotorDiameter from EIA860_WindData_Staging';
END

-- Add DesignWindSpeed
IF NOT EXISTS (SELECT * FROM sys.columns 
               WHERE object_id = OBJECT_ID('EIA.EIA860_WindData') AND name = 'DesignWindSpeed')
BEGIN
    ALTER TABLE EIA.EIA860_WindData ADD DesignWindSpeed NVARCHAR(50);
    PRINT 'Added DesignWindSpeed to EIA860_WindData';
END

IF NOT EXISTS (SELECT * FROM sys.columns 
               WHERE object_id = OBJECT_ID('EIA.EIA860_WindData_Staging') AND name = 'DesignWindSpeed')
BEGIN
    ALTER TABLE EIA.EIA860_WindData_Staging ADD DesignWindSpeed NVARCHAR(50);
    PRINT 'Added DesignWindSpeed to EIA860_WindData_Staging';
END
GO

-- ============================================================
-- Fix Solar Table - remove SolarTechnology (split into booleans in file)
-- ============================================================
IF EXISTS (SELECT * FROM sys.columns 
           WHERE object_id = OBJECT_ID('EIA.EIA860_SolarData') AND name = 'SolarTechnology')
BEGIN
    ALTER TABLE EIA.EIA860_SolarData DROP COLUMN SolarTechnology;
    PRINT 'Removed SolarTechnology from EIA860_SolarData';
END

IF EXISTS (SELECT * FROM sys.columns 
           WHERE object_id = OBJECT_ID('EIA.EIA860_SolarData_Staging') AND name = 'SolarTechnology')
BEGIN
    ALTER TABLE EIA.EIA860_SolarData_Staging DROP COLUMN SolarTechnology;
    PRINT 'Removed SolarTechnology from EIA860_SolarData_Staging';
END
GO

-- ============================================================
-- Fix Storage Table - StorageTechnology -> StorageTechnology1-4
-- ============================================================
IF EXISTS (SELECT * FROM sys.columns 
           WHERE object_id = OBJECT_ID('EIA.EIA860_StorageData') AND name = 'StorageTechnology')
BEGIN
    ALTER TABLE EIA.EIA860_StorageData DROP COLUMN StorageTechnology;
    PRINT 'Removed StorageTechnology from EIA860_StorageData';
END

IF EXISTS (SELECT * FROM sys.columns 
           WHERE object_id = OBJECT_ID('EIA.EIA860_StorageData_Staging') AND name = 'StorageTechnology')
BEGIN
    ALTER TABLE EIA.EIA860_StorageData_Staging DROP COLUMN StorageTechnology;
    PRINT 'Removed StorageTechnology from EIA860_StorageData_Staging';
END

-- Add StorageTechnology1-4
IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('EIA.EIA860_StorageData') AND name = 'StorageTechnology1')
    ALTER TABLE EIA.EIA860_StorageData ADD StorageTechnology1 NVARCHAR(100);
IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('EIA.EIA860_StorageData') AND name = 'StorageTechnology2')
    ALTER TABLE EIA.EIA860_StorageData ADD StorageTechnology2 NVARCHAR(100);
IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('EIA.EIA860_StorageData') AND name = 'StorageTechnology3')
    ALTER TABLE EIA.EIA860_StorageData ADD StorageTechnology3 NVARCHAR(100);
IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('EIA.EIA860_StorageData') AND name = 'StorageTechnology4')
    ALTER TABLE EIA.EIA860_StorageData ADD StorageTechnology4 NVARCHAR(100);

IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('EIA.EIA860_StorageData_Staging') AND name = 'StorageTechnology1')
    ALTER TABLE EIA.EIA860_StorageData_Staging ADD StorageTechnology1 NVARCHAR(100);
IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('EIA.EIA860_StorageData_Staging') AND name = 'StorageTechnology2')
    ALTER TABLE EIA.EIA860_StorageData_Staging ADD StorageTechnology2 NVARCHAR(100);
IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('EIA.EIA860_StorageData_Staging') AND name = 'StorageTechnology3')
    ALTER TABLE EIA.EIA860_StorageData_Staging ADD StorageTechnology3 NVARCHAR(100);
IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('EIA.EIA860_StorageData_Staging') AND name = 'StorageTechnology4')
    ALTER TABLE EIA.EIA860_StorageData_Staging ADD StorageTechnology4 NVARCHAR(100);

PRINT 'Added StorageTechnology1-4 columns';
GO

-- ============================================================
-- Fix MultiFuel Table - AltEnergySource -> CofireEnergySource
-- ============================================================
IF EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('EIA.EIA860_MultiFuelData') AND name = 'AltEnergySource1')
    EXEC sp_rename 'EIA.EIA860_MultiFuelData.AltEnergySource1', 'CofireEnergySource1', 'COLUMN';
IF EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('EIA.EIA860_MultiFuelData') AND name = 'AltEnergySource2')
    EXEC sp_rename 'EIA.EIA860_MultiFuelData.AltEnergySource2', 'CofireEnergySource2', 'COLUMN';
IF EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('EIA.EIA860_MultiFuelData') AND name = 'AltEnergySource3')
    EXEC sp_rename 'EIA.EIA860_MultiFuelData.AltEnergySource3', 'CofireEnergySource3', 'COLUMN';

IF EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('EIA.EIA860_MultiFuelData_Staging') AND name = 'AltEnergySource1')
    EXEC sp_rename 'EIA.EIA860_MultiFuelData_Staging.AltEnergySource1', 'CofireEnergySource1', 'COLUMN';
IF EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('EIA.EIA860_MultiFuelData_Staging') AND name = 'AltEnergySource2')
    EXEC sp_rename 'EIA.EIA860_MultiFuelData_Staging.AltEnergySource2', 'CofireEnergySource2', 'COLUMN';
IF EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('EIA.EIA860_MultiFuelData_Staging') AND name = 'AltEnergySource3')
    EXEC sp_rename 'EIA.EIA860_MultiFuelData_Staging.AltEnergySource3', 'CofireEnergySource3', 'COLUMN';
PRINT 'Renamed AltEnergySource to CofireEnergySource columns';
GO

-- ============================================================
-- Verify all tables
-- ============================================================
SELECT t.name AS TableName, c.name AS ColumnName, tp.name AS DataType
FROM sys.tables t
JOIN sys.columns c ON t.object_id = c.object_id
JOIN sys.types tp ON c.user_type_id = tp.user_type_id
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = 'EIA'
AND t.name NOT LIKE '%Staging%'
ORDER BY t.name, c.column_id;
GO

PRINT '============================================';
PRINT 'Table alterations complete!';
PRINT 'Run the updated PowerShell scripts next.';
PRINT '============================================';

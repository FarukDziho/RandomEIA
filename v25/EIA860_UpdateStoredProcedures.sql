-- ============================================================
-- EIA860_UpdateStoredProcedures.sql
-- Updates all stored procedures to match actual column names
-- from EIA 2023/2024 files
-- RUN THIS IN SSMS BEFORE RUNNING THE POWERSHELL SCRIPTS
-- ============================================================
USE YOUR_DATABASE_NAME;
GO

-- ============================================================
-- SP: Merge Utility - removed Phone column
-- ============================================================
IF EXISTS (SELECT * FROM sys.procedures p JOIN sys.schemas s ON p.schema_id = s.schema_id
           WHERE p.name = 'usp_MergeEIA860UtilityData' AND s.name = 'EIA')
    DROP PROCEDURE EIA.usp_MergeEIA860UtilityData;
GO
CREATE PROCEDURE EIA.usp_MergeEIA860UtilityData
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @InsertCount INT = 0;
    DECLARE @UpdateCount INT = 0;
    DECLARE @MergeOutput TABLE (MergeAction NVARCHAR(10));

    MERGE EIA.EIA860_UtilityData AS target
    USING EIA.EIA860_UtilityData_Staging AS source
        ON target.ReportYear = source.ReportYear
        AND target.UtilityId = source.UtilityId
    WHEN MATCHED AND (
        ISNULL(target.UtilityName,'') != ISNULL(source.UtilityName,'') OR
        ISNULL(target.EntityType,'')  != ISNULL(source.EntityType,'')  OR
        ISNULL(target.State,'')       != ISNULL(source.State,'')
    )
    THEN UPDATE SET
        target.UtilityName   = source.UtilityName,
        target.StreetAddress = source.StreetAddress,
        target.City          = source.City,
        target.State         = source.State,
        target.Zip           = source.Zip,
        target.EntityType    = source.EntityType,
        target.LoadedAt      = GETDATE()
    WHEN NOT MATCHED BY TARGET
    THEN INSERT (ReportYear, UtilityId, UtilityName, StreetAddress, City, State, Zip, EntityType)
    VALUES (source.ReportYear, source.UtilityId, source.UtilityName,
            source.StreetAddress, source.City, source.State,
            source.Zip, source.EntityType)
    OUTPUT $action INTO @MergeOutput(MergeAction);

    SELECT @InsertCount = SUM(CASE WHEN MergeAction='INSERT' THEN 1 ELSE 0 END),
           @UpdateCount = SUM(CASE WHEN MergeAction='UPDATE' THEN 1 ELSE 0 END)
    FROM @MergeOutput;

    SELECT ISNULL(@InsertCount,0) AS RowsInserted,
           ISNULL(@UpdateCount,0) AS RowsUpdated,
           (SELECT COUNT(*) FROM EIA.EIA860_UtilityData) AS TotalRowsInTable;

    TRUNCATE TABLE EIA.EIA860_UtilityData_Staging;
END
GO
PRINT 'Updated: EIA.usp_MergeEIA860UtilityData';

-- ============================================================
-- SP: Merge Wind - removed RotorDiameter, TurbineRatedCapacityMW
--                  added DesignWindSpeed, HubHeight now in Feet
-- ============================================================
IF EXISTS (SELECT * FROM sys.procedures p JOIN sys.schemas s ON p.schema_id = s.schema_id
           WHERE p.name = 'usp_MergeEIA860WindData' AND s.name = 'EIA')
    DROP PROCEDURE EIA.usp_MergeEIA860WindData;
GO
CREATE PROCEDURE EIA.usp_MergeEIA860WindData
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @InsertCount INT = 0;
    DECLARE @UpdateCount INT = 0;
    DECLARE @MergeOutput TABLE (MergeAction NVARCHAR(10));

    MERGE EIA.EIA860_WindData AS target
    USING EIA.EIA860_WindData_Staging AS source
        ON target.ReportYear   = source.ReportYear
        AND target.PlantCode   = source.PlantCode
        AND target.GeneratorId = source.GeneratorId
        AND target.StatusTab   = source.StatusTab
    WHEN MATCHED AND (
        ISNULL(target.TurbineManufacturer,'') != ISNULL(source.TurbineManufacturer,'') OR
        ISNULL(target.NumberOfTurbines,'')    != ISNULL(source.NumberOfTurbines,'')    OR
        ISNULL(target.WindQualityClass,'')    != ISNULL(source.WindQualityClass,'')
    )
    THEN UPDATE SET
        target.TurbineManufacturer = source.TurbineManufacturer,
        target.TurbineModel        = source.TurbineModel,
        target.NumberOfTurbines    = source.NumberOfTurbines,
        target.HubHeight           = source.HubHeight,
        target.DesignWindSpeed     = source.DesignWindSpeed,
        target.WindQualityClass    = source.WindQualityClass,
        target.LoadedAt            = GETDATE()
    WHEN NOT MATCHED BY TARGET
    THEN INSERT (ReportYear, UtilityId, UtilityName, PlantCode, PlantName,
                 State, GeneratorId, TurbineManufacturer, TurbineModel,
                 NumberOfTurbines, HubHeight, DesignWindSpeed, WindQualityClass, StatusTab)
    VALUES (source.ReportYear, source.UtilityId, source.UtilityName,
            source.PlantCode, source.PlantName, source.State,
            source.GeneratorId, source.TurbineManufacturer, source.TurbineModel,
            source.NumberOfTurbines, source.HubHeight, source.DesignWindSpeed,
            source.WindQualityClass, source.StatusTab)
    OUTPUT $action INTO @MergeOutput(MergeAction);

    SELECT @InsertCount = SUM(CASE WHEN MergeAction='INSERT' THEN 1 ELSE 0 END),
           @UpdateCount = SUM(CASE WHEN MergeAction='UPDATE' THEN 1 ELSE 0 END)
    FROM @MergeOutput;

    SELECT ISNULL(@InsertCount,0) AS RowsInserted,
           ISNULL(@UpdateCount,0) AS RowsUpdated,
           (SELECT COUNT(*) FROM EIA.EIA860_WindData) AS TotalRowsInTable;

    TRUNCATE TABLE EIA.EIA860_WindData_Staging;
END
GO
PRINT 'Updated: EIA.usp_MergeEIA860WindData';

-- ============================================================
-- SP: Merge Solar - removed SolarTechnology column
-- ============================================================
IF EXISTS (SELECT * FROM sys.procedures p JOIN sys.schemas s ON p.schema_id = s.schema_id
           WHERE p.name = 'usp_MergeEIA860SolarData' AND s.name = 'EIA')
    DROP PROCEDURE EIA.usp_MergeEIA860SolarData;
GO
CREATE PROCEDURE EIA.usp_MergeEIA860SolarData
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @InsertCount INT = 0;
    DECLARE @UpdateCount INT = 0;
    DECLARE @MergeOutput TABLE (MergeAction NVARCHAR(10));

    MERGE EIA.EIA860_SolarData AS target
    USING EIA.EIA860_SolarData_Staging AS source
        ON target.ReportYear   = source.ReportYear
        AND target.PlantCode   = source.PlantCode
        AND target.GeneratorId = source.GeneratorId
        AND target.StatusTab   = source.StatusTab
    WHEN MATCHED AND (
        ISNULL(target.DCNetCapacity,'')      != ISNULL(source.DCNetCapacity,'')      OR
        ISNULL(target.SingleAxisTracking,'') != ISNULL(source.SingleAxisTracking,'') OR
        ISNULL(target.TiltAngle,'')          != ISNULL(source.TiltAngle,'')
    )
    THEN UPDATE SET
        target.SingleAxisTracking = source.SingleAxisTracking,
        target.DualAxisTracking   = source.DualAxisTracking,
        target.FixedTilt          = source.FixedTilt,
        target.DCNetCapacity      = source.DCNetCapacity,
        target.TiltAngle          = source.TiltAngle,
        target.AzimuthAngle       = source.AzimuthAngle,
        target.LoadedAt           = GETDATE()
    WHEN NOT MATCHED BY TARGET
    THEN INSERT (ReportYear, UtilityId, UtilityName, PlantCode, PlantName,
                 State, GeneratorId, SingleAxisTracking, DualAxisTracking,
                 FixedTilt, DCNetCapacity, TiltAngle, AzimuthAngle, StatusTab)
    VALUES (source.ReportYear, source.UtilityId, source.UtilityName,
            source.PlantCode, source.PlantName, source.State, source.GeneratorId,
            source.SingleAxisTracking, source.DualAxisTracking, source.FixedTilt,
            source.DCNetCapacity, source.TiltAngle, source.AzimuthAngle, source.StatusTab)
    OUTPUT $action INTO @MergeOutput(MergeAction);

    SELECT @InsertCount = SUM(CASE WHEN MergeAction='INSERT' THEN 1 ELSE 0 END),
           @UpdateCount = SUM(CASE WHEN MergeAction='UPDATE' THEN 1 ELSE 0 END)
    FROM @MergeOutput;

    SELECT ISNULL(@InsertCount,0) AS RowsInserted,
           ISNULL(@UpdateCount,0) AS RowsUpdated,
           (SELECT COUNT(*) FROM EIA.EIA860_SolarData) AS TotalRowsInTable;

    TRUNCATE TABLE EIA.EIA860_SolarData_Staging;
END
GO
PRINT 'Updated: EIA.usp_MergeEIA860SolarData';

-- ============================================================
-- SP: Merge Storage - StorageTechnology -> StorageTechnology1-4
--                     EnergyCapacityMWH confirmed correct name
-- ============================================================
IF EXISTS (SELECT * FROM sys.procedures p JOIN sys.schemas s ON p.schema_id = s.schema_id
           WHERE p.name = 'usp_MergeEIA860StorageData' AND s.name = 'EIA')
    DROP PROCEDURE EIA.usp_MergeEIA860StorageData;
GO
CREATE PROCEDURE EIA.usp_MergeEIA860StorageData
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @InsertCount INT = 0;
    DECLARE @UpdateCount INT = 0;
    DECLARE @MergeOutput TABLE (MergeAction NVARCHAR(10));

    MERGE EIA.EIA860_StorageData AS target
    USING EIA.EIA860_StorageData_Staging AS source
        ON target.ReportYear   = source.ReportYear
        AND target.PlantCode   = source.PlantCode
        AND target.GeneratorId = source.GeneratorId
        AND target.StatusTab   = source.StatusTab
    WHEN MATCHED AND (
        ISNULL(target.EnergyCapacityMWH,'')  != ISNULL(source.EnergyCapacityMWH,'')  OR
        ISNULL(target.StorageTechnology1,'') != ISNULL(source.StorageTechnology1,'') OR
        ISNULL(target.MaxChargeRateMW,'')    != ISNULL(source.MaxChargeRateMW,'')
    )
    THEN UPDATE SET
        target.StorageTechnology1   = source.StorageTechnology1,
        target.StorageTechnology2   = source.StorageTechnology2,
        target.StorageTechnology3   = source.StorageTechnology3,
        target.StorageTechnology4   = source.StorageTechnology4,
        target.EnergyCapacityMWH    = source.EnergyCapacityMWH,
        target.MaxChargeRateMW      = source.MaxChargeRateMW,
        target.MaxDischargeRateMW   = source.MaxDischargeRateMW,
        target.StorageEnclosureType = source.StorageEnclosureType,
        target.LoadedAt             = GETDATE()
    WHEN NOT MATCHED BY TARGET
    THEN INSERT (ReportYear, UtilityId, UtilityName, PlantCode, PlantName,
                 State, GeneratorId, StorageTechnology1, StorageTechnology2,
                 StorageTechnology3, StorageTechnology4, EnergyCapacityMWH,
                 MaxChargeRateMW, MaxDischargeRateMW, StorageEnclosureType, StatusTab)
    VALUES (source.ReportYear, source.UtilityId, source.UtilityName,
            source.PlantCode, source.PlantName, source.State, source.GeneratorId,
            source.StorageTechnology1, source.StorageTechnology2,
            source.StorageTechnology3, source.StorageTechnology4,
            source.EnergyCapacityMWH, source.MaxChargeRateMW,
            source.MaxDischargeRateMW, source.StorageEnclosureType, source.StatusTab)
    OUTPUT $action INTO @MergeOutput(MergeAction);

    SELECT @InsertCount = SUM(CASE WHEN MergeAction='INSERT' THEN 1 ELSE 0 END),
           @UpdateCount = SUM(CASE WHEN MergeAction='UPDATE' THEN 1 ELSE 0 END)
    FROM @MergeOutput;

    SELECT ISNULL(@InsertCount,0) AS RowsInserted,
           ISNULL(@UpdateCount,0) AS RowsUpdated,
           (SELECT COUNT(*) FROM EIA.EIA860_StorageData) AS TotalRowsInTable;

    TRUNCATE TABLE EIA.EIA860_StorageData_Staging;
END
GO
PRINT 'Updated: EIA.usp_MergeEIA860StorageData';

-- ============================================================
-- SP: Merge MultiFuel - AltEnergySource -> CofireEnergySource
-- ============================================================
IF EXISTS (SELECT * FROM sys.procedures p JOIN sys.schemas s ON p.schema_id = s.schema_id
           WHERE p.name = 'usp_MergeEIA860MultiFuelData' AND s.name = 'EIA')
    DROP PROCEDURE EIA.usp_MergeEIA860MultiFuelData;
GO
CREATE PROCEDURE EIA.usp_MergeEIA860MultiFuelData
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @InsertCount INT = 0;
    DECLARE @UpdateCount INT = 0;
    DECLARE @MergeOutput TABLE (MergeAction NVARCHAR(10));

    MERGE EIA.EIA860_MultiFuelData AS target
    USING EIA.EIA860_MultiFuelData_Staging AS source
        ON target.ReportYear   = source.ReportYear
        AND target.PlantCode   = source.PlantCode
        AND target.GeneratorId = source.GeneratorId
        AND target.StatusTab   = source.StatusTab
    WHEN MATCHED AND (
        ISNULL(target.FuelSwitchCapable,'')    != ISNULL(source.FuelSwitchCapable,'')    OR
        ISNULL(target.CofireEnergySource1,'')  != ISNULL(source.CofireEnergySource1,'')  OR
        ISNULL(target.CofireEnergySource2,'')  != ISNULL(source.CofireEnergySource2,'')
    )
    THEN UPDATE SET
        target.FuelSwitchCapable   = source.FuelSwitchCapable,
        target.CofireEnergySource1 = source.CofireEnergySource1,
        target.CofireEnergySource2 = source.CofireEnergySource2,
        target.CofireEnergySource3 = source.CofireEnergySource3,
        target.LoadedAt            = GETDATE()
    WHEN NOT MATCHED BY TARGET
    THEN INSERT (ReportYear, UtilityId, UtilityName, PlantCode, PlantName,
                 State, GeneratorId, FuelSwitchCapable, CofireEnergySource1,
                 CofireEnergySource2, CofireEnergySource3, StatusTab)
    VALUES (source.ReportYear, source.UtilityId, source.UtilityName,
            source.PlantCode, source.PlantName, source.State, source.GeneratorId,
            source.FuelSwitchCapable, source.CofireEnergySource1,
            source.CofireEnergySource2, source.CofireEnergySource3, source.StatusTab)
    OUTPUT $action INTO @MergeOutput(MergeAction);

    SELECT @InsertCount = SUM(CASE WHEN MergeAction='INSERT' THEN 1 ELSE 0 END),
           @UpdateCount = SUM(CASE WHEN MergeAction='UPDATE' THEN 1 ELSE 0 END)
    FROM @MergeOutput;

    SELECT ISNULL(@InsertCount,0) AS RowsInserted,
           ISNULL(@UpdateCount,0) AS RowsUpdated,
           (SELECT COUNT(*) FROM EIA.EIA860_MultiFuelData) AS TotalRowsInTable;

    TRUNCATE TABLE EIA.EIA860_MultiFuelData_Staging;
END
GO
PRINT 'Updated: EIA.usp_MergeEIA860MultiFuelData';

-- ============================================================
-- Verify all stored procedures updated
-- ============================================================
SELECT s.name AS SchemaName, p.name AS ProcedureName, p.modify_date AS LastModified
FROM sys.procedures p
JOIN sys.schemas s ON p.schema_id = s.schema_id
WHERE s.name = 'EIA'
ORDER BY p.name;
GO

PRINT '============================================';
PRINT 'All stored procedures updated!';
PRINT 'Now fix the null error in PowerShell scripts';
PRINT '============================================';

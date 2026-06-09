USE SolarDataDB;
GO

SET XACT_ABORT ON;
GO

BEGIN TRAN;

IF OBJECT_ID('dbo.dim_solaredge_equipment', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.dim_solaredge_equipment
    (
        equipment_id bigint IDENTITY(1,1) NOT NULL,
        source_system_code varchar(30) NOT NULL,
        internal_plant_code nvarchar(100) NOT NULL,
        source_plant_code nvarchar(100) NOT NULL,

        equipment_type varchar(50) NOT NULL,
        source_device_id nvarchar(100) NOT NULL,
        source_device_name nvarchar(255) NULL,

        manufacturer nvarchar(255) NULL,
        model nvarchar(255) NULL,
        firmware_version nvarchar(255) NULL,
        communication_method nvarchar(100) NULL,
        connected_optimizers int NULL,

        is_active bit NOT NULL CONSTRAINT DF_dim_solaredge_equipment_is_active DEFAULT (1),
        first_seen_utc datetime2(3) NOT NULL CONSTRAINT DF_dim_solaredge_equipment_first_seen DEFAULT (SYSUTCDATETIME()),
        last_seen_utc datetime2(3) NOT NULL CONSTRAINT DF_dim_solaredge_equipment_last_seen DEFAULT (SYSUTCDATETIME()),
        last_raw_id bigint NULL,
        raw_payload_json nvarchar(max) NULL,

        created_at_utc datetime2(3) NOT NULL CONSTRAINT DF_dim_solaredge_equipment_created DEFAULT (SYSUTCDATETIME()),
        updated_at_utc datetime2(3) NULL,

        CONSTRAINT PK_dim_solaredge_equipment PRIMARY KEY CLUSTERED (equipment_id),
        CONSTRAINT UQ_dim_solaredge_equipment UNIQUE
        (
            source_system_code,
            source_plant_code,
            equipment_type,
            source_device_id
        )
    );
END;

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_dim_solaredge_equipment_active_inverter'
      AND object_id = OBJECT_ID('dbo.dim_solaredge_equipment')
)
BEGIN
    CREATE INDEX IX_dim_solaredge_equipment_active_inverter
    ON dbo.dim_solaredge_equipment
    (
        source_system_code,
        internal_plant_code,
        source_plant_code,
        equipment_type,
        is_active
    )
    INCLUDE
    (
        source_device_id,
        source_device_name,
        manufacturer,
        model,
        firmware_version,
        connected_optimizers,
        last_seen_utc,
        last_raw_id
    );
END;

COMMIT;
GO

CREATE OR ALTER VIEW dbo.vw_solaredge_active_inverter
AS
SELECT
    e.source_system_code,
    e.internal_plant_code,
    e.source_plant_code,
    m.source_plant_name,
    m.timezone_name,
    e.source_device_id AS serial_number,
    e.source_device_name AS inverter_name,
    e.manufacturer,
    e.model,
    e.firmware_version,
    e.communication_method,
    e.connected_optimizers,
    e.last_seen_utc,
    e.last_raw_id
FROM dbo.dim_solaredge_equipment e
LEFT JOIN dbo.dim_plant_source_map m
    ON  m.source_system_code = e.source_system_code
    AND m.source_plant_code = e.source_plant_code
WHERE e.source_system_code = 'SOLAREDGE'
  AND e.equipment_type IN ('INVERTER', 'SMI', 'THIRD_PARTY_INVERTER')
  AND e.is_active = 1;
GO

USE SolarDataDB;
GO

SET XACT_ABORT ON;
GO

IF OBJECT_ID('dbo.dim_solaredge_equipment', 'U') IS NULL
BEGIN
    THROW 51000, 'dbo.dim_solaredge_equipment does not exist. Run SolarEdge equipment inventory migration first.', 1;
END;
GO

IF NOT EXISTS
(
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_dim_solaredge_equipment_active_sensor'
      AND object_id = OBJECT_ID('dbo.dim_solaredge_equipment')
)
BEGIN
    CREATE INDEX IX_dim_solaredge_equipment_active_sensor
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
        last_seen_utc,
        last_raw_id
    );
END;
GO

CREATE OR ALTER VIEW dbo.vw_solaredge_active_sensor
AS
SELECT
    e.source_system_code,
    e.internal_plant_code,
    e.source_plant_code,
    m.source_plant_name,
    m.timezone_name,
    e.equipment_type,
    e.source_device_id AS sensor_key,
    e.source_device_name AS sensor_name,
    JSON_VALUE(e.raw_payload_json, '$.connectedTo') AS connected_to,
    COALESCE(
        JSON_VALUE(e.raw_payload_json, '$.sensor.measurement'),
        JSON_VALUE(e.raw_payload_json, '$.sensor.id'),
        e.source_device_id
    ) AS sensor_measurement,
    COALESCE(
        JSON_VALUE(e.raw_payload_json, '$.sensor.type'),
        JSON_VALUE(e.raw_payload_json, '$.sensor.category')
    ) AS sensor_type,
    CASE
        WHEN e.equipment_type = 'SENSOR_IRRADIANCE'
          OR JSON_VALUE(e.raw_payload_json, '$.sensor.measurement') LIKE '%Irradiance%'
          OR JSON_VALUE(e.raw_payload_json, '$.sensor.measurement') LIKE '%Irradiation%'
          OR JSON_VALUE(e.raw_payload_json, '$.sensor.type') LIKE '%IRRADIANCE%'
        THEN CAST(1 AS bit)
        ELSE CAST(0 AS bit)
    END AS is_irradiance_sensor,
    e.last_seen_utc,
    e.last_raw_id
FROM dbo.dim_solaredge_equipment e
LEFT JOIN dbo.dim_plant_source_map m
    ON  m.source_system_code = e.source_system_code
    AND m.source_plant_code = e.source_plant_code
WHERE e.source_system_code = 'SOLAREDGE'
  AND e.equipment_type LIKE 'SENSOR_%'
  AND e.is_active = 1;
GO

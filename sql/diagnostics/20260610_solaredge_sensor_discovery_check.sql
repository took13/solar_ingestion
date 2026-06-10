USE SolarDataDB;
GO

PRINT '1) Latest SolarEdge sensor raw calls';
SELECT TOP (50)
    raw_id,
    source_system_code,
    endpoint_name,
    internal_plant_code,
    source_plant_code,
    request_window_start_utc,
    request_window_end_utc,
    http_status,
    CAST(api_success_flag AS int) AS api_success_flag,
    response_size_bytes,
    inserted_at_utc
FROM raw.api_call_v2
WHERE source_system_code = 'SOLAREDGE'
  AND endpoint_name IN ('sensorList', 'sensorData')
ORDER BY raw_id DESC;
GO

PRINT '2) Active SolarEdge sensors discovered';
SELECT
    internal_plant_code,
    source_plant_code,
    equipment_type,
    is_irradiance_sensor,
    COUNT(*) AS sensor_count,
    MAX(last_seen_utc) AS max_last_seen_utc,
    MAX(last_raw_id) AS max_last_raw_id
FROM dbo.vw_solaredge_active_sensor
GROUP BY
    internal_plant_code,
    source_plant_code,
    equipment_type,
    is_irradiance_sensor
ORDER BY
    internal_plant_code,
    equipment_type;
GO

PRINT '3) Active SolarEdge sensor detail';
SELECT
    internal_plant_code,
    source_plant_code,
    sensor_key,
    sensor_name,
    connected_to,
    sensor_measurement,
    sensor_type,
    is_irradiance_sensor,
    last_seen_utc,
    last_raw_id
FROM dbo.vw_solaredge_active_sensor
ORDER BY
    internal_plant_code,
    is_irradiance_sensor DESC,
    sensor_key;
GO

PRINT '4) sensorData gateway/group counts';
;WITH latest AS
(
    SELECT TOP (20)
        raw_id,
        internal_plant_code,
        source_plant_code,
        response_json
    FROM raw.api_call_v2
    WHERE source_system_code = 'SOLAREDGE'
      AND endpoint_name = 'sensorData'
    ORDER BY raw_id DESC
)
SELECT
    l.raw_id,
    l.internal_plant_code,
    l.source_plant_code,
    g.connectedTo,
    g.[count] AS reported_count,
    CASE WHEN g.telemetries IS NULL THEN 0 ELSE 1 END AS has_telemetries_json
FROM latest l
CROSS APPLY OPENJSON(COALESCE(JSON_QUERY(l.response_json, '$.siteSensors.data'), JSON_QUERY(l.response_json, '$.SiteSensors.data')))
WITH
(
    connectedTo nvarchar(255) '$.connectedTo',
    [count] int '$.count',
    telemetries nvarchar(max) '$.telemetries' AS JSON
) g
ORDER BY
    l.raw_id DESC,
    g.connectedTo;
GO

PRINT '5) sensorData measurement keys from sample telemetries';
;WITH latest AS
(
    SELECT TOP (20)
        raw_id,
        internal_plant_code,
        source_plant_code,
        response_json
    FROM raw.api_call_v2
    WHERE source_system_code = 'SOLAREDGE'
      AND endpoint_name = 'sensorData'
    ORDER BY raw_id DESC
)
SELECT DISTINCT
    l.raw_id,
    l.internal_plant_code,
    l.source_plant_code,
    g.connectedTo,
    k.[key] AS measurement_key,
    k.[type] AS json_type
FROM latest l
CROSS APPLY OPENJSON(COALESCE(JSON_QUERY(l.response_json, '$.siteSensors.data'), JSON_QUERY(l.response_json, '$.SiteSensors.data')))
WITH
(
    connectedTo nvarchar(255) '$.connectedTo',
    telemetries nvarchar(max) '$.telemetries' AS JSON
) g
CROSS APPLY OPENJSON(g.telemetries) t
CROSS APPLY OPENJSON(t.[value]) k
WHERE k.[key] NOT IN ('date', 'time', 'timestamp')
ORDER BY
    l.raw_id DESC,
    g.connectedTo,
    k.[key];
GO

PRINT '6) Irradiance-like measurement keys';
;WITH latest AS
(
    SELECT TOP (20)
        raw_id,
        internal_plant_code,
        source_plant_code,
        response_json
    FROM raw.api_call_v2
    WHERE source_system_code = 'SOLAREDGE'
      AND endpoint_name = 'sensorData'
    ORDER BY raw_id DESC
)
SELECT DISTINCT
    l.raw_id,
    l.internal_plant_code,
    l.source_plant_code,
    g.connectedTo,
    k.[key] AS irradiance_key,
    LEFT(CONVERT(nvarchar(max), k.[value]), 100) AS sample_value
FROM latest l
CROSS APPLY OPENJSON(COALESCE(JSON_QUERY(l.response_json, '$.siteSensors.data'), JSON_QUERY(l.response_json, '$.SiteSensors.data')))
WITH
(
    connectedTo nvarchar(255) '$.connectedTo',
    telemetries nvarchar(max) '$.telemetries' AS JSON
) g
CROSS APPLY OPENJSON(g.telemetries) t
CROSS APPLY OPENJSON(t.[value]) k
WHERE LOWER(k.[key]) LIKE '%irradiance%'
   OR LOWER(k.[key]) LIKE '%irradiation%'
ORDER BY
    l.raw_id DESC,
    g.connectedTo,
    k.[key];
GO

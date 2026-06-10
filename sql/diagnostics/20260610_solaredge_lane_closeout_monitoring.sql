/*
SolarEdge lane closeout monitoring.
Run in SolarDataDB after M5-M8 have been applied and smoke-tested.

Purpose:
- Verify raw API health for plant, inverter, and sensor endpoints.
- Verify mart freshness for plant power/energy, sensor/irradiance, and inverter technical data.
- Identify SolarEdge inverters with HTTP 200 but no telemetry in the nearline window.
- Confirm duplicate-key guardrails remain clean.
*/

SET NOCOUNT ON;

DECLARE @source_system_code varchar(50) = 'SOLAREDGE';
DECLARE @lookback_hours int = 24;
DECLARE @freshness_warn_minutes int = 90;

PRINT '0) Parameters';
SELECT
    @source_system_code AS source_system_code,
    @lookback_hours AS lookback_hours,
    @freshness_warn_minutes AS freshness_warn_minutes,
    SYSUTCDATETIME() AS checked_at_utc;

PRINT '1) Raw API health by endpoint in last lookback window';
SELECT
    endpoint_name,
    COUNT(*) AS raw_call_count,
    SUM(CASE WHEN http_status = 200 AND ISNULL(api_success_flag, 0) = 1 THEN 1 ELSE 0 END) AS success_count,
    SUM(CASE WHEN http_status <> 200 OR ISNULL(api_success_flag, 0) <> 1 THEN 1 ELSE 0 END) AS failed_count,
    MIN(inserted_at_utc) AS min_inserted_at_utc,
    MAX(inserted_at_utc) AS max_inserted_at_utc,
    MIN(response_size_bytes) AS min_response_size_bytes,
    MAX(response_size_bytes) AS max_response_size_bytes
FROM raw.api_call_v2
WHERE source_system_code = @source_system_code
  AND inserted_at_utc >= DATEADD(HOUR, -@lookback_hours, SYSUTCDATETIME())
  AND endpoint_name IN ('sitePower', 'energyDetails', 'inverterTechnicalData', 'sensorData')
GROUP BY endpoint_name
ORDER BY endpoint_name;

PRINT '2) Latest raw failures in last lookback window';
SELECT TOP (100)
    raw_id,
    endpoint_name,
    internal_plant_code,
    source_plant_code,
    source_device_id,
    request_window_start_utc,
    request_window_end_utc,
    http_status,
    api_success_flag,
    response_size_bytes,
    inserted_at_utc
FROM raw.api_call_v2
WHERE source_system_code = @source_system_code
  AND inserted_at_utc >= DATEADD(HOUR, -@lookback_hours, SYSUTCDATETIME())
  AND endpoint_name IN ('sitePower', 'energyDetails', 'inverterTechnicalData', 'sensorData')
  AND (http_status <> 200 OR ISNULL(api_success_flag, 0) <> 1)
ORDER BY raw_id DESC;

PRINT '3) Plant power mart freshness';
SELECT
    internal_plant_code,
    source_plant_code,
    MAX(collect_time_local) AS latest_collect_time_local,
    DATEDIFF(MINUTE, MAX(collect_time_utc), SYSUTCDATETIME()) AS staleness_minutes,
    COUNT(*) AS rows_in_lookback,
    MAX(raw_id) AS max_raw_id,
    CASE
        WHEN DATEDIFF(MINUTE, MAX(collect_time_utc), SYSUTCDATETIME()) > @freshness_warn_minutes THEN 'WARN_STALE'
        ELSE 'OK'
    END AS freshness_status
FROM mart.fact_solar_plant_power_15min
WHERE source_system_code = @source_system_code
  AND collect_time_utc >= DATEADD(HOUR, -@lookback_hours, SYSUTCDATETIME())
GROUP BY internal_plant_code, source_plant_code
ORDER BY internal_plant_code;

PRINT '4) Plant energy mart freshness';
SELECT
    internal_plant_code,
    source_plant_code,
    MAX(collect_time_local) AS latest_collect_time_local,
    DATEDIFF(MINUTE, MAX(collect_time_utc), SYSUTCDATETIME()) AS staleness_minutes,
    COUNT(*) AS rows_in_lookback,
    MAX(raw_id) AS max_raw_id,
    CASE
        WHEN DATEDIFF(MINUTE, MAX(collect_time_utc), SYSUTCDATETIME()) > @freshness_warn_minutes THEN 'WARN_STALE'
        ELSE 'OK'
    END AS freshness_status
FROM mart.fact_solar_plant_energy_15min
WHERE source_system_code = @source_system_code
  AND collect_time_utc >= DATEADD(HOUR, -@lookback_hours, SYSUTCDATETIME())
GROUP BY internal_plant_code, source_plant_code
ORDER BY internal_plant_code;

PRINT '5) Sensor / irradiance mart freshness and coverage';
SELECT
    internal_plant_code,
    source_plant_code,
    source_device_id,
    MAX(collect_time_local) AS latest_collect_time_local,
    DATEDIFF(MINUTE, MAX(collect_time_utc), SYSUTCDATETIME()) AS staleness_minutes,
    COUNT(*) AS rows_in_lookback,
    SUM(CASE WHEN irradiance_wm2_best_effort IS NOT NULL THEN 1 ELSE 0 END) AS rows_with_irradiance,
    MIN(irradiance_wm2_best_effort) AS min_irradiance_wm2,
    MAX(irradiance_wm2_best_effort) AS max_irradiance_wm2,
    MAX(raw_id) AS max_raw_id,
    CASE
        WHEN DATEDIFF(MINUTE, MAX(collect_time_utc), SYSUTCDATETIME()) > @freshness_warn_minutes THEN 'WARN_STALE'
        WHEN SUM(CASE WHEN irradiance_wm2_best_effort IS NOT NULL THEN 1 ELSE 0 END) = 0 THEN 'NO_IRRADIANCE_GROUP'
        ELSE 'OK'
    END AS freshness_status
FROM mart.fact_solaredge_sensor_5min
WHERE source_system_code = @source_system_code
  AND collect_time_utc >= DATEADD(HOUR, -@lookback_hours, SYSUTCDATETIME())
GROUP BY internal_plant_code, source_plant_code, source_device_id
ORDER BY internal_plant_code, source_device_id;

PRINT '6) Inverter nearline expected active count vs mart count';
;WITH expected AS
(
    SELECT
        internal_plant_code,
        source_plant_code,
        COUNT(*) AS expected_active_inverters
    FROM dbo.vw_solaredge_active_inverter
    GROUP BY internal_plant_code, source_plant_code
),
latest_raw AS
(
    SELECT
        internal_plant_code,
        source_plant_code,
        source_device_id,
        MAX(raw_id) AS max_raw_id
    FROM raw.api_call_v2
    WHERE source_system_code = @source_system_code
      AND endpoint_name = 'inverterTechnicalData'
      AND inserted_at_utc >= DATEADD(HOUR, -@lookback_hours, SYSUTCDATETIME())
    GROUP BY internal_plant_code, source_plant_code, source_device_id
),
latest_raw_detail AS
(
    SELECT
        r.internal_plant_code,
        r.source_plant_code,
        r.source_device_id,
        r.raw_id,
        r.http_status,
        r.api_success_flag,
        r.response_size_bytes
    FROM raw.api_call_v2 r
    JOIN latest_raw lr
        ON lr.max_raw_id = r.raw_id
),
latest_mart AS
(
    SELECT
        internal_plant_code,
        source_plant_code,
        source_device_id,
        MAX(collect_time_utc) AS latest_collect_time_utc,
        MAX(collect_time_local) AS latest_collect_time_local
    FROM mart.fact_solaredge_inverter_technical_5min
    WHERE source_system_code = @source_system_code
      AND collect_time_utc >= DATEADD(HOUR, -@lookback_hours, SYSUTCDATETIME())
    GROUP BY internal_plant_code, source_plant_code, source_device_id
)
SELECT
    e.internal_plant_code,
    e.source_plant_code,
    e.expected_active_inverters,
    COUNT(DISTINCT lrd.source_device_id) AS raw_inverters_seen,
    COUNT(DISTINCT CASE WHEN lrd.http_status = 200 AND ISNULL(lrd.api_success_flag, 0) = 1 THEN lrd.source_device_id END) AS raw_success_inverters,
    COUNT(DISTINCT CASE WHEN lrd.response_size_bytes <= 100 THEN lrd.source_device_id END) AS possible_no_telemetry_inverters,
    COUNT(DISTINCT lm.source_device_id) AS mart_inverters_with_rows,
    e.expected_active_inverters - COUNT(DISTINCT lm.source_device_id) AS expected_minus_mart_count,
    MAX(lm.latest_collect_time_local) AS newest_latest_local,
    DATEDIFF(MINUTE, MAX(lm.latest_collect_time_utc), SYSUTCDATETIME()) AS staleness_minutes,
    CASE
        WHEN DATEDIFF(MINUTE, MAX(lm.latest_collect_time_utc), SYSUTCDATETIME()) > @freshness_warn_minutes THEN 'WARN_STALE'
        WHEN e.expected_active_inverters - COUNT(DISTINCT lm.source_device_id) > COUNT(DISTINCT CASE WHEN lrd.response_size_bytes <= 100 THEN lrd.source_device_id END) THEN 'WARN_MISSING_MART_ROWS'
        ELSE 'OK'
    END AS freshness_status
FROM expected e
LEFT JOIN latest_raw_detail lrd
    ON lrd.internal_plant_code = e.internal_plant_code
   AND lrd.source_plant_code = e.source_plant_code
LEFT JOIN latest_mart lm
    ON lm.internal_plant_code = e.internal_plant_code
   AND lm.source_plant_code = e.source_plant_code
GROUP BY e.internal_plant_code, e.source_plant_code, e.expected_active_inverters
ORDER BY e.internal_plant_code;

PRINT '7) Possible NO_TELEMETRY inverter responses';
SELECT TOP (100)
    raw_id,
    internal_plant_code,
    source_plant_code,
    source_device_id AS serial_number,
    request_window_start_utc,
    request_window_end_utc,
    http_status,
    api_success_flag,
    response_size_bytes,
    inserted_at_utc
FROM raw.api_call_v2
WHERE source_system_code = @source_system_code
  AND endpoint_name = 'inverterTechnicalData'
  AND inserted_at_utc >= DATEADD(HOUR, -@lookback_hours, SYSUTCDATETIME())
  AND http_status = 200
  AND ISNULL(api_success_flag, 0) = 1
  AND response_size_bytes <= 100
ORDER BY raw_id DESC;

PRINT '8) Duplicate check: inverter canonical';
SELECT TOP (100)
    source_system_code,
    internal_plant_code,
    source_plant_code,
    device_scope,
    source_device_id,
    collect_time_utc,
    canonical_metric_code,
    COUNT(*) AS duplicate_count
FROM norm.canonical_metric_selected
WHERE source_system_code = @source_system_code
  AND inserted_at_utc >= DATEADD(HOUR, -@lookback_hours, SYSUTCDATETIME())
  AND device_scope = 'INVERTER'
GROUP BY
    source_system_code,
    internal_plant_code,
    source_plant_code,
    device_scope,
    source_device_id,
    collect_time_utc,
    canonical_metric_code
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

PRINT '9) Duplicate check: sensor mart';
SELECT TOP (100)
    source_system_code,
    internal_plant_code,
    source_plant_code,
    source_device_id,
    collect_time_utc,
    COUNT(*) AS duplicate_count
FROM mart.fact_solaredge_sensor_5min
WHERE source_system_code = @source_system_code
  AND collect_time_utc >= DATEADD(HOUR, -@lookback_hours, SYSUTCDATETIME())
GROUP BY
    source_system_code,
    internal_plant_code,
    source_plant_code,
    source_device_id,
    collect_time_utc
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

/*
SolarEdge M8 inverter nearline diagnostics.
Run in SolarDataDB after scripts.run_solaredge_inverter_technical_nearline.

Hotfix checks added:
- expected active inverter count vs mart count
- raw calls with tiny response payloads that likely mean HTTP 200 but no telemetry
- latest request-window consistency by nearline run
*/

SET NOCOUNT ON;

DECLARE @source_system_code varchar(50) = 'SOLAREDGE';
DECLARE @endpoint_name varchar(100) = 'inverterTechnicalData';
DECLARE @lookback_hours int = 24;

PRINT '1) Latest raw inverterTechnicalData calls';
SELECT TOP (100)
    raw_id,
    source_system_code,
    endpoint_name,
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
  AND endpoint_name = @endpoint_name
ORDER BY raw_id DESC;


PRINT '2) Expected active inverter count vs raw/mart telemetry count';
;WITH expected AS
(
    SELECT
        internal_plant_code,
        source_plant_code,
        COUNT(*) AS expected_active_inverters
    FROM dbo.vw_solaredge_active_inverter
    GROUP BY
        internal_plant_code,
        source_plant_code
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
      AND endpoint_name = @endpoint_name
      AND inserted_at_utc >= DATEADD(HOUR, -@lookback_hours, SYSUTCDATETIME())
    GROUP BY
        internal_plant_code,
        source_plant_code,
        source_device_id
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
        MAX(collect_time_utc) AS latest_collect_time_utc
    FROM mart.fact_solaredge_inverter_technical_5min
    WHERE source_system_code = @source_system_code
      AND collect_time_utc >= DATEADD(HOUR, -@lookback_hours, SYSUTCDATETIME())
    GROUP BY
        internal_plant_code,
        source_plant_code,
        source_device_id
)
SELECT
    e.internal_plant_code,
    e.source_plant_code,
    e.expected_active_inverters,
    COUNT(DISTINCT lrd.source_device_id) AS raw_inverters_seen,
    COUNT(DISTINCT CASE WHEN lrd.http_status = 200 AND ISNULL(lrd.api_success_flag, 0) = 1 THEN lrd.source_device_id END) AS raw_success_inverters,
    COUNT(DISTINCT CASE WHEN lrd.response_size_bytes <= 100 THEN lrd.source_device_id END) AS possible_no_telemetry_inverters,
    COUNT(DISTINCT lm.source_device_id) AS mart_inverters_with_rows,
    e.expected_active_inverters - COUNT(DISTINCT lm.source_device_id) AS expected_minus_mart_count
FROM expected e
LEFT JOIN latest_raw_detail lrd
    ON lrd.internal_plant_code = e.internal_plant_code
   AND lrd.source_plant_code = e.source_plant_code
LEFT JOIN latest_mart lm
    ON lm.internal_plant_code = e.internal_plant_code
   AND lm.source_plant_code = e.source_plant_code
GROUP BY
    e.internal_plant_code,
    e.source_plant_code,
    e.expected_active_inverters
ORDER BY e.internal_plant_code;

PRINT '3) Possible NO_TELEMETRY raw responses: HTTP 200 with tiny payload';
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
  AND endpoint_name = @endpoint_name
  AND inserted_at_utc >= DATEADD(HOUR, -@lookback_hours, SYSUTCDATETIME())
  AND http_status = 200
  AND ISNULL(api_success_flag, 0) = 1
  AND response_size_bytes <= 100
ORDER BY raw_id DESC;

PRINT '4) Latest request windows by plant to confirm frozen-window behavior';
;WITH latest_raw_window AS
(
    SELECT TOP (200)
        raw_id,
        internal_plant_code,
        source_plant_code,
        source_device_id,
        request_window_start_utc,
        request_window_end_utc,
        inserted_at_utc
    FROM raw.api_call_v2
    WHERE source_system_code = @source_system_code
      AND endpoint_name = @endpoint_name
    ORDER BY raw_id DESC
)
SELECT
    internal_plant_code,
    source_plant_code,
    request_window_start_utc,
    request_window_end_utc,
    COUNT(*) AS raw_call_count,
    MIN(inserted_at_utc) AS min_inserted_at_utc,
    MAX(inserted_at_utc) AS max_inserted_at_utc
FROM latest_raw_window
GROUP BY
    internal_plant_code,
    source_plant_code,
    request_window_start_utc,
    request_window_end_utc
ORDER BY
    MAX(inserted_at_utc) DESC,
    internal_plant_code;

PRINT '5) Inverter mart freshness by plant';
;WITH latest_per_inverter AS
(
    SELECT
        internal_plant_code,
        source_plant_code,
        source_device_id,
        MAX(collect_time_utc) AS latest_collect_time_utc,
        MAX(collect_time_local) AS latest_collect_time_local,
        MAX(raw_id) AS max_raw_id
    FROM mart.fact_solaredge_inverter_technical_5min
    WHERE source_system_code = @source_system_code
    GROUP BY
        internal_plant_code,
        source_plant_code,
        source_device_id
)
SELECT
    internal_plant_code,
    source_plant_code,
    COUNT(*) AS inverter_count,
    MIN(latest_collect_time_local) AS oldest_latest_local,
    MAX(latest_collect_time_local) AS newest_latest_local,
    MIN(DATEDIFF(MINUTE, latest_collect_time_utc, SYSUTCDATETIME())) AS min_staleness_minutes,
    MAX(DATEDIFF(MINUTE, latest_collect_time_utc, SYSUTCDATETIME())) AS max_staleness_minutes,
    MAX(max_raw_id) AS max_raw_id
FROM latest_per_inverter
GROUP BY
    internal_plant_code,
    source_plant_code
ORDER BY internal_plant_code;

PRINT '6) Mart coverage by plant/inverter for last lookback window';
SELECT
    internal_plant_code,
    source_plant_code,
    source_device_id AS serial_number,
    CAST(collect_time_local AS date) AS local_date,
    COUNT(*) AS row_count,
    MIN(collect_time_local) AS min_local_time,
    MAX(collect_time_local) AS max_local_time,
    MIN(active_power_kw) AS min_active_power_kw,
    MAX(active_power_kw) AS max_active_power_kw,
    MIN(total_energy_kwh) AS min_total_energy_kwh,
    MAX(total_energy_kwh) AS max_total_energy_kwh,
    MAX(raw_id) AS max_raw_id
FROM mart.fact_solaredge_inverter_technical_5min
WHERE source_system_code = @source_system_code
  AND collect_time_utc >= DATEADD(HOUR, -@lookback_hours, SYSUTCDATETIME())
GROUP BY
    internal_plant_code,
    source_plant_code,
    source_device_id,
    CAST(collect_time_local AS date)
ORDER BY
    internal_plant_code,
    serial_number,
    local_date DESC;

PRINT '7) Duplicate check in canonical selected for inverterTechnicalData';
SELECT
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
  AND device_scope = 'INVERTER'
  AND collect_time_utc >= DATEADD(HOUR, -@lookback_hours, SYSUTCDATETIME())
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

PRINT '8) Mart data quality status summary';
SELECT
    internal_plant_code,
    data_quality_status,
    COUNT(*) AS row_count,
    MIN(collect_time_local) AS min_local_time,
    MAX(collect_time_local) AS max_local_time
FROM mart.fact_solaredge_inverter_technical_5min
WHERE source_system_code = @source_system_code
  AND collect_time_utc >= DATEADD(HOUR, -@lookback_hours, SYSUTCDATETIME())
GROUP BY
    internal_plant_code,
    data_quality_status
ORDER BY
    internal_plant_code,
    data_quality_status;

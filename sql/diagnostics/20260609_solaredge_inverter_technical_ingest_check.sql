/*
Diagnostic: SolarEdge inverter technical ingest raw/canonical/mart check.
Run after scripts.run_solaredge_inverter_technical_ingest.
*/

USE SolarDataDB;
GO

DECLARE @RawId bigint =
(
    SELECT MAX(raw_id)
    FROM raw.api_call_v2
    WHERE source_system_code = 'SOLAREDGE'
      AND endpoint_name = 'inverterTechnicalData'
);

SELECT
    @RawId AS latest_raw_id;

SELECT TOP (20)
    raw_id,
    source_system_code,
    endpoint_name,
    internal_plant_code,
    source_plant_code,
    source_device_id AS serial_number,
    request_window_start_utc,
    request_window_end_utc,
    http_status,
    CAST(api_success_flag AS int) AS api_success_flag,
    response_size_bytes,
    inserted_at_utc
FROM raw.api_call_v2
WHERE source_system_code = 'SOLAREDGE'
  AND endpoint_name = 'inverterTechnicalData'
ORDER BY raw_id DESC;

SELECT
    raw_id,
    internal_plant_code,
    source_plant_code,
    source_device_id AS serial_number,
    source_metric_name,
    canonical_metric_code,
    unit_code,
    COUNT_BIG(*) AS row_count,
    MIN(collect_time_utc) AS min_time_utc,
    MAX(collect_time_utc) AS max_time_utc,
    MIN(metric_value_num) AS min_value,
    MAX(metric_value_num) AS max_value
FROM norm.canonical_metric_selected
WHERE source_system_code = 'SOLAREDGE'
  AND raw_id = @RawId
GROUP BY
    raw_id,
    internal_plant_code,
    source_plant_code,
    source_device_id,
    source_metric_name,
    canonical_metric_code,
    unit_code
ORDER BY
    source_metric_name,
    canonical_metric_code;

SELECT
    internal_plant_code,
    source_plant_code,
    source_device_id AS serial_number,
    source_device_name,
    collect_time_utc,
    collect_time_local,
    active_power_kw,
    total_energy_kwh,
    dc_voltage_v,
    temperature_c,
    power_limit_pct,
    raw_id,
    data_quality_status,
    inserted_at_utc,
    updated_at_utc
FROM mart.fact_solaredge_inverter_technical_5min
WHERE source_system_code = 'SOLAREDGE'
  AND raw_id = @RawId
ORDER BY collect_time_utc;
GO

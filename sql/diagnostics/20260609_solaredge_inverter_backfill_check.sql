/*
SolarEdge inverter technical controlled backfill diagnostics.
Run after scripts.run_solaredge_inverter_technical_backfill.
*/

USE SolarDataDB;
GO

PRINT '1) Latest inverter technical raw calls';
SELECT TOP (30)
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
GO

PRINT '2) Backfill checkpoint latest';
SELECT
    internal_plant_code,
    source_plant_code,
    serial_number,
    inverter_name,
    endpoint_name,
    requested_start_local,
    requested_end_local,
    last_success_start_local,
    last_success_end_local,
    last_success_start_utc,
    last_success_end_utc,
    last_raw_id,
    last_status,
    consecutive_failures,
    total_success_windows,
    total_failed_windows,
    last_error_message,
    updated_at_utc
FROM ctl.vw_solaredge_inverter_backfill_checkpoint_latest
ORDER BY updated_at_utc DESC, internal_plant_code, serial_number;
GO

PRINT '3) Canonical row count by latest backfill raw_id';
WITH latest_raw AS
(
    SELECT TOP (10)
        raw_id
    FROM raw.api_call_v2
    WHERE source_system_code = 'SOLAREDGE'
      AND endpoint_name = 'inverterTechnicalData'
    ORDER BY raw_id DESC
)
SELECT
    r.raw_id,
    c.internal_plant_code,
    c.source_plant_code,
    c.source_device_id AS serial_number,
    c.canonical_metric_code,
    c.unit_code,
    COUNT_BIG(*) AS row_count,
    MIN(c.collect_time_utc) AS min_time_utc,
    MAX(c.collect_time_utc) AS max_time_utc,
    MIN(c.metric_value_num) AS min_value,
    MAX(c.metric_value_num) AS max_value
FROM latest_raw r
LEFT JOIN norm.canonical_metric_selected c
    ON c.raw_id = r.raw_id
GROUP BY
    r.raw_id,
    c.internal_plant_code,
    c.source_plant_code,
    c.source_device_id,
    c.canonical_metric_code,
    c.unit_code
ORDER BY r.raw_id DESC, c.canonical_metric_code;
GO

PRINT '4) Mart latest inverter technical rows';
SELECT TOP (100)
    source_system_code,
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
ORDER BY collect_time_utc DESC, internal_plant_code, serial_number;
GO

PRINT '5) Mart coverage by inverter/date';
SELECT
    internal_plant_code,
    source_plant_code,
    source_device_id AS serial_number,
    CAST(collect_time_local AS date) AS local_date,
    COUNT(*) AS row_count,
    MIN(collect_time_local) AS min_local_time,
    MAX(collect_time_local) AS max_local_time,
    MIN(total_energy_kwh) AS min_total_energy_kwh,
    MAX(total_energy_kwh) AS max_total_energy_kwh,
    MAX(raw_id) AS max_raw_id
FROM mart.fact_solaredge_inverter_technical_5min
WHERE source_system_code = 'SOLAREDGE'
GROUP BY
    internal_plant_code,
    source_plant_code,
    source_device_id,
    CAST(collect_time_local AS date)
ORDER BY local_date DESC, internal_plant_code, serial_number;
GO

/*
M7 diagnostics: SolarEdge sensor / irradiance 5-minute ingest
Run in SolarDataDB after scripts.run_solaredge_sensor_5min_ingest.
*/

SET NOCOUNT ON;

PRINT '1) Latest raw sensorData calls';
SELECT TOP (50)
    raw_id,
    source_system_code,
    endpoint_name,
    internal_plant_code,
    source_plant_code,
    request_window_start_utc,
    request_window_end_utc,
    http_status,
    api_success_flag,
    response_size_bytes,
    inserted_at_utc
FROM raw.api_call_v2
WHERE source_system_code = 'SOLAREDGE'
  AND endpoint_name = 'sensorData'
ORDER BY raw_id DESC;

PRINT '2) Canonical sensor metric coverage by raw_id';
SELECT TOP (200)
    c.raw_id,
    c.internal_plant_code,
    c.source_plant_code,
    c.source_device_id,
    c.canonical_metric_code,
    c.unit_code,
    COUNT(*) AS row_count,
    MIN(c.collect_time_utc) AS min_time_utc,
    MAX(c.collect_time_utc) AS max_time_utc,
    MIN(c.metric_value_num) AS min_value,
    MAX(c.metric_value_num) AS max_value
FROM norm.canonical_metric_selected c
WHERE c.source_system_code = 'SOLAREDGE'
  AND c.device_scope = 'SENSOR'
GROUP BY
    c.raw_id,
    c.internal_plant_code,
    c.source_plant_code,
    c.source_device_id,
    c.canonical_metric_code,
    c.unit_code
ORDER BY c.raw_id DESC, c.internal_plant_code, c.source_device_id, c.canonical_metric_code;

PRINT '3) Latest mart sensor rows';
SELECT TOP (100)
    source_system_code,
    internal_plant_code,
    source_plant_code,
    source_device_id,
    source_device_name,
    collect_time_utc,
    collect_time_local,
    global_horizontal_irradiance_wm2,
    plane_of_array_irradiance_wm2,
    irradiance_wm2_best_effort,
    ambient_temperature_c,
    module_temperature_c,
    wind_speed_raw,
    raw_id,
    data_quality_status,
    inserted_at_utc,
    updated_at_utc
FROM mart.fact_solaredge_sensor_5min
ORDER BY collect_time_utc DESC, internal_plant_code, source_device_id;

PRINT '4) Mart daily coverage by plant / sensor group';
SELECT
    internal_plant_code,
    source_plant_code,
    source_device_id,
    CAST(collect_time_local AS date) AS local_date,
    COUNT(*) AS row_count,
    MIN(collect_time_local) AS min_local_time,
    MAX(collect_time_local) AS max_local_time,
    MIN(irradiance_wm2_best_effort) AS min_irradiance_wm2,
    MAX(irradiance_wm2_best_effort) AS max_irradiance_wm2,
    MIN(ambient_temperature_c) AS min_ambient_temperature_c,
    MAX(ambient_temperature_c) AS max_ambient_temperature_c,
    MAX(raw_id) AS max_raw_id
FROM mart.fact_solaredge_sensor_5min
WHERE source_system_code = 'SOLAREDGE'
GROUP BY
    internal_plant_code,
    source_plant_code,
    source_device_id,
    CAST(collect_time_local AS date)
ORDER BY local_date DESC, internal_plant_code, source_device_id;

PRINT '5) Potential duplicate canonical sensor rows check';
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
WHERE source_system_code = 'SOLAREDGE'
  AND device_scope = 'SENSOR'
GROUP BY
    source_system_code,
    internal_plant_code,
    source_plant_code,
    device_scope,
    source_device_id,
    collect_time_utc,
    canonical_metric_code
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC, collect_time_utc DESC;

PRINT '6) Data quality summary';
SELECT
    internal_plant_code,
    data_quality_status,
    COUNT(*) AS row_count,
    MIN(collect_time_local) AS min_local_time,
    MAX(collect_time_local) AS max_local_time
FROM mart.fact_solaredge_sensor_5min
WHERE source_system_code = 'SOLAREDGE'
GROUP BY internal_plant_code, data_quality_status
ORDER BY internal_plant_code, data_quality_status;

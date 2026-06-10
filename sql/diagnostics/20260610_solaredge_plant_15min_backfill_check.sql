USE SolarDataDB;
GO

/* SolarEdge plant-level 15-min backfill diagnostics */

DECLARE @SourceSystem varchar(50) = 'SOLAREDGE';

PRINT '1) Latest SolarEdge plant-level raw calls';
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
WHERE source_system_code = @SourceSystem
  AND endpoint_name IN ('sitePower', 'energyDetails')
ORDER BY raw_id DESC;

PRINT '2) Plant power mart coverage by plant/date';
SELECT TOP (200)
    source_system_code,
    internal_plant_code,
    source_plant_code,
    CAST(collect_time_local AS date) AS local_date,
    COUNT_BIG(*) AS row_count,
    MIN(collect_time_local) AS min_local_time,
    MAX(collect_time_local) AS max_local_time,
    MIN(active_power_kw) AS min_active_power_kw,
    MAX(active_power_kw) AS max_active_power_kw,
    MAX(raw_id) AS max_raw_id,
    MAX(COALESCE(updated_at_utc, inserted_at_utc)) AS last_touched_at_utc
FROM mart.fact_solar_plant_power_15min
WHERE source_system_code = @SourceSystem
GROUP BY
    source_system_code,
    internal_plant_code,
    source_plant_code,
    CAST(collect_time_local AS date)
ORDER BY local_date DESC, internal_plant_code;

PRINT '3) Plant energy mart coverage by plant/date';
SELECT TOP (200)
    source_system_code,
    internal_plant_code,
    source_plant_code,
    CAST(collect_time_local AS date) AS local_date,
    COUNT_BIG(*) AS row_count,
    MIN(collect_time_local) AS min_local_time,
    MAX(collect_time_local) AS max_local_time,
    SUM(COALESCE(production_energy_kwh, 0)) AS sum_production_energy_kwh,
    SUM(COALESCE(feed_in_energy_kwh, 0)) AS sum_feed_in_energy_kwh,
    SUM(COALESCE(purchased_energy_kwh, 0)) AS sum_purchased_energy_kwh,
    SUM(COALESCE(self_consumption_energy_kwh, 0)) AS sum_self_consumption_energy_kwh,
    MAX(raw_id) AS max_raw_id,
    MAX(COALESCE(updated_at_utc, inserted_at_utc)) AS last_touched_at_utc
FROM mart.fact_solar_plant_energy_15min
WHERE source_system_code = @SourceSystem
GROUP BY
    source_system_code,
    internal_plant_code,
    source_plant_code,
    CAST(collect_time_local AS date)
ORDER BY local_date DESC, internal_plant_code;

PRINT '4) Nearline checkpoint state (not historical backfill completeness)';
SELECT
    internal_plant_code,
    source_plant_code,
    endpoint_name,
    last_success_start_local,
    last_success_end_local,
    last_success_start_utc,
    last_success_end_utc,
    last_raw_id,
    last_status,
    consecutive_failures,
    updated_at_utc
FROM ctl.solaredge_ingest_checkpoint
WHERE source_system_code = @SourceSystem
ORDER BY endpoint_name, internal_plant_code;

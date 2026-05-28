CREATE OR ALTER VIEW mart.vw_plant_inverter_realtime_latest AS
SELECT
    plant_code,

    MAX(collect_time_utc) AS collect_time_utc,

    SUM(CASE WHEN active_power_kw IS NOT NULL THEN active_power_kw ELSE 0 END) AS plant_active_power_kw,

    COUNT(*) AS inverter_seen_count,

    SUM(CASE WHEN active_power_kw IS NOT NULL THEN 1 ELSE 0 END) AS reporting_inverter_count,

    MAX(data_age_minutes) AS max_data_age_minutes,

    CASE
        WHEN SUM(CASE WHEN active_power_kw IS NOT NULL THEN 1 ELSE 0 END) = 0
            THEN 'MISSING_POWER'
        WHEN MAX(data_age_minutes) > 30
            THEN 'STALE'
        WHEN SUM(CASE WHEN data_quality_status <> 'GOOD' THEN 1 ELSE 0 END) > 0
            THEN 'PARTIAL'
        ELSE 'GOOD'
    END AS data_quality_status
FROM mart.vw_inverter_realtime_latest
GROUP BY
    plant_code;
GO
/*
SolarToPI Milestone 23 validation
Scope: Reduce realtime inter-step delay 60s -> 30s controlled scheduler/manual test.
Run after each 30s timed cycle round.

Expected:
1) No 305/401/407/429 in recent API calls
2) Latest requests exist for getStationRealKpi and getDevRealKpi devType 1/10/17
3) Selected inverter latest view is fresh
4) Whitelist leakage = 0
5) PV/MPPT leakage = 0
6) No stuck lock file should be confirmed at OS level: logs\realtime_cycle_timed_30s.lock absent
*/

SET NOCOUNT ON;

DECLARE @SinceUtc datetime2(0) = DATEADD(hour, -2, SYSUTCDATETIME());

PRINT '1) API error check: expect zero rows';
SELECT
    api_name,
    dev_type_id,
    fail_code,
    COUNT(*) AS error_count,
    MAX(request_started_at_utc) AS latest_error_utc
FROM raw.api_call
WHERE request_started_at_utc >= @SinceUtc
  AND fail_code IN (305,401,407,429)
GROUP BY api_name, dev_type_id, fail_code
ORDER BY latest_error_utc DESC;

PRINT '2) Latest API requests: expect getStationRealKpi and getDevRealKpi devType 1/10/17';
SELECT
    api_name,
    dev_type_id,
    MAX(request_started_at_utc) AS latest_request_started_utc,
    COUNT(*) AS call_count
FROM raw.api_call
WHERE request_started_at_utc >= @SinceUtc
  AND api_name IN ('getStationRealKpi', 'getDevRealKpi')
GROUP BY api_name, dev_type_id
ORDER BY api_name, dev_type_id;

PRINT '3) Selected inverter latest view: expect fresh selected plants';
SELECT
    plant_code,
    collect_time_utc,
    plant_active_power_kw,
    inverter_seen_count,
    reporting_inverter_count,
    max_data_age_minutes,
    data_quality_status
FROM mart.vw_plant_inverter_realtime_latest
WHERE plant_code IN (
    'NE=50281829',
    'NE=50979503',
    'NE=49768564',
    'NE=50173922',
    'NE=56663402'
)
ORDER BY plant_code;

PRINT '4) Whitelist leakage by latest raw_id: expect zero rows';
;WITH latest_raw AS (
    SELECT raw_id
    FROM raw.api_call
    WHERE request_started_at_utc >= @SinceUtc
      AND api_name = 'getDevRealKpi'
      AND dev_type_id IN (1, 10, 17)
      AND api_success_flag = 1
)
SELECT
    n.source_api,
    n.dev_type_id,
    n.metric_name,
    COUNT_BIG(*) AS row_count
FROM norm.device_metric_long n
JOIN latest_raw r
    ON r.raw_id = n.raw_id
LEFT JOIN norm.metric_whitelist w
    ON w.source_system_code = 'HUAWEI'
   AND w.source_api = n.source_api
   AND w.dev_type_id = n.dev_type_id
   AND w.metric_name = n.metric_name
   AND w.is_enabled = 1
WHERE n.source_api = 'getDevRealKpi'
  AND w.metric_name IS NULL
GROUP BY
    n.source_api,
    n.dev_type_id,
    n.metric_name
ORDER BY row_count DESC;

PRINT '5) PV/MPPT leakage by latest raw_id: expect zero rows';
;WITH latest_raw AS (
    SELECT raw_id
    FROM raw.api_call
    WHERE request_started_at_utc >= @SinceUtc
      AND api_name = 'getDevRealKpi'
      AND dev_type_id IN (1, 10, 17)
      AND api_success_flag = 1
)
SELECT
    n.source_api,
    n.dev_type_id,
    n.metric_name,
    COUNT_BIG(*) AS row_count
FROM norm.device_metric_long n
JOIN latest_raw r
    ON r.raw_id = n.raw_id
WHERE n.source_api = 'getDevRealKpi'
  AND (
        n.metric_name LIKE 'pv%[_]u'
     OR n.metric_name LIKE 'pv%[_]i'
     OR n.metric_name LIKE 'mppt%'
  )
GROUP BY
    n.source_api,
    n.dev_type_id,
    n.metric_name
ORDER BY row_count DESC;

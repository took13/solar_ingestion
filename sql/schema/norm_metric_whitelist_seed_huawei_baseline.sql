/* ============================================================
   SolarToPI - Huawei Metric Whitelist Seed
   Scope:
   - PI / Realtime Export
   - Report Mart
   - Forecast / Analytics
   - RCA registered but disabled

   Assumption:
   - norm.metric_whitelist is the NEW source-aware schema:
       source_system_code, source_api, dev_type_id, metric_name
   ============================================================ */

SET XACT_ABORT ON;
BEGIN TRAN;

/* ------------------------------------------------------------
   1) Add governance columns if not exists
------------------------------------------------------------ */

IF COL_LENGTH('norm.metric_whitelist', 'keep_for_pi') IS NULL
BEGIN
    ALTER TABLE norm.metric_whitelist ADD
        keep_for_pi bit NOT NULL
            CONSTRAINT DF_metric_whitelist_keep_for_pi DEFAULT (0),
        keep_for_report bit NOT NULL
            CONSTRAINT DF_metric_whitelist_keep_for_report DEFAULT (0),
        keep_for_forecast bit NOT NULL
            CONSTRAINT DF_metric_whitelist_keep_for_forecast DEFAULT (0),
        keep_for_analytics bit NOT NULL
            CONSTRAINT DF_metric_whitelist_keep_for_analytics DEFAULT (0),
        keep_for_rca bit NOT NULL
            CONSTRAINT DF_metric_whitelist_keep_for_rca DEFAULT (0);
END;
GO

SET XACT_ABORT ON;
BEGIN TRAN;

/* ------------------------------------------------------------
   2) Baseline enabled metrics
   - enabled = safe for production baseline
   - no pv*_u / pv*_i / mppt detail here
------------------------------------------------------------ */

;WITH src AS (
    SELECT *
    FROM (VALUES
        /* ====================================================
           Inverter devType 1 - Realtime
           ==================================================== */
        ('HUAWEI','getDevRealKpi',1,'active_power',1,0,0,'mart','PI/export/realtime power','hot',365,1,1,1,1,1),
        ('HUAWEI','getDevRealKpi',1,'run_state',1,0,0,'mart','PI status / availability','hot',365,1,0,0,1,1),
        ('HUAWEI','getDevRealKpi',1,'inverter_state',1,0,0,'mart','PI status / availability','hot',365,1,0,0,1,1),
        ('HUAWEI','getDevRealKpi',1,'day_cap',1,0,0,'mart','daily yield validation','hot',365,1,1,1,1,1),
        ('HUAWEI','getDevRealKpi',1,'total_cap',1,0,0,'mart','counter for PVYield / report','hot',365,0,1,1,1,1),
        ('HUAWEI','getDevRealKpi',1,'temperature',1,0,0,'mart','thermal monitoring / analytics','warm',180,1,0,1,1,1),
        ('HUAWEI','getDevRealKpi',1,'efficiency',1,0,0,'mart','performance analytics','warm',180,0,0,1,1,1),
        ('HUAWEI','getDevRealKpi',1,'power_factor',1,0,0,'mart','power quality','warm',180,1,0,0,1,1),
        ('HUAWEI','getDevRealKpi',1,'elec_freq',1,0,0,'mart','grid frequency','warm',180,0,0,0,1,1),
        ('HUAWEI','getDevRealKpi',1,'reactive_power',1,0,0,'mart','reactive power / power quality','warm',180,1,0,0,1,1),
        ('HUAWEI','getDevRealKpi',1,'mppt_power',1,0,0,'mart','aggregate MPPT power analytics','warm',90,0,0,0,1,1),

        /* ====================================================
           Inverter devType 1 - Historical
           ==================================================== */
        ('HUAWEI','getDevHistoryKpi',1,'active_power',1,0,0,'mart','historical power / analytics','hot',365,0,1,1,1,1),
        ('HUAWEI','getDevHistoryKpi',1,'day_cap',1,0,0,'mart','daily yield validation','hot',365,0,1,1,1,1),
        ('HUAWEI','getDevHistoryKpi',1,'total_cap',1,0,0,'mart','counter for report mart PVYield','hot',365,0,1,1,1,1),
        ('HUAWEI','getDevHistoryKpi',1,'temperature',1,0,0,'mart','thermal analytics','warm',180,0,0,1,1,1),
        ('HUAWEI','getDevHistoryKpi',1,'efficiency',1,0,0,'mart','performance analytics','warm',180,0,0,1,1,1),
        ('HUAWEI','getDevHistoryKpi',1,'power_factor',1,0,0,'mart','power quality analytics','warm',180,0,0,0,1,1),
        ('HUAWEI','getDevHistoryKpi',1,'elec_freq',1,0,0,'mart','grid frequency analytics','warm',180,0,0,0,1,1),
        ('HUAWEI','getDevHistoryKpi',1,'reactive_power',1,0,0,'mart','reactive power analytics','warm',180,0,0,0,1,1),
        ('HUAWEI','getDevHistoryKpi',1,'mppt_power',1,0,0,'mart','aggregate MPPT power analytics','warm',90,0,0,0,1,1),

        /* ====================================================
           EMI devType 10 - Realtime
           ==================================================== */
        ('HUAWEI','getDevRealKpi',10,'radiant_line',1,0,0,'mart','irradiance for PI/export/forecast','hot',365,1,1,1,1,1),
        ('HUAWEI','getDevRealKpi',10,'horiz_radiant_line',1,0,0,'mart','horizontal irradiance forecast input','hot',365,1,1,1,1,1),
        ('HUAWEI','getDevRealKpi',10,'temperature',1,0,0,'mart','ambient temperature','hot',365,1,1,1,1,1),
        ('HUAWEI','getDevRealKpi',10,'pv_temperature',1,0,0,'mart','module temperature forecast input','warm',180,1,0,1,1,1),
        ('HUAWEI','getDevRealKpi',10,'wind_speed',1,0,0,'mart','weather forecast/analytics input','warm',180,0,0,1,1,1),
        ('HUAWEI','getDevRealKpi',10,'wind_direction',1,0,0,'mart','weather analytics input','warm',180,0,0,0,1,1),
        ('HUAWEI','getDevRealKpi',10,'run_state',1,0,0,'mart','EMI status','warm',180,1,0,0,1,1),

        /* ====================================================
           EMI devType 10 - Historical
           ==================================================== */
        ('HUAWEI','getDevHistoryKpi',10,'radiant_line',1,0,0,'mart','irradiance historical analytics','hot',365,0,1,1,1,1),
        ('HUAWEI','getDevHistoryKpi',10,'horiz_radiant_line',1,0,0,'mart','horizontal irradiance historical forecast','hot',365,0,1,1,1,1),
        ('HUAWEI','getDevHistoryKpi',10,'radiant_total',1,0,0,'mart','counter for irradiation report mart','hot',365,0,1,1,1,1),
        ('HUAWEI','getDevHistoryKpi',10,'horiz_radiant_total',1,0,0,'mart','horizontal irradiation counter','hot',365,0,1,1,1,1),
        ('HUAWEI','getDevHistoryKpi',10,'temperature',1,0,0,'mart','ambient temperature historical','hot',365,0,1,1,1,1),
        ('HUAWEI','getDevHistoryKpi',10,'pv_temperature',1,0,0,'mart','module temperature historical','warm',180,0,0,1,1,1),
        ('HUAWEI','getDevHistoryKpi',10,'wind_speed',1,0,0,'mart','weather historical forecast input','warm',180,0,0,1,1,1),
        ('HUAWEI','getDevHistoryKpi',10,'wind_direction',1,0,0,'mart','weather historical analytics','warm',180,0,0,0,1,1),

        /* ====================================================
           Meter devType 17 - Realtime
           ==================================================== */
        ('HUAWEI','getDevRealKpi',17,'active_power',1,0,0,'mart','meter active power','hot',365,1,1,1,1,1),
        ('HUAWEI','getDevRealKpi',17,'reactive_power',1,0,0,'mart','meter reactive power','warm',180,1,0,0,1,1),
        ('HUAWEI','getDevRealKpi',17,'power_factor',1,0,0,'mart','meter power factor','warm',180,1,0,0,1,1),
        ('HUAWEI','getDevRealKpi',17,'grid_frequency',1,0,0,'mart','grid frequency','warm',180,0,0,0,1,1),
        ('HUAWEI','getDevRealKpi',17,'ab_u',1,0,0,'mart','line voltage AB','warm',180,1,0,0,1,1),
        ('HUAWEI','getDevRealKpi',17,'bc_u',1,0,0,'mart','line voltage BC','warm',180,1,0,0,1,1),
        ('HUAWEI','getDevRealKpi',17,'ca_u',1,0,0,'mart','line voltage CA','warm',180,1,0,0,1,1),
        ('HUAWEI','getDevRealKpi',17,'a_i',1,0,0,'mart','phase current A','warm',180,1,0,0,1,1),
        ('HUAWEI','getDevRealKpi',17,'b_i',1,0,0,'mart','phase current B','warm',180,1,0,0,1,1),
        ('HUAWEI','getDevRealKpi',17,'c_i',1,0,0,'mart','phase current C','warm',180,1,0,0,1,1),

        /* ====================================================
           Meter devType 17 - Historical
           ==================================================== */
        ('HUAWEI','getDevHistoryKpi',17,'active_power',1,0,0,'mart','meter active power history','hot',365,0,1,1,1,1),
        ('HUAWEI','getDevHistoryKpi',17,'active_cap',1,0,0,'mart','on-grid energy counter','hot',365,0,1,1,1,1),
        ('HUAWEI','getDevHistoryKpi',17,'reverse_active_cap',1,0,0,'mart','reverse meter energy counter','hot',365,0,1,1,1,1),
        ('HUAWEI','getDevHistoryKpi',17,'reactive_power',1,0,0,'mart','reactive power history','warm',180,0,0,0,1,1),
        ('HUAWEI','getDevHistoryKpi',17,'power_factor',1,0,0,'mart','power factor history','warm',180,0,0,0,1,1),
        ('HUAWEI','getDevHistoryKpi',17,'grid_frequency',1,0,0,'mart','grid frequency history','warm',180,0,0,0,1,1),
        ('HUAWEI','getDevHistoryKpi',17,'ab_u',1,0,0,'mart','line voltage AB history','warm',180,0,0,0,1,1),
        ('HUAWEI','getDevHistoryKpi',17,'bc_u',1,0,0,'mart','line voltage BC history','warm',180,0,0,0,1,1),
        ('HUAWEI','getDevHistoryKpi',17,'ca_u',1,0,0,'mart','line voltage CA history','warm',180,0,0,0,1,1),
        ('HUAWEI','getDevHistoryKpi',17,'a_i',1,0,0,'mart','phase current A history','warm',180,0,0,0,1,1),
        ('HUAWEI','getDevHistoryKpi',17,'b_i',1,0,0,'mart','phase current B history','warm',180,0,0,0,1,1),
        ('HUAWEI','getDevHistoryKpi',17,'c_i',1,0,0,'mart','phase current C history','warm',180,0,0,0,1,1)
    ) v (
        source_system_code,
        source_api,
        dev_type_id,
        metric_name,
        is_enabled,
        keep_null,
        keep_raw_text,
        target_layer,
        use_case,
        retention_level,
        min_keep_days,
        keep_for_pi,
        keep_for_report,
        keep_for_forecast,
        keep_for_analytics,
        keep_for_rca
    )
)
MERGE norm.metric_whitelist AS tgt
USING src
ON  tgt.source_system_code = src.source_system_code
AND tgt.source_api = src.source_api
AND tgt.dev_type_id = src.dev_type_id
AND tgt.metric_name = src.metric_name
WHEN MATCHED THEN
    UPDATE SET
        tgt.is_enabled = src.is_enabled,
        tgt.keep_null = src.keep_null,
        tgt.keep_raw_text = src.keep_raw_text,
        tgt.target_layer = src.target_layer,
        tgt.use_case = src.use_case,
        tgt.retention_level = src.retention_level,
        tgt.min_keep_days = src.min_keep_days,
        tgt.keep_for_pi = src.keep_for_pi,
        tgt.keep_for_report = src.keep_for_report,
        tgt.keep_for_forecast = src.keep_for_forecast,
        tgt.keep_for_analytics = src.keep_for_analytics,
        tgt.keep_for_rca = src.keep_for_rca,
        tgt.updated_at_utc = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
    INSERT (
        source_system_code,
        source_api,
        dev_type_id,
        metric_name,
        is_enabled,
        keep_null,
        keep_raw_text,
        target_layer,
        use_case,
        retention_level,
        min_keep_days,
        keep_for_pi,
        keep_for_report,
        keep_for_forecast,
        keep_for_analytics,
        keep_for_rca
    )
    VALUES (
        src.source_system_code,
        src.source_api,
        src.dev_type_id,
        src.metric_name,
        src.is_enabled,
        src.keep_null,
        src.keep_raw_text,
        src.target_layer,
        src.use_case,
        src.retention_level,
        src.min_keep_days,
        src.keep_for_pi,
        src.keep_for_report,
        src.keep_for_forecast,
        src.keep_for_analytics,
        src.keep_for_rca
    );

/* ------------------------------------------------------------
   3) Register high-volume RCA metrics but keep disabled
   - PV string voltage/current
   - MPPT detailed metrics
------------------------------------------------------------ */

;WITH n AS (
    SELECT TOP (36)
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i
    FROM sys.objects
),
rca_metrics AS (
    SELECT
        'HUAWEI' AS source_system_code,
        api.source_api,
        CAST(1 AS int) AS dev_type_id,
        CONCAT('pv', n.i, '_u') AS metric_name,
        CAST(0 AS bit) AS is_enabled,
        CAST(0 AS bit) AS keep_null,
        CAST(0 AS bit) AS keep_raw_text,
        CAST('rca' AS varchar(50)) AS target_layer,
        CAST('PV string voltage RCA only - disabled baseline' AS nvarchar(200)) AS use_case,
        CAST('warm' AS varchar(50)) AS retention_level,
        CAST(30 AS int) AS min_keep_days,
        CAST(0 AS bit) AS keep_for_pi,
        CAST(0 AS bit) AS keep_for_report,
        CAST(0 AS bit) AS keep_for_forecast,
        CAST(0 AS bit) AS keep_for_analytics,
        CAST(1 AS bit) AS keep_for_rca
    FROM n
    CROSS JOIN (VALUES ('getDevRealKpi'), ('getDevHistoryKpi')) api(source_api)

    UNION ALL

    SELECT
        'HUAWEI',
        api.source_api,
        1,
        CONCAT('pv', n.i, '_i'),
        0,
        0,
        0,
        'rca',
        N'PV string current RCA only - disabled baseline',
        'warm',
        30,
        0,
        0,
        0,
        0,
        1
    FROM n
    CROSS JOIN (VALUES ('getDevRealKpi'), ('getDevHistoryKpi')) api(source_api)

    UNION ALL

    SELECT
        'HUAWEI',
        api.source_api,
        1,
        metric_name,
        0,
        0,
        0,
        'rca',
        N'MPPT detail RCA only - disabled baseline',
        'warm',
        30,
        0,
        0,
        0,
        0,
        1
    FROM (VALUES
        ('mppt_total_cap'),
        ('mppt_1_cap'),
        ('mppt_2_cap'),
        ('mppt_3_cap'),
        ('mppt_4_cap'),
        ('mppt_5_cap'),
        ('mppt_6_cap'),
        ('mppt_7_cap'),
        ('mppt_8_cap'),
        ('mppt_9_cap'),
        ('mppt_10_cap')
    ) m(metric_name)
    CROSS JOIN (VALUES ('getDevRealKpi'), ('getDevHistoryKpi')) api(source_api)
)
MERGE norm.metric_whitelist AS tgt
USING rca_metrics AS src
ON  tgt.source_system_code = src.source_system_code
AND tgt.source_api = src.source_api
AND tgt.dev_type_id = src.dev_type_id
AND tgt.metric_name = src.metric_name
WHEN MATCHED THEN
    UPDATE SET
        tgt.is_enabled = src.is_enabled,
        tgt.keep_null = src.keep_null,
        tgt.keep_raw_text = src.keep_raw_text,
        tgt.target_layer = src.target_layer,
        tgt.use_case = src.use_case,
        tgt.retention_level = src.retention_level,
        tgt.min_keep_days = src.min_keep_days,
        tgt.keep_for_pi = src.keep_for_pi,
        tgt.keep_for_report = src.keep_for_report,
        tgt.keep_for_forecast = src.keep_for_forecast,
        tgt.keep_for_analytics = src.keep_for_analytics,
        tgt.keep_for_rca = src.keep_for_rca,
        tgt.updated_at_utc = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
    INSERT (
        source_system_code,
        source_api,
        dev_type_id,
        metric_name,
        is_enabled,
        keep_null,
        keep_raw_text,
        target_layer,
        use_case,
        retention_level,
        min_keep_days,
        keep_for_pi,
        keep_for_report,
        keep_for_forecast,
        keep_for_analytics,
        keep_for_rca
    )
    VALUES (
        src.source_system_code,
        src.source_api,
        src.dev_type_id,
        src.metric_name,
        src.is_enabled,
        src.keep_null,
        src.keep_raw_text,
        src.target_layer,
        src.use_case,
        src.retention_level,
        src.min_keep_days,
        src.keep_for_pi,
        src.keep_for_report,
        src.keep_for_forecast,
        src.keep_for_analytics,
        src.keep_for_rca
    );

COMMIT;

/* Summary */
SELECT
    source_system_code,
    source_api,
    dev_type_id,
    is_enabled,
    target_layer,
    COUNT(*) AS metric_count
FROM norm.metric_whitelist
WHERE source_system_code = 'HUAWEI'
GROUP BY
    source_system_code,
    source_api,
    dev_type_id,
    is_enabled,
    target_layer
ORDER BY
    source_api,
    dev_type_id,
    is_enabled DESC,
    target_layer;

/* Must be zero for enabled high-volume baseline */
SELECT *
FROM norm.metric_whitelist
WHERE source_system_code = 'HUAWEI'
  AND is_enabled = 1
  AND (
        metric_name LIKE 'pv%[_]u'
     OR metric_name LIKE 'pv%[_]i'
     OR metric_name LIKE 'mppt%[_]cap'
     OR metric_name = 'mppt_total_cap'
  );

/* Review enabled baseline */
SELECT
    source_api,
    dev_type_id,
    metric_name,
    target_layer,
    keep_for_pi,
    keep_for_report,
    keep_for_forecast,
    keep_for_analytics,
    keep_for_rca,
    retention_level,
    min_keep_days
FROM norm.metric_whitelist
WHERE source_system_code = 'HUAWEI'
  AND is_enabled = 1
ORDER BY
    dev_type_id,
    source_api,
    metric_name;


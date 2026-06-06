from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.main import build_app


SQL = """
SET NOCOUNT ON;

DECLARE @FromUtc datetime2(0) = '2024-12-31T17:00:00';
DECLARE @ToUtc   datetime2(0) = '2026-05-20T17:00:00';

WITH inv_device_5min AS (
    SELECT
        f.plant_code,
        f.dev_id,
        DATEADD(minute, DATEDIFF(minute, 0, f.collect_time_utc) / 5 * 5, 0) AS bucket_utc,
        AVG(f.active_power_kw) AS active_power_kw,
        MAX(f.total_cap_kwh) AS total_cap_kwh
    FROM mart.fact_dev_inverter_5min f
    WHERE f.plant_code IN ('NE=50281829','NE=50979503')
      AND f.collect_time_utc >= @FromUtc
      AND f.collect_time_utc <  @ToUtc
    GROUP BY
        f.plant_code,
        f.dev_id,
        DATEADD(minute, DATEDIFF(minute, 0, f.collect_time_utc) / 5 * 5, 0)
),
plant_inv_5min AS (
    SELECT
        plant_code,
        bucket_utc AS collect_time_utc,
        SUM(total_cap_kwh) AS accumulated_energy_kwh,
        SUM(COALESCE(active_power_kw, 0)) AS inverter_kw,
        COUNT(DISTINCT dev_id) AS inverter_count,
        SUM(CASE WHEN active_power_kw IS NOT NULL THEN 1 ELSE 0 END) AS reporting_inverter_count
    FROM inv_device_5min
    GROUP BY
        plant_code,
        bucket_utc
),
plant_inv_delta AS (
    SELECT
        p.*,
        LAG(p.collect_time_utc) OVER (
            PARTITION BY p.plant_code
            ORDER BY p.collect_time_utc
        ) AS prev_collect_time_utc,
        LAG(p.accumulated_energy_kwh) OVER (
            PARTITION BY p.plant_code
            ORDER BY p.collect_time_utc
        ) AS prev_accumulated_energy_kwh
    FROM plant_inv_5min p
),
emi_5min AS (
    SELECT
        e.plant_code,
        DATEADD(minute, DATEDIFF(minute, 0, e.collect_time_utc) / 5 * 5, 0) AS collect_time_utc,
        AVG(e.radiant_line_wm2) AS irradiance_wm2,
        AVG(e.temperature_c) AS ambient_temperature_c,
        MAX(e.data_quality_status) AS emi_quality_status
    FROM mart.vw_plant_emi_5min_selected_qc e
    WHERE e.plant_code IN ('NE=50281829','NE=50979503')
      AND e.collect_time_utc >= @FromUtc
      AND e.collect_time_utc <  @ToUtc
    GROUP BY
        e.plant_code,
        DATEADD(minute, DATEDIFF(minute, 0, e.collect_time_utc) / 5 * 5, 0)
)
SELECT
    d.plant_code,
    ISNULL(REPLACE(dp.plant_name, ',', ' '), '') AS plant_name,
    CONVERT(varchar(19), DATEADD(hour, 7, d.collect_time_utc), 120) AS collect_time_local,
    CONVERT(varchar(19), d.collect_time_utc, 120) AS collect_time_utc,

    CAST(d.accumulated_energy_kwh AS decimal(18,4)) AS accumulated_energy_kwh,

    CAST(
        CASE
            WHEN d.prev_accumulated_energy_kwh IS NULL THEN NULL
            WHEN DATEDIFF(minute, d.prev_collect_time_utc, d.collect_time_utc) <> 5 THEN NULL
            WHEN d.accumulated_energy_kwh < d.prev_accumulated_energy_kwh THEN NULL
            ELSE d.accumulated_energy_kwh - d.prev_accumulated_energy_kwh
        END AS decimal(18,4)
    ) AS energy_delta_5min_kwh,

    CAST(
        CASE
            WHEN d.prev_accumulated_energy_kwh IS NULL THEN NULL
            WHEN DATEDIFF(minute, d.prev_collect_time_utc, d.collect_time_utc) <> 5 THEN NULL
            WHEN d.accumulated_energy_kwh < d.prev_accumulated_energy_kwh THEN NULL
            ELSE (d.accumulated_energy_kwh - d.prev_accumulated_energy_kwh) * 12.0
        END AS decimal(18,4)
    ) AS avg_power_from_energy_kw,

    CAST(d.inverter_kw AS decimal(18,4)) AS inverter_kw,
    CAST(e.irradiance_wm2 AS decimal(18,4)) AS irradiance_wm2,
    CAST(e.ambient_temperature_c AS decimal(18,4)) AS ambient_temperature_c,

    d.inverter_count,
    d.reporting_inverter_count,

    CASE
        WHEN d.prev_accumulated_energy_kwh IS NULL THEN 'FIRST_ROW'
        WHEN DATEDIFF(minute, d.prev_collect_time_utc, d.collect_time_utc) <> 5 THEN 'GAP_INTERVAL'
        WHEN d.accumulated_energy_kwh < d.prev_accumulated_energy_kwh THEN 'NEGATIVE_COUNTER_DELTA'
        WHEN e.irradiance_wm2 IS NULL AND e.ambient_temperature_c IS NULL THEN 'MISSING_EMI'
        WHEN e.emi_quality_status = 'SUSPECT' THEN 'SUSPECT_EMI'
        ELSE 'GOOD'
    END AS data_quality_status
FROM plant_inv_delta d
LEFT JOIN dbo.dim_plant dp
    ON dp.plant_code = d.plant_code
LEFT JOIN emi_5min e
    ON e.plant_code = d.plant_code
   AND e.collect_time_utc = d.collect_time_utc
ORDER BY
    d.plant_code,
    d.collect_time_utc;
"""


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output",
        default=r"C:\SOLAR\exports\gc5_polyplex_plant_5min_20250101_20260520.csv",
    )
    parser.add_argument("--fetch-size", type=int, default=10000)
    args = parser.parse_args()

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    app = build_app()
    cursor = app.conn.cursor()
    cursor.execute(SQL)

    columns = [col[0] for col in cursor.description]

    row_count = 0
    with output_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(columns)

        while True:
            rows = cursor.fetchmany(args.fetch_size)
            if not rows:
                break

            writer.writerows(rows)
            row_count += len(rows)
            print(f"[EXPORT] rows={row_count:,}")

    print(f"[DONE] Exported rows={row_count:,}")
    print(f"[DONE] File={output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
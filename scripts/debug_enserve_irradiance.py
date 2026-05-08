from __future__ import annotations

import argparse
from datetime import timezone

from src.main import build_app


DEFAULT_PLANT_CODES = ["NE=50281829", "NE=50979503"]


def to_iso_utc(dt):
    if dt is None:
        return None
    if isinstance(dt, str):
        return dt
    return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def fetch_latest_view_rows(cursor, plant_code: str, top_n: int):
    cursor.execute(
        """
        SELECT TOP (?)
            plant_code,
            collect_time_utc,
            power_kw,
            number_inverter,
            irradiance_wm2,
            temperature_c
        FROM mart.vw_enserve_15min_export
        WHERE plant_code = ?
        ORDER BY collect_time_utc DESC;
        """,
        top_n,
        plant_code,
    )
    return cursor.fetchall()


def fetch_metric_summary(cursor, plant_code: str, hours: int):
    cursor.execute(
        """
        SELECT
            metric_name,
            COUNT(*) AS row_count,
            MIN(metric_value_num) AS min_value,
            MAX(metric_value_num) AS max_value,
            AVG(metric_value_num) AS avg_value,
            MAX(collect_time_utc) AS latest_time_utc
        FROM norm.device_metric_long
        WHERE plant_code = ?
          AND dev_type_id = 10
          AND collect_time_utc >= DATEADD(hour, -?, SYSUTCDATETIME())
          AND metric_name IN
          (
              'horiz_radiant_line',
              'radiant_line',
              'irradiance',
              'irradiance_wm2',
              'temperature',
              'ambient_temperature',
              'pv_temperature'
          )
        GROUP BY metric_name
        ORDER BY metric_name;
        """,
        plant_code,
        hours,
    )
    return cursor.fetchall()


def fetch_debug_rows(cursor, plant_code: str, top_n: int):
    """
    Compare Enserve 15-min rows with nearest EMI metrics.
    This intentionally does NOT hide missing values with COALESCE until payload preview.
    """
    cursor.execute(
        """
        WITH latest AS
        (
            SELECT TOP (?)
                e.plant_code,
                e.collect_time_utc,
                e.power_kw,
                e.number_inverter,
                e.irradiance_wm2 AS view_irradiance_wm2,
                e.temperature_c AS view_temperature_c
            FROM mart.vw_enserve_15min_export e
            WHERE e.plant_code = ?
            ORDER BY e.collect_time_utc DESC
        )
        SELECT
            l.plant_code,
            l.collect_time_utc,
            l.power_kw,
            l.number_inverter,

            l.view_irradiance_wm2,
            l.view_temperature_c,

            horiz.metric_value_num AS horiz_radiant_line,
            horiz.collect_time_utc AS horiz_time_utc,

            radiant.metric_value_num AS radiant_line,
            radiant.collect_time_utc AS radiant_time_utc,

            irr.metric_value_num AS generic_irradiance,
            irr.collect_time_utc AS generic_irr_time_utc,

            temp.metric_value_num AS temperature,
            temp.collect_time_utc AS temp_time_utc,

            pvtemp.metric_value_num AS pv_temperature,
            pvtemp.collect_time_utc AS pvtemp_time_utc,

            COALESCE
            (
                horiz.metric_value_num,
                radiant.metric_value_num,
                irr.metric_value_num
            ) AS recommended_irradiance_wm2,

            COALESCE
            (
                temp.metric_value_num,
                pvtemp.metric_value_num
            ) AS recommended_temperature_c

        FROM latest l

        OUTER APPLY
        (
            SELECT TOP 1
                d.metric_value_num,
                d.collect_time_utc
            FROM norm.device_metric_long d
            WHERE d.plant_code = l.plant_code
              AND d.dev_type_id = 10
              AND d.metric_name = 'horiz_radiant_line'
              AND d.metric_value_num IS NOT NULL
              AND d.collect_time_utc <= l.collect_time_utc
              AND d.collect_time_utc >= DATEADD(minute, -30, l.collect_time_utc)
            ORDER BY d.collect_time_utc DESC
        ) horiz

        OUTER APPLY
        (
            SELECT TOP 1
                d.metric_value_num,
                d.collect_time_utc
            FROM norm.device_metric_long d
            WHERE d.plant_code = l.plant_code
              AND d.dev_type_id = 10
              AND d.metric_name = 'radiant_line'
              AND d.metric_value_num IS NOT NULL
              AND d.collect_time_utc <= l.collect_time_utc
              AND d.collect_time_utc >= DATEADD(minute, -30, l.collect_time_utc)
            ORDER BY d.collect_time_utc DESC
        ) radiant

        OUTER APPLY
        (
            SELECT TOP 1
                d.metric_value_num,
                d.collect_time_utc
            FROM norm.device_metric_long d
            WHERE d.plant_code = l.plant_code
              AND d.dev_type_id = 10
              AND d.metric_name IN ('irradiance', 'irradiance_wm2')
              AND d.metric_value_num IS NOT NULL
              AND d.collect_time_utc <= l.collect_time_utc
              AND d.collect_time_utc >= DATEADD(minute, -30, l.collect_time_utc)
            ORDER BY d.collect_time_utc DESC
        ) irr

        OUTER APPLY
        (
            SELECT TOP 1
                d.metric_value_num,
                d.collect_time_utc
            FROM norm.device_metric_long d
            WHERE d.plant_code = l.plant_code
              AND d.dev_type_id = 10
              AND d.metric_name IN ('temperature', 'ambient_temperature')
              AND d.metric_value_num IS NOT NULL
              AND d.collect_time_utc <= l.collect_time_utc
              AND d.collect_time_utc >= DATEADD(minute, -30, l.collect_time_utc)
            ORDER BY d.collect_time_utc DESC
        ) temp

        OUTER APPLY
        (
            SELECT TOP 1
                d.metric_value_num,
                d.collect_time_utc
            FROM norm.device_metric_long d
            WHERE d.plant_code = l.plant_code
              AND d.dev_type_id = 10
              AND d.metric_name = 'pv_temperature'
              AND d.metric_value_num IS NOT NULL
              AND d.collect_time_utc <= l.collect_time_utc
              AND d.collect_time_utc >= DATEADD(minute, -30, l.collect_time_utc)
            ORDER BY d.collect_time_utc DESC
        ) pvtemp

        ORDER BY l.collect_time_utc DESC;
        """,
        top_n,
        plant_code,
    )
    return cursor.fetchall()


def build_payload_preview(rows):
    records = []

    for r in rows:
        irradiance = r.recommended_irradiance_wm2
        temperature = r.recommended_temperature_c

        data = {
            "power_kw": float(r.power_kw or 0.0),
            "number_inverter": int(r.number_inverter or 0),
            "irradiance_wm2": float(irradiance or 0.0),
            "temperature_c": float(temperature or 0.0),
        }

        records.append(
            {
                "timestamp": to_iso_utc(r.collect_time_utc),
                "data": data,
                "_debug": {
                    "view_irradiance_wm2": None if r.view_irradiance_wm2 is None else float(r.view_irradiance_wm2),
                    "horiz_radiant_line": None if r.horiz_radiant_line is None else float(r.horiz_radiant_line),
                    "radiant_line": None if r.radiant_line is None else float(r.radiant_line),
                    "generic_irradiance": None if r.generic_irradiance is None else float(r.generic_irradiance),
                    "view_temperature_c": None if r.view_temperature_c is None else float(r.view_temperature_c),
                    "temperature": None if r.temperature is None else float(r.temperature),
                    "pv_temperature": None if r.pv_temperature is None else float(r.pv_temperature),
                },
            }
        )

    return records


def print_metric_summary(rows):
    print("\n=== EMI metric summary from norm.device_metric_long ===")
    if not rows:
        print("No devType 10 EMI metrics found in selected period.")
        return

    for r in rows:
        print(
            f"{r.metric_name:22s} "
            f"count={r.row_count:5d} "
            f"min={r.min_value} "
            f"max={r.max_value} "
            f"avg={r.avg_value} "
            f"latest={r.latest_time_utc}"
        )


def print_debug_rows(rows):
    print("\n=== Compare view vs recommended fallback ===")
    if not rows:
        print("No Enserve view rows found.")
        return

    for r in rows:
        print(
            f"{r.collect_time_utc} | "
            f"power={r.power_kw} | "
            f"view_irr={r.view_irradiance_wm2} | "
            f"horiz={r.horiz_radiant_line} | "
            f"radiant={r.radiant_line} | "
            f"generic_irr={r.generic_irradiance} | "
            f"recommended_irr={r.recommended_irradiance_wm2} | "
            f"view_temp={r.view_temperature_c} | "
            f"temp={r.temperature} | "
            f"pv_temp={r.pv_temperature} | "
            f"recommended_temp={r.recommended_temperature_c}"
        )


def print_payload_preview(records):
    print("\n=== Payload preview, not sent ===")
    for rec in records:
        print(rec)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--plant",
        action="append",
        dest="plants",
        help="Plant code, e.g. NE=50281829. Can be used multiple times.",
    )
    parser.add_argument("--top", type=int, default=8)
    parser.add_argument("--hours", type=int, default=24)
    args = parser.parse_args()

    plants = args.plants or DEFAULT_PLANT_CODES

    app = build_app()
    cursor = app.conn.cursor()

    print("[INFO] Debug Enserve irradiance/temperature")
    print(f"[INFO] plants={plants}")
    print(f"[INFO] top={args.top}, hours={args.hours}")

    for plant_code in plants:
        print("\n" + "=" * 100)
        print(f"Plant: {plant_code}")
        print("=" * 100)

        metric_summary = fetch_metric_summary(cursor, plant_code, args.hours)
        print_metric_summary(metric_summary)

        debug_rows = fetch_debug_rows(cursor, plant_code, args.top)
        print_debug_rows(debug_rows)

        records = build_payload_preview(debug_rows)
        print_payload_preview(records)

    cursor.close()
    app.conn.close()


if __name__ == "__main__":
    main()
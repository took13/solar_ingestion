"""
Enserve 15-minute backfill range sender.

Purpose:
- Backfill Enserve for a date range using 15-minute records generated from norm.device_metric_long.
- Does NOT update ops.api_egress_checkpoint. Hourly incremental checkpoint is owned by
  scripts.run_enserve_15min_hourly_egress.
- Supports dry-run by default. Use --send to actually POST to Enserve and write ops logs.

Usage examples:

Dry run:
    python -m scripts.run_enserve_15min_backfill_range --start-date 2026-05-07 --end-date 2026-05-11 --plant NE=50281829 --plant NE=50979503

Send:
    python -m scripts.run_enserve_15min_backfill_range --start-date 2026-05-07 --end-date 2026-05-11 --plant NE=50281829 --plant NE=50979503 --send --sleep-seconds 1
"""

import argparse
import json
import time
from datetime import date, datetime, timedelta, timezone

import requests

from src.main import build_app


BACKFILL_VIEW_NAME = "mart.vw_enserve_15min_backfill_from_norm"
DEFAULT_PLANTS = ("NE=50281829", "NE=50979503")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Backfill Enserve 15-minute data from a date range."
    )

    parser.add_argument(
        "--date",
        help="Single UTC date to backfill, format YYYY-MM-DD.",
    )
    parser.add_argument(
        "--start-date",
        help="Start UTC date, inclusive, format YYYY-MM-DD.",
    )
    parser.add_argument(
        "--end-date",
        help="End UTC date, inclusive, format YYYY-MM-DD.",
    )
    parser.add_argument(
        "--plant",
        dest="plants",
        action="append",
        help=(
            "Plant code to backfill. Can be specified multiple times. "
            "Default: NE=50281829 and NE=50979503."
        ),
    )
    parser.add_argument(
        "--send",
        action="store_true",
        help="Actually send to Enserve. Without this flag the script only does dry-run.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=1.0,
        help="Sleep seconds between API calls when --send is used. Default: 1.0.",
    )

    args = parser.parse_args()

    if args.date and (args.start_date or args.end_date):
        parser.error("Use either --date OR --start-date/--end-date, not both.")

    if args.date:
        args.start_date = args.date
        args.end_date = args.date

    if not args.start_date or not args.end_date:
        parser.error("Either --date or both --start-date and --end-date are required.")

    return args


def parse_yyyy_mm_dd(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def utc_midnight(d: date) -> datetime:
    return datetime(d.year, d.month, d.day)


def to_iso_utc(dt) -> str:
    if isinstance(dt, str):
        return dt

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def date_range_inclusive(start_date: date, end_date: date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def create_egress_run(cursor, conn, run_mode: str):
    cursor.execute(
        """
        INSERT INTO ops.api_egress_run
        (
            run_mode,
            triggered_by,
            status,
            started_at_utc,
            ended_at_utc,
            message
        )
        OUTPUT INSERTED.egress_run_id
        VALUES
        (
            ?,
            'python_script',
            'RUNNING',
            SYSUTCDATETIME(),
            NULL,
            'Enserve 15-min backfill range started'
        )
        """,
        run_mode,
    )

    row = cursor.fetchone()
    conn.commit()

    return int(row.egress_run_id)


def update_egress_run(cursor, conn, egress_run_id, status, message):
    cursor.execute(
        """
        UPDATE ops.api_egress_run
        SET
            status = ?,
            ended_at_utc = SYSUTCDATETIME(),
            message = ?
        WHERE egress_run_id = ?
        """,
        (
            status,
            message[:4000] if message else None,
            egress_run_id,
        ),
    )
    conn.commit()


def load_enabled_targets(cursor, plants):
    placeholders = ",".join("?" for _ in plants)

    cursor.execute(
        f"""
        SELECT
            egress_target_id,
            plant_code,
            endpoint_url,
            auth_token,
            timeout_seconds
        FROM ops.api_egress_target
        WHERE is_enabled = 1
          AND plant_code IN ({placeholders})
        ORDER BY plant_code
        """,
        plants,
    )

    rows = cursor.fetchall()

    targets = {}
    for r in rows:
        if not r.endpoint_url:
            raise RuntimeError(f"endpoint_url is empty for plant_code={r.plant_code}")

        if not r.auth_token:
            raise RuntimeError(f"auth_token is empty for plant_code={r.plant_code}")

        targets[r.plant_code] = {
            "egress_target_id": int(r.egress_target_id),
            "plant_code": r.plant_code,
            "endpoint_url": r.endpoint_url,
            "auth_token": r.auth_token,
            "timeout_seconds": int(r.timeout_seconds or 30),
        }

    missing = [p for p in plants if p not in targets]
    if missing:
        raise RuntimeError(f"No enabled Enserve target found for plant(s): {missing}")

    return targets


def load_rows_for_window(cursor, plant_code, window_start_utc, window_end_utc):
    cursor.execute(
        f"""
        SELECT
            plant_code,
            collect_time_utc,
            power_kw,
            number_inverter,
            irradiance_wm2,
            temperature_c
        FROM {BACKFILL_VIEW_NAME}
        WHERE plant_code = ?
          AND collect_time_utc >= ?
          AND collect_time_utc < ?
          AND power_kw IS NOT NULL
          AND number_inverter IS NOT NULL
        ORDER BY collect_time_utc ASC
        """,
        plant_code,
        window_start_utc,
        window_end_utc,
    )

    return cursor.fetchall()


def build_records(rows):
    records = []

    for r in rows:
        records.append(
            {
                "timestamp": to_iso_utc(r.collect_time_utc),
                "data": {
                    "power_kw": float(r.power_kw or 0.0),
                    "number_inverter": int(r.number_inverter or 0),
                    "irradiance_wm2": float(r.irradiance_wm2 or 0.0),
                    "temperature_c": float(r.temperature_c or 0.0),
                },
            }
        )

    return records


def post_records(target, records):
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {target['auth_token']}",
    }

    body = {"records": records}

    response = requests.post(
        target["endpoint_url"],
        headers=headers,
        json=body,
        timeout=target["timeout_seconds"],
    )

    return response, body


def insert_egress_log(
    cursor,
    conn,
    egress_run_id,
    target,
    window_start_utc,
    window_end_utc,
    records,
    request_body,
    response,
    status,
    error_message=None,
):
    cursor.execute(
        """
        INSERT INTO ops.api_egress_log
        (
            egress_run_id,
            egress_target_id,
            plant_code,
            window_start_utc,
            window_end_utc,
            record_count,
            request_json,
            response_text,
            http_status,
            status,
            error_message,
            request_started_at_utc,
            request_finished_at_utc
        )
        VALUES
        (
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            SYSUTCDATETIME(),
            SYSUTCDATETIME()
        )
        """,
        (
            egress_run_id,
            target["egress_target_id"],
            target["plant_code"],
            window_start_utc,
            window_end_utc,
            len(records),
            json.dumps(request_body or {"records": records}, ensure_ascii=False),
            response.text[:4000] if response is not None else None,
            response.status_code if response is not None else None,
            status,
            error_message[:1000] if error_message else None,
        ),
    )
    conn.commit()


def main():
    args = parse_args()

    start_date = parse_yyyy_mm_dd(args.start_date)
    end_date = parse_yyyy_mm_dd(args.end_date)

    if end_date < start_date:
        raise ValueError("--end-date must be greater than or equal to --start-date")

    plants = tuple(args.plants or DEFAULT_PLANTS)

    app = build_app()
    conn = app.conn
    cursor = conn.cursor()

    print("[BACKFILL] Starting Enserve 15-min backfill range")
    print(f"[BACKFILL] source_view={BACKFILL_VIEW_NAME}")
    print(f"[BACKFILL] start_date={start_date} end_date={end_date}")
    print(f"[BACKFILL] plants={plants}")
    print(f"[BACKFILL] send={args.send}")

    targets = load_enabled_targets(cursor, plants)

    egress_run_id = None
    if args.send:
        egress_run_id = create_egress_run(cursor, conn, run_mode="backfill_15min_range")
        print(f"[BACKFILL] Created egress_run_id={egress_run_id}")

    success_count = 0
    skipped_count = 0
    failed_count = 0
    summary = []

    for d in date_range_inclusive(start_date, end_date):
        window_start = utc_midnight(d)
        window_end = utc_midnight(d + timedelta(days=1))

        for plant_code in plants:
            target = targets[plant_code]

            print(
                f"[BACKFILL] plant={plant_code} "
                f"window={window_start} -> {window_end}"
            )

            rows = load_rows_for_window(
                cursor=cursor,
                plant_code=plant_code,
                window_start_utc=window_start,
                window_end_utc=window_end,
            )

            records = build_records(rows)

            print(f"[BACKFILL] prepared records={len(records)}")

            if records:
                print(json.dumps(records[:5], ensure_ascii=False, indent=2))
                if len(records) > 5:
                    print(f"[BACKFILL] ... {len(records) - 5} more record(s)")
            else:
                msg = f"plant={plant_code} date={d} no records"
                print(f"[BACKFILL][SKIP] {msg}")
                skipped_count += 1
                summary.append(msg)
                continue

            if not args.send:
                skipped_count += 1
                summary.append(
                    f"DRY-RUN plant={plant_code} date={d} records={len(records)}"
                )
                continue

            response = None
            request_body = None

            try:
                response, request_body = post_records(target, records)

                print(
                    f"[BACKFILL] plant={plant_code} "
                    f"HTTP status={response.status_code}"
                )
                print(f"[BACKFILL] response={response.text[:1000]}")

                status = "SUCCESS" if response.ok else "FAILED"
                error_message = None if response.ok else response.text[:1000]

                insert_egress_log(
                    cursor=cursor,
                    conn=conn,
                    egress_run_id=egress_run_id,
                    target=target,
                    window_start_utc=window_start,
                    window_end_utc=window_end,
                    records=records,
                    request_body=request_body,
                    response=response,
                    status=status,
                    error_message=error_message,
                )

                if response.ok:
                    success_count += 1
                    summary.append(
                        f"plant={plant_code} date={d} sent={len(records)}"
                    )
                else:
                    failed_count += 1
                    summary.append(
                        f"plant={plant_code} date={d} failed HTTP {response.status_code}"
                    )

                response.raise_for_status()

            except Exception as exc:
                failed_count += 1
                error_text = str(exc)
                print(f"[BACKFILL][FAILED] plant={plant_code} date={d} error={error_text}")
                summary.append(f"plant={plant_code} date={d} failed: {error_text}")

                try:
                    insert_egress_log(
                        cursor=cursor,
                        conn=conn,
                        egress_run_id=egress_run_id,
                        target=target,
                        window_start_utc=window_start,
                        window_end_utc=window_end,
                        records=records,
                        request_body=request_body or {"records": records},
                        response=response,
                        status="FAILED",
                        error_message=error_text,
                    )
                except Exception as log_exc:
                    print(
                        f"[BACKFILL][WARN] failed to write failure log "
                        f"plant={plant_code} date={d}: {log_exc}"
                    )

            if args.sleep_seconds and args.sleep_seconds > 0:
                time.sleep(args.sleep_seconds)

    if args.send and egress_run_id is not None:
        if failed_count == 0 and success_count > 0:
            final_status = "SUCCESS"
        elif success_count > 0 and failed_count > 0:
            final_status = "PARTIAL_SUCCESS"
        elif failed_count == 0 and success_count == 0:
            final_status = "SKIPPED"
        else:
            final_status = "FAILED"

        update_egress_run(
            cursor=cursor,
            conn=conn,
            egress_run_id=egress_run_id,
            status=final_status,
            message=" | ".join(summary)[:4000],
        )

        print(f"[BACKFILL] Completed status={final_status}")
    else:
        print("[BACKFILL] Completed dry-run")


if __name__ == "__main__":
    main()

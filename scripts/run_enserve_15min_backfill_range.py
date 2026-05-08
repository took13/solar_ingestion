from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, date, timedelta, timezone

import requests

from src.main import build_app


DEFAULT_PLANT_CODES = ("NE=50281829", "NE=50979503")


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def daterange(start_date: date, end_date: date):
    """
    end_date is inclusive for CLI convenience.
    Example:
      --start-date 2026-05-01 --end-date 2026-05-08
    will run 2026-05-01 ... 2026-05-08.
    """
    d = start_date
    while d <= end_date:
        yield d
        d += timedelta(days=1)


def to_iso_utc(dt):
    if dt is None:
        return None

    if isinstance(dt, str):
        return dt

    return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def load_enabled_targets(cursor, plant_codes):
    placeholders = ",".join("?" for _ in plant_codes)

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
        plant_codes,
    )

    rows = cursor.fetchall()

    targets = []
    for r in rows:
        if not r.endpoint_url:
            raise RuntimeError(f"endpoint_url is empty for plant_code={r.plant_code}")

        if not r.auth_token:
            raise RuntimeError(f"auth_token is empty for plant_code={r.plant_code}")

        targets.append(
            {
                "egress_target_id": int(r.egress_target_id),
                "plant_code": r.plant_code,
                "endpoint_url": r.endpoint_url,
                "auth_token": r.auth_token,
                "timeout_seconds": int(r.timeout_seconds or 30),
            }
        )

    if not targets:
        raise RuntimeError("No enabled Enserve egress targets found")

    return targets


def create_egress_run(cursor, conn, run_date: date, dry_run: bool):
    mode = "bf15_dryrun" if dry_run else "bf15_send"

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
            ?
        )
        """,
        mode,
        f"Enserve 15-min backfill started for date={run_date.isoformat()}",
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
        status,
        message[:4000] if message else None,
        egress_run_id,
    )
    conn.commit()


def load_rows_for_plant_day(cursor, plant_code: str, run_date: date):
    """
    Uses mart.vw_enserve_15min_export.
    Important:
    - Please run the latest CREATE OR ALTER VIEW first.
    - The view should already fallback irradiance:
        horiz_radiant_line -> radiant_line -> 0.0
    """
    from_time = datetime.combine(run_date, datetime.min.time())
    to_time = from_time + timedelta(days=1)

    cursor.execute(
        """
        SELECT
            plant_code,
            collect_time_utc,
            power_kw,
            number_inverter,
            irradiance_wm2,
            temperature_c,
            reporting_inverter_count
        FROM mart.vw_enserve_15min_export
        WHERE plant_code = ?
          AND collect_time_utc >= ?
          AND collect_time_utc <  ?
        ORDER BY collect_time_utc
        """,
        plant_code,
        from_time,
        to_time,
    )

    return cursor.fetchall(), from_time, to_time


def build_records(rows):
    records = []

    for r in rows:
        if r.power_kw is None:
            print(
                f"[BACKFILL][SKIP] plant={r.plant_code} "
                f"time={r.collect_time_utc} power_kw is NULL"
            )
            continue

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


def print_records_summary(plant_code, run_date, rows, records):
    if not rows:
        print(f"[BACKFILL] plant={plant_code} date={run_date} rows=0")
        return

    irradiance_values = [
        float(r.irradiance_wm2)
        for r in rows
        if r.irradiance_wm2 is not None
    ]
    temperature_values = [
        float(r.temperature_c)
        for r in rows
        if r.temperature_c is not None
    ]

    print(
        f"[BACKFILL] plant={plant_code} date={run_date} "
        f"db_rows={len(rows)} payload_records={len(records)} "
        f"first={rows[0].collect_time_utc} last={rows[-1].collect_time_utc}"
    )

    if irradiance_values:
        print(
            f"[BACKFILL] irradiance_wm2 "
            f"min={min(irradiance_values)} "
            f"max={max(irradiance_values)} "
            f"non_zero={sum(1 for v in irradiance_values if v != 0.0)}"
        )

    if temperature_values:
        print(
            f"[BACKFILL] temperature_c "
            f"min={min(temperature_values)} "
            f"max={max(temperature_values)} "
            f"non_zero={sum(1 for v in temperature_values if v != 0.0)}"
        )

    print("[BACKFILL] payload preview first 3 records:")
    print(json.dumps(records[:3], ensure_ascii=False, indent=2))


def send_records(endpoint_url, auth_token, timeout_seconds, records):
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {auth_token}",
    }

    body = {
        "records": records,
    }

    response = requests.post(
        endpoint_url,
        headers=headers,
        json=body,
        timeout=timeout_seconds,
    )

    return response, body


def send_records_with_retry(
    endpoint_url,
    auth_token,
    timeout_seconds,
    records,
    retry_max=3,
    retry_wait_seconds=30,
):
    last_response = None
    last_body = None

    for attempt in range(1, retry_max + 1):
        response, body = send_records(
            endpoint_url=endpoint_url,
            auth_token=auth_token,
            timeout_seconds=timeout_seconds,
            records=records,
        )

        last_response = response
        last_body = body

        # Do not retry client/schema/auth errors.
        if response.status_code < 500:
            return response, body

        if attempt < retry_max:
            print(
                f"[BACKFILL][RETRY] HTTP {response.status_code}, "
                f"attempt={attempt}/{retry_max}, waiting {retry_wait_seconds}s"
            )
            time.sleep(retry_wait_seconds)

    return last_response, last_body


def insert_egress_log(
    cursor,
    conn,
    egress_run_id,
    egress_target_id,
    plant_code,
    window_start,
    window_end,
    request_body,
    response,
    status,
    error_message=None,
):
    records = (request_body or {}).get("records", [])

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
        egress_run_id,
        egress_target_id,
        plant_code,
        window_start,
        window_end,
        len(records),
        json.dumps(request_body or {}, ensure_ascii=False),
        response.text[:4000] if response is not None else None,
        response.status_code if response is not None else None,
        status,
        error_message[:1000] if error_message else None,
    )

    conn.commit()


def run_one_day(cursor, conn, targets, run_date: date, dry_run: bool, sleep_seconds: int):
    egress_run_id = create_egress_run(cursor, conn, run_date, dry_run)
    print(f"[BACKFILL] Created egress_run_id={egress_run_id} date={run_date}")

    success_count = 0
    failed_count = 0
    skipped_count = 0
    summary_messages = []

    try:
        for target in targets:
            plant_code = target["plant_code"]

            print("")
            print("=" * 100)
            print(f"[BACKFILL] Processing plant={plant_code} date={run_date} dry_run={dry_run}")
            print("=" * 100)

            response = None
            request_body = None

            try:
                rows, window_start, window_end = load_rows_for_plant_day(
                    cursor=cursor,
                    plant_code=plant_code,
                    run_date=run_date,
                )

                records = build_records(rows)
                print_records_summary(plant_code, run_date, rows, records)

                if not records:
                    skipped_count += 1
                    msg = f"plant={plant_code} date={run_date} skipped no records"
                    summary_messages.append(msg)
                    print(f"[BACKFILL][SKIP] {msg}")
                    continue

                request_body = {"records": records}

                if dry_run:
                    skipped_count += 1
                    msg = f"plant={plant_code} date={run_date} dry-run {len(records)} records"
                    summary_messages.append(msg)

                    insert_egress_log(
                        cursor=cursor,
                        conn=conn,
                        egress_run_id=egress_run_id,
                        egress_target_id=target["egress_target_id"],
                        plant_code=plant_code,
                        window_start=window_start,
                        window_end=window_end,
                        request_body=request_body,
                        response=None,
                        status="DRY_RUN",
                        error_message=None,
                    )

                    print(f"[BACKFILL][DRY-RUN] Not sent. records={len(records)}")
                    continue

                response, request_body = send_records_with_retry(
                    endpoint_url=target["endpoint_url"],
                    auth_token=target["auth_token"],
                    timeout_seconds=target["timeout_seconds"],
                    records=records,
                    retry_max=3,
                    retry_wait_seconds=30,
                )

                print(f"[BACKFILL] plant={plant_code} HTTP status={response.status_code}")
                print(f"[BACKFILL] plant={plant_code} Response={response.text[:1000]}")

                status = "SUCCESS" if response.ok else "FAILED"
                error_message = None if response.ok else response.text[:1000]

                insert_egress_log(
                    cursor=cursor,
                    conn=conn,
                    egress_run_id=egress_run_id,
                    egress_target_id=target["egress_target_id"],
                    plant_code=plant_code,
                    window_start=window_start,
                    window_end=window_end,
                    request_body=request_body,
                    response=response,
                    status=status,
                    error_message=error_message,
                )

                if response.ok:
                    success_count += 1
                    summary_messages.append(
                        f"plant={plant_code} date={run_date} sent {len(records)} records"
                    )
                else:
                    failed_count += 1
                    summary_messages.append(
                        f"plant={plant_code} date={run_date} failed HTTP {response.status_code}"
                    )

                response.raise_for_status()

                if sleep_seconds > 0:
                    print(f"[BACKFILL] Sleeping {sleep_seconds}s before next plant")
                    time.sleep(sleep_seconds)

            except Exception as plant_error:
                failed_count += 1
                error_text = str(plant_error)
                print(f"[BACKFILL][FAILED] plant={plant_code} date={run_date} error={error_text}")
                summary_messages.append(
                    f"plant={plant_code} date={run_date} failed: {error_text}"
                )

                try:
                    rows, window_start, window_end = load_rows_for_plant_day(
                        cursor=cursor,
                        plant_code=plant_code,
                        run_date=run_date,
                    )
                    request_body = request_body or {"records": build_records(rows)}

                    insert_egress_log(
                        cursor=cursor,
                        conn=conn,
                        egress_run_id=egress_run_id,
                        egress_target_id=target["egress_target_id"],
                        plant_code=plant_code,
                        window_start=window_start,
                        window_end=window_end,
                        request_body=request_body,
                        response=response,
                        status="FAILED",
                        error_message=error_text,
                    )
                except Exception as log_error:
                    print(
                        f"[BACKFILL][WARN] failed to insert failure log "
                        f"plant={plant_code}: {log_error}"
                    )

        if failed_count == 0 and success_count > 0:
            final_status = "SUCCESS"
        elif failed_count == 0 and dry_run:
            final_status = "DRY_RUN"
        elif success_count > 0 and failed_count > 0:
            final_status = "PARTIAL_SUCCESS"
        else:
            final_status = "FAILED"

        final_message = " | ".join(summary_messages) or f"Backfill completed date={run_date}"

        update_egress_run(
            cursor=cursor,
            conn=conn,
            egress_run_id=egress_run_id,
            status=final_status,
            message=final_message,
        )

        print(f"[BACKFILL] Completed date={run_date} status={final_status}")

    except Exception as e:
        error_text = str(e)
        print(f"[BACKFILL][FAILED] run date={run_date} error={error_text}")

        update_egress_run(
            cursor=cursor,
            conn=conn,
            egress_run_id=egress_run_id,
            status="FAILED",
            message=error_text,
        )

        raise


def main():
    parser = argparse.ArgumentParser(
        description="Backfill Enserve 15-min payload by day from mart.vw_enserve_15min_export"
    )

    parser.add_argument(
        "--date",
        help="Single date to backfill, format YYYY-MM-DD, e.g. 2026-05-01",
    )
    parser.add_argument(
        "--start-date",
        help="Start date, inclusive, format YYYY-MM-DD",
    )
    parser.add_argument(
        "--end-date",
        help="End date, inclusive, format YYYY-MM-DD",
    )
    parser.add_argument(
        "--plant",
        action="append",
        dest="plants",
        help="Plant code. Can be used multiple times. Default: GC5 and Polyplex",
    )
    parser.add_argument(
        "--send",
        action="store_true",
        help="Actually send to Enserve. If omitted, script runs dry-run only.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=int,
        default=5,
        help="Sleep seconds between plants when sending. Default 5.",
    )

    args = parser.parse_args()

    if args.date and (args.start_date or args.end_date):
        raise ValueError("Use either --date OR --start-date/--end-date, not both.")

    if args.date:
        start_date = parse_date(args.date)
        end_date = start_date
    else:
        if not args.start_date or not args.end_date:
            raise ValueError("Please provide --date OR both --start-date and --end-date.")
        start_date = parse_date(args.start_date)
        end_date = parse_date(args.end_date)

    if end_date < start_date:
        raise ValueError("--end-date must be >= --start-date")

    plant_codes = tuple(args.plants or DEFAULT_PLANT_CODES)
    dry_run = not args.send

    print("[BACKFILL] Starting Enserve 15-min backfill")
    print(f"[BACKFILL] date range: {start_date} to {end_date} inclusive")
    print(f"[BACKFILL] plants: {plant_codes}")
    print(f"[BACKFILL] dry_run: {dry_run}")
    print("")
    print("[BACKFILL] IMPORTANT:")
    print("[BACKFILL] - If dry_run=True, nothing will be sent.")
    print("[BACKFILL] - Add --send to actually send/replace data in Enserve.")
    print("[BACKFILL] - Make sure mart.vw_enserve_15min_export has irradiance fallback fixed.")
    print("")

    app = build_app()
    conn = app.conn
    cursor = conn.cursor()

    try:
        targets = load_enabled_targets(cursor, plant_codes)
        print(f"[BACKFILL] Loaded enabled targets={len(targets)}")

        for run_date in daterange(start_date, end_date):
            run_one_day(
                cursor=cursor,
                conn=conn,
                targets=targets,
                run_date=run_date,
                dry_run=dry_run,
                sleep_seconds=args.sleep_seconds,
            )

    finally:
        cursor.close()
        conn.close()

    print("[BACKFILL] All done")


if __name__ == "__main__":
    main()
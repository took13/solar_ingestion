import json
import time
from datetime import timezone

import requests

from src.main import build_app


PLANT_CODES = ("NE=50281829", "NE=50979503")


def to_iso_utc(dt):
    if dt is None:
        return None

    if isinstance(dt, str):
        return dt

    return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def is_valid_optional(value):
    if value is None:
        return False

    if isinstance(value, str) and value.strip() in ("", "-"):
        return False

    return True


def create_egress_run(cursor, conn):
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
            'daily_24h_recovery',
            'python_script',
            'RUNNING',
            SYSUTCDATETIME(),
            NULL,
            'Enserve 15-min daily 24-hour recovery egress started'
        )
        """
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


def load_enabled_targets(cursor):
    placeholders = ",".join("?" for _ in PLANT_CODES)

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
        PLANT_CODES,
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
                "egress_target_id": r.egress_target_id,
                "plant_code": r.plant_code,
                "endpoint_url": r.endpoint_url,
                "auth_token": r.auth_token,
                "timeout_seconds": int(r.timeout_seconds or 30),
            }
        )

    if not targets:
        raise RuntimeError("No enabled Enserve egress targets found")

    return targets


def load_previous_day_rows_for_plant(cursor, plant_code):
    """
    Daily recovery:
    - Runs daily at around 00:15
    - Sends the previous day's 15-min data
    - Expected: 96 records per plant if data is complete

    Note:
    This uses GETDATE() because the mart timestamp currently follows local operational time.
    If the timestamp pipeline is fully normalized to UTC later, this can be changed to UTC logic.
    """
    cursor.execute(
        """
        DECLARE @TodayLocal date = CAST(GETDATE() AS date);
        DECLARE @FromTime datetime2(0) = DATEADD(day, -1, CAST(@TodayLocal AS datetime2(0)));
        DECLARE @ToTime   datetime2(0) = CAST(@TodayLocal AS datetime2(0));

        SELECT
            plant_code,
            collect_time_utc,
            power_kw,
            number_inverter,
            CAST(NULL AS FLOAT) AS irradiance_wm2,
            CAST(NULL AS FLOAT) AS temperature_c,
            reporting_inverter_count
        FROM mart.vw_enserve_15min_export
        WHERE plant_code = ?
          AND collect_time_utc >= @FromTime
          AND collect_time_utc <  @ToTime
        ORDER BY collect_time_utc
        """,
        plant_code,
    )

    return cursor.fetchall()


def build_records(rows):
    records = []

    for r in rows:
        if r.power_kw is None:
            print(
                f"[RECOVERY][SKIP] plant={r.plant_code} "
                f"time={r.collect_time_utc} power_kw is NULL"
            )
            continue

        data = {
            "power_kw": float(r.power_kw or 0.0),
            "number_inverter": int(r.number_inverter or 0),
            "irradiance_wm2": float(r.irradiance_wm2 or 0.0),
            "temperature_c": float(r.temperature_c or 0.0),
        }

        # Optional numeric fields: omit if missing
        if is_valid_optional(r.irradiance_wm2):
            data["irradiance_wm2"] = float(r.irradiance_wm2)

        if is_valid_optional(r.temperature_c):
            data["temperature_c"] = float(r.temperature_c)

        records.append(
            {
                "timestamp": to_iso_utc(r.collect_time_utc),
                "data": data,
            }
        )

    return records


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

        # Do not retry client-side errors such as 400/401/403/422
        if response.status_code < 500:
            return response, body

        if attempt < retry_max:
            print(
                f"[RECOVERY][RETRY] HTTP {response.status_code}, "
                f"attempt={attempt}/{retry_max}, waiting {retry_wait_seconds}s"
            )
            time.sleep(retry_wait_seconds)

    return last_response, last_body


def _fallback_window_from_request_body(request_body):
    try:
        timestamps = [
            rec.get("timestamp")
            for rec in request_body.get("records", [])
            if rec.get("timestamp")
        ]

        if timestamps:
            return min(timestamps), max(timestamps)

    except Exception:
        pass

    return "1900-01-01T00:00:00Z", "1900-01-01T00:00:00Z"


def insert_egress_log(
    cursor,
    conn,
    egress_run_id,
    egress_target_id,
    plant_code,
    rows,
    request_body,
    response,
    status,
    error_message=None,
):
    if rows:
        window_start = min(r.collect_time_utc for r in rows)
        window_end = max(r.collect_time_utc for r in rows)
    else:
        window_start, window_end = _fallback_window_from_request_body(request_body or {})

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
            egress_target_id,
            plant_code,
            window_start,
            window_end,
            len(rows) if rows else len((request_body or {}).get("records", [])),
            json.dumps(request_body or {}, ensure_ascii=False),
            response.text[:4000] if response is not None else None,
            response.status_code if response is not None else None,
            status,
            error_message[:1000] if error_message else None,
        ),
    )

    conn.commit()


def main():
    app = build_app()
    conn = app.conn
    cursor = conn.cursor()

    print("[RECOVERY] Starting Enserve 15-min daily 24-hour recovery egress")

    egress_run_id = create_egress_run(cursor, conn)
    print(f"[RECOVERY] Created egress_run_id = {egress_run_id}")

    overall_success = True
    success_count = 0
    failed_count = 0
    summary_messages = []

    try:
        targets = load_enabled_targets(cursor)
        print(f"[RECOVERY] Loaded enabled targets = {len(targets)}")

        for target in targets:
            plant_code = target["plant_code"]
            response = None
            request_body = None
            rows = []
            records = []

            print(f"[RECOVERY] Processing plant={plant_code}")

            try:
                rows = load_previous_day_rows_for_plant(cursor, plant_code)

                if not rows:
                    msg = f"No rows to send for plant={plant_code}"
                    print(f"[RECOVERY] {msg}")
                    summary_messages.append(msg)
                    continue

                records = build_records(rows)

                if not records:
                    msg = f"No valid records after filtering for plant={plant_code}"
                    print(f"[RECOVERY] {msg}")
                    summary_messages.append(msg)
                    continue

                print(f"[RECOVERY] Prepared records for plant={plant_code}: {len(records)}")
                print(json.dumps(records[:5], ensure_ascii=False, indent=2)[:2000])

                response, request_body = send_records_with_retry(
                    endpoint_url=target["endpoint_url"],
                    auth_token=target["auth_token"],
                    timeout_seconds=target["timeout_seconds"],
                    records=records,
                    retry_max=3,
                    retry_wait_seconds=30,
                )

                print(f"[RECOVERY] plant={plant_code} HTTP status = {response.status_code}")
                print(f"[RECOVERY] plant={plant_code} Response = {response.text[:1000]}")

                status = "SUCCESS" if response.ok else "FAILED"
                error_message = None if response.ok else response.text[:1000]

                insert_egress_log(
                    cursor=cursor,
                    conn=conn,
                    egress_run_id=egress_run_id,
                    egress_target_id=target["egress_target_id"],
                    plant_code=plant_code,
                    rows=rows,
                    request_body=request_body,
                    response=response,
                    status=status,
                    error_message=error_message,
                )

                if response.ok:
                    success_count += 1
                    summary_messages.append(
                        f"plant={plant_code} recovered {len(records)} records successfully"
                    )
                else:
                    overall_success = False
                    failed_count += 1
                    summary_messages.append(
                        f"plant={plant_code} failed HTTP {response.status_code}"
                    )

                response.raise_for_status()

            except Exception as plant_error:
                overall_success = False
                failed_count += 1
                error_text = str(plant_error)

                print(f"[RECOVERY][FAILED] plant={plant_code} error={error_text}")
                summary_messages.append(f"plant={plant_code} failed: {error_text}")

                try:
                    if request_body is None:
                        request_body = {"records": records}

                    insert_egress_log(
                        cursor=cursor,
                        conn=conn,
                        egress_run_id=egress_run_id,
                        egress_target_id=target["egress_target_id"],
                        plant_code=plant_code,
                        rows=rows,
                        request_body=request_body,
                        response=response,
                        status="FAILED",
                        error_message=error_text,
                    )
                except Exception as log_error:
                    print(
                        f"[RECOVERY][WARN] failed to insert failure log "
                        f"for plant={plant_code}: {log_error}"
                    )

        if failed_count == 0 and success_count > 0:
            final_status = "SUCCESS"
        elif success_count > 0 and failed_count > 0:
            final_status = "PARTIAL_SUCCESS"
        else:
            final_status = "FAILED" if not overall_success else "SUCCESS"

        final_message = " | ".join(summary_messages) or "Enserve daily recovery completed"

        update_egress_run(
            cursor=cursor,
            conn=conn,
            egress_run_id=egress_run_id,
            status=final_status,
            message=final_message,
        )

        print(f"[RECOVERY] Completed Enserve daily recovery status={final_status}")

    except Exception as e:
        error_text = str(e)
        print(f"[RECOVERY][FAILED] run error={error_text}")

        update_egress_run(
            cursor=cursor,
            conn=conn,
            egress_run_id=egress_run_id,
            status="FAILED",
            message=error_text,
        )

        raise


if __name__ == "__main__":
    main()
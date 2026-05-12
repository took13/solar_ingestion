import json
import time
from datetime import timezone

import requests

from src.main import build_app


PLANT_CODES = ("NE=50281829", "NE=50979503")
VIEW_NAME = "mart.vw_enserve_15min_export"
MAX_RECORDS_PER_TARGET = 4


def to_iso_utc(dt):
    if dt is None:
        return None

    if isinstance(dt, str):
        return dt

    return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


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
            'hourly_15min',
            'python_script',
            'RUNNING',
            SYSUTCDATETIME(),
            NULL,
            'Enserve 15-min hourly egress started'
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


def load_checkpoint(cursor, egress_target_id, plant_code):
    cursor.execute(
        """
        SELECT
            last_success_end_utc,
            last_attempt_end_utc,
            last_status,
            last_error_message
        FROM ops.api_egress_checkpoint
        WHERE egress_target_id = ?
          AND plant_code = ?
        """,
        egress_target_id,
        plant_code,
    )

    row = cursor.fetchone()

    if row is None:
        raise RuntimeError(
            f"Missing ops.api_egress_checkpoint for "
            f"egress_target_id={egress_target_id}, plant_code={plant_code}"
        )

    return row


def load_next_rows_after_checkpoint(cursor, plant_code, last_success_end_utc):
    """
    Load the next 4 x 15-minute records after checkpoint.

    This intentionally does NOT use latest 4 rows. It sends records in ascending
    chronological order to prevent duplicate resend.
    """
    cursor.execute(
        f"""
        SELECT TOP ({MAX_RECORDS_PER_TARGET})
            plant_code,
            collect_time_utc,
            power_kw,
            number_inverter,
            irradiance_wm2,
            temperature_c
        FROM {VIEW_NAME}
        WHERE plant_code = ?
          AND collect_time_utc > ?
          AND power_kw IS NOT NULL
          AND number_inverter IS NOT NULL
        ORDER BY collect_time_utc ASC
        """,
        plant_code,
        last_success_end_utc,
    )

    return cursor.fetchall()


def build_records(rows):
    records = []

    for r in rows:
        data = {
            "power_kw": float(r.power_kw or 0.0),
            "number_inverter": int(r.number_inverter or 0),
            "irradiance_wm2": float(r.irradiance_wm2 or 0.0),
            "temperature_c": float(r.temperature_c or 0.0),
        }

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
    """
    Retry only for server-side / temporary failures.
    Do not retry 400/401/403/422 because those are usually data/token/schema issues.
    """
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

        if response.status_code < 500:
            return response, body

        if attempt < retry_max:
            print(
                f"[EGRESS][RETRY] HTTP {response.status_code}, "
                f"attempt={attempt}/{retry_max}, waiting {retry_wait_seconds}s"
            )
            time.sleep(retry_wait_seconds)

    return last_response, last_body


def _fallback_window_from_request_body(request_body):
    """
    Used only as a safety fallback.
    ops.api_egress_log.window_start_utc/window_end_utc are NOT nullable.
    """
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


def update_checkpoint_success(
    cursor,
    conn,
    egress_target_id,
    plant_code,
    success_end_utc,
):
    cursor.execute(
        """
        UPDATE ops.api_egress_checkpoint
        SET
            last_success_end_utc = ?,
            last_attempt_end_utc = ?,
            last_status = 'SUCCESS',
            last_error_message = NULL,
            updated_at_utc = SYSUTCDATETIME()
        WHERE egress_target_id = ?
          AND plant_code = ?
        """,
        success_end_utc,
        success_end_utc,
        egress_target_id,
        plant_code,
    )
    conn.commit()


def update_checkpoint_failure(
    cursor,
    conn,
    egress_target_id,
    plant_code,
    attempt_end_utc,
    error_message,
):
    cursor.execute(
        """
        UPDATE ops.api_egress_checkpoint
        SET
            last_attempt_end_utc = ?,
            last_status = 'FAILED',
            last_error_message = ?,
            updated_at_utc = SYSUTCDATETIME()
        WHERE egress_target_id = ?
          AND plant_code = ?
        """,
        attempt_end_utc,
        error_message[:1000] if error_message else None,
        egress_target_id,
        plant_code,
    )
    conn.commit()


def main():
    app = build_app()
    conn = app.conn
    cursor = conn.cursor()

    print("[EGRESS] Starting Enserve 15-min hourly egress")

    egress_run_id = create_egress_run(cursor, conn)
    print(f"[EGRESS] Created egress_run_id = {egress_run_id}")

    overall_success = True
    success_count = 0
    skipped_count = 0
    failed_count = 0
    summary_messages = []

    try:
        targets = load_enabled_targets(cursor)
        print(f"[EGRESS] Loaded enabled targets = {len(targets)}")

        for target in targets:
            plant_code = target["plant_code"]
            egress_target_id = target["egress_target_id"]
            response = None
            request_body = None
            rows = []
            records = []

            print(f"[EGRESS] Processing plant={plant_code}")

            try:
                cp = load_checkpoint(cursor, egress_target_id, plant_code)
                last_success_end_utc = cp.last_success_end_utc

                if last_success_end_utc is None:
                    raise RuntimeError(
                        f"last_success_end_utc is NULL for "
                        f"egress_target_id={egress_target_id}, plant_code={plant_code}"
                    )

                print(
                    f"[EGRESS] checkpoint plant={plant_code} "
                    f"last_success_end_utc={last_success_end_utc}"
                )

                rows = load_next_rows_after_checkpoint(
                    cursor=cursor,
                    plant_code=plant_code,
                    last_success_end_utc=last_success_end_utc,
                )

                if not rows:
                    msg = (
                        f"No new rows after checkpoint for plant={plant_code}, "
                        f"checkpoint={last_success_end_utc}"
                    )
                    print(f"[EGRESS][SKIP] {msg}")
                    skipped_count += 1
                    summary_messages.append(msg)
                    continue

                records = build_records(rows)

                if not records:
                    msg = f"No valid records after filtering for plant={plant_code}"
                    print(f"[EGRESS][SKIP] {msg}")
                    skipped_count += 1
                    summary_messages.append(msg)
                    continue

                window_start = min(r.collect_time_utc for r in rows)
                window_end = max(r.collect_time_utc for r in rows)

                # Defensive guard against duplicate resend.
                if window_end <= last_success_end_utc:
                    msg = (
                        f"Defensive skip: window_end={window_end} <= "
                        f"checkpoint={last_success_end_utc} for plant={plant_code}"
                    )
                    print(f"[EGRESS][SKIP] {msg}")
                    skipped_count += 1
                    summary_messages.append(msg)
                    continue

                print(
                    f"[EGRESS] Prepared records for plant={plant_code}: "
                    f"{len(records)} window={window_start} -> {window_end}"
                )
                print(json.dumps(records, ensure_ascii=False, indent=2)[:2000])

                response, request_body = send_records_with_retry(
                    endpoint_url=target["endpoint_url"],
                    auth_token=target["auth_token"],
                    timeout_seconds=target["timeout_seconds"],
                    records=records,
                    retry_max=3,
                    retry_wait_seconds=30,
                )

                print(f"[EGRESS] plant={plant_code} HTTP status = {response.status_code}")
                print(f"[EGRESS] plant={plant_code} Response = {response.text[:1000]}")

                status = "SUCCESS" if response.ok else "FAILED"
                error_message = None if response.ok else response.text[:1000]

                insert_egress_log(
                    cursor=cursor,
                    conn=conn,
                    egress_run_id=egress_run_id,
                    egress_target_id=egress_target_id,
                    plant_code=plant_code,
                    rows=rows,
                    request_body=request_body,
                    response=response,
                    status=status,
                    error_message=error_message,
                )

                if response.ok:
                    update_checkpoint_success(
                        cursor=cursor,
                        conn=conn,
                        egress_target_id=egress_target_id,
                        plant_code=plant_code,
                        success_end_utc=window_end,
                    )

                    success_count += 1
                    summary_messages.append(
                        f"plant={plant_code} sent {len(records)} records successfully "
                        f"checkpoint={window_end}"
                    )
                else:
                    update_checkpoint_failure(
                        cursor=cursor,
                        conn=conn,
                        egress_target_id=egress_target_id,
                        plant_code=plant_code,
                        attempt_end_utc=window_end,
                        error_message=error_message,
                    )

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

                print(f"[EGRESS][FAILED] plant={plant_code} error={error_text}")
                summary_messages.append(f"plant={plant_code} failed: {error_text}")

                # Best effort DB log for plant failure.
                try:
                    if request_body is None:
                        request_body = {"records": records}

                    insert_egress_log(
                        cursor=cursor,
                        conn=conn,
                        egress_run_id=egress_run_id,
                        egress_target_id=egress_target_id,
                        plant_code=plant_code,
                        rows=rows,
                        request_body=request_body,
                        response=response,
                        status="FAILED",
                        error_message=error_text,
                    )

                    if rows:
                        window_end = max(r.collect_time_utc for r in rows)
                        update_checkpoint_failure(
                            cursor=cursor,
                            conn=conn,
                            egress_target_id=egress_target_id,
                            plant_code=plant_code,
                            attempt_end_utc=window_end,
                            error_message=error_text,
                        )

                except Exception as log_error:
                    print(
                        f"[EGRESS][WARN] failed to insert failure log "
                        f"for plant={plant_code}: {log_error}"
                    )

        if failed_count == 0 and success_count > 0:
            final_status = "SUCCESS"
        elif failed_count == 0 and success_count == 0 and skipped_count > 0:
            final_status = "SKIPPED"
        elif success_count > 0 and failed_count > 0:
            final_status = "PARTIAL_SUCCESS"
        else:
            final_status = "FAILED" if not overall_success else "SUCCESS"

        final_message = " | ".join(summary_messages) or "Enserve 15-min hourly egress completed"

        update_egress_run(
            cursor=cursor,
            conn=conn,
            egress_run_id=egress_run_id,
            status=final_status,
            message=final_message,
        )

        print(f"[EGRESS] Completed Enserve 15-min hourly egress status={final_status}")

    except Exception as e:
        error_text = str(e)
        print(f"[EGRESS][FAILED] run error={error_text}")

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

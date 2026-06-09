from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests

from src.main import build_app


RETENTION_JOBS = [
    "dev_history_backfill_retention_acc1",
    "dev_history_backfill_retention_acc2",
    "dev_history_backfill_retention_acc3",
]

DEFAULT_PLANTS = ("NE=50281829", "NE=50979503")
SEND_VIEW = "mart.vw_enserve_15min_energy_avg_adhoc_send"

DEFAULT_MART_PROC_BY_DEVTYPE = {
    1: "mart.usp_load_fact_dev_inverter_5min",
    10: "mart.usp_load_fact_dev_emi_5min",
    17: "mart.usp_load_fact_dev_meter_5min",
}

STAGE_LOAD_SQL = """
EXEC ops.usp_load_enserve_energy_avg_adhoc_stage
    @FromUtc = ?,
    @ToUtc   = ?;
"""

PATCH_STAGE_EMI_SQL = """
EXEC ops.usp_patch_enserve_energy_avg_stage_emi_from_mart
    @FromUtc = ?,
    @ToUtc   = ?;
"""

SNAPSHOT_SQL = """
EXEC ops.usp_refresh_solar_plant_completeness_snapshot;
"""

DISABLE_RETENTION_SQL = """
UPDATE t
SET
    is_enabled = 0,
    updated_at_utc = SYSUTCDATETIME()
FROM ctl.ingest_target t
JOIN ctl.ingest_job j
    ON j.job_id = t.job_id
WHERE j.job_name IN (
    'dev_history_backfill_retention_acc1',
    'dev_history_backfill_retention_acc2',
    'dev_history_backfill_retention_acc3'
)
AND t.endpoint_name = 'getDevHistoryKpi';
"""


def parse_yyyy_mm_dd(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def date_range_inclusive(start_date: date, end_date: date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def fmt_z(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def to_iso_utc(dt) -> str:
    if isinstance(dt, str):
        return dt

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def local_daylight_window_utc(local_date: date):
    """
    Thailand daylight send window:
    06:00–19:00 local.

    Query window is half-open:
      start_utc <= collect_time_utc < end_utc_exclusive

    This includes 19:00 local by making exclusive end = 19:15 local.
    """
    start_local = datetime(local_date.year, local_date.month, local_date.day, 6, 0, 0)
    end_local_inclusive = datetime(local_date.year, local_date.month, local_date.day, 19, 0, 0)
    end_local_exclusive = end_local_inclusive + timedelta(minutes=15)

    start_utc = start_local - timedelta(hours=7)
    end_utc_inclusive = end_local_inclusive - timedelta(hours=7)
    end_utc_exclusive = end_local_exclusive - timedelta(hours=7)

    return start_utc, end_utc_inclusive, end_utc_exclusive


def validate_plant_code(plant_code: str) -> None:
    if not re.fullmatch(r"NE=\d+", plant_code):
        raise ValueError(f"Invalid plant_code: {plant_code}")


def validate_proc_name(proc_name: str | None) -> None:
    if not proc_name:
        return

    if not re.fullmatch(r"[A-Za-z0-9_]+\.[A-Za-z0-9_]+", proc_name):
        raise ValueError(f"Invalid procedure name: {proc_name}")


def sql_literal_list(values: tuple[str, ...]) -> str:
    for v in values:
        validate_plant_code(v)

    return ", ".join("'" + v.replace("'", "''") + "'" for v in values)


def open_conn():
    app = build_app()
    return app.conn


def exec_sql(sql: str, params: tuple | None = None) -> None:
    conn = open_conn()
    cur = conn.cursor()

    try:
        if params:
            cur.execute(sql, *params)
        else:
            cur.execute(sql)

        conn.commit()

    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise

    finally:
        try:
            conn.close()
        except Exception:
            pass


def query_sql(sql: str, params: tuple | None = None) -> list:
    conn = open_conn()
    cur = conn.cursor()

    try:
        if params:
            cur.execute(sql, *params)
        else:
            cur.execute(sql)

        return cur.fetchall()

    finally:
        try:
            conn.close()
        except Exception:
            pass


def build_prepare_devtype_sql(dev_type_id: int, plants: tuple[str, ...]) -> str:
    plant_list = sql_literal_list(plants)

    return f"""
    -- Safety: disable all retention getDevHistoryKpi targets first.
    UPDATE t
    SET
        is_enabled = 0,
        updated_at_utc = SYSUTCDATETIME()
    FROM ctl.ingest_target t
    JOIN ctl.ingest_job j
        ON j.job_id = t.job_id
    WHERE j.job_name IN (
        'dev_history_backfill_retention_acc1',
        'dev_history_backfill_retention_acc2',
        'dev_history_backfill_retention_acc3'
    )
    AND t.endpoint_name = 'getDevHistoryKpi';

    -- Enable selected plants and selected devType only.
    UPDATE t
    SET
        is_enabled = 1,
        batch_size = 10,
        requested_batch_size = 10,
        max_batches_per_run = CASE
            WHEN x.device_count IS NULL OR x.device_count <= 0 THEN 1
            ELSE CEILING(1.0 * x.device_count / 10.0)
        END,
        rotation_enabled = 0,
        hard_window_mode = 'slot',
        lag_minutes = 15,
        max_window_minutes = 780,
        updated_at_utc = SYSUTCDATETIME()
    FROM ctl.ingest_target t
    JOIN ctl.ingest_job j
        ON j.job_id = t.job_id
    JOIN (
        SELECT
            d.plant_code,
            COUNT(DISTINCT d.dev_id) AS device_count
        FROM dbo.dim_device d
        LEFT JOIN cfg.plant_data_scope_override o
            ON o.plant_code = d.plant_code
        WHERE d.dev_type_id = {dev_type_id}
          AND d.plant_code IN ({plant_list})
          AND ISNULL(o.is_history_expected, 1) = 1
        GROUP BY d.plant_code
    ) x
        ON x.plant_code = t.plant_code
    WHERE j.job_name IN (
        'dev_history_backfill_retention_acc1',
        'dev_history_backfill_retention_acc2',
        'dev_history_backfill_retention_acc3'
    )
    AND t.endpoint_name = 'getDevHistoryKpi'
    AND t.dev_type_id = {dev_type_id}
    AND t.plant_code IN ({plant_list});
    """


def build_account_plan_sql(dev_type_id: int, plants: tuple[str, ...]) -> str:
    plant_list = sql_literal_list(plants)

    return f"""
    SELECT
        j.job_name,
        t.account_id,
        COUNT(*) AS target_count,
        SUM(CASE WHEN t.is_enabled = 1 THEN 1 ELSE 0 END) AS enabled_target_count,
        SUM(CASE WHEN t.is_enabled = 1 THEN ISNULL(t.max_batches_per_run, 0) ELSE 0 END) AS planned_batches
    FROM ctl.ingest_target t
    JOIN ctl.ingest_job j
        ON j.job_id = t.job_id
    WHERE j.job_name IN (
        'dev_history_backfill_retention_acc1',
        'dev_history_backfill_retention_acc2',
        'dev_history_backfill_retention_acc3'
    )
    AND t.endpoint_name = 'getDevHistoryKpi'
    AND t.dev_type_id = {dev_type_id}
    AND t.plant_code IN ({plant_list})
    GROUP BY
        j.job_name,
        t.account_id
    ORDER BY
        j.job_name,
        t.account_id;
    """


def build_target_coverage_sql(dev_type_id: int, plants: tuple[str, ...]) -> str:
    plant_list = sql_literal_list(plants)

    return f"""
    WITH device_scope AS (
        SELECT
            d.plant_code,
            COUNT(DISTINCT d.dev_id) AS device_count
        FROM dbo.dim_device d
        WHERE d.dev_type_id = {dev_type_id}
          AND d.plant_code IN ({plant_list})
        GROUP BY d.plant_code
    ),
    enabled_targets AS (
        SELECT
            t.plant_code,
            COUNT(*) AS enabled_target_count
        FROM ctl.ingest_target t
        JOIN ctl.ingest_job j
            ON j.job_id = t.job_id
        WHERE j.job_name IN (
            'dev_history_backfill_retention_acc1',
            'dev_history_backfill_retention_acc2',
            'dev_history_backfill_retention_acc3'
        )
          AND t.endpoint_name = 'getDevHistoryKpi'
          AND t.dev_type_id = {dev_type_id}
          AND t.plant_code IN ({plant_list})
          AND t.is_enabled = 1
        GROUP BY t.plant_code
    )
    SELECT
        s.plant_code,
        s.device_count,
        ISNULL(e.enabled_target_count, 0) AS enabled_target_count
    FROM device_scope s
    LEFT JOIN enabled_targets e
        ON e.plant_code = s.plant_code
    ORDER BY s.plant_code;
    """

def validate_account_isolation(dev_type_id: int, plants: tuple[str, ...]) -> list[str]:
    rows = query_sql(build_account_plan_sql(dev_type_id, plants))

    print(f"[PLAN] devType={dev_type_id} account plan")
    print("[PLAN] job_name | account_id | target_count | enabled_target_count | planned_batches")

    accounts_by_job: dict[str, set[int]] = defaultdict(set)
    enabled_jobs: list[str] = []

    for r in rows:
        job_name = str(r.job_name)
        account_id = int(r.account_id)
        target_count = int(r.target_count or 0)
        enabled_target_count = int(r.enabled_target_count or 0)
        planned_batches = int(r.planned_batches or 0)

        print(
            f"[PLAN] {job_name} | account={account_id} | "
            f"targets={target_count} | enabled={enabled_target_count} | "
            f"planned_batches={planned_batches}"
        )

        if enabled_target_count > 0:
            accounts_by_job[job_name].add(account_id)
            enabled_jobs.append(job_name)

    errors = []

    for job in RETENTION_JOBS:
        accounts = accounts_by_job.get(job, set())
        if len(accounts) > 1:
            errors.append(f"{job} has multiple account_id: {sorted(accounts)}")

    if errors:
        raise RuntimeError("Account isolation failed: " + " | ".join(errors))

    if not enabled_jobs:
        raise RuntimeError(f"No enabled retention jobs found for devType={dev_type_id}")

    print(f"[PLAN] Account isolation PASS. enabled_jobs={enabled_jobs}")
    return enabled_jobs


def validate_target_coverage(dev_type_id: int, plants: tuple[str, ...]) -> None:
    rows = query_sql(build_target_coverage_sql(dev_type_id, plants))

    print(f"[COVERAGE] devType={dev_type_id}")
    errors = []

    for r in rows:
        plant_code = str(r.plant_code)
        device_count = int(r.device_count or 0)
        enabled_target_count = int(r.enabled_target_count or 0)

        print(
            f"[COVERAGE] plant={plant_code} "
            f"device_count={device_count} enabled_target_count={enabled_target_count}"
        )

        if device_count > 0 and enabled_target_count == 0:
            errors.append(
                f"devType={dev_type_id} plant={plant_code} has devices but no enabled target"
            )

    if errors:
        raise RuntimeError("Target coverage failed: " + " | ".join(errors))


def build_backfill_command(
    job: str,
    start_utc: datetime,
    end_utc_exclusive: datetime,
    chunk_minutes: int,
) -> list[str]:
    return [
        sys.executable,
        "-m",
        "scripts.run_backfill",
        "--job",
        job,
        "--start",
        fmt_z(start_utc),
        "--end",
        fmt_z(end_utc_exclusive),
        "--chunk-minutes",
        str(chunk_minutes),
    ]


def run_cmd_sequential(commands: list[tuple[str, list[str]]], cwd: Path) -> None:
    for label, cmd in commands:
        print(f"[CMD][SEQ][START] {label}")
        print("[CMD]", " ".join(cmd))

        started = time.monotonic()
        result = subprocess.run(cmd, cwd=str(cwd), text=True)
        elapsed = time.monotonic() - started

        print(f"[CMD][SEQ][END] {label} rc={result.returncode} elapsed_sec={elapsed:.1f}")

        if result.returncode != 0:
            raise RuntimeError(f"Command failed: {label} rc={result.returncode}")


def run_cmd_parallel(commands: list[tuple[str, list[str]]], cwd: Path) -> None:
    processes = []

    print(f"[CMD][PARALLEL] Starting {len(commands)} job(s)")

    for label, cmd in commands:
        print(f"[CMD][PARALLEL][START] {label}")
        print("[CMD]", " ".join(cmd))

        started = time.monotonic()
        proc = subprocess.Popen(cmd, cwd=str(cwd), text=True)
        processes.append((label, proc, started))

    failures = []

    for label, proc, started in processes:
        rc = proc.wait()
        elapsed = time.monotonic() - started

        print(f"[CMD][PARALLEL][END] {label} rc={rc} elapsed_sec={elapsed:.1f}")

        if rc != 0:
            failures.append((label, rc))

    if failures:
        detail = " | ".join(f"{label} rc={rc}" for label, rc in failures)
        raise RuntimeError(f"Parallel command failed: {detail}")


def run_mart_proc(proc_name: str, start_utc: datetime, end_utc_exclusive: datetime) -> None:
    validate_proc_name(proc_name)

    sql = f"""
    EXEC {proc_name}
        @FromUtc = ?,
        @ToUtc   = ?;
    """

    exec_sql(sql, (start_utc, end_utc_exclusive))


def patch_stage_emi_from_mart(start_utc: datetime, end_utc_exclusive: datetime) -> None:
    """
    Fill ops.enserve_energy_avg_adhoc_stage.irradiance_wm2 / temperature_c
    from mart.fact_dev_emi_5min using the DB-side patch procedure.

    This is intentionally separated from the stage loader because the original
    stage loader may calculate power from devType 1 correctly while leaving EMI
    fields NULL. Sending NULL as 0.0 is not allowed.
    """
    exec_sql(PATCH_STAGE_EMI_SQL, (start_utc, end_utc_exclusive))


def prepare_huawei_devtype(
    app_dir: Path,
    dev_type_id: int,
    plants: tuple[str, ...],
    start_utc: datetime,
    end_utc_exclusive: datetime,
    chunk_minutes: int,
    sequential_backfill: bool,
    dry_run: bool,
) -> None:
    print("==================================================")
    print(f"[SOURCE] Prepare Huawei getDevHistoryKpi devType={dev_type_id}")
    print(f"[SOURCE] window={fmt_z(start_utc)} -> {fmt_z(end_utc_exclusive)}")
    print(f"[SOURCE] plants={plants}")

    exec_sql(build_prepare_devtype_sql(dev_type_id, plants))
    validate_target_coverage(dev_type_id, plants)
    enabled_jobs = validate_account_isolation(dev_type_id, plants)

    commands = [
        (
            f"{job}_devType{dev_type_id}",
            build_backfill_command(
                job=job,
                start_utc=start_utc,
                end_utc_exclusive=end_utc_exclusive,
                chunk_minutes=chunk_minutes,
            ),
        )
        for job in enabled_jobs
    ]

    if dry_run:
        print(f"[DRY-RUN] devType={dev_type_id} commands:")
        for label, cmd in commands:
            print(f"[DRY-RUN] {label}: {' '.join(cmd)}")
        return

    if sequential_backfill:
        run_cmd_sequential(commands, cwd=app_dir)
    else:
        run_cmd_parallel(commands, cwd=app_dir)


def normalize_generic(app_dir: Path, normalize_limit_raw: int) -> None:
    run_cmd_sequential(
        [
            (
                "normalize_generic",
                [
                    sys.executable,
                    "-m",
                    "scripts.run_normalize_generic",
                    "--limit-raw",
                    str(normalize_limit_raw),
                ],
            )
        ],
        cwd=app_dir,
    )


def load_enabled_targets(plants: tuple[str, ...]) -> dict:
    placeholders = ",".join("?" for _ in plants)

    sql = f"""
    SELECT
        egress_target_id,
        plant_code,
        endpoint_url,
        auth_token,
        timeout_seconds
    FROM ops.api_egress_target
    WHERE is_enabled = 1
      AND plant_code IN ({placeholders})
    ORDER BY plant_code;
    """

    rows = query_sql(sql, plants)

    targets = {}

    for r in rows:
        targets[str(r.plant_code)] = {
            "egress_target_id": int(r.egress_target_id),
            "plant_code": str(r.plant_code),
            "endpoint_url": str(r.endpoint_url),
            "auth_token": str(r.auth_token),
            "timeout_seconds": int(r.timeout_seconds or 30),
        }

    missing = [p for p in plants if p not in targets]
    if missing:
        raise RuntimeError(f"No enabled Enserve target found for plant(s): {missing}")

    return targets


def load_rows_for_window(plant_code: str, start_utc: datetime, end_utc_exclusive: datetime) -> list:
    sql = f"""
    SELECT
        plant_code,
        collect_time_utc,
        power_kw,
        number_inverter,
        irradiance_wm2,
        temperature_c
    FROM {SEND_VIEW}
    WHERE plant_code = ?
      AND collect_time_utc >= ?
      AND collect_time_utc < ?
      AND power_kw IS NOT NULL
      AND number_inverter IS NOT NULL
    ORDER BY collect_time_utc ASC;
    """

    return query_sql(sql, (plant_code, start_utc, end_utc_exclusive))


def build_records(rows: list) -> list[dict]:
    records = []

    for r in rows:
        data = {
            "power_kw": float(r.power_kw),
            "number_inverter": int(r.number_inverter),
        }

        if r.irradiance_wm2 is not None:
            data["irradiance_wm2"] = float(r.irradiance_wm2)

        if r.temperature_c is not None:
            data["temperature_c"] = float(r.temperature_c)

        records.append(
            {
                "timestamp": to_iso_utc(r.collect_time_utc),
                "data": data,
            }
        )
    return records


def summarize_record_field_coverage(records: list[dict]) -> dict:
    total = len(records)
    with_irradiance = sum(1 for r in records if "irradiance_wm2" in r.get("data", {}))
    with_temperature = sum(1 for r in records if "temperature_c" in r.get("data", {}))
    missing_irradiance = total - with_irradiance
    missing_temperature = total - with_temperature

    return {
        "total": total,
        "with_irradiance": with_irradiance,
        "with_temperature": with_temperature,
        "missing_irradiance": missing_irradiance,
        "missing_temperature": missing_temperature,
    }


def create_egress_run(message: str) -> int:
    conn = open_conn()
    cur = conn.cursor()

    try:
        cur.execute(
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
                'ens_range_recovery',
                'python_script',
                'RUNNING',
                SYSUTCDATETIME(),
                NULL,
                ?
            );
            """,
            message[:4000],
        )

        row = cur.fetchone()
        conn.commit()
        return int(row.egress_run_id)

    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise

    finally:
        try:
            conn.close()
        except Exception:
            pass


def update_egress_run(egress_run_id: int, status: str, message: str) -> None:
    exec_sql(
        """
        UPDATE ops.api_egress_run
        SET
            status = ?,
            ended_at_utc = SYSUTCDATETIME(),
            message = ?
        WHERE egress_run_id = ?;
        """,
        (status, message[:4000], egress_run_id),
    )


def insert_egress_log(
    egress_run_id: int,
    target: dict,
    window_start_utc,
    window_end_utc,
    records: list,
    request_body: dict | None,
    response,
    status: str,
    error_message: str | None = None,
) -> None:
    exec_sql(
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
        );
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


def post_records_with_retry(target: dict, records: list, retry_max: int, retry_wait_seconds: int):
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {target['auth_token']}",
    }

    body = {"records": records}
    last_response = None

    for attempt in range(1, retry_max + 1):
        response = requests.post(
            target["endpoint_url"],
            headers=headers,
            json=body,
            timeout=target["timeout_seconds"],
        )

        last_response = response

        if response.status_code < 500:
            return response, body

        if attempt < retry_max:
            print(
                f"[SEND][RETRY] plant={target['plant_code']} "
                f"HTTP={response.status_code}, attempt={attempt}/{retry_max}, "
                f"wait={retry_wait_seconds}s"
            )
            time.sleep(retry_wait_seconds)

    return last_response, body


def send_day_to_enserve(
    egress_run_id: int | None,
    targets: dict,
    plants: tuple[str, ...],
    local_date: date,
    start_utc: datetime,
    end_utc_inclusive: datetime,
    end_utc_exclusive: datetime,
    expected_records: int,
    send: bool,
    retry_max: int,
    retry_wait_seconds: int,
    sleep_seconds: float,
    allow_missing_emi_fields: bool,
) -> tuple[int, int, int]:
    success_count = 0
    failed_count = 0
    skipped_count = 0

    for plant_code in plants:
        target = targets[plant_code]
        response = None
        request_body = None

        rows = load_rows_for_window(
            plant_code=plant_code,
            start_utc=start_utc,
            end_utc_exclusive=end_utc_exclusive,
        )

        records = build_records(rows)
        coverage = summarize_record_field_coverage(records)

        if records:
            print(
                f"[PREVIEW] local_date={local_date} plant={plant_code} "
                f"first={records[0]['timestamp']} last={records[-1]['timestamp']} "
                f"count={len(records)} "
                f"with_irradiance={coverage['with_irradiance']} "
                f"with_temperature={coverage['with_temperature']}"
            )
        else:
            print(f"[SKIP] local_date={local_date} plant={plant_code} no records")
            skipped_count += 1
            continue

        if len(records) < expected_records:
            print(
                f"[WARN] local_date={local_date} plant={plant_code} "
                f"records={len(records)} expected={expected_records}"
            )

        if coverage["missing_irradiance"] > 0 or coverage["missing_temperature"] > 0:
            msg = (
                f"local_date={local_date} plant={plant_code} "
                f"missing EMI fields: "
                f"missing_irradiance={coverage['missing_irradiance']}, "
                f"missing_temperature={coverage['missing_temperature']}, "
                f"total={coverage['total']}. "
                f"Stage EMI patch may not have populated this window."
            )

            if allow_missing_emi_fields:
                print(f"[WARN] {msg}")
            else:
                raise RuntimeError(
                    msg + " Use --allow-missing-emi-fields only if you intentionally want to send partial payloads."
                )

        if not send:
            continue

        try:
            response, request_body = post_records_with_retry(
                target=target,
                records=records,
                retry_max=retry_max,
                retry_wait_seconds=retry_wait_seconds,
            )

            status = "SUCCESS" if response.ok else "FAILED"
            error_message = None if response.ok else response.text[:1000]

            print(
                f"[SEND] local_date={local_date} plant={plant_code} "
                f"HTTP={response.status_code} response={response.text[:500]}"
            )

            insert_egress_log(
                egress_run_id=egress_run_id,
                target=target,
                window_start_utc=rows[0].collect_time_utc,
                window_end_utc=rows[-1].collect_time_utc,
                records=records,
                request_body=request_body,
                response=response,
                status=status,
                error_message=error_message,
            )

            if response.ok:
                success_count += 1
            else:
                failed_count += 1

            response.raise_for_status()

        except Exception as exc:
            failed_count += 1
            error_text = str(exc)

            print(
                f"[FAILED] local_date={local_date} plant={plant_code} "
                f"error={error_text}"
            )

            if send and egress_run_id is not None:
                try:
                    insert_egress_log(
                        egress_run_id=egress_run_id,
                        target=target,
                        window_start_utc=start_utc,
                        window_end_utc=end_utc_inclusive,
                        records=records,
                        request_body=request_body or {"records": records},
                        response=response,
                        status="FAILED",
                        error_message=error_text,
                    )
                except Exception as log_exc:
                    print(f"[WARN] failed to insert failure log: {log_exc}")

        if sleep_seconds and sleep_seconds > 0:
            time.sleep(sleep_seconds)

    return success_count, failed_count, skipped_count


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Complete Enserve Energy Avg range recovery: devType 1 + 10 + 17."
    )

    parser.add_argument("--start-local-date", required=True)
    parser.add_argument("--end-local-date", required=True)

    parser.add_argument(
        "--plant",
        dest="plants",
        action="append",
        help="Plant code. Can be specified multiple times. Default: GC5 and Polyplex.",
    )

    parser.add_argument(
        "--prepare-all-huawei-source",
        action="store_true",
        help="Backfill devType 1, 10, 17 before loading mart/stage/sending.",
    )

    parser.add_argument("--skip-devtype-1", action="store_true")
    parser.add_argument("--skip-devtype-10", action="store_true")
    parser.add_argument("--skip-devtype-17", action="store_true")

    parser.add_argument("--skip-normalize", action="store_true")
    parser.add_argument("--skip-mart-load", action="store_true")
    parser.add_argument("--skip-stage-load", action="store_true")
    parser.add_argument(
        "--skip-emi-patch",
        action="store_true",
        help="Do not patch stage irradiance/temperature from mart.fact_dev_emi_5min before preview/send.",
    )
    parser.add_argument(
        "--allow-missing-emi-fields",
        action="store_true",
        help="Allow POST even if some records do not contain irradiance_wm2 or temperature_c.",
    )
    parser.add_argument("--skip-snapshot", action="store_true")

    parser.add_argument("--chunk-minutes", type=int, default=780)
    parser.add_argument("--normalize-limit-raw", type=int, default=2000)
    parser.add_argument("--sequential-backfill", action="store_true")

    parser.add_argument("--mart-proc-devtype-1", default=DEFAULT_MART_PROC_BY_DEVTYPE[1])
    parser.add_argument("--mart-proc-devtype-10", default=DEFAULT_MART_PROC_BY_DEVTYPE[10])
    parser.add_argument("--mart-proc-devtype-17", default=DEFAULT_MART_PROC_BY_DEVTYPE[17])

    parser.add_argument("--send", action="store_true")
    parser.add_argument("--dry-run-source", action="store_true")
    parser.add_argument("--sleep-seconds", type=float, default=1.0)
    parser.add_argument("--retry-max", type=int, default=3)
    parser.add_argument("--retry-wait-seconds", type=int, default=30)
    parser.add_argument("--expected-records", type=int, default=53)

    args = parser.parse_args()

    start_date = parse_yyyy_mm_dd(args.start_local_date)
    end_date = parse_yyyy_mm_dd(args.end_local_date)

    if end_date < start_date:
        raise ValueError("--end-local-date must be >= --start-local-date")

    plants = tuple(args.plants or DEFAULT_PLANTS)
    for p in plants:
        validate_plant_code(p)

    proc_by_devtype = {
        1: args.mart_proc_devtype_1,
        10: args.mart_proc_devtype_10,
        17: args.mart_proc_devtype_17,
    }

    for proc_name in proc_by_devtype.values():
        validate_proc_name(proc_name)

    devtypes_to_prepare = []

    if not args.skip_devtype_1:
        devtypes_to_prepare.append(1)
    if not args.skip_devtype_10:
        devtypes_to_prepare.append(10)
    if not args.skip_devtype_17:
        devtypes_to_prepare.append(17)

    app_dir = Path(__file__).resolve().parents[1]

    print("=== Complete Enserve Energy Avg Range Recovery ===")
    print(f"local_date_range={start_date} -> {end_date}")
    print(f"plants={plants}")
    print(f"prepare_all_huawei_source={args.prepare_all_huawei_source}")
    print(f"devtypes_to_prepare={devtypes_to_prepare}")
    print(f"send={args.send}")
    print(f"skip_emi_patch={args.skip_emi_patch}")
    print(f"allow_missing_emi_fields={args.allow_missing_emi_fields}")
    print("checkpoint_update=NO")

    targets = load_enabled_targets(plants)

    egress_run_id = None

    if args.send:
        egress_run_id = create_egress_run(
            message=(
                f"Complete Enserve energy avg range recovery "
                f"local_date_range={start_date}->{end_date}, plants={plants}, "
                f"devtypes={devtypes_to_prepare}"
            )
        )
        print(f"[RUN] Created egress_run_id={egress_run_id}")

    total_success = 0
    total_failed = 0
    total_skipped = 0

    try:
        for local_date in date_range_inclusive(start_date, end_date):
            start_utc, end_utc_inclusive, end_utc_exclusive = local_daylight_window_utc(local_date)

            print("##################################################")
            print(
                f"[DAY] local_date={local_date} "
                f"utc_window={fmt_z(start_utc)} -> {fmt_z(end_utc_exclusive)} "
                f"last_expected_send_ts={fmt_z(end_utc_inclusive)}"
            )

            if args.prepare_all_huawei_source:
                for dev_type_id in devtypes_to_prepare:
                    prepare_huawei_devtype(
                        app_dir=app_dir,
                        dev_type_id=dev_type_id,
                        plants=plants,
                        start_utc=start_utc,
                        end_utc_exclusive=end_utc_exclusive,
                        chunk_minutes=args.chunk_minutes,
                        sequential_backfill=args.sequential_backfill,
                        dry_run=args.dry_run_source,
                    )

                    exec_sql(DISABLE_RETENTION_SQL)

                if args.dry_run_source:
                    print("[DRY-RUN] Source dry-run only for this day. Skip normalize/mart/stage/send.")
                    continue

            else:
                print("[SOURCE] Skip Huawei source prepare. Use --prepare-all-huawei-source to backfill devType 1/10/17.")

            if not args.skip_normalize:
                normalize_generic(app_dir, args.normalize_limit_raw)
            else:
                print("[NORMALIZE] Skip")

            if not args.skip_mart_load:
                for dev_type_id in devtypes_to_prepare:
                    proc_name = proc_by_devtype[dev_type_id]
                    print(f"[MART] devType={dev_type_id} proc={proc_name}")
                    run_mart_proc(proc_name, start_utc, end_utc_exclusive)
            else:
                print("[MART] Skip")

            if not args.skip_stage_load:
                print("[STAGE] Load Enserve energy avg stage")
                exec_sql(STAGE_LOAD_SQL, (start_utc, end_utc_exclusive))
            else:
                print("[STAGE] Skip")

            if not args.skip_emi_patch:
                print("[STAGE] Patch EMI fields from mart.fact_dev_emi_5min")
                patch_stage_emi_from_mart(start_utc, end_utc_exclusive)
            else:
                print("[STAGE] Skip EMI patch")

            if not args.skip_snapshot:
                print("[SNAPSHOT] Refresh completeness snapshot")
                exec_sql(SNAPSHOT_SQL)
            else:
                print("[SNAPSHOT] Skip")

            success, failed, skipped = send_day_to_enserve(
                egress_run_id=egress_run_id,
                targets=targets,
                plants=plants,
                local_date=local_date,
                start_utc=start_utc,
                end_utc_inclusive=end_utc_inclusive,
                end_utc_exclusive=end_utc_exclusive,
                expected_records=args.expected_records,
                send=args.send,
                retry_max=args.retry_max,
                retry_wait_seconds=args.retry_wait_seconds,
                sleep_seconds=args.sleep_seconds,
                allow_missing_emi_fields=args.allow_missing_emi_fields,
            )

            total_success += success
            total_failed += failed
            total_skipped += skipped

            exec_sql(DISABLE_RETENTION_SQL)

        if args.send and egress_run_id is not None:
            if total_failed == 0 and total_success > 0:
                final_status = "SUCCESS"
            elif total_success > 0 and total_failed > 0:
                final_status = "PARTIAL_SUCCESS"
            elif total_success == 0 and total_failed == 0:
                final_status = "SKIPPED"
            else:
                final_status = "FAILED"

            update_egress_run(
                egress_run_id=egress_run_id,
                status=final_status,
                message=(
                    f"Complete range recovery finished. "
                    f"success={total_success}, failed={total_failed}, skipped={total_skipped}, "
                    f"date_range={start_date}->{end_date}, plants={plants}, devtypes={devtypes_to_prepare}"
                ),
            )

            print(f"[RUN] Completed egress_run_id={egress_run_id} status={final_status}")
        else:
            print(
                f"[PREVIEW] Completed without POST. "
                f"success={total_success}, failed={total_failed}, skipped={total_skipped}"
            )

        return 0

    except Exception as exc:
        print(f"[FATAL] {exc}")
        print("[CLEANUP] Disable retention targets")

        try:
            exec_sql(DISABLE_RETENTION_SQL)
        except Exception as cleanup_exc:
            print(f"[CLEANUP][FAILED] {cleanup_exc}")

        if args.send and egress_run_id is not None:
            update_egress_run(
                egress_run_id=egress_run_id,
                status="FAILED",
                message=f"fatal error: {exc}",
            )

        raise


if __name__ == "__main__":
    sys.exit(main())
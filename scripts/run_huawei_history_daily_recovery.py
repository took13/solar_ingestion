from __future__ import annotations

import argparse
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.main import build_app


RETENTION_JOBS = [
    "dev_history_backfill_retention_acc1",
    "dev_history_backfill_retention_acc2",
    "dev_history_backfill_retention_acc3",
]


PREPARE_SQL = """
-- Disable devType 10/17 first
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
AND t.endpoint_name = 'getDevHistoryKpi'
AND t.dev_type_id IN (10,17);

-- Enable and tune devType 1 only for selected active/history-expected plants
UPDATE t
SET
    is_enabled = 1,
    batch_size = 10,
    requested_batch_size = 10,
    max_batches_per_run = CEILING(1.0 * x.inverter_count / 10.0),
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
        COUNT(DISTINCT d.dev_id) AS inverter_count
    FROM dbo.dim_device d
    LEFT JOIN cfg.plant_data_scope_override o
        ON o.plant_code = d.plant_code
    WHERE d.dev_type_id = 1
      AND ISNULL(o.is_history_expected, 1) = 1
      AND d.plant_code IN (
          SELECT plant_code
          FROM cfg.inverter_realtime_selected_plant
          WHERE is_enabled = 1
      )
    GROUP BY d.plant_code
) x
    ON x.plant_code = t.plant_code
WHERE j.job_name IN (
    'dev_history_backfill_retention_acc1',
    'dev_history_backfill_retention_acc2',
    'dev_history_backfill_retention_acc3'
)
AND t.endpoint_name = 'getDevHistoryKpi'
AND t.dev_type_id = 1;

-- Force disable non-history-expected plants such as shutdown plants
UPDATE t
SET
    is_enabled = 0,
    updated_at_utc = SYSUTCDATETIME()
FROM ctl.ingest_target t
JOIN ctl.ingest_job j
    ON j.job_id = t.job_id
JOIN cfg.plant_data_scope_override o
    ON o.plant_code = t.plant_code
WHERE j.job_name IN (
    'dev_history_backfill_retention_acc1',
    'dev_history_backfill_retention_acc2',
    'dev_history_backfill_retention_acc3'
)
AND t.endpoint_name = 'getDevHistoryKpi'
AND ISNULL(o.is_history_expected, 1) = 0;
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


ACCOUNT_PLAN_SQL = """
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
AND t.dev_type_id = 1
GROUP BY
    j.job_name,
    t.account_id
ORDER BY
    j.job_name,
    t.account_id;
"""


MART_LOAD_SQL = """
EXEC mart.usp_load_fact_dev_inverter_5min
    @FromUtc = ?,
    @ToUtc   = ?;
"""


SNAPSHOT_SQL = """
EXEC ops.usp_refresh_solar_plant_completeness_snapshot;
"""


def previous_daylight_window_utc() -> tuple[datetime, datetime]:
    """
    Previous local day daylight window:
    Thailand 06:00–19:00 converted to UTC.
    Thailand = UTC+7, no DST.
    """
    now_utc = datetime.utcnow().replace(microsecond=0)
    now_local = now_utc + timedelta(hours=7)

    today_local_midnight = datetime(
        now_local.year,
        now_local.month,
        now_local.day,
        0,
        0,
        0,
    )

    start_local = today_local_midnight - timedelta(days=1) + timedelta(hours=6)
    end_local = today_local_midnight - timedelta(days=1) + timedelta(hours=19)

    start_utc = start_local - timedelta(hours=7)
    end_utc = end_local - timedelta(hours=7)

    return start_utc, end_utc


def parse_utc(value: str) -> datetime:
    """
    Parse UTC ISO string into naive UTC datetime.

    Accepted:
      2026-06-06T23:00:00Z
      2026-06-06T23:00:00+00:00
      2026-06-06 23:00:00
    """
    v = value.strip().replace("Z", "+00:00")
    dt = datetime.fromisoformat(v)

    if dt.tzinfo is None:
        return dt

    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def fmt_z(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


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


def query_sql(sql: str) -> list:
    conn = open_conn()
    cur = conn.cursor()

    try:
        cur.execute(sql)
        rows = cur.fetchall()
        return rows

    finally:
        try:
            conn.close()
        except Exception:
            pass


def validate_account_isolation() -> None:
    """
    Validate that each retention job uses only one account_id among enabled targets.
    This is required before parallel execution.
    """
    rows = query_sql(ACCOUNT_PLAN_SQL)

    print("[PLAN] Retention job account plan")
    print("[PLAN] job_name | account_id | target_count | enabled_target_count | planned_batches")

    accounts_by_job: dict[str, set[int]] = defaultdict(set)

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

    errors = []

    for job in RETENTION_JOBS:
        accounts = accounts_by_job.get(job, set())

        if len(accounts) > 1:
            errors.append(
                f"{job} has multiple account_id among enabled targets: {sorted(accounts)}"
            )

    if errors:
        raise RuntimeError(
            "Account isolation validation failed. "
            "Do not run parallel until each job maps to one account only. "
            + " | ".join(errors)
        )

    print("[PLAN] Account isolation validation PASS")


def build_backfill_command(
    job: str,
    start_utc: datetime,
    end_utc: datetime,
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
        fmt_z(end_utc),
        "--chunk-minutes",
        str(chunk_minutes),
    ]


def run_cmd_sequential(commands: list[tuple[str, list[str]]], cwd: Path) -> None:
    for label, cmd in commands:
        print(f"[RUN][SEQ][START] {label}")
        print("[RUN]", " ".join(cmd))

        started = time.monotonic()
        result = subprocess.run(cmd, cwd=str(cwd), text=True)
        elapsed = time.monotonic() - started

        print(f"[RUN][SEQ][END] {label} rc={result.returncode} elapsed_sec={elapsed:.1f}")

        if result.returncode != 0:
            raise RuntimeError(f"Command failed: {label} rc={result.returncode}")


def run_cmd_parallel(commands: list[tuple[str, list[str]]], cwd: Path) -> None:
    """
    Start all account jobs in parallel.
    Child process stdout/stderr are inherited by parent,
    so logs still flow into the wrapper .cmd log redirection.
    """
    processes = []

    print(f"[RUN][PARALLEL] Starting {len(commands)} job(s)")

    for label, cmd in commands:
        print(f"[RUN][PARALLEL][START] {label}")
        print("[RUN]", " ".join(cmd))

        started = time.monotonic()
        proc = subprocess.Popen(cmd, cwd=str(cwd), text=True)
        processes.append((label, cmd, proc, started))

    failures = []

    for label, cmd, proc, started in processes:
        rc = proc.wait()
        elapsed = time.monotonic() - started

        print(f"[RUN][PARALLEL][END] {label} rc={rc} elapsed_sec={elapsed:.1f}")

        if rc != 0:
            failures.append((label, rc, cmd))

    if failures:
        details = " | ".join(
            f"{label} rc={rc}" for label, rc, _ in failures
        )
        raise RuntimeError(f"One or more parallel jobs failed: {details}")

    print("[RUN][PARALLEL] All jobs completed successfully")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Controlled Huawei inverter history daily recovery."
    )

    parser.add_argument("--start", help="UTC ISO, e.g. 2026-06-06T23:00:00Z")
    parser.add_argument("--end", help="UTC ISO, e.g. 2026-06-07T12:00:00Z")
    parser.add_argument("--chunk-minutes", type=int, default=780)
    parser.add_argument("--normalize-limit-raw", type=int, default=2000)

    parser.add_argument(
        "--sequential-backfill",
        action="store_true",
        help="Run acc1/acc2/acc3 sequentially instead of parallel.",
    )

    parser.add_argument("--skip-backfill", action="store_true")
    parser.add_argument("--skip-normalize", action="store_true")
    parser.add_argument("--skip-mart", action="store_true")
    parser.add_argument("--skip-snapshot", action="store_true")

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Prepare plan only. Do not execute backfill/normalize/mart/snapshot.",
    )

    args = parser.parse_args()

    if args.start and args.end:
        start_utc = parse_utc(args.start)
        end_utc = parse_utc(args.end)
    elif not args.start and not args.end:
        start_utc, end_utc = previous_daylight_window_utc()
    else:
        raise ValueError("Provide both --start and --end, or neither.")

    if end_utc <= start_utc:
        raise ValueError("--end must be greater than --start")

    if args.chunk_minutes <= 0:
        raise ValueError("--chunk-minutes must be > 0")

    app_dir = Path(__file__).resolve().parents[1]

    print("=== Huawei Inverter History Daily Recovery ===")
    print(f"START_UTC={fmt_z(start_utc)}")
    print(f"END_UTC  ={fmt_z(end_utc)}")
    print(f"chunk_minutes={args.chunk_minutes}")
    print(f"parallel_backfill={not args.sequential_backfill}")

    try:
        print("[1/6] Prepare controlled retention targets")
        exec_sql(PREPARE_SQL)

        print("[1.5/6] Validate account isolation")
        validate_account_isolation()

        backfill_commands = [
            (
                job,
                build_backfill_command(
                    job=job,
                    start_utc=start_utc,
                    end_utc=end_utc,
                    chunk_minutes=args.chunk_minutes,
                ),
            )
            for job in RETENTION_JOBS
        ]

        if args.dry_run:
            print("[DRY-RUN] Commands that would run:")
            for label, cmd in backfill_commands:
                print(f"[DRY-RUN] {label}: {' '.join(cmd)}")

            print("[DRY-RUN] Disable retention targets before exit")
            exec_sql(DISABLE_RETENTION_SQL)
            print("=== Huawei Inverter History Daily Recovery DRY-RUN DONE ===")
            return 0

        if not args.skip_backfill:
            print("[2/6] Run controlled backfill jobs")

            if args.sequential_backfill:
                run_cmd_sequential(backfill_commands, cwd=app_dir)
            else:
                run_cmd_parallel(backfill_commands, cwd=app_dir)

        else:
            print("[2/6] Skip backfill")

        if not args.skip_normalize:
            print("[3/6] Normalize generic")
            normalize_cmd = [
                sys.executable,
                "-m",
                "scripts.run_normalize_generic",
                "--limit-raw",
                str(args.normalize_limit_raw),
            ]
            run_cmd_sequential([("normalize_generic", normalize_cmd)], cwd=app_dir)
        else:
            print("[3/6] Skip normalize")

        if not args.skip_mart:
            print("[4/6] Load mart.fact_dev_inverter_5min")
            exec_sql(MART_LOAD_SQL, (start_utc, end_utc))
        else:
            print("[4/6] Skip mart load")

        if not args.skip_snapshot:
            print("[5/6] Refresh completeness snapshot")
            exec_sql(SNAPSHOT_SQL)
        else:
            print("[5/6] Skip snapshot")

        print("[6/6] Disable retention targets")
        exec_sql(DISABLE_RETENTION_SQL)

        print("=== Huawei Inverter History Daily Recovery SUCCESS ===")
        return 0

    except Exception as exc:
        print(f"[FAILED] {exc}")
        print("[CLEANUP] Disable retention targets")

        try:
            exec_sql(DISABLE_RETENTION_SQL)
        except Exception as cleanup_exc:
            print(f"[CLEANUP][FAILED] {cleanup_exc}")

        raise


if __name__ == "__main__":
    sys.exit(main())
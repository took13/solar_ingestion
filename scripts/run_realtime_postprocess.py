from __future__ import annotations

import argparse
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from src.main import build_app


@dataclass(frozen=True)
class RawToNormalize:
    raw_id: int
    api_name: str
    dev_type_id: int
    plant_code: str
    request_started_at_utc: str | None
    request_finished_at_utc: str | None


def main() -> int:
    args = parse_args()

    app = build_app()

    rows = find_recent_device_raw(
        conn=app.conn,
        lookback_minutes=args.lookback_minutes,
        include_inverter=not args.exclude_inverter,
    )

    print("")
    print("=== Realtime Postprocess ===")
    print(f"lookback_minutes : {args.lookback_minutes}")
    print(f"dry_run          : {args.dry_run}")
    print(f"skip_mart_load   : {args.skip_mart_load}")
    print(f"raw_rows_found   : {len(rows)}")
    print("")

    for row in rows:
        print(
            f"[POST] raw_id={row.raw_id} "
            f"api={row.api_name} "
            f"devType={row.dev_type_id} "
            f"plant={row.plant_code}"
        )

    if not rows:
        print("[POST] No recent pending device raw rows to normalize.")
        return 0

    for row in rows:
        run_normalize_raw_id(
            raw_id=row.raw_id,
            dry_run=args.dry_run,
        )

    if args.dry_run:
        print("[POST][DRY-RUN] Skip mart load.")
        return 0

    if not args.skip_mart_load:
        load_meter_emi_mart(
            conn=app.conn,
            lookback_minutes=args.lookback_minutes,
        )

    print("[POST] Completed realtime postprocess.")
    return 0


def parse_args():
    parser = argparse.ArgumentParser(
        description="Auto-normalize recent successful realtime device raw rows and load meter/EMI mart."
    )

    parser.add_argument(
        "--lookback-minutes",
        type=int,
        default=60,
        help="Look back this many minutes for successful getDevRealKpi raw rows. Default: 60.",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run normalize dry-run only. No DB writes and no mart load.",
    )

    parser.add_argument(
        "--skip-mart-load",
        action="store_true",
        help="Normalize only. Do not load meter/EMI mart.",
    )

    parser.add_argument(
        "--exclude-inverter",
        action="store_true",
        help="Exclude devType 1 inverter realtime raw from normalization.",
    )

    return parser.parse_args()


def find_recent_device_raw(
    *,
    conn,
    lookback_minutes: int,
    include_inverter: bool,
) -> list[RawToNormalize]:
    dev_types = (10, 17, 1) if include_inverter else (10, 17)
    placeholders = ",".join("?" for _ in dev_types)

    sql = f"""
        SELECT
            r.raw_id,
            r.api_name,
            r.dev_type_id,
            r.plant_code,
            CONVERT(varchar(19), r.request_started_at_utc, 120) AS request_started_at_utc,
            CONVERT(varchar(19), r.request_finished_at_utc, 120) AS request_finished_at_utc
        FROM raw.api_call r
        LEFT JOIN norm.raw_normalization_status s
            ON s.raw_id = r.raw_id
        WHERE r.api_success_flag = 1
          AND ISNULL(r.fail_code, 0) = 0
          AND r.api_name = 'getDevRealKpi'
          AND r.dev_type_id IN ({placeholders})
          AND r.request_started_at_utc >= DATEADD(minute, -?, SYSUTCDATETIME())
          AND (
                s.raw_id IS NULL
             OR ISNULL(s.generic_status, 'PENDING') IN ('PENDING', 'FAILED')
          )
        ORDER BY
            r.request_started_at_utc,
            CASE r.dev_type_id
                WHEN 17 THEN 1
                WHEN 10 THEN 2
                WHEN 1  THEN 3
                ELSE 9
            END,
            r.raw_id;
    """

    params = list(dev_types) + [lookback_minutes]

    cursor = conn.cursor()
    cursor.execute(sql, params)

    return [
        RawToNormalize(
            raw_id=int(r.raw_id),
            api_name=str(r.api_name),
            dev_type_id=int(r.dev_type_id),
            plant_code=str(r.plant_code),
            request_started_at_utc=r.request_started_at_utc,
            request_finished_at_utc=r.request_finished_at_utc,
        )
        for r in cursor.fetchall()
    ]


def run_normalize_raw_id(*, raw_id: int, dry_run: bool) -> None:
    cmd = [
        sys.executable,
        "-m",
        "scripts.run_normalize_generic",
    ]

    if dry_run:
        cmd.append("--dry-run")

    cmd.extend(["--raw-id", str(raw_id)])

    print("")
    print(f"[POST] Running: {' '.join(cmd)}")

    result = subprocess.run(
        cmd,
        cwd=str(PROJECT_ROOT),
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(f"Normalization failed for raw_id={raw_id}, returncode={result.returncode}")


def load_meter_emi_mart(*, conn, lookback_minutes: int) -> None:
    print("")
    print("[POST] Loading meter/EMI mart for recent window")

    sql = """
        DECLARE @FromUtc datetime2(0) = DATEADD(minute, -?, SYSUTCDATETIME());
        DECLARE @ToUtc   datetime2(0) = DATEADD(minute, 10, SYSUTCDATETIME());

        PRINT CONCAT(
            '[POST][MART] Window FromUtc=',
            CONVERT(varchar(19), @FromUtc, 120),
            ' ToUtc=',
            CONVERT(varchar(19), @ToUtc, 120)
        );

        EXEC mart.usp_load_fact_dev_meter_5min
            @FromUtc = @FromUtc,
            @ToUtc   = @ToUtc;

        PRINT '[POST][MART] Completed meter 5min load';

        EXEC mart.usp_load_fact_dev_emi_5min
            @FromUtc = @FromUtc,
            @ToUtc   = @ToUtc;

        PRINT '[POST][MART] Completed EMI 5min load';
    """

    cursor = conn.cursor()
    cursor.execute(sql, (lookback_minutes,))

    while cursor.nextset():
        pass

    conn.commit()

    print("[POST] Completed meter/EMI mart load.")


if __name__ == "__main__":
    raise SystemExit(main())
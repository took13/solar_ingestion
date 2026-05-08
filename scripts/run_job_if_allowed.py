# scripts/run_job_if_allowed.py
"""
DB-driven wrapper for Solar Data Platform Day/Night Operating Mode.

Purpose:
- Check current operating mode from ctl.operating_mode_profile
- Check job permission from ctl.job_mode_policy
- Enforce hard stop time for night/backfill jobs
- Run the real Python module only when allowed

Example:
python -m scripts.run_job_if_allowed --job-name plant_realtime_online --module scripts.run_pipeline_plant_realtime

With extra module arguments:
python -m scripts.run_job_if_allowed --job-name inverter_history_wave_A --module scripts.run_pipeline_inverter_nearline_wave -- --wave-group A
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime, time
from pathlib import Path
from typing import Any, Optional, Tuple
from zoneinfo import ZoneInfo


# ---------------------------------------------------------------------------
# Project import path
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))


# ---------------------------------------------------------------------------
# Reuse existing project DB connection
# ---------------------------------------------------------------------------
from src.config_loader import ConfigLoader
from src.db.connection import create_connection


TZ_NAME = "Asia/Bangkok"


def _to_time(value: Any) -> time:
    """Convert SQL/Python time-like value to datetime.time."""
    if isinstance(value, time):
        return value

    if hasattr(value, "time"):
        return value.time()

    if isinstance(value, str):
        parts = value.split(":")
        if len(parts) >= 2:
            hour = int(parts[0])
            minute = int(parts[1])
            second = int(parts[2]) if len(parts) >= 3 else 0
            return time(hour, minute, second)

    raise ValueError(f"Cannot convert value to time: {value!r}")


def load_app_config() -> dict:
    """
    Load config/app.yaml using the existing ConfigLoader.

    Supports both possible constructor styles:
    - ConfigLoader(PROJECT_ROOT / "config")
    - ConfigLoader()
    """
    try:
        config_loader = ConfigLoader(PROJECT_ROOT / "config")
    except TypeError:
        config_loader = ConfigLoader()

    return config_loader.load_app_config()


def open_connection():
    """Open SQL Server connection using existing app.yaml database.connection_string."""
    app_config = load_app_config()
    conn_str = app_config["database"]["connection_string"]
    return create_connection(conn_str)


def get_current_mode(conn) -> str:
    """
    Return current operating mode based on local Bangkok time.

    Supports:
    - Same-day window: 05:30 to 18:30
    - Overnight window: 19:00 to 05:00
    """
    now_local = datetime.now(ZoneInfo(TZ_NAME)).time()

    sql = """
    SELECT mode_name, start_local_time, end_local_time
    FROM ctl.operating_mode_profile
    WHERE is_enabled = 1
    """

    rows = conn.cursor().execute(sql).fetchall()

    for row in rows:
        mode_name = row[0]
        start_t = _to_time(row[1])
        end_t = _to_time(row[2])

        if start_t <= end_t:
            if start_t <= now_local < end_t:
                return mode_name
        else:
            if now_local >= start_t or now_local < end_t:
                return mode_name

    return "UNKNOWN"


def get_job_policy(conn, job_name: str, mode_name: str) -> Tuple[bool, Optional[time]]:
    """
    Return:
      (is_allowed, hard_stop_local_time)

    job_name here is the wrapper job name, not necessarily ctl.ingest_job.job_name.
    """
    sql = """
    SELECT TOP 1
        is_allowed,
        hard_stop_local_time
    FROM ctl.job_mode_policy
    WHERE job_name = ?
      AND mode_name = ?
    ORDER BY policy_id DESC
    """

    row = conn.cursor().execute(sql, job_name, mode_name).fetchone()

    if not row:
        return False, None

    is_allowed = bool(row[0])
    hard_stop = _to_time(row[1]) if row[1] is not None else None

    return is_allowed, hard_stop


def is_past_hard_stop(mode_name: str, hard_stop: Optional[time]) -> bool:
    """
    Enforce hard stop safely.

    For NIGHT mode with hard_stop = 04:50, do not block at 19:00.
    Block only during the after-midnight part, e.g. 04:50-11:59.
    """
    if hard_stop is None:
        return False

    now_local = datetime.now(ZoneInfo(TZ_NAME)).time()

    if mode_name == "NIGHT" and hard_stop < time(12, 0, 0):
        return hard_stop <= now_local < time(12, 0, 0)

    return now_local >= hard_stop


def run_module(module_name: str, module_args: list[str]) -> int:
    """Run the real module with the current Python executable."""
    cmd = [sys.executable, "-m", module_name] + module_args

    print(f"[EXEC] {' '.join(cmd)}")
    print(f"[CWD]  {PROJECT_ROOT}")

    result = subprocess.run(
        cmd,
        cwd=str(PROJECT_ROOT),
    )

    return int(result.returncode)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run a Solar job only when allowed by Day/Night DB policy."
    )
    parser.add_argument("--job-name", required=True, help="Wrapper job name in ctl.job_mode_policy")
    parser.add_argument("--module", required=True, help="Python module to execute")

    args, extra_args = parser.parse_known_args()

    if extra_args and extra_args[0] == "--":
        extra_args = extra_args[1:]

    conn = None

    try:
        conn = open_connection()

        mode_name = get_current_mode(conn)
        is_allowed, hard_stop = get_job_policy(conn, args.job_name, mode_name)

        if mode_name == "UNKNOWN":
            print(f"[SKIP] job={args.job_name} mode=UNKNOWN reason=no active operating mode matched")
            return 0

        if not is_allowed:
            print(f"[SKIP] job={args.job_name} mode={mode_name} reason=policy_not_allowed")
            return 0

        if is_past_hard_stop(mode_name, hard_stop):
            print(
                f"[SKIP] job={args.job_name} mode={mode_name} "
                f"reason=past_hard_stop hard_stop={hard_stop}"
            )
            return 0

        print(f"[RUN] job={args.job_name} mode={mode_name} module={args.module}")

    except Exception as exc:
        print(f"[ERROR] wrapper failed before running job={args.job_name}: {exc}")
        return 2

    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    return run_module(args.module, extra_args)


if __name__ == "__main__":
    raise SystemExit(main())

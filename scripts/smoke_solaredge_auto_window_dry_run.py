"""
Smoke test: SolarEdge auto-window dry-run from checkpoint.

Purpose:
- Read SolarEdge plant mapping from dbo.dim_plant_source_map
- Read last successful checkpoint from ctl.solaredge_ingest_checkpoint
- Calculate the next ingestion window
- Print only

Safe by design:
- No SolarEdge API call
- No raw insert
- No canonical insert
- No mart insert
- No checkpoint update
- No API key value printed

Run example:
python scripts\\smoke_solaredge_auto_window_dry_run.py --plant-code SE_GC5 --endpoint-name sitePower --window-minutes 60 --lag-minutes 30
"""

from __future__ import annotations

import argparse
import importlib
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Optional
from zoneinfo import ZoneInfo


# Ensure project root is importable when running as:
# python scripts\smoke_solaredge_auto_window_dry_run.py
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


@dataclass
class PlantMapping:
    internal_plant_code: str
    site_id: str
    timezone_name: str
    api_key_secret_name: str


@dataclass
class Checkpoint:
    last_success_start_local: Optional[datetime]
    last_success_end_local: Optional[datetime]
    last_success_start_utc: Optional[datetime]
    last_success_end_utc: Optional[datetime]
    last_raw_id: Optional[int]
    last_status: Optional[str]


def load_app_config() -> dict[str, Any]:
    """
    Try to load app_config using common project config patterns.
    Existing SolarToPI scripts use:
        create_connection(app_config["database"]["connection_string"])
    """

    candidates = [
        "src.config",
        "src.config.settings",
        "src.config.config",
        "src.core.config",
        "src.config_loader",
    ]

    last_errors: list[str] = []

    for module_name in candidates:
        try:
            module = importlib.import_module(module_name)

            if hasattr(module, "app_config"):
                return getattr(module, "app_config")

            if hasattr(module, "config"):
                config = getattr(module, "config")
                if isinstance(config, dict):
                    return config

            if hasattr(module, "load_config"):
                config = module.load_config()
                if isinstance(config, dict):
                    return config

            if hasattr(module, "get_config"):
                config = module.get_config()
                if isinstance(config, dict):
                    return config

            last_errors.append(f"{module_name}: imported but no app_config/config/load_config/get_config")

        except Exception as exc:
            last_errors.append(f"{module_name}: {type(exc).__name__}: {exc}")

    raise RuntimeError(
        "Cannot load project app_config. "
        "Please check import pattern from existing scripts such as smoke_mapping_read.py.\n\n"
        + "\n".join(last_errors)
    )


def get_conn() -> Any:
    """
    Use the same DB connection pattern as existing project scripts:
        from src.db.connection import create_connection
        conn = create_connection(app_config['database']['connection_string'])

    Note:
    - This script intentionally does not import pyodbc directly.
    - pyodbc is used only inside src.db.connection, same as existing scripts.
    """

    try:
        from src.db.connection import create_connection
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Cannot import src.db.connection.create_connection. "
            "Please run from project root C:\\SOLAR\\solar_ingestion and activate .venv."
        ) from exc

    app_config = load_app_config()

    try:
        conn_str = app_config["database"]["connection_string"]
    except Exception as exc:
        raise RuntimeError(
            "app_config was loaded but app_config['database']['connection_string'] was not found."
        ) from exc

    return create_connection(conn_str)


def normalize_col_map(row: Any, description: Any) -> dict[str, Any]:
    return {col[0].lower(): value for col, value in zip(description, row)}


def pick_value(row_dict: dict[str, Any], candidates: Iterable[str], default: Any = None) -> Any:
    for name in candidates:
        key = name.lower()
        if key in row_dict and row_dict[key] is not None:
            return row_dict[key]
    return default


def table_has_column(conn: Any, schema_name: str, table_name: str, column_name: str) -> bool:
    sql = """
    SELECT 1
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = ?
      AND TABLE_NAME = ?
      AND COLUMN_NAME = ?;
    """
    cur = conn.cursor()
    cur.execute(sql, schema_name, table_name, column_name)
    return cur.fetchone() is not None


def floor_to_15min(dt: datetime) -> datetime:
    minute = (dt.minute // 15) * 15
    return dt.replace(minute=minute, second=0, microsecond=0)


def ensure_local_tz(dt: Optional[datetime], tz: ZoneInfo) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=tz)
    return dt.astimezone(tz)


def to_utc_naive(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def read_active_mapping(conn: Any, plant_code: str) -> PlantMapping:
    """
    Read SolarEdge mapping for one internal plant code.

    Expected table:
        dbo.dim_plant_source_map

    Important:
        api_key_secret_name stores ENV VAR NAME only, not actual API key.
    """

    has_is_active = table_has_column(conn, "dbo", "dim_plant_source_map", "is_active")

    where_is_active = "AND ISNULL(is_active, 1) = 1" if has_is_active else ""

    sql = f"""
    SELECT TOP (1)
        *
    FROM dbo.dim_plant_source_map
    WHERE internal_plant_code = ?
      AND api_key_secret_name LIKE 'SOLAREDGE_API_KEY_%'
      {where_is_active}
    ORDER BY internal_plant_code;
    """

    cur = conn.cursor()
    cur.execute(sql, plant_code)
    row = cur.fetchone()

    if not row:
        raise RuntimeError(
            f"No active SolarEdge mapping found for internal_plant_code={plant_code}. "
            "Please check dbo.dim_plant_source_map."
        )

    d = normalize_col_map(row, cur.description)

    internal_plant_code = pick_value(
        d,
        ["internal_plant_code", "plant_code", "canonical_plant_code"],
    )

    site_id = pick_value(
        d,
        [
            "source_plant_id",
            "source_site_id",
            "site_id",
            "external_plant_id",
            "source_siteid",
            "solaredge_site_id",
        ],
    )

    timezone_name = pick_value(
        d,
        [
            "timezone",
            "time_zone",
            "timezone_name",
            "source_timezone",
            "plant_timezone",
        ],
        default="Asia/Bangkok",
    )

    api_key_secret_name = pick_value(d, ["api_key_secret_name"])

    missing = []
    if not internal_plant_code:
        missing.append("internal_plant_code")
    if not site_id:
        missing.append("source_plant_id / site_id")
    if not api_key_secret_name:
        missing.append("api_key_secret_name")

    if missing:
        available_cols = ", ".join(sorted(d.keys()))
        raise RuntimeError(
            "Missing required mapping field(s): "
            + ", ".join(missing)
            + "\nAvailable columns: "
            + available_cols
        )

    return PlantMapping(
        internal_plant_code=str(internal_plant_code),
        site_id=str(site_id),
        timezone_name=str(timezone_name),
        api_key_secret_name=str(api_key_secret_name),
    )


def read_checkpoint(
    conn: Any,
    plant_code: str,
    endpoint_name: str,
) -> Checkpoint:
    sql = """
    SELECT TOP (1)
        last_success_start_local,
        last_success_end_local,
        last_success_start_utc,
        last_success_end_utc,
        last_raw_id,
        last_status
    FROM ctl.solaredge_ingest_checkpoint
    WHERE internal_plant_code = ?
      AND endpoint_name = ?
    ORDER BY updated_at_utc DESC;
    """

    cur = conn.cursor()
    cur.execute(sql, plant_code, endpoint_name)
    row = cur.fetchone()

    if not row:
        return Checkpoint(
            last_success_start_local=None,
            last_success_end_local=None,
            last_success_start_utc=None,
            last_success_end_utc=None,
            last_raw_id=None,
            last_status=None,
        )

    d = normalize_col_map(row, cur.description)

    return Checkpoint(
        last_success_start_local=d.get("last_success_start_local"),
        last_success_end_local=d.get("last_success_end_local"),
        last_success_start_utc=d.get("last_success_start_utc"),
        last_success_end_utc=d.get("last_success_end_utc"),
        last_raw_id=d.get("last_raw_id"),
        last_status=d.get("last_status"),
    )


def parse_local_datetime(value: str, tz: ZoneInfo) -> datetime:
    dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    return dt.replace(tzinfo=tz)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="SolarEdge auto-window dry-run from checkpoint. No API call. No DB write."
    )

    parser.add_argument(
        "--plant-code",
        required=True,
        help="Internal plant code, example: SE_GC5",
    )

    parser.add_argument(
        "--endpoint-name",
        required=True,
        choices=["sitePower", "energyDetails"],
        help="SolarEdge logical endpoint name",
    )

    parser.add_argument(
        "--window-minutes",
        type=int,
        default=60,
        help="Max next window size in minutes. Default = 60",
    )

    parser.add_argument(
        "--lag-minutes",
        type=int,
        default=30,
        help="Safety lag from current local time. Default = 30",
    )

    parser.add_argument(
        "--bootstrap-start-local",
        default=None,
        help="Used only when checkpoint does not exist. Format: YYYY-MM-DD HH:MM:SS",
    )

    args = parser.parse_args()

    if args.window_minutes <= 0:
        raise RuntimeError("--window-minutes must be greater than 0")

    if args.lag_minutes < 0:
        raise RuntimeError("--lag-minutes must be greater than or equal to 0")

    conn = get_conn()

    mapping = read_active_mapping(conn, args.plant_code)
    tz = ZoneInfo(mapping.timezone_name)

    checkpoint = read_checkpoint(conn, args.plant_code, args.endpoint_name)

    if checkpoint.last_success_end_local:
        next_start_local = ensure_local_tz(checkpoint.last_success_end_local, tz)
        start_reason = "checkpoint.last_success_end_local"
    elif args.bootstrap_start_local:
        next_start_local = parse_local_datetime(args.bootstrap_start_local, tz)
        start_reason = "bootstrap-start-local"
    else:
        raise RuntimeError(
            "No checkpoint found and --bootstrap-start-local was not provided. "
            "For pilot, either use a plant/endpoint with checkpoint or provide bootstrap start."
        )

    if next_start_local is None:
        raise RuntimeError("next_start_local could not be calculated")

    now_local = datetime.now(tz)
    available_end_local = floor_to_15min(now_local - timedelta(minutes=args.lag_minutes))

    proposed_end_local = next_start_local + timedelta(minutes=args.window_minutes)
    next_end_local = min(proposed_end_local, available_end_local)

    status = "READY" if next_end_local > next_start_local else "NOT_DUE"

    next_start_utc = to_utc_naive(next_start_local)
    next_end_utc = to_utc_naive(next_end_local)

    # Safe secret check:
    # Print only env var name and existence. Never print API key value.
    api_key_env_exists = bool(os.getenv(mapping.api_key_secret_name))

    print("")
    print("=== SolarEdge Auto Window Dry Run ===")
    print(f"status                  : {status}")
    print(f"internal_plant_code     : {mapping.internal_plant_code}")
    print(f"site_id                 : {mapping.site_id}")
    print(f"endpoint_name           : {args.endpoint_name}")
    print(f"timezone                : {mapping.timezone_name}")
    print(f"api_key_secret_name     : {mapping.api_key_secret_name}")
    print(f"api_key_env_exists      : {api_key_env_exists}")
    print("")
    print("--- checkpoint ---")
    print(f"last_status             : {checkpoint.last_status}")
    print(f"last_raw_id             : {checkpoint.last_raw_id}")
    print(f"last_success_start_local: {checkpoint.last_success_start_local}")
    print(f"last_success_end_local  : {checkpoint.last_success_end_local}")
    print(f"last_success_start_utc  : {checkpoint.last_success_start_utc}")
    print(f"last_success_end_utc    : {checkpoint.last_success_end_utc}")
    print("")
    print("--- next window ---")
    print(f"start_reason            : {start_reason}")
    print(f"now_local               : {now_local.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"available_end_local     : {available_end_local.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"next_start_local        : {next_start_local.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"next_end_local          : {next_end_local.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"next_start_utc          : {next_start_utc.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"next_end_utc            : {next_end_utc.strftime('%Y-%m-%d %H:%M:%S')}")
    print("")
    print("No API call executed. No DB write executed.")
    print("")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
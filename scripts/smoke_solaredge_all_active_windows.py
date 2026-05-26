from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from src.config_loader import ConfigLoader
from src.db.connection import create_connection
from src.db.repositories.source_mapping_repo import SourceMappingRepository
from src.db.repositories.solaredge_checkpoint_repo import SolarEdgeCheckpointRepository
from src.db.repositories.source_credential_repo import SourceCredentialRepository


SOURCE_SYSTEM = "SOLAREDGE"
DEFAULT_ENDPOINTS = ["sitePower", "energyDetails"]


def floor_to_15min(dt: datetime) -> datetime:
    minute = (dt.minute // 15) * 15
    return dt.replace(minute=minute, second=0, microsecond=0)


def parse_local_with_tz(date_text: str, timezone_name: str) -> datetime:
    local_tz = ZoneInfo(timezone_name)
    local_dt = datetime.strptime(date_text, "%Y-%m-%d %H:%M:%S")
    return local_dt.replace(tzinfo=local_tz)


def ensure_local_datetime(value: Any, timezone_name: str) -> datetime:
    local_tz = ZoneInfo(timezone_name)

    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=local_tz)
        return value.astimezone(local_tz)

    if isinstance(value, str):
        return parse_local_with_tz(value, timezone_name)

    raise RuntimeError(f"Unsupported datetime value type: {type(value).__name__}")


def to_utc_naive(local_dt: datetime) -> datetime:
    return local_dt.astimezone(timezone.utc).replace(tzinfo=None)


def format_dt(value: datetime | None) -> str:
    if value is None:
        return "-"
    return value.strftime("%Y-%m-%d %H:%M:%S")


def get_credential_status(
    *,
    credential_repo: SourceCredentialRepository,
    credential_name: str | None,
) -> str:
    if not credential_name:
        return "MISSING_NAME"

    credential = credential_repo.get_active_credential(
        source_system_code=SOURCE_SYSTEM,
        credential_name=credential_name,
        credential_type="API_KEY",
    )

    if not credential:
        return "MISSING_DB_CREDENTIAL"

    if not credential.get("secret_value"):
        return "EMPTY_SECRET"

    return "OK"


def resolve_window_plan(
    *,
    checkpoint_repo: SolarEdgeCheckpointRepository,
    source_plant_code: str,
    endpoint_name: str,
    timezone_name: str,
    window_minutes: int,
    lag_minutes: int,
    bootstrap_start_local: str | None,
) -> dict[str, Any]:
    tz = ZoneInfo(timezone_name)

    checkpoint = checkpoint_repo.get_checkpoint(
        source_system_code=SOURCE_SYSTEM,
        source_plant_code=source_plant_code,
        endpoint_name=endpoint_name,
    )

    if checkpoint and checkpoint.get("last_success_end_local"):
        start_local_dt = ensure_local_datetime(
            checkpoint["last_success_end_local"],
            timezone_name=timezone_name,
        )
        start_reason = "CHECKPOINT"

    elif bootstrap_start_local:
        start_local_dt = parse_local_with_tz(
            bootstrap_start_local,
            timezone_name=timezone_name,
        )
        start_reason = "BOOTSTRAP"

    else:
        return {
            "status": "NO_CHECKPOINT",
            "start_reason": "-",
            "checkpoint": checkpoint,
            "next_start_local": None,
            "next_end_local": None,
            "next_start_utc": None,
            "next_end_utc": None,
            "available_end_local": None,
        }

    now_local = datetime.now(tz)
    available_end_local = floor_to_15min(
        now_local - timedelta(minutes=lag_minutes)
    )

    proposed_end_local = start_local_dt + timedelta(minutes=window_minutes)
    end_local_dt = min(proposed_end_local, available_end_local)

    status = "READY" if end_local_dt > start_local_dt else "NOT_DUE"

    return {
        "status": status,
        "start_reason": start_reason,
        "checkpoint": checkpoint,
        "next_start_local": start_local_dt,
        "next_end_local": end_local_dt,
        "next_start_utc": to_utc_naive(start_local_dt),
        "next_end_utc": to_utc_naive(end_local_dt),
        "available_end_local": available_end_local,
    }


def print_plan_row(row: dict[str, Any]) -> None:
    print(
        f"{row['internal_plant_code']:<14} "
        f"{row['site_id']:<12} "
        f"{row['endpoint']:<14} "
        f"{row['credential_status']:<22} "
        f"{row['window_status']:<14} "
        f"{row['start_reason']:<10} "
        f"{row['next_start_local']:<19} "
        f"{row['next_end_local']:<19} "
        f"{row['last_status']:<10} "
        f"{row['last_raw_id']}"
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Dry-run SolarEdge auto-window plan for all active plants. No API call. No DB write."
    )

    parser.add_argument(
        "--endpoint",
        choices=["sitePower", "energyDetails", "both"],
        default="both",
        help="Endpoint to evaluate. Default = both.",
    )

    parser.add_argument(
        "--window-minutes",
        type=int,
        default=60,
        help="Auto-window size in minutes. Default = 60.",
    )

    parser.add_argument(
        "--lag-minutes",
        type=int,
        default=30,
        help="Safety lag from current local time. Default = 30.",
    )

    parser.add_argument(
        "--bootstrap-start-local",
        default=None,
        help='Optional bootstrap if checkpoint is missing. Format "YYYY-MM-DD HH:MM:SS".',
    )

    args = parser.parse_args()

    if args.window_minutes <= 0:
        raise RuntimeError("--window-minutes must be greater than 0")

    if args.lag_minutes < 0:
        raise RuntimeError("--lag-minutes must be greater than or equal to 0")

    endpoints = DEFAULT_ENDPOINTS if args.endpoint == "both" else [args.endpoint]

    app_config = ConfigLoader().load_app_config()
    conn = create_connection(app_config["database"]["connection_string"])

    try:
        source_repo = SourceMappingRepository(conn)
        checkpoint_repo = SolarEdgeCheckpointRepository(conn)
        credential_repo = SourceCredentialRepository(conn)

        plant_maps = source_repo.get_active_plant_maps(
            source_system_code=SOURCE_SYSTEM,
        )

        if not plant_maps:
            raise RuntimeError("No active SOLAREDGE plant mapping found.")

        rows: list[dict[str, Any]] = []

        for plant_map in plant_maps:
            internal_plant_code = plant_map["internal_plant_code"]
            source_plant_code = plant_map["source_plant_code"]
            timezone_name = plant_map.get("timezone_name") or "Asia/Bangkok"
            credential_name = plant_map.get("api_key_secret_name")

            credential_status = get_credential_status(
                credential_repo=credential_repo,
                credential_name=credential_name,
            )

            for endpoint_name in endpoints:
                plan = resolve_window_plan(
                    checkpoint_repo=checkpoint_repo,
                    source_plant_code=source_plant_code,
                    endpoint_name=endpoint_name,
                    timezone_name=timezone_name,
                    window_minutes=args.window_minutes,
                    lag_minutes=args.lag_minutes,
                    bootstrap_start_local=args.bootstrap_start_local,
                )

                checkpoint = plan.get("checkpoint") or {}

                rows.append(
                    {
                        "internal_plant_code": internal_plant_code,
                        "site_id": source_plant_code,
                        "endpoint": endpoint_name,
                        "timezone": timezone_name,
                        "credential_name": credential_name,
                        "credential_status": credential_status,
                        "window_status": plan["status"],
                        "start_reason": plan["start_reason"],
                        "next_start_local": format_dt(plan["next_start_local"]),
                        "next_end_local": format_dt(plan["next_end_local"]),
                        "next_start_utc": format_dt(plan["next_start_utc"]),
                        "next_end_utc": format_dt(plan["next_end_utc"]),
                        "available_end_local": format_dt(plan["available_end_local"]),
                        "last_status": checkpoint.get("last_status") or "-",
                        "last_raw_id": checkpoint.get("last_raw_id") or "-",
                    }
                )

        print("")
        print("=== SolarEdge All Active Auto-Window Dry Run ===")
        print(f"source_system_code : {SOURCE_SYSTEM}")
        print(f"plant_count        : {len(plant_maps)}")
        print(f"endpoints          : {', '.join(endpoints)}")
        print(f"window_minutes     : {args.window_minutes}")
        print(f"lag_minutes        : {args.lag_minutes}")
        print("")
        print(
            f"{'plant':<14} "
            f"{'site_id':<12} "
            f"{'endpoint':<14} "
            f"{'credential':<22} "
            f"{'window':<14} "
            f"{'reason':<10} "
            f"{'next_start_local':<19} "
            f"{'next_end_local':<19} "
            f"{'last':<10} "
            f"raw_id"
        )
        print("-" * 150)

        for row in rows:
            print_plan_row(row)

        print("")
        print("--- Summary ---")

        summary: dict[tuple[str, str], int] = {}
        for row in rows:
            key = (row["credential_status"], row["window_status"])
            summary[key] = summary.get(key, 0) + 1

        for (credential_status, window_status), count in sorted(summary.items()):
            print(
                f"credential_status={credential_status:<22} "
                f"window_status={window_status:<14} "
                f"count={count}"
            )

        print("")
        print("[DRY-RUN] No API call executed. No DB write executed.")
        print("")

        return 0

    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
from __future__ import annotations

import argparse
from datetime import datetime, timedelta

from src.config_loader import ConfigLoader
from src.db.connection import create_connection
from src.db.repositories.solaredge_checkpoint_repo import SolarEdgeCheckpointRepository


def main():
    args = parse_args()

    app_config = ConfigLoader().load_app_config()
    conn = create_connection(app_config["database"]["connection_string"])

    repo = SolarEdgeCheckpointRepository(conn)
    checkpoints = repo.list_checkpoints()

    conn.close()

    if not checkpoints:
        print("[WARN] No SOLAREDGE checkpoints found")
        return

    print("=== SolarEdge Next Window Dry-run ===")
    print(f"window_minutes={args.window_minutes}")
    print(f"bootstrap_start_local={args.bootstrap_start_local}")
    print("")

    for row in checkpoints:
        start_local, end_local = calculate_next_window(
            last_success_end_local=row.get("last_success_end_local"),
            bootstrap_start_local=args.bootstrap_start_local,
            window_minutes=args.window_minutes,
        )

        print(
            f"- {row['internal_plant_code']} "
            f"site_id={row['source_plant_code']} "
            f"endpoint={row['endpoint_name']} "
            f"status={row['last_status']} "
            f"last_end_local={row.get('last_success_end_local')} "
            f"next_start_local={start_local} "
            f"next_end_local={end_local}"
        )

    print("")
    print("[OK] checkpoint next-window dry-run completed")


def calculate_next_window(
    *,
    last_success_end_local,
    bootstrap_start_local: str,
    window_minutes: int,
):
    if last_success_end_local:
        start_local = normalize_datetime(last_success_end_local)
    else:
        start_local = datetime.strptime(bootstrap_start_local, "%Y-%m-%d %H:%M:%S")

    end_local = start_local + timedelta(minutes=window_minutes)

    return (
        start_local.strftime("%Y-%m-%d %H:%M:%S"),
        end_local.strftime("%Y-%m-%d %H:%M:%S"),
    )


def normalize_datetime(value):
    if isinstance(value, datetime):
        return value.replace(microsecond=0)

    if isinstance(value, str):
        return datetime.strptime(value[:19], "%Y-%m-%d %H:%M:%S")

    raise ValueError(f"Unsupported datetime value={value}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Dry-run SolarEdge next windows from checkpoint. Does not call API or update DB."
    )

    parser.add_argument(
        "--window-minutes",
        type=int,
        default=60,
        help="Window size for next ingestion run.",
    )

    parser.add_argument(
        "--bootstrap-start-local",
        default="2026-05-21 12:00:00",
        help="Used only when checkpoint has no last_success_end_local.",
    )

    return parser.parse_args()


if __name__ == "__main__":
    main()
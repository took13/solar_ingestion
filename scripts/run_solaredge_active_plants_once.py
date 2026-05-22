from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time

from src.config_loader import ConfigLoader
from src.db.connection import create_connection
from src.db.repositories.source_mapping_repo import SourceMappingRepository


SOURCE_SYSTEM = "SOLAREDGE"


def main():
    args = parse_args()

    app_config = ConfigLoader().load_app_config()
    conn = create_connection(app_config["database"]["connection_string"])

    source_repo = SourceMappingRepository(conn)
    plant_maps = source_repo.get_active_plant_maps(SOURCE_SYSTEM)

    conn.close()

    if not plant_maps:
        raise RuntimeError("No active SOLAREDGE plant mapping found.")

    print("=== Active SolarEdge Plants ===")
    for row in plant_maps:
        print(
            f"- internal={row['internal_plant_code']} "
            f"site_id={row['source_plant_code']} "
            f"name={row['source_plant_name']} "
            f"secret={row.get('api_key_secret_name')}"
        )

    print("")
    print("=== Pre-check environment variables ===")
    validate_env_vars(plant_maps)

    print("")
    print("=== Run SolarEdge pilot ingestion for all active plants ===")
    print(f"start_local={args.start_local}")
    print(f"end_local={args.end_local}")
    print(f"endpoint={args.endpoint}")
    print(f"dry_run={args.dry_run}")
    print("")

    success_count = 0
    failed_count = 0

    for index, row in enumerate(plant_maps, start=1):
        site_id = row["source_plant_code"]
        internal_plant_code = row["internal_plant_code"]

        print("")
        print("=" * 80)
        print(f"[{index}/{len(plant_maps)}] Running {internal_plant_code} site_id={site_id}")
        print("=" * 80)

        cmd = [
            sys.executable,
            "-m",
            "scripts.run_solaredge_pilot_ingest",
            "--site-id",
            str(site_id),
            "--start-local",
            args.start_local,
            "--end-local",
            args.end_local,
            "--endpoint",
            args.endpoint,
            "--meters",
            args.meters,
        ]

        safe_cmd_text = " ".join(cmd)
        print(f"[CMD] {safe_cmd_text}")

        if args.dry_run:
            continue

        try:
            subprocess.run(cmd, check=True)
            success_count += 1
            print(f"[OK] {internal_plant_code} completed")

        except subprocess.CalledProcessError as exc:
            failed_count += 1
            print(f"[ERROR] {internal_plant_code} failed with returncode={exc.returncode}")

            if not args.continue_on_error:
                raise

        if args.delay_sec > 0 and index < len(plant_maps):
            print(f"[INFO] sleep {args.delay_sec} sec before next plant")
            time.sleep(args.delay_sec)

    print("")
    print("=== Summary ===")
    print(f"total_plants={len(plant_maps)}")
    print(f"success_count={success_count}")
    print(f"failed_count={failed_count}")
    print("[OK] run all active SolarEdge plants completed")


def validate_env_vars(plant_maps):
    missing = []

    for row in plant_maps:
        secret_name = row.get("api_key_secret_name")

        if not secret_name:
            missing.append(
                f"{row['internal_plant_code']} site_id={row['source_plant_code']} has no api_key_secret_name"
            )
            continue

        if os.getenv(secret_name):
            print(f"- {row['internal_plant_code']}: {secret_name} is set")
        else:
            print(f"- {row['internal_plant_code']}: {secret_name} is missing")
            missing.append(
                f"{row['internal_plant_code']} site_id={row['source_plant_code']} missing env var {secret_name}"
            )

    if missing:
        message = "\n".join(missing)
        raise RuntimeError(
            "Missing SolarEdge environment variables:\n"
            f"{message}\n"
            "Please set all API keys before running ingestion."
        )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run SolarEdge pilot ingestion for all active SOLAREDGE plants sequentially."
    )

    parser.add_argument(
        "--start-local",
        required=True,
        help='Local site start time, format "YYYY-MM-DD HH:MM:SS"',
    )

    parser.add_argument(
        "--end-local",
        required=True,
        help='Local site end time, format "YYYY-MM-DD HH:MM:SS"',
    )

    parser.add_argument(
        "--endpoint",
        choices=["sitePower", "energyDetails", "both"],
        default="both",
    )

    parser.add_argument(
        "--meters",
        default="PRODUCTION,FEEDIN,PURCHASED,SELFCONSUMPTION",
        help="Comma-separated SolarEdge energyDetails meters",
    )

    parser.add_argument(
        "--delay-sec",
        type=int,
        default=5,
        help="Delay between plants to reduce API pressure.",
    )

    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue running next plant even if one plant fails.",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print commands only. Do not call API.",
    )

    return parser.parse_args()


if __name__ == "__main__":
    main()
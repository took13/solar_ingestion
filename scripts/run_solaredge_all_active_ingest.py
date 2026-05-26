from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from src.config_loader import ConfigLoader
from src.db.connection import create_connection

from src.db.repositories.raw_v2_repo import RawV2Repository
from src.db.repositories.source_mapping_repo import SourceMappingRepository
from src.db.repositories.metric_mapping_repo import MetricMappingRepository
from src.db.repositories.canonical_metric_repo import CanonicalMetricRepository
from src.db.repositories.solar_plant_mart_repo import SolarPlantMartRepository
from src.db.repositories.solaredge_checkpoint_repo import SolarEdgeCheckpointRepository

from src.solaredge.client import SolarEdgeClient
from src.solaredge.credential_resolver import SolarEdgeCredentialResolver

from scripts.run_solaredge_pilot_ingest import (
    SOURCE_SYSTEM,
    resolve_auto_window,
    print_auto_window_plan,
    run_site_power,
    run_energy_details,
)


def main() -> int:
    args = parse_args()

    if args.endpoint == "both" and not args.allow_both:
        raise RuntimeError(
            "For controlled rollout, --endpoint both is blocked by default. "
            "Use --endpoint sitePower first, then energyDetails. "
            "If you intentionally want both, add --allow-both."
        )

    app_config = ConfigLoader().load_app_config()
    conn = create_connection(app_config["database"]["connection_string"])

    source_repo = SourceMappingRepository(conn)
    raw_repo = RawV2Repository(conn)
    metric_repo = MetricMappingRepository(conn)
    canonical_repo = CanonicalMetricRepository(conn)
    mart_repo = SolarPlantMartRepository(conn)
    checkpoint_repo = SolarEdgeCheckpointRepository(conn)

    try:
        plant_maps = source_repo.get_active_plant_maps(
            source_system_code=SOURCE_SYSTEM,
        )

        if args.site_id:
            plant_maps = [
                p for p in plant_maps
                if str(p.get("source_plant_code")) == str(args.site_id)
            ]

        if args.plant_code:
            plant_maps = [
                p for p in plant_maps
                if str(p.get("internal_plant_code")) == str(args.plant_code)
            ]

        if args.max_plants is not None:
            plant_maps = plant_maps[: args.max_plants]

        if not plant_maps:
            raise RuntimeError("No active SOLAREDGE plant mapping found for selected filter.")

        endpoints = ["sitePower", "energyDetails"] if args.endpoint == "both" else [args.endpoint]

        print("")
        print("=== SolarEdge All Active Ingestion ===")
        print(f"mode             : {'DRY-RUN' if args.dry_run else 'REAL-RUN'}")
        print(f"source_system    : {SOURCE_SYSTEM}")
        print(f"plant_count      : {len(plant_maps)}")
        print(f"endpoint(s)      : {', '.join(endpoints)}")
        print(f"window_minutes   : {args.window_minutes}")
        print(f"lag_minutes      : {args.lag_minutes}")
        print(f"sleep_seconds    : {args.sleep_seconds}")
        print("")

        total_ready = 0
        total_skipped = 0
        total_success = 0
        total_failed = 0

        for plant_map in plant_maps:
            internal_plant_code = plant_map["internal_plant_code"]
            source_plant_code = plant_map["source_plant_code"]
            timezone_name = plant_map.get("timezone_name") or args.timezone

            print("")
            print("=" * 100)
            print(f"Plant: {internal_plant_code} | site_id={source_plant_code} | timezone={timezone_name}")
            print("=" * 100)

            for endpoint_name in endpoints:
                try:
                    window = resolve_auto_window(
                        checkpoint_repo=checkpoint_repo,
                        source_plant_code=source_plant_code,
                        endpoint_name=endpoint_name,
                        timezone_name=timezone_name,
                        window_minutes=args.window_minutes,
                        lag_minutes=args.lag_minutes,
                        bootstrap_start_local=args.bootstrap_start_local,
                    )

                    print_auto_window_plan(
                        internal_plant_code=internal_plant_code,
                        source_plant_code=source_plant_code,
                        timezone_name=timezone_name,
                        endpoint_name=endpoint_name,
                        window=window,
                    )

                    if window["status"] != "READY":
                        total_skipped += 1
                        print("")
                        print(f"[SKIP] {internal_plant_code}/{endpoint_name}: window_status={window['status']}")
                        continue

                    total_ready += 1

                    if args.dry_run:
                        print("")
                        print(f"[DRY-RUN] {internal_plant_code}/{endpoint_name}: No API call. No DB write.")
                        continue

                    credential_resolver = SolarEdgeCredentialResolver(conn=conn)
                    api_key = credential_resolver.get_api_key(plant_map.get("api_key_secret_name"))
                    client = SolarEdgeClient(api_key=api_key)

                    start_local = window["start_local"]
                    end_local = window["end_local"]
                    start_utc = window["start_utc"]
                    end_utc = window["end_utc"]

                    if endpoint_name == "sitePower":
                        run_site_power(
                            client=client,
                            raw_repo=raw_repo,
                            metric_repo=metric_repo,
                            canonical_repo=canonical_repo,
                            mart_repo=mart_repo,
                            checkpoint_repo=checkpoint_repo,
                            internal_plant_code=internal_plant_code,
                            source_plant_code=source_plant_code,
                            timezone_name=timezone_name,
                            start_local=start_local,
                            end_local=end_local,
                            start_utc=start_utc,
                            end_utc=end_utc,
                        )

                    elif endpoint_name == "energyDetails":
                        run_energy_details(
                            client=client,
                            raw_repo=raw_repo,
                            metric_repo=metric_repo,
                            canonical_repo=canonical_repo,
                            mart_repo=mart_repo,
                            checkpoint_repo=checkpoint_repo,
                            internal_plant_code=internal_plant_code,
                            source_plant_code=source_plant_code,
                            timezone_name=timezone_name,
                            start_local=start_local,
                            end_local=end_local,
                            start_utc=start_utc,
                            end_utc=end_utc,
                            meters=args.meters,
                        )

                    else:
                        raise RuntimeError(f"Unsupported endpoint: {endpoint_name}")

                    total_success += 1

                    if args.sleep_seconds > 0:
                        time.sleep(args.sleep_seconds)

                except Exception as exc:
                    total_failed += 1
                    print("")
                    print(f"[FAILED] {internal_plant_code}/{endpoint_name}: {type(exc).__name__}: {exc}")

                    if args.stop_on_error:
                        raise

        print("")
        print("=== Summary ===")
        print(f"ready_windows : {total_ready}")
        print(f"skipped       : {total_skipped}")
        print(f"success       : {total_success}")
        print(f"failed        : {total_failed}")
        print("")

        if total_failed > 0:
            return 1

        return 0

    finally:
        conn.close()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run SolarEdge ingestion for all active plants using auto-window checkpoint."
    )

    parser.add_argument(
        "--endpoint",
        choices=["sitePower", "energyDetails", "both"],
        default="sitePower",
        help="Controlled rollout default = sitePower.",
    )

    parser.add_argument(
        "--allow-both",
        action="store_true",
        help="Allow --endpoint both. Not recommended for first rollout.",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print window plan only. No API call. No DB write.",
    )

    parser.add_argument(
        "--site-id",
        default=None,
        help="Optional filter for one SolarEdge siteId.",
    )

    parser.add_argument(
        "--plant-code",
        default=None,
        help="Optional filter for one internal plant code.",
    )

    parser.add_argument(
        "--max-plants",
        type=int,
        default=None,
        help="Optional limit for controlled rollout.",
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

    parser.add_argument(
        "--timezone",
        default="Asia/Bangkok",
        help="Fallback timezone if mapping timezone_name is NULL.",
    )

    parser.add_argument(
        "--meters",
        default="PRODUCTION,FEEDIN,PURCHASED,SELFCONSUMPTION",
        help="Comma-separated SolarEdge energyDetails meters.",
    )

    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=2.0,
        help="Sleep between real API calls. Default = 2 seconds.",
    )

    parser.add_argument(
        "--stop-on-error",
        action="store_true",
        help="Stop immediately on first plant/endpoint error.",
    )

    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(main())
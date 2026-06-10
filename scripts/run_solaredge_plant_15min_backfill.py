from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

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
    run_site_power,
    run_energy_details,
)


def main() -> int:
    args = parse_args()
    validate_args(args)

    app_config = ConfigLoader().load_app_config()
    conn = create_connection(app_config["database"]["connection_string"])

    source_repo = SourceMappingRepository(conn)
    raw_repo = RawV2Repository(conn)
    metric_repo = MetricMappingRepository(conn)
    canonical_repo = CanonicalMetricRepository(conn)
    mart_repo = SolarPlantMartRepository(conn)
    checkpoint_repo = SolarEdgeCheckpointRepository(conn)
    credential_resolver = SolarEdgeCredentialResolver(conn=conn)

    try:
        plant_maps = source_repo.get_active_plant_maps(source_system_code=SOURCE_SYSTEM)
        if args.site_id:
            plant_maps = [p for p in plant_maps if str(p.get("source_plant_code")) == str(args.site_id)]
        if args.plant_code:
            plant_maps = [p for p in plant_maps if str(p.get("internal_plant_code")) == str(args.plant_code)]
        if args.max_plants is not None:
            plant_maps = plant_maps[: args.max_plants]
        if not plant_maps:
            raise RuntimeError("No active SOLAREDGE plant mapping found for selected filter.")

        endpoints = ["sitePower", "energyDetails"] if args.endpoint == "both" else [args.endpoint]
        if args.endpoint == "both" and not args.allow_both:
            raise RuntimeError("--endpoint both requires --allow-both for controlled rollout.")

        requested_start_local = parse_local_naive(args.backfill_start_local)
        requested_end_local = parse_local_naive(args.backfill_end_local)
        windows = build_windows(
            start_local=requested_start_local,
            end_local=requested_end_local,
            window_minutes=args.window_minutes,
            max_windows=args.max_windows_per_plant,
        )

        print("")
        print("=== SolarEdge Plant 15-min Controlled Backfill ===")
        print(f"mode                 : {'DRY-RUN' if args.dry_run else 'REAL-RUN'}")
        print(f"source_system        : {SOURCE_SYSTEM}")
        print(f"plant_count          : {len(plant_maps)}")
        print(f"endpoints            : {', '.join(endpoints)}")
        print(f"backfill_start_local : {args.backfill_start_local}")
        print(f"backfill_end_local   : {args.backfill_end_local}")
        print(f"window_minutes       : {args.window_minutes}")
        print(f"max_windows/plant    : {args.max_windows_per_plant}")
        print(f"sleep_seconds        : {args.sleep_seconds}")
        print("")

        total_success = 0
        total_failed = 0
        total_windows = 0

        for plant_map in plant_maps:
            internal_plant_code = plant_map["internal_plant_code"]
            source_plant_code = str(plant_map["source_plant_code"])
            timezone_name = plant_map.get("timezone_name") or args.timezone

            print("=" * 108)
            print(f"Plant: {internal_plant_code} | site_id={source_plant_code} | timezone={timezone_name}")
            print("=" * 108)

            if not windows:
                print("[SKIP] No windows selected.")
                continue

            if args.dry_run:
                for endpoint_name in endpoints:
                    print(f"--- endpoint={endpoint_name} ---")
                    for idx, (start_local, end_local) in enumerate(windows, start=1):
                        start_utc = local_to_utc_naive(start_local, timezone_name)
                        end_utc = local_to_utc_naive(end_local, timezone_name)
                        print(
                            f"[DRY-RUN] window#{idx} local={fmt_dt(start_local)} -> {fmt_dt(end_local)} "
                            f"utc={start_utc} -> {end_utc}"
                        )
                continue

            api_key = credential_resolver.get_api_key(plant_map.get("api_key_secret_name"))
            client = SolarEdgeClient(api_key=api_key)

            for endpoint_name in endpoints:
                print(f"--- endpoint={endpoint_name} ---")
                for idx, (start_local, end_local) in enumerate(windows, start=1):
                    total_windows += 1
                    start_text = fmt_dt(start_local)
                    end_text = fmt_dt(end_local)
                    start_utc = local_to_utc_naive(start_local, timezone_name)
                    end_utc = local_to_utc_naive(end_local, timezone_name)
                    print(
                        f"window#{idx}: local={start_text} -> {end_text} | "
                        f"utc={start_utc} -> {end_utc}"
                    )

                    try:
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
                                start_local=start_text,
                                end_local=end_text,
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
                                start_local=start_text,
                                end_local=end_text,
                                start_utc=start_utc,
                                end_utc=end_utc,
                                meters=args.meters,
                            )
                        else:
                            raise RuntimeError(f"Unsupported endpoint={endpoint_name}")

                        total_success += 1
                    except Exception as exc:
                        total_failed += 1
                        print(f"[FAIL] {internal_plant_code}/{endpoint_name} {start_text}->{end_text}: {exc}")
                        if args.stop_on_error:
                            raise

                    if args.sleep_seconds > 0:
                        time.sleep(args.sleep_seconds)

        print("")
        print("=== Summary ===")
        print(f"selected_windows : {total_windows}")
        print(f"success_windows  : {total_success}")
        print(f"failed_windows   : {total_failed}")
        print("")

        return 0 if total_failed == 0 else 2
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Controlled historical backfill for SolarEdge plant-level 15-min sitePower/energyDetails."
    )
    parser.add_argument("--dry-run", action="store_true", help="Print selected plants/windows only. No API call. No DB write.")
    parser.add_argument("--endpoint", choices=["sitePower", "energyDetails", "both"], default="sitePower")
    parser.add_argument("--allow-both", action="store_true", help="Required when --endpoint both is used.")
    parser.add_argument("--plant-code", help="Optional internal plant filter, e.g. SE_TPRC.")
    parser.add_argument("--site-id", help="Optional SolarEdge siteId filter.")
    parser.add_argument("--max-plants", type=int, help="Optional plant limit for controlled rollout.")
    parser.add_argument("--backfill-start-local", required=True, help='Local start time: "YYYY-MM-DD HH:MM:SS"')
    parser.add_argument("--backfill-end-local", required=True, help='Local end time: "YYYY-MM-DD HH:MM:SS"')
    parser.add_argument("--window-minutes", type=int, default=60, help="Window size in minutes. Default = 60.")
    parser.add_argument("--max-windows-per-plant", type=int, default=1, help="Safety limit per plant per endpoint. Default = 1.")
    parser.add_argument("--timezone", default="Asia/Bangkok", help="Fallback timezone if plant mapping has NULL timezone_name.")
    parser.add_argument("--meters", default="PRODUCTION,FEEDIN,PURCHASED,SELFCONSUMPTION", help="Comma-separated SolarEdge energyDetails meters.")
    parser.add_argument("--sleep-seconds", type=float, default=3.0, help="Sleep between API calls.")
    parser.add_argument("--stop-on-error", action="store_true", help="Stop immediately on first error.")
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    start = parse_local_naive(args.backfill_start_local)
    end = parse_local_naive(args.backfill_end_local)
    if end <= start:
        raise ValueError("--backfill-end-local must be greater than --backfill-start-local.")
    if args.window_minutes < 15:
        raise ValueError("--window-minutes must be >= 15.")
    if args.max_windows_per_plant < 1:
        raise ValueError("--max-windows-per-plant must be >= 1.")


def build_windows(
    *,
    start_local: datetime,
    end_local: datetime,
    window_minutes: int,
    max_windows: int,
) -> list[tuple[datetime, datetime]]:
    windows: list[tuple[datetime, datetime]] = []
    cursor = start_local
    delta = timedelta(minutes=window_minutes)
    while cursor < end_local and len(windows) < max_windows:
        window_end = min(cursor + delta, end_local)
        windows.append((cursor, window_end))
        cursor = window_end
    return windows


def parse_local_naive(local_text: str | datetime) -> datetime:
    if isinstance(local_text, datetime):
        return local_text.replace(tzinfo=None)
    return datetime.strptime(str(local_text), "%Y-%m-%d %H:%M:%S")


def local_to_utc_naive(local_dt: datetime, timezone_name: str) -> datetime:
    tz = ZoneInfo(timezone_name)
    return local_dt.replace(tzinfo=tz).astimezone(timezone.utc).replace(tzinfo=None)


def fmt_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


if __name__ == "__main__":
    raise SystemExit(main())

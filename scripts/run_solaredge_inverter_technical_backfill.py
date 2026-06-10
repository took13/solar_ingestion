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
from src.db.repositories.canonical_metric_repo import CanonicalMetricRepository
from src.db.repositories.metric_mapping_repo import MetricMappingRepository
from src.db.repositories.raw_v2_repo import RawV2Repository
from src.db.repositories.source_mapping_repo import SourceMappingRepository
from src.db.repositories.solaredge_equipment_repo import SolarEdgeEquipmentRepository
from src.db.repositories.solaredge_inverter_backfill_checkpoint_repo import (
    SolarEdgeInverterBackfillCheckpointRepository,
)
from src.db.repositories.solaredge_inverter_mart_repo import SolarEdgeInverterMartRepository
from src.solaredge.client import SolarEdgeClient
from src.solaredge.credential_resolver import SolarEdgeCredentialResolver
from src.solaredge.inverter_technical_normalizer import SolarEdgeInverterTechnicalNormalizer


SOURCE_SYSTEM = "SOLAREDGE"
ENDPOINT_NAME = "inverterTechnicalData"
MAX_SOLAREDGE_WINDOW_DAYS = 7


def main() -> int:
    args = parse_args()
    validate_args(args)

    app_config = ConfigLoader().load_app_config()
    conn = create_connection(app_config["database"]["connection_string"])

    equipment_repo = SolarEdgeEquipmentRepository(conn)
    source_repo = SourceMappingRepository(conn)
    raw_repo = RawV2Repository(conn)
    mapping_repo = MetricMappingRepository(conn)
    canonical_repo = CanonicalMetricRepository(conn)
    mart_repo = SolarEdgeInverterMartRepository(conn)
    checkpoint_repo = SolarEdgeInverterBackfillCheckpointRepository(conn)
    credential_resolver = SolarEdgeCredentialResolver(conn=conn)

    try:
        inverters = equipment_repo.list_active_inverters(
            source_system_code=SOURCE_SYSTEM,
            internal_plant_code=args.plant_code,
            source_plant_code=args.site_id,
        )

        if args.serial_number:
            inverters = [
                inv for inv in inverters
                if str(inv.get("serial_number")).upper() == args.serial_number.upper()
            ]

        if args.max_inverters is not None:
            inverters = inverters[: args.max_inverters]

        if not inverters:
            raise RuntimeError(
                "No active SolarEdge inverter found for selected filter. "
                "Run scripts.run_solaredge_equipment_inventory first."
            )

        plant_maps = {
            str(row["source_plant_code"]): row
            for row in source_repo.get_active_plant_maps(source_system_code=SOURCE_SYSTEM)
        }

        mapping_lookup = mapping_repo.build_mapping_lookup(
            source_system_code=SOURCE_SYSTEM,
            endpoint_name=ENDPOINT_NAME,
        )
        normalizer = SolarEdgeInverterTechnicalNormalizer(mapping_lookup=mapping_lookup)

        print("")
        print("=== SolarEdge Inverter Technical Controlled Backfill ===")
        print(f"mode                  : {'DRY-RUN' if args.dry_run else 'REAL-RUN'}")
        print(f"source_system         : {SOURCE_SYSTEM}")
        print(f"endpoint              : {ENDPOINT_NAME}")
        print(f"inverter_count        : {len(inverters)}")
        print(f"backfill_start_local  : {args.backfill_start_local}")
        print(f"backfill_end_local    : {args.backfill_end_local}")
        print(f"window_days           : {args.window_days}")
        print(f"max_windows/inverter  : {args.max_windows_per_inverter}")
        print(f"resume_checkpoint     : {args.resume_checkpoint}")
        print(f"sleep_seconds         : {args.sleep_seconds}")
        print("")

        total_success = 0
        total_failed = 0
        total_skipped = 0
        total_raw = 0
        total_canonical = 0
        total_mart = 0

        for inverter in inverters:
            internal_plant_code = inverter["internal_plant_code"]
            source_plant_code = str(inverter["source_plant_code"])
            serial_number = inverter["serial_number"]
            inverter_name = inverter.get("inverter_name")
            timezone_name = inverter.get("timezone_name") or args.timezone
            plant_map = plant_maps.get(source_plant_code)

            if not plant_map:
                raise RuntimeError(
                    f"Missing dbo.dim_plant_source_map for SOLAREDGE site_id={source_plant_code}"
                )

            requested_start_local = parse_local_naive(args.backfill_start_local)
            requested_end_local = parse_local_naive(args.backfill_end_local)

            checkpoint = checkpoint_repo.ensure_checkpoint(
                source_system_code=SOURCE_SYSTEM,
                internal_plant_code=internal_plant_code,
                source_plant_code=source_plant_code,
                source_device_id=serial_number,
                source_device_name=inverter_name,
                endpoint_name=ENDPOINT_NAME,
                requested_start_local=requested_start_local,
                requested_end_local=requested_end_local,
            )

            effective_start_local = requested_start_local
            if args.resume_checkpoint and checkpoint.get("last_success_end_local"):
                last_end = checkpoint["last_success_end_local"]
                if isinstance(last_end, str):
                    last_end = parse_local_naive(last_end)
                if last_end > effective_start_local:
                    effective_start_local = last_end

            windows = build_windows(
                start_local=effective_start_local,
                end_local=requested_end_local,
                window_days=args.window_days,
                max_windows=args.max_windows_per_inverter,
            )

            print("-" * 124)
            print(
                f"Plant: {internal_plant_code} | site_id={source_plant_code} | "
                f"inverter={inverter_name} | serial={serial_number} | timezone={timezone_name}"
            )
            print(
                f"requested={requested_start_local} -> {requested_end_local} | "
                f"effective_start={effective_start_local} | windows={len(windows)}"
            )

            if not windows:
                print("[SKIP] No remaining window for this inverter.")
                total_skipped += 1
                continue

            if args.dry_run:
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

            for idx, (start_local, end_local) in enumerate(windows, start=1):
                start_utc = local_to_utc_naive(start_local, timezone_name)
                end_utc = local_to_utc_naive(end_local, timezone_name)
                start_text = fmt_dt(start_local)
                end_text = fmt_dt(end_local)

                print(
                    f"window#{idx}: local={start_text} -> {end_text} | "
                    f"utc={start_utc} -> {end_utc}"
                )

                try:
                    request_started_at_utc = datetime.now(timezone.utc)
                    response = client.get_inverter_technical_data(
                        site_id=source_plant_code,
                        serial_number=serial_number,
                        start_time_local=start_text,
                        end_time_local=end_text,
                    )
                    request_finished_at_utc = datetime.now(timezone.utc)

                    raw_id = raw_repo.insert_api_call_v2(
                        {
                            "source_system_code": SOURCE_SYSTEM,
                            "endpoint_name": ENDPOINT_NAME,
                            "endpoint_path": response.endpoint_path,
                            "internal_plant_code": internal_plant_code,
                            "source_plant_code": source_plant_code,
                            "source_device_id": serial_number,
                            "request_window_start_utc": start_utc,
                            "request_window_end_utc": end_utc,
                            "request_grain_sec": 300,
                            # Never store api_key in DB.
                            "request_json": {
                                "site_id": source_plant_code,
                                "serial_number": serial_number,
                                "startTime": start_text,
                                "endTime": end_text,
                                "backfill_mode": "controlled",
                                "bucket_rule": "floor_to_5min_local_then_convert_utc",
                            },
                            "response_json": response.response_json,
                            "http_status": response.http_status,
                            "api_success_flag": response.http_status == 200,
                            "fail_code": None,
                            "fail_message": None,
                            "request_started_at_utc": request_started_at_utc,
                            "request_finished_at_utc": request_finished_at_utc,
                        }
                    )

                    canonical_rows = normalizer.normalize(
                        raw_id=raw_id,
                        response_json=response.response_json,
                        internal_plant_code=internal_plant_code,
                        source_plant_code=source_plant_code,
                        source_device_id=serial_number,
                        source_device_name=inverter_name,
                        timezone_name=timezone_name,
                    )
                    canonical_count = canonical_repo.upsert_many(canonical_rows)

                    mart_count = mart_repo.load_technical_5min(
                        source_system_code=SOURCE_SYSTEM,
                        internal_plant_code=internal_plant_code,
                        source_plant_code=source_plant_code,
                        source_device_id=serial_number,
                        start_utc=start_utc,
                        end_utc=end_utc,
                    )

                    checkpoint_repo.mark_success(
                        source_system_code=SOURCE_SYSTEM,
                        internal_plant_code=internal_plant_code,
                        source_plant_code=source_plant_code,
                        source_device_id=serial_number,
                        endpoint_name=ENDPOINT_NAME,
                        start_local=start_local,
                        end_local=end_local,
                        start_utc=start_utc,
                        end_utc=end_utc,
                        raw_id=raw_id,
                    )

                    total_success += 1
                    total_raw += 1
                    total_canonical += canonical_count
                    total_mart += mart_count

                    print(
                        f"[OK] raw_id={raw_id} http_status={response.http_status} "
                        f"elapsed_sec={response.elapsed_sec:.2f} "
                        f"canonical_rows={canonical_count} mart_rows={mart_count} "
                        f"telemetries={telemetry_count(response.response_json)}"
                    )

                except Exception as exc:
                    total_failed += 1
                    checkpoint_repo.mark_failure(
                        source_system_code=SOURCE_SYSTEM,
                        internal_plant_code=internal_plant_code,
                        source_plant_code=source_plant_code,
                        source_device_id=serial_number,
                        endpoint_name=ENDPOINT_NAME,
                        error_message=str(exc),
                    )
                    print(f"[FAIL] {internal_plant_code}/{serial_number} window={start_text}->{end_text}: {exc}")
                    if args.stop_on_error:
                        raise

                if args.sleep_seconds > 0:
                    time.sleep(args.sleep_seconds)

        print("")
        print("=== Summary ===")
        print(f"success_windows : {total_success}")
        print(f"failed_windows  : {total_failed}")
        print(f"skipped_inverters: {total_skipped}")
        print(f"raw_calls       : {total_raw}")
        print(f"canonical_rows  : {total_canonical}")
        print(f"mart_rows       : {total_mart}")

        return 0 if total_failed == 0 else 2

    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Controlled backfill for SolarEdge inverter technical data."
    )
    parser.add_argument("--dry-run", action="store_true", help="Print selected inverters/windows only. No API call. No DB write.")
    parser.add_argument("--plant-code", help="Optional internal plant filter, e.g. SE_TPRC.")
    parser.add_argument("--site-id", help="Optional SolarEdge siteId filter.")
    parser.add_argument("--serial-number", help="Optional inverter serial number filter.")
    parser.add_argument("--max-inverters", type=int, default=1, help="Limit selected inverters. Default = 1 for safe controlled rollout.")
    parser.add_argument("--backfill-start-local", required=True, help='Local start time: "YYYY-MM-DD HH:MM:SS"')
    parser.add_argument("--backfill-end-local", required=True, help='Local end time: "YYYY-MM-DD HH:MM:SS"')
    parser.add_argument("--window-days", type=int, default=1, help="Window size in days. Must be 1-7. Default = 1.")
    parser.add_argument("--max-windows-per-inverter", type=int, default=1, help="Safety limit per run. Default = 1.")
    parser.add_argument("--resume-checkpoint", action="store_true", help="Continue from last_success_end_local when available.")
    parser.add_argument("--timezone", default="Asia/Bangkok", help="Fallback timezone if inventory mapping has NULL timezone_name.")
    parser.add_argument("--sleep-seconds", type=float, default=3.0, help="Sleep between API calls.")
    parser.add_argument("--stop-on-error", action="store_true", help="Stop immediately on first error.")
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if args.window_days < 1 or args.window_days > MAX_SOLAREDGE_WINDOW_DAYS:
        raise ValueError("--window-days must be between 1 and 7 for SolarEdge inverter technical API.")
    if args.max_windows_per_inverter < 1:
        raise ValueError("--max-windows-per-inverter must be >= 1.")

    start = parse_local_naive(args.backfill_start_local)
    end = parse_local_naive(args.backfill_end_local)
    if end <= start:
        raise ValueError("--backfill-end-local must be greater than --backfill-start-local.")


def build_windows(
    *,
    start_local: datetime,
    end_local: datetime,
    window_days: int,
    max_windows: int,
) -> list[tuple[datetime, datetime]]:
    windows: list[tuple[datetime, datetime]] = []
    cursor = start_local
    delta = timedelta(days=window_days)

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


def telemetry_count(payload: dict) -> int:
    data = payload.get("data") or {}
    telemetries = data.get("telemetries") or []
    return len(telemetries)


if __name__ == "__main__":
    raise SystemExit(main())

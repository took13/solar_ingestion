from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
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
from src.db.repositories.solaredge_inverter_mart_repo import SolarEdgeInverterMartRepository
from src.solaredge.client import SolarEdgeClient
from src.solaredge.credential_resolver import SolarEdgeCredentialResolver
from src.solaredge.inverter_technical_normalizer import SolarEdgeInverterTechnicalNormalizer


SOURCE_SYSTEM = "SOLAREDGE"
ENDPOINT_NAME = "inverterTechnicalData"


def main() -> int:
    args = parse_args()

    app_config = ConfigLoader().load_app_config()
    conn = create_connection(app_config["database"]["connection_string"])

    equipment_repo = SolarEdgeEquipmentRepository(conn)
    source_repo = SourceMappingRepository(conn)
    raw_repo = RawV2Repository(conn)
    mapping_repo = MetricMappingRepository(conn)
    canonical_repo = CanonicalMetricRepository(conn)
    mart_repo = SolarEdgeInverterMartRepository(conn)
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
        print("=== SolarEdge Inverter Technical Data Ingest ===")
        print(f"mode            : {'DRY-RUN' if args.dry_run else 'REAL-RUN'}")
        print(f"source_system   : {SOURCE_SYSTEM}")
        print(f"endpoint        : {ENDPOINT_NAME}")
        print(f"inverter_count  : {len(inverters)}")
        print(f"start_local     : {args.start_local}")
        print(f"end_local       : {args.end_local}")
        print(f"sleep_seconds   : {args.sleep_seconds}")
        print("")

        total_success = 0
        total_failed = 0
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

            start_utc = parse_local_to_utc_naive(args.start_local, timezone_name)
            end_utc = parse_local_to_utc_naive(args.end_local, timezone_name)

            print("-" * 118)
            print(
                f"Plant: {internal_plant_code} | site_id={source_plant_code} | "
                f"inverter={inverter_name} | serial={serial_number} | timezone={timezone_name}"
            )
            print(f"UTC window: {start_utc} -> {end_utc}")

            if args.dry_run:
                continue

            try:
                api_key = credential_resolver.get_api_key(plant_map.get("api_key_secret_name"))
                client = SolarEdgeClient(api_key=api_key)

                request_started_at_utc = datetime.now(timezone.utc)
                response = client.get_inverter_technical_data(
                    site_id=source_plant_code,
                    serial_number=serial_number,
                    start_time_local=args.start_local,
                    end_time_local=args.end_local,
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
                            "startTime": args.start_local,
                            "endTime": args.end_local,
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

                total_raw += 1
                total_success += 1
                total_canonical += canonical_count
                total_mart += mart_count

                print(
                    f"[OK] raw_id={raw_id} http_status={response.http_status} "
                    f"elapsed_sec={response.elapsed_sec:.2f} "
                    f"canonical_rows={canonical_count} mart_rows={mart_count} "
                    f"response_summary={summarize_payload(response.response_json)}"
                )

                if args.print_sample_json:
                    print("--- response_json sample ---")
                    print(json.dumps(response.response_json, ensure_ascii=False, indent=2)[: args.sample_chars])

            except Exception as exc:
                total_failed += 1
                print(f"[FAIL] {internal_plant_code}/{serial_number}: {exc}")
                if args.stop_on_error:
                    raise

            if args.sleep_seconds > 0:
                time.sleep(args.sleep_seconds)

        print("")
        print("=== Summary ===")
        print(f"success_inverters : {total_success}")
        print(f"failed_inverters  : {total_failed}")
        print(f"raw_calls         : {total_raw}")
        print(f"canonical_rows    : {total_canonical}")
        print(f"mart_rows         : {total_mart}")

        return 0 if total_failed == 0 else 2

    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest SolarEdge inverter technical data into raw/canonical/mart for selected inverter(s)."
    )
    parser.add_argument("--dry-run", action="store_true", help="Print selected inverters/window only. No API call. No DB write.")
    parser.add_argument("--plant-code", help="Optional internal plant filter, e.g. SE_TPRC.")
    parser.add_argument("--site-id", help="Optional SolarEdge siteId filter.")
    parser.add_argument("--serial-number", help="Optional inverter serial number filter.")
    parser.add_argument("--max-inverters", type=int, default=1, help="Limit selected inverters. Default = 1 for safe controlled test.")
    parser.add_argument("--start-local", required=True, help='Local site start time: "YYYY-MM-DD HH:MM:SS"')
    parser.add_argument("--end-local", required=True, help='Local site end time: "YYYY-MM-DD HH:MM:SS"')
    parser.add_argument("--timezone", default="Asia/Bangkok", help="Fallback timezone if inventory mapping has NULL timezone_name.")
    parser.add_argument("--sleep-seconds", type=float, default=3.0, help="Sleep between inverter API calls.")
    parser.add_argument("--stop-on-error", action="store_true", help="Stop immediately on first inverter error.")
    parser.add_argument("--print-sample-json", action="store_true", help="Print truncated response_json sample for probe/debug only.")
    parser.add_argument("--sample-chars", type=int, default=5000, help="Max chars printed when --print-sample-json is used.")
    return parser.parse_args()


def parse_local_to_utc_naive(local_text: str, timezone_name: str) -> datetime:
    tz = ZoneInfo(timezone_name)
    local_dt = datetime.strptime(local_text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=tz)
    return local_dt.astimezone(timezone.utc).replace(tzinfo=None)


def summarize_payload(payload: dict) -> str:
    data = payload.get("data") or {}
    telemetries = data.get("telemetries") or []
    return f"telemetries={len(telemetries)} count={data.get('count')}"


if __name__ == "__main__":
    raise SystemExit(main())

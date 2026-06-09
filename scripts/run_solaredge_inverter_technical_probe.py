from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config_loader import ConfigLoader
from src.db.connection import create_connection
from src.db.repositories.raw_v2_repo import RawV2Repository
from src.db.repositories.source_mapping_repo import SourceMappingRepository
from src.db.repositories.solaredge_equipment_repo import SolarEdgeEquipmentRepository
from src.solaredge.client import SolarEdgeClient
from src.solaredge.credential_resolver import SolarEdgeCredentialResolver


SOURCE_SYSTEM = "SOLAREDGE"
ENDPOINT_NAME = "inverterTechnicalData"


def main() -> int:
    args = parse_args()

    app_config = ConfigLoader().load_app_config()
    conn = create_connection(app_config["database"]["connection_string"])

    equipment_repo = SolarEdgeEquipmentRepository(conn)
    source_repo = SourceMappingRepository(conn)
    raw_repo = RawV2Repository(conn)
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

        print("")
        print("=== SolarEdge Inverter Technical Data Probe ===")
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

            print("-" * 110)
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
                        "request_grain_sec": None,
                        # Never store api_key in DB.
                        "request_json": {
                            "site_id": source_plant_code,
                            "serial_number": serial_number,
                            "startTime": args.start_local,
                            "endTime": args.end_local,
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

                summary = summarize_payload(response.response_json)
                total_raw += 1
                total_success += 1

                print(
                    f"[OK] raw_id={raw_id} http_status={response.http_status} "
                    f"elapsed_sec={response.elapsed_sec:.2f} response_summary={summary}"
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

        return 0 if total_failed == 0 else 2

    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Probe SolarEdge inverter technical data for selected active inverter(s)."
    )
    parser.add_argument("--dry-run", action="store_true", help="Print selected inverters/window only. No API call. No DB write.")
    parser.add_argument("--plant-code", help="Optional internal plant filter, e.g. SE_TPRC.")
    parser.add_argument("--site-id", help="Optional SolarEdge siteId filter.")
    parser.add_argument("--serial-number", help="Optional inverter serial number filter.")
    parser.add_argument("--max-inverters", type=int, default=1, help="Limit selected inverters. Default = 1 for safe controlled test.")
    parser.add_argument("--start-local", required=True, help='Local site start time: "YYYY-MM-DD HH:MM:SS"')
    parser.add_argument("--end-local", required=True, help='Local site end time: "YYYY-MM-DD HH:MM:SS"')
    parser.add_argument("--timezone", default="Asia/Bangkok", help="Fallback timezone if inventory mapping has NULL timezone_name.")
    parser.add_argument("--sleep-seconds", type=float, default=3.0, help="Sleep between API calls. Default = 3 seconds.")
    parser.add_argument("--stop-on-error", action="store_true", help="Stop on first inverter error.")
    parser.add_argument("--print-sample-json", action="store_true", help="Print truncated response JSON to console for shape review. Do not use for huge ranges.")
    parser.add_argument("--sample-chars", type=int, default=6000, help="Max sample JSON characters to print.")
    return parser.parse_args()


def parse_local_to_utc_naive(value: str, timezone_name: str) -> datetime:
    local_dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=ZoneInfo(timezone_name)
    )
    return local_dt.astimezone(timezone.utc).replace(tzinfo=None)


def summarize_payload(payload: dict[str, Any]) -> str:
    if not isinstance(payload, dict):
        return f"payload_type={type(payload).__name__}"

    top_keys = list(payload.keys())
    list_paths = find_list_paths(payload)
    scalar_paths = find_scalar_paths(payload, max_paths=12)

    parts = [f"top_keys={top_keys}"]
    if list_paths:
        parts.append("lists=" + ",".join(f"{path}:{count}" for path, count in list_paths[:8]))
    if scalar_paths:
        parts.append("scalars=" + ",".join(scalar_paths[:8]))
    return " | ".join(parts)


def find_list_paths(value: Any, path: str = "$", max_depth: int = 6) -> list[tuple[str, int]]:
    if max_depth < 0:
        return []
    paths: list[tuple[str, int]] = []
    if isinstance(value, list):
        paths.append((path, len(value)))
        for idx, item in enumerate(value[:3]):
            paths.extend(find_list_paths(item, f"{path}[{idx}]", max_depth - 1))
    elif isinstance(value, dict):
        for key, item in value.items():
            paths.extend(find_list_paths(item, f"{path}.{key}", max_depth - 1))
    return paths


def find_scalar_paths(value: Any, path: str = "$", max_paths: int = 20) -> list[str]:
    paths: list[str] = []

    def walk(node: Any, current_path: str, depth: int) -> None:
        if len(paths) >= max_paths or depth > 5:
            return
        if isinstance(node, dict):
            for key, item in node.items():
                walk(item, f"{current_path}.{key}", depth + 1)
        elif isinstance(node, list):
            if node:
                walk(node[0], f"{current_path}[0]", depth + 1)
        else:
            paths.append(current_path)

    walk(value, path, 0)
    return paths


if __name__ == "__main__":
    raise SystemExit(main())

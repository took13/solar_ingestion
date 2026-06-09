from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

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
ENDPOINT_NAME = "inventory"


def main() -> int:
    args = parse_args()

    app_config = ConfigLoader().load_app_config()
    conn = create_connection(app_config["database"]["connection_string"])

    source_repo = SourceMappingRepository(conn)
    raw_repo = RawV2Repository(conn)
    equipment_repo = SolarEdgeEquipmentRepository(conn)
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

        print("")
        print("=== SolarEdge Equipment Inventory ===")
        print(f"mode          : {'DRY-RUN' if args.dry_run else 'REAL-RUN'}")
        print(f"source_system : {SOURCE_SYSTEM}")
        print(f"plant_count   : {len(plant_maps)}")
        print(f"sleep_seconds : {args.sleep_seconds}")
        print("")

        total_success = 0
        total_failed = 0
        total_inverters = 0

        for plant_map in plant_maps:
            internal_plant_code = plant_map["internal_plant_code"]
            source_plant_code = plant_map["source_plant_code"]
            source_plant_name = plant_map.get("source_plant_name")
            secret_name = plant_map.get("api_key_secret_name")

            print("-" * 100)
            print(
                f"Plant: {internal_plant_code} | site_id={source_plant_code} | "
                f"name={source_plant_name} | credential={'OK' if secret_name else 'MISSING_SECRET_NAME'}"
            )

            if args.dry_run:
                continue

            try:
                api_key = credential_resolver.get_api_key(secret_name)
                client = SolarEdgeClient(api_key=api_key)

                request_started_at_utc = datetime.now(timezone.utc)
                response = client.get_site_inventory(site_id=str(source_plant_code))
                request_finished_at_utc = datetime.now(timezone.utc)

                raw_id = raw_repo.insert_api_call_v2(
                    {
                        "source_system_code": SOURCE_SYSTEM,
                        "endpoint_name": ENDPOINT_NAME,
                        "endpoint_path": response.endpoint_path,
                        "internal_plant_code": internal_plant_code,
                        "source_plant_code": source_plant_code,
                        "source_device_id": None,
                        "request_window_start_utc": request_started_at_utc,
                        "request_window_end_utc": request_finished_at_utc,
                        "request_grain_sec": None,
                        "request_json": {"site_id": source_plant_code},
                        "response_json": response.response_json,
                        "http_status": response.http_status,
                        "api_success_flag": response.http_status == 200,
                        "fail_code": None,
                        "fail_message": None,
                        "request_started_at_utc": request_started_at_utc,
                        "request_finished_at_utc": request_finished_at_utc,
                    }
                )

                equipment_rows = build_equipment_rows(
                    response_json=response.response_json,
                    raw_id=raw_id,
                    internal_plant_code=internal_plant_code,
                    source_plant_code=source_plant_code,
                )

                affected = equipment_repo.upsert_many(equipment_rows)
                inverter_count = len(equipment_rows)
                total_inverters += inverter_count
                total_success += 1

                print(f"[OK] raw_id={raw_id} inventory_equipment_rows={inverter_count} upserted={affected}")

                if args.sleep_seconds > 0:
                    time.sleep(args.sleep_seconds)

            except Exception as exc:
                total_failed += 1
                print(f"[FAILED] {internal_plant_code}: {type(exc).__name__}: {exc}")
                if args.stop_on_error:
                    raise

        print("")
        print("=== Summary ===")
        print(f"success_plants   : {total_success}")
        print(f"failed_plants    : {total_failed}")
        print(f"equipment_rows   : {total_inverters}")
        print("")

        return 1 if total_failed else 0

    finally:
        conn.close()


def build_equipment_rows(
    *,
    response_json: dict[str, Any],
    raw_id: int,
    internal_plant_code: str,
    source_plant_code: str,
) -> list[dict[str, Any]]:
    root = (
        response_json.get("Inventory")
        or response_json.get("inventory")
        or response_json.get("siteInventory")
        or response_json
    )

    rows: list[dict[str, Any]] = []

    # SolarEdge inventory response may use SN or serialNumber depending on API/version.
    equipment_groups = [
        ("INVERTER", root.get("inverters") or root.get("Inverters") or []),
        ("THIRD_PARTY_INVERTER", root.get("thirdPartyInverters") or []),
        ("SMI", root.get("smiList") or root.get("SMIList") or []),
    ]

    for equipment_type, items in equipment_groups:
        for item in items:
            serial_number = (
                item.get("SN")
                or item.get("sn")
                or item.get("serialNumber")
                or item.get("serial_number")
            )

            if not serial_number:
                continue

            rows.append(
                {
                    "source_system_code": SOURCE_SYSTEM,
                    "internal_plant_code": internal_plant_code,
                    "source_plant_code": str(source_plant_code),
                    "equipment_type": equipment_type,
                    "source_device_id": str(serial_number),
                    "source_device_name": item.get("name"),
                    "manufacturer": item.get("manufacturer"),
                    "model": item.get("model"),
                    "firmware_version": (
                        item.get("firmwareVersion")
                        or item.get("cpuVersion")
                        or item.get("CPUFirmwareVersion")
                    ),
                    "communication_method": item.get("communicationMethod"),
                    "connected_optimizers": safe_int(item.get("connectedOptimizers")),
                    "last_raw_id": raw_id,
                    "raw_payload": item,
                }
            )

    return rows


def safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def parse_args():
    parser = argparse.ArgumentParser(
        description="Fetch SolarEdge site inventory and upsert inverter serial numbers."
    )

    parser.add_argument("--site-id", default=None, help="Optional filter for one SolarEdge siteId.")
    parser.add_argument("--plant-code", default=None, help="Optional filter for one internal plant code.")
    parser.add_argument("--max-plants", type=int, default=None, help="Optional limit for controlled rollout.")
    parser.add_argument("--dry-run", action="store_true", help="Print selected plants only. No API call. No DB write.")
    parser.add_argument("--sleep-seconds", type=float, default=3.0, help="Sleep between API calls. Default = 3 seconds.")
    parser.add_argument("--stop-on-error", action="store_true", help="Stop immediately on first plant error.")

    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(main())

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
SENSOR_LIST_ENDPOINT = "sensorList"
SENSOR_DATA_ENDPOINT = "sensorData"
INVENTORY_FALLBACK_ENDPOINT = "siteInventoryForSensorDiscovery"


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

        if args.probe_data and (not args.start_local or not args.end_local):
            raise RuntimeError("--probe-data requires --start-local and --end-local.")

        print("")
        print("=== SolarEdge Sensor / Irradiance Discovery Probe ===")
        print(f"mode            : {'DRY-RUN' if args.dry_run else 'REAL-RUN'}")
        print(f"source_system   : {SOURCE_SYSTEM}")
        print(f"plant_count     : {len(plant_maps)}")
        print(f"probe_data      : {args.probe_data}")
        print(f"start_local     : {args.start_local}")
        print(f"end_local       : {args.end_local}")
        print(f"sleep_seconds   : {args.sleep_seconds}")
        print("")

        total_success = 0
        total_failed = 0
        total_sensor_rows = 0
        total_irradiance_sensors = 0
        total_sensor_data_calls = 0
        total_sensor_data_telemetries = 0

        for plant_map in plant_maps:
            internal_plant_code = plant_map["internal_plant_code"]
            source_plant_code = str(plant_map["source_plant_code"])
            source_plant_name = plant_map.get("source_plant_name")
            timezone_name = plant_map.get("timezone_name") or args.timezone
            secret_name = plant_map.get("api_key_secret_name")

            print("-" * 110)
            print(
                f"Plant: {internal_plant_code} | site_id={source_plant_code} | "
                f"name={source_plant_name} | timezone={timezone_name} | "
                f"credential={'OK' if secret_name else 'MISSING_SECRET_NAME'}"
            )

            if args.dry_run:
                print("[DRY-RUN] would call sensorList. No API call / no DB write.")
                if args.probe_data:
                    print("[DRY-RUN] would call sensorData after sensorList if sensors exist.")
                continue

            try:
                api_key = credential_resolver.get_api_key(secret_name)
                client = SolarEdgeClient(api_key=api_key)

                # Use the client's internal safe GET helper to avoid changing src/solaredge/client.py in this milestone.
                list_started_utc = datetime.now(timezone.utc)
                list_source = SENSOR_LIST_ENDPOINT
                try:
                    sensor_list_response = client._get_json(  # noqa: SLF001 - controlled project probe
                        endpoint_name=SENSOR_LIST_ENDPOINT,
                        endpoint_path=f"/equipment/{source_plant_code}/sensors",
                        params={"api_key": client.api_key},
                    )
                except Exception as list_exc:
                    if args.inventory_fallback and is_access_denied_error(list_exc):
                        print(
                            "[WARN] sensorList returned HTTP 403 / Access Denied. "
                            "Falling back to site inventory sensors for discovery."
                        )
                        list_source = INVENTORY_FALLBACK_ENDPOINT
                        sensor_list_response = client._get_json(  # noqa: SLF001 - controlled project probe
                            endpoint_name=INVENTORY_FALLBACK_ENDPOINT,
                            endpoint_path=f"/site/{source_plant_code}/inventory",
                            params={"api_key": client.api_key},
                        )
                    else:
                        raise
                list_finished_utc = datetime.now(timezone.utc)

                list_raw_id = raw_repo.insert_api_call_v2(
                    {
                        "source_system_code": SOURCE_SYSTEM,
                        "endpoint_name": list_source,
                        "endpoint_path": sensor_list_response.endpoint_path,
                        "internal_plant_code": internal_plant_code,
                        "source_plant_code": source_plant_code,
                        "source_device_id": None,
                        "request_window_start_utc": list_started_utc,
                        "request_window_end_utc": list_finished_utc,
                        "request_grain_sec": 0,
                        "request_json": {"site_id": source_plant_code},
                        "response_json": sensor_list_response.response_json,
                        "http_status": sensor_list_response.http_status,
                        "api_success_flag": sensor_list_response.http_status == 200,
                        "fail_code": None,
                        "fail_message": None,
                        "request_started_at_utc": list_started_utc,
                        "request_finished_at_utc": list_finished_utc,
                    }
                )

                sensor_rows = build_sensor_equipment_rows(
                    response_json=sensor_list_response.response_json,
                    raw_id=list_raw_id,
                    internal_plant_code=internal_plant_code,
                    source_plant_code=source_plant_code,
                )
                affected = equipment_repo.upsert_many(sensor_rows)
                sensor_summary = summarize_sensor_rows(sensor_rows)
                total_sensor_rows += len(sensor_rows)
                total_irradiance_sensors += sensor_summary["irradiance_count"]
                total_success += 1

                print(
                    f"[OK] {list_source} raw_id={list_raw_id} "
                    f"sensors={len(sensor_rows)} irradiance={sensor_summary['irradiance_count']} "
                    f"temperature={sensor_summary['temperature_count']} wind={sensor_summary['wind_count']} "
                    f"upserted={affected} elapsed_sec={sensor_list_response.elapsed_sec:.2f}"
                )

                if args.print_sample_json:
                    print("--- sensorList response_json sample ---")
                    print(to_sample_json(sensor_list_response.response_json, args.sample_chars))

                if args.probe_data:
                    if not sensor_rows and not args.force_probe_data:
                        print("[SKIP] sensorData: no sensors found from sensorList. Use --force-probe-data to call anyway.")
                    else:
                        start_utc = local_time_to_utc(args.start_local, timezone_name)
                        end_utc = local_time_to_utc(args.end_local, timezone_name)

                        data_started_utc = datetime.now(timezone.utc)
                        try:
                            sensor_data_response = client._get_json(  # noqa: SLF001 - controlled project probe
                                endpoint_name=SENSOR_DATA_ENDPOINT,
                                endpoint_path=f"/site/{source_plant_code}/sensors",
                                params={
                                    "startDate": args.start_local,
                                    "endDate": args.end_local,
                                    "api_key": client.api_key,
                                },
                            )
                        except Exception as data_exc:
                            if is_access_denied_error(data_exc):
                                print(
                                    "[WARN] sensorData returned HTTP 403 / Access Denied. "
                                    "Sensor inventory discovery may still be valid, but telemetry is not accessible with this key."
                                )
                                sensor_data_response = None
                            else:
                                raise

                        if sensor_data_response is not None:
                            data_finished_utc = datetime.now(timezone.utc)

                            data_raw_id = raw_repo.insert_api_call_v2(
                                {
                                    "source_system_code": SOURCE_SYSTEM,
                                    "endpoint_name": SENSOR_DATA_ENDPOINT,
                                    "endpoint_path": sensor_data_response.endpoint_path,
                                    "internal_plant_code": internal_plant_code,
                                    "source_plant_code": source_plant_code,
                                    "source_device_id": None,
                                    "request_window_start_utc": start_utc,
                                    "request_window_end_utc": end_utc,
                                    "request_grain_sec": 0,
                                    "request_json": {
                                        "site_id": source_plant_code,
                                        "startDate": args.start_local,
                                        "endDate": args.end_local,
                                    },
                                    "response_json": sensor_data_response.response_json,
                                    "http_status": sensor_data_response.http_status,
                                    "api_success_flag": sensor_data_response.http_status == 200,
                                    "fail_code": None,
                                    "fail_message": None,
                                    "request_started_at_utc": data_started_utc,
                                    "request_finished_at_utc": data_finished_utc,
                                }
                            )

                            data_summary = summarize_sensor_data(sensor_data_response.response_json)
                            total_sensor_data_calls += 1
                            total_sensor_data_telemetries += data_summary["telemetry_count"]

                            print(
                                f"[OK] sensorData raw_id={data_raw_id} "
                                f"groups={data_summary['group_count']} telemetries={data_summary['telemetry_count']} "
                                f"measurement_keys={data_summary['measurement_keys']} "
                                f"irradiance_keys={data_summary['irradiance_keys']} "
                                f"elapsed_sec={sensor_data_response.elapsed_sec:.2f}"
                            )

                            if args.print_sample_json:
                                print("--- sensorData response_json sample ---")
                                print(to_sample_json(sensor_data_response.response_json, args.sample_chars))

                if args.sleep_seconds > 0:
                    time.sleep(args.sleep_seconds)

            except Exception as exc:
                total_failed += 1
                print(f"[FAILED] {internal_plant_code}: {type(exc).__name__}: {exc}")
                if args.stop_on_error:
                    raise

        print("")
        print("=== Summary ===")
        print(f"success_plants          : {total_success}")
        print(f"failed_plants           : {total_failed}")
        print(f"sensor_rows             : {total_sensor_rows}")
        print(f"irradiance_sensors      : {total_irradiance_sensors}")
        print(f"sensor_data_calls       : {total_sensor_data_calls}")
        print(f"sensor_data_telemetries : {total_sensor_data_telemetries}")
        print("")

        return 1 if total_failed else 0

    finally:
        conn.close()


def build_sensor_equipment_rows(
    *,
    response_json: dict[str, Any],
    raw_id: int,
    internal_plant_code: str,
    source_plant_code: str,
) -> list[dict[str, Any]]:
    root = (
        response_json.get("SiteSensors")
        or response_json.get("siteSensors")
        or response_json.get("sensors")
        or response_json
    )

    rows: list[dict[str, Any]] = []

    inventory = response_json.get("Inventory") or response_json.get("inventory")
    if isinstance(inventory, dict) and isinstance(inventory.get("sensors"), list):
        for sensor in inventory.get("sensors") or []:
            row = build_single_sensor_equipment_row(
                sensor=sensor,
                connected_to=sensor.get("connectedTo") or sensor.get("connected_to"),
                raw_id=raw_id,
                internal_plant_code=internal_plant_code,
                source_plant_code=source_plant_code,
            )
            if row is not None:
                rows.append(row)
        return rows

    groups = root.get("list") if isinstance(root, dict) else []
    if groups is None:
        groups = []

    for group in groups:
        connected_to = group.get("connectedTo") or group.get("connected_to") or group.get("gateway")
        sensors = group.get("sensors") or []

        for sensor in sensors:
            row = build_single_sensor_equipment_row(
                sensor=sensor,
                connected_to=connected_to,
                raw_id=raw_id,
                internal_plant_code=internal_plant_code,
                source_plant_code=source_plant_code,
            )
            if row is not None:
                rows.append(row)

    return rows


def build_single_sensor_equipment_row(
    *,
    sensor: dict[str, Any],
    connected_to: str | None,
    raw_id: int,
    internal_plant_code: str,
    source_plant_code: str,
) -> dict[str, Any] | None:
    measurement = (
        sensor.get("measurement")
        or sensor.get("id")
        or sensor.get("name")
        or sensor.get("type")
    )
    if not measurement:
        return None

    sensor_type = sensor.get("type") or sensor.get("category")
    equipment_type = classify_sensor_equipment_type(
        measurement=str(measurement),
        sensor_type=str(sensor_type or ""),
    )

    # sensorList / inventory may not provide a serial number. Use connectedTo + measurement as a stable site-local key.
    source_device_id = f"{connected_to}|{measurement}" if connected_to else str(measurement)

    raw_payload = {
        "connectedTo": connected_to,
        "sensor": sensor,
    }

    return {
        "source_system_code": SOURCE_SYSTEM,
        "internal_plant_code": internal_plant_code,
        "source_plant_code": str(source_plant_code),
        "equipment_type": equipment_type,
        "source_device_id": source_device_id,
        "source_device_name": sensor.get("name") or measurement,
        "manufacturer": None,
        "model": None,
        "firmware_version": None,
        "communication_method": None,
        "connected_optimizers": None,
        "last_raw_id": raw_id,
        "raw_payload": raw_payload,
    }


def is_access_denied_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return "http 403" in text and ("access denied" in text or "forbidden" in text)


def classify_sensor_equipment_type(*, measurement: str, sensor_type: str) -> str:
    combined = f"{measurement} {sensor_type}".upper()

    if "IRRADIANCE" in combined or "IRREDIANCE" in combined or "IRRADIATION" in combined:
        return "SENSOR_IRRADIANCE"

    if "TEMPERATURE" in combined or "TEMP" in combined:
        return "SENSOR_TEMPERATURE"

    if "WIND" in combined:
        return "SENSOR_WIND"

    return "SENSOR_OTHER"


def summarize_sensor_rows(rows: list[dict[str, Any]]) -> dict[str, int]:
    return {
        "irradiance_count": sum(1 for r in rows if r.get("equipment_type") == "SENSOR_IRRADIANCE"),
        "temperature_count": sum(1 for r in rows if r.get("equipment_type") == "SENSOR_TEMPERATURE"),
        "wind_count": sum(1 for r in rows if r.get("equipment_type") == "SENSOR_WIND"),
        "other_count": sum(1 for r in rows if r.get("equipment_type") == "SENSOR_OTHER"),
    }


def summarize_sensor_data(response_json: dict[str, Any]) -> dict[str, Any]:
    root = (
        response_json.get("siteSensors")
        or response_json.get("SiteSensors")
        or response_json
    )

    data_groups = root.get("data") if isinstance(root, dict) else []
    if data_groups is None:
        data_groups = []

    measurement_keys: set[str] = set()
    irradiance_keys: set[str] = set()
    telemetry_count = 0

    for group in data_groups:
        telemetries = group.get("telemetries") or []
        telemetry_count += len(telemetries)

        for telemetry in telemetries:
            if not isinstance(telemetry, dict):
                continue

            for key, value in telemetry.items():
                if key in {"date", "time", "timestamp"}:
                    continue
                if value is None:
                    continue
                measurement_keys.add(str(key))
                if "irradiance" in str(key).lower() or "irradiation" in str(key).lower():
                    irradiance_keys.add(str(key))

    return {
        "group_count": len(data_groups),
        "telemetry_count": telemetry_count,
        "measurement_keys": sorted(measurement_keys),
        "irradiance_keys": sorted(irradiance_keys),
    }


def local_time_to_utc(local_text: str, timezone_name: str) -> datetime:
    local_tz = ZoneInfo(timezone_name)
    local_dt = datetime.strptime(local_text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=local_tz)
    return local_dt.astimezone(timezone.utc).replace(tzinfo=None)


def to_sample_json(payload: dict[str, Any], max_chars: int) -> str:
    text = json.dumps(payload, ensure_ascii=False, indent=2, default=str)
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + "\n... <truncated>"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Discover SolarEdge sensors and optionally probe sensor telemetry raw shape."
    )

    parser.add_argument("--site-id", default=None, help="Optional filter for one SolarEdge siteId.")
    parser.add_argument("--plant-code", default=None, help="Optional filter for one internal plant code.")
    parser.add_argument("--max-plants", type=int, default=None, help="Optional limit for controlled rollout.")
    parser.add_argument("--timezone", default="Asia/Bangkok", help="Fallback timezone name.")
    parser.add_argument("--dry-run", action="store_true", help="Print selected plants only. No API call. No DB write.")
    parser.add_argument("--probe-data", action="store_true", help="After sensorList, call sensorData for the selected window if sensors exist.")
    parser.add_argument("--force-probe-data", action="store_true", help="Call sensorData even when sensorList returns zero sensors.")
    parser.add_argument("--no-inventory-fallback", dest="inventory_fallback", action="store_false", help="Disable fallback to /site/{siteId}/inventory when sensorList returns 403 Access Denied.")
    parser.set_defaults(inventory_fallback=True)
    parser.add_argument("--start-local", default=None, help="Sensor data start local time, format YYYY-MM-DD HH:MM:SS.")
    parser.add_argument("--end-local", default=None, help="Sensor data end local time, format YYYY-MM-DD HH:MM:SS.")
    parser.add_argument("--sleep-seconds", type=float, default=3.0, help="Sleep between plants. Default = 3 seconds.")
    parser.add_argument("--stop-on-error", action="store_true", help="Stop immediately on first plant error.")
    parser.add_argument("--print-sample-json", action="store_true", help="Print truncated sensorList/sensorData JSON samples.")
    parser.add_argument("--sample-chars", type=int, default=4000, help="Max characters for sample JSON print.")

    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(main())

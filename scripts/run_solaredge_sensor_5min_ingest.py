from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config_loader import ConfigLoader
from src.db.connection import create_connection
from src.db.repositories.canonical_metric_repo import CanonicalMetricRepository
from src.db.repositories.metric_mapping_repo import MetricMappingRepository
from src.db.repositories.raw_v2_repo import RawV2Repository
from src.db.repositories.solaredge_sensor_mart_repo import SolarEdgeSensorMartRepository
from src.db.repositories.source_mapping_repo import SourceMappingRepository
from src.solaredge.client import SolarEdgeClient
from src.solaredge.credential_resolver import SolarEdgeCredentialResolver
from src.solaredge.sensor_data_normalizer import SolarEdgeSensorDataNormalizer


SOURCE_SYSTEM = "SOLAREDGE"
SENSOR_DATA_ENDPOINT = "sensorData"


@dataclass(frozen=True)
class Window:
    start_local: str
    end_local: str
    start_utc: datetime
    end_utc: datetime


def main() -> int:
    args = parse_args()

    app_config = ConfigLoader().load_app_config()
    conn = create_connection(app_config["database"]["connection_string"])

    source_repo = SourceMappingRepository(conn)
    raw_repo = RawV2Repository(conn)
    canonical_repo = CanonicalMetricRepository(conn)
    mapping_repo = MetricMappingRepository(conn)
    mart_repo = SolarEdgeSensorMartRepository(conn)
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

        if not args.start_local or not args.end_local:
            raise RuntimeError("--start-local and --end-local are required for controlled M7 sensor ingest.")

        mapping_lookup = mapping_repo.build_mapping_lookup(
            source_system_code=SOURCE_SYSTEM,
            endpoint_name=SENSOR_DATA_ENDPOINT,
        )
        if not mapping_lookup:
            raise RuntimeError(
                "No enabled SOLAREDGE sensorData mappings found. "
                "Run sql/migrations/20260610_solaredge_sensor_5min_mart.sql first."
            )

        normalizer = SolarEdgeSensorDataNormalizer(mapping_lookup=mapping_lookup)

        print("")
        print("=== SolarEdge Sensor / Irradiance 5-min Ingest ===")
        print(f"mode             : {'DRY-RUN' if args.dry_run else 'REAL-RUN'}")
        print(f"source_system    : {SOURCE_SYSTEM}")
        print(f"endpoint         : {SENSOR_DATA_ENDPOINT}")
        print(f"plant_count      : {len(plant_maps)}")
        print(f"start_local      : {args.start_local}")
        print(f"end_local        : {args.end_local}")
        print(f"window_minutes   : {args.window_minutes}")
        print(f"max_windows/plant: {args.max_windows_per_plant}")
        print(f"sleep_seconds    : {args.sleep_seconds}")
        print("")

        total_windows = 0
        total_success = 0
        total_failed = 0
        total_raw = 0
        total_canonical = 0
        total_mart = 0
        total_telemetries = 0

        for plant_map in plant_maps:
            internal_plant_code = plant_map["internal_plant_code"]
            source_plant_code = str(plant_map["source_plant_code"])
            source_plant_name = plant_map.get("source_plant_name")
            timezone_name = plant_map.get("timezone_name") or args.timezone
            secret_name = plant_map.get("api_key_secret_name")

            windows = build_windows(
                start_local_text=args.start_local,
                end_local_text=args.end_local,
                timezone_name=timezone_name,
                window_minutes=args.window_minutes,
                max_windows=args.max_windows_per_plant,
            )
            total_windows += len(windows)

            print("-" * 110)
            print(
                f"Plant: {internal_plant_code} | site_id={source_plant_code} | "
                f"name={source_plant_name} | timezone={timezone_name} | windows={len(windows)} | "
                f"credential={'OK' if secret_name else 'MISSING_SECRET_NAME'}"
            )

            if args.dry_run:
                for idx, window in enumerate(windows, start=1):
                    print(
                        f"[DRY-RUN] window={idx}/{len(windows)} "
                        f"{window.start_local} -> {window.end_local}"
                    )
                continue

            try:
                api_key = credential_resolver.get_api_key(secret_name)
                client = SolarEdgeClient(api_key=api_key)

                for idx, window in enumerate(windows, start=1):
                    try:
                        data_started_utc = datetime.now(timezone.utc)
                        response = client._get_json(  # noqa: SLF001 - controlled project ingest
                            endpoint_name=SENSOR_DATA_ENDPOINT,
                            endpoint_path=f"/site/{source_plant_code}/sensors",
                            params={
                                "startDate": window.start_local,
                                "endDate": window.end_local,
                                "api_key": client.api_key,
                            },
                        )
                        data_finished_utc = datetime.now(timezone.utc)

                        raw_id = raw_repo.insert_api_call_v2(
                            {
                                "source_system_code": SOURCE_SYSTEM,
                                "endpoint_name": SENSOR_DATA_ENDPOINT,
                                "endpoint_path": response.endpoint_path,
                                "internal_plant_code": internal_plant_code,
                                "source_plant_code": source_plant_code,
                                "source_device_id": None,
                                "request_window_start_utc": window.start_utc,
                                "request_window_end_utc": window.end_utc,
                                "request_grain_sec": 300,
                                "request_json": {
                                    "site_id": source_plant_code,
                                    "startDate": window.start_local,
                                    "endDate": window.end_local,
                                },
                                "response_json": response.response_json,
                                "http_status": response.http_status,
                                "api_success_flag": response.http_status == 200,
                                "fail_code": None,
                                "fail_message": None,
                                "request_started_at_utc": data_started_utc,
                                "request_finished_at_utc": data_finished_utc,
                            }
                        )

                        canonical_rows = normalizer.normalize(
                            raw_id=raw_id,
                            response_json=response.response_json,
                            internal_plant_code=internal_plant_code,
                            source_plant_code=source_plant_code,
                            timezone_name=timezone_name,
                        )
                        canonical_affected = canonical_repo.upsert_many(canonical_rows)
                        mart_affected = mart_repo.load_sensor_5min(
                            source_system_code=SOURCE_SYSTEM,
                            internal_plant_code=internal_plant_code,
                            source_plant_code=source_plant_code,
                            start_utc=window.start_utc,
                            end_utc=window.end_utc,
                        )
                        data_summary = summarize_sensor_data(response.response_json)

                        total_success += 1
                        total_raw += 1
                        total_canonical += canonical_affected
                        total_mart += mart_affected
                        total_telemetries += data_summary["telemetry_count"]

                        print(
                            f"[OK] window={idx}/{len(windows)} raw_id={raw_id} "
                            f"telemetries={data_summary['telemetry_count']} "
                            f"keys={data_summary['measurement_keys']} "
                            f"canonical={canonical_affected} mart={mart_affected} "
                            f"elapsed_sec={response.elapsed_sec:.2f}"
                        )

                        if args.print_sample_json:
                            print("--- sensorData response_json sample ---")
                            print(to_sample_json(response.response_json, args.sample_chars))

                        if args.sleep_seconds > 0:
                            time.sleep(args.sleep_seconds)

                    except Exception as exc:
                        total_failed += 1
                        print(
                            f"[FAILED] {internal_plant_code} window={idx}/{len(windows)} "
                            f"{window.start_local}->{window.end_local}: {type(exc).__name__}: {exc}"
                        )
                        if args.stop_on_error:
                            raise

            except Exception as exc:
                total_failed += 1
                print(f"[FAILED] {internal_plant_code}: {type(exc).__name__}: {exc}")
                if args.stop_on_error:
                    raise

        print("")
        print("=== Summary ===")
        print(f"windows_planned      : {total_windows}")
        print(f"windows_success      : {total_success}")
        print(f"windows_failed       : {total_failed}")
        print(f"raw_calls            : {total_raw}")
        print(f"sensor_telemetries   : {total_telemetries}")
        print(f"canonical_rows       : {total_canonical}")
        print(f"mart_rows_merged     : {total_mart}")
        print("")

        return 1 if total_failed else 0

    finally:
        conn.close()


def build_windows(
    *,
    start_local_text: str,
    end_local_text: str,
    timezone_name: str,
    window_minutes: int,
    max_windows: int | None,
) -> list[Window]:
    if window_minutes <= 0:
        raise ValueError("window_minutes must be > 0")

    local_tz = ZoneInfo(timezone_name)
    start_local = datetime.strptime(start_local_text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=local_tz)
    end_local = datetime.strptime(end_local_text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=local_tz)

    if end_local <= start_local:
        raise ValueError("end_local must be greater than start_local")

    windows: list[Window] = []
    current = start_local
    delta = timedelta(minutes=window_minutes)

    while current < end_local:
        window_end = min(current + delta, end_local)
        windows.append(
            Window(
                start_local=current.strftime("%Y-%m-%d %H:%M:%S"),
                end_local=window_end.strftime("%Y-%m-%d %H:%M:%S"),
                start_utc=current.astimezone(timezone.utc).replace(tzinfo=None),
                end_utc=window_end.astimezone(timezone.utc).replace(tzinfo=None),
            )
        )
        current = window_end

        if max_windows is not None and len(windows) >= max_windows:
            break

    return windows


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

    return {
        "group_count": len(data_groups),
        "telemetry_count": telemetry_count,
        "measurement_keys": sorted(measurement_keys),
    }


def to_sample_json(payload: dict[str, Any], max_chars: int) -> str:
    text = json.dumps(payload, ensure_ascii=False, indent=2, default=str)
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + "\n... <truncated>"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run controlled SolarEdge sensor/irradiance 5-minute ingest into canonical and mart."
    )

    parser.add_argument("--site-id", default=None, help="Optional filter for one SolarEdge siteId.")
    parser.add_argument("--plant-code", default=None, help="Optional filter for one internal plant code.")
    parser.add_argument("--max-plants", type=int, default=None, help="Optional limit for controlled rollout.")
    parser.add_argument("--timezone", default="Asia/Bangkok", help="Fallback timezone name.")
    parser.add_argument("--dry-run", action="store_true", help="Print window plan only. No API call / no DB write.")
    parser.add_argument("--start-local", required=True, help="Start local time, format YYYY-MM-DD HH:MM:SS.")
    parser.add_argument("--end-local", required=True, help="End local time, format YYYY-MM-DD HH:MM:SS.")
    parser.add_argument("--window-minutes", type=int, default=120, help="Chunk size. Default = 120 minutes.")
    parser.add_argument("--max-windows-per-plant", type=int, default=None, help="Optional window limit per plant.")
    parser.add_argument("--sleep-seconds", type=float, default=3.0, help="Sleep between API windows. Default = 3 seconds.")
    parser.add_argument("--stop-on-error", action="store_true", help="Stop immediately on first error.")
    parser.add_argument("--print-sample-json", action="store_true", help="Print truncated sensorData JSON sample.")
    parser.add_argument("--sample-chars", type=int, default=3000, help="Max characters for sample JSON print.")

    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(main())

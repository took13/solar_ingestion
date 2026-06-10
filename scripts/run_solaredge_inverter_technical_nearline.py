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
from src.db.repositories.solaredge_inverter_mart_repo import SolarEdgeInverterMartRepository
from src.solaredge.client import SolarEdgeClient
from src.solaredge.credential_resolver import SolarEdgeCredentialResolver
from src.solaredge.inverter_technical_normalizer import SolarEdgeInverterTechnicalNormalizer


SOURCE_SYSTEM = "SOLAREDGE"
ENDPOINT_NAME = "inverterTechnicalData"
DEFAULT_TIMEZONE = "Asia/Bangkok"


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
    credential_resolver = SolarEdgeCredentialResolver(conn=conn)

    try:
        plant_maps = {
            str(row["source_plant_code"]): row
            for row in source_repo.get_active_plant_maps(source_system_code=SOURCE_SYSTEM)
        }

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

        if args.max_plants is not None:
            allowed_plants = []
            seen = set()
            for inv in inverters:
                plant_code = inv["internal_plant_code"]
                if plant_code not in seen:
                    seen.add(plant_code)
                    allowed_plants.append(plant_code)
                if len(allowed_plants) >= args.max_plants:
                    break
            inverters = [inv for inv in inverters if inv["internal_plant_code"] in set(allowed_plants)]

        if args.max_inverters is not None:
            inverters = inverters[: args.max_inverters]

        if not inverters:
            raise RuntimeError(
                "No active SolarEdge inverter found for selected filter. "
                "Run scripts.run_solaredge_equipment_inventory first."
            )

        mapping_lookup = mapping_repo.build_mapping_lookup(
            source_system_code=SOURCE_SYSTEM,
            endpoint_name=ENDPOINT_NAME,
        )
        normalizer = SolarEdgeInverterTechnicalNormalizer(mapping_lookup=mapping_lookup)

        run_started_at_utc = datetime.now(timezone.utc)
        frozen_windows = build_frozen_windows(args=args, inverters=inverters, run_started_at_utc=run_started_at_utc)

        print("")
        print("=== SolarEdge Inverter Technical Nearline Ingest ===")
        print(f"mode             : {'DRY-RUN' if args.dry_run else 'REAL-RUN'}")
        print(f"source_system    : {SOURCE_SYSTEM}")
        print(f"endpoint         : {ENDPOINT_NAME}")
        print(f"inverter_count   : {len(inverters)}")
        print(f"plant_filter     : {args.plant_code or '*'}")
        print(f"site_filter      : {args.site_id or '*'}")
        print(f"serial_filter    : {args.serial_number or '*'}")
        print(f"max_plants       : {args.max_plants if args.max_plants is not None else '*'}")
        print(f"max_inverters    : {args.max_inverters if args.max_inverters is not None else '*'}")
        print(f"sleep_seconds    : {args.sleep_seconds}")
        print(f"stop_on_error    : {args.stop_on_error}")
        if args.start_local and args.end_local:
            print(f"window_mode      : explicit")
            print(f"start_local      : {args.start_local}")
            print(f"end_local        : {args.end_local}")
        else:
            print(f"window_mode      : dynamic")
            print(f"lookback_minutes : {args.lookback_minutes}")
            print(f"lag_minutes      : {args.lag_minutes}")
            print(f"now_local        : {args.now_local or 'frozen at run start'}")
        print(f"run_started_utc  : {fmt_dt(run_started_at_utc.replace(tzinfo=None))}")
        print("frozen_windows   :")
        for tz_name, frozen_window in sorted(frozen_windows.items()):
            print(f"  - {tz_name}: {fmt_dt(frozen_window[0])} -> {fmt_dt(frozen_window[1])}")
        print("")

        total_success = 0
        total_failed = 0
        total_raw = 0
        total_canonical = 0
        total_mart = 0
        total_with_telemetry = 0
        total_no_telemetry = 0
        client_cache: dict[str, SolarEdgeClient] = {}

        for idx, inverter in enumerate(inverters, start=1):
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

            start_local, end_local = frozen_windows[timezone_name]
            start_utc = local_to_utc_naive(start_local, timezone_name)
            end_utc = local_to_utc_naive(end_local, timezone_name)
            start_text = fmt_dt(start_local)
            end_text = fmt_dt(end_local)

            print("-" * 124)
            print(
                f"#{idx}/{len(inverters)} Plant={internal_plant_code} | site_id={source_plant_code} | "
                f"inverter={inverter_name} | serial={serial_number} | timezone={timezone_name}"
            )
            print(f"nearline_window local={start_text} -> {end_text} | utc={start_utc} -> {end_utc}")

            if args.dry_run:
                continue

            try:
                client = client_cache.get(source_plant_code)
                if client is None:
                    api_key = credential_resolver.get_api_key(plant_map.get("api_key_secret_name"))
                    client = SolarEdgeClient(api_key=api_key)
                    client_cache[source_plant_code] = client

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
                        # Never store api_key in DB/logs.
                        "request_json": {
                            "site_id": source_plant_code,
                            "serial_number": serial_number,
                            "startTime": start_text,
                            "endTime": end_text,
                            "nearline_mode": True,
                            "nearline_window_mode": "frozen_once_per_run",
                            "run_started_at_utc": fmt_dt(run_started_at_utc.replace(tzinfo=None)),
                            "lookback_minutes": None if args.start_local else args.lookback_minutes,
                            "lag_minutes": None if args.start_local else args.lag_minutes,
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

                telemetries = telemetry_count(response.response_json)

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

                total_success += 1
                total_raw += 1
                total_canonical += canonical_count
                total_mart += mart_count
                if telemetries > 0:
                    total_with_telemetry += 1
                    status_label = "OK"
                else:
                    total_no_telemetry += 1
                    status_label = "NO_TELEMETRY"

                print(
                    f"[{status_label}] raw_id={raw_id} http_status={response.http_status} "
                    f"elapsed_sec={response.elapsed_sec:.2f} "
                    f"telemetries={telemetries} "
                    f"canonical_rows={canonical_count} mart_rows={mart_count}"
                )

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
        print(f"with_telemetry    : {total_with_telemetry}")
        print(f"no_telemetry      : {total_no_telemetry}")
        print(f"failed_inverters  : {total_failed}")
        print(f"raw_calls         : {total_raw}")
        print(f"canonical_rows    : {total_canonical}")
        print(f"mart_rows         : {total_mart}")

        return 0 if total_failed == 0 else 2

    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Nearline SolarEdge inverter technical ingest for scheduler use."
    )
    parser.add_argument("--dry-run", action="store_true", help="Print selected inverters/window only. No API call. No DB write.")
    parser.add_argument("--plant-code", help="Optional internal plant filter, e.g. SE_TPRC.")
    parser.add_argument("--site-id", help="Optional SolarEdge siteId filter.")
    parser.add_argument("--serial-number", help="Optional inverter serial number filter.")
    parser.add_argument("--max-plants", type=int, help="Optional plant-count limit for controlled rollout.")
    parser.add_argument("--max-inverters", type=int, help="Optional inverter-count limit for controlled rollout.")
    parser.add_argument("--start-local", help='Explicit local start time: "YYYY-MM-DD HH:MM:SS"')
    parser.add_argument("--end-local", help='Explicit local end time: "YYYY-MM-DD HH:MM:SS"')
    parser.add_argument("--lookback-minutes", type=int, default=45, help="Dynamic mode start = now - lookback. Default = 45.")
    parser.add_argument("--lag-minutes", type=int, default=15, help="Dynamic mode end = now - lag. Default = 15.")
    parser.add_argument("--now-local", help='Optional deterministic dynamic-mode anchor: "YYYY-MM-DD HH:MM:SS"')
    parser.add_argument("--timezone", default=DEFAULT_TIMEZONE, help="Fallback timezone if inventory mapping has NULL timezone_name.")
    parser.add_argument("--sleep-seconds", type=float, default=1.0, help="Sleep between inverter API calls. Default = 1.")
    parser.add_argument("--stop-on-error", action="store_true", help="Stop immediately on first inverter error.")
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if bool(args.start_local) != bool(args.end_local):
        raise ValueError("--start-local and --end-local must be provided together.")

    if args.start_local and args.end_local:
        start = parse_local_naive(args.start_local)
        end = parse_local_naive(args.end_local)
        if end <= start:
            raise ValueError("--end-local must be greater than --start-local.")
    else:
        if args.lookback_minutes <= 0:
            raise ValueError("--lookback-minutes must be > 0.")
        if args.lag_minutes < 0:
            raise ValueError("--lag-minutes must be >= 0.")
        if args.lookback_minutes <= args.lag_minutes:
            raise ValueError("--lookback-minutes must be greater than --lag-minutes.")

    if args.max_plants is not None and args.max_plants < 1:
        raise ValueError("--max-plants must be >= 1.")
    if args.max_inverters is not None and args.max_inverters < 1:
        raise ValueError("--max-inverters must be >= 1.")



def build_frozen_windows(
    *,
    args: argparse.Namespace,
    inverters: list[dict],
    run_started_at_utc: datetime,
) -> dict[str, tuple[datetime, datetime]]:
    """Resolve each timezone window once at job start.

    This prevents audit drift where early inverters use one dynamic nearline
    window and later inverters slide into the next 5-minute bucket while the
    same job is still running.
    """
    timezone_names = sorted({str(inv.get("timezone_name") or args.timezone) for inv in inverters})
    windows: dict[str, tuple[datetime, datetime]] = {}
    for timezone_name in timezone_names:
        windows[timezone_name] = resolve_window(
            args=args,
            timezone_name=timezone_name,
            run_started_at_utc=run_started_at_utc,
        )
    return windows

def resolve_window(args: argparse.Namespace, timezone_name: str, run_started_at_utc: datetime) -> tuple[datetime, datetime]:
    if args.start_local and args.end_local:
        return parse_local_naive(args.start_local), parse_local_naive(args.end_local)

    tz = ZoneInfo(timezone_name)
    if args.now_local:
        now_local = parse_local_naive(args.now_local).replace(tzinfo=tz)
    else:
        now_local = run_started_at_utc.astimezone(tz)

    end_local = floor_to_5min((now_local - timedelta(minutes=args.lag_minutes)).replace(tzinfo=None))
    start_local = floor_to_5min((now_local - timedelta(minutes=args.lookback_minutes)).replace(tzinfo=None))

    if end_local <= start_local:
        raise ValueError(f"Resolved dynamic window is invalid: {start_local} -> {end_local}")

    return start_local, end_local


def parse_local_naive(local_text: str | datetime) -> datetime:
    if isinstance(local_text, datetime):
        return local_text.replace(tzinfo=None)
    return datetime.strptime(str(local_text), "%Y-%m-%d %H:%M:%S")


def floor_to_5min(dt: datetime) -> datetime:
    bucket_minute = (dt.minute // 5) * 5
    return dt.replace(minute=bucket_minute, second=0, microsecond=0)


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

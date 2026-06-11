from __future__ import annotations

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
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
from src.solaredge.client import SolarEdgeClient, SolarEdgeResponse
from src.solaredge.credential_resolver import SolarEdgeCredentialResolver
from src.solaredge.inverter_technical_normalizer import SolarEdgeInverterTechnicalNormalizer


SOURCE_SYSTEM = "SOLAREDGE"
ENDPOINT_NAME = "inverterTechnicalData"
DEFAULT_TIMEZONE = "Asia/Bangkok"


@dataclass(frozen=True)
class InverterWorkItem:
    idx: int
    total: int
    internal_plant_code: str
    source_plant_code: str
    serial_number: str
    inverter_name: str | None
    timezone_name: str
    start_local: datetime
    end_local: datetime
    start_utc: datetime
    end_utc: datetime
    api_key: str
    request_timeout_sec: int


@dataclass(frozen=True)
class InverterFetchResult:
    item: InverterWorkItem
    response: SolarEdgeResponse
    request_started_at_utc: datetime
    request_finished_at_utc: datetime


@dataclass(frozen=True)
class PersistTiming:
    raw_insert_sec: float
    normalize_sec: float
    canonical_upsert_sec: float
    mart_load_sec: float

    @property
    def total_sec(self) -> float:
        return self.raw_insert_sec + self.normalize_sec + self.canonical_upsert_sec + self.mart_load_sec


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
        print(f"max_workers      : {args.max_workers}")
        print(f"request_timeout  : {args.request_timeout_sec}s")
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
        total_api_elapsed_sec = 0.0
        total_raw_insert_sec = 0.0
        total_normalize_sec = 0.0
        total_canonical_upsert_sec = 0.0
        total_mart_load_sec = 0.0
        work_items: list[InverterWorkItem] = []
        api_key_cache: dict[str, str] = {}

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

            api_key = api_key_cache.get(source_plant_code)
            if api_key is None:
                api_key = credential_resolver.get_api_key(plant_map.get("api_key_secret_name"))
                api_key_cache[source_plant_code] = api_key

            start_local, end_local = frozen_windows[timezone_name]
            start_utc = local_to_utc_naive(start_local, timezone_name)
            end_utc = local_to_utc_naive(end_local, timezone_name)

            work_items.append(
                InverterWorkItem(
                    idx=idx,
                    total=len(inverters),
                    internal_plant_code=internal_plant_code,
                    source_plant_code=source_plant_code,
                    serial_number=serial_number,
                    inverter_name=inverter_name,
                    timezone_name=timezone_name,
                    start_local=start_local,
                    end_local=end_local,
                    start_utc=start_utc,
                    end_utc=end_utc,
                    api_key=api_key,
                    request_timeout_sec=args.request_timeout_sec,
                )
            )

        if args.dry_run:
            for item in work_items:
                print_work_item(item)
            return 0

        if args.max_workers == 1:
            for item in work_items:
                print_work_item(item)
                try:
                    result = fetch_inverter_technical(item=item)
                    raw_count, canonical_count, mart_count, telemetries, timing = persist_inverter_technical_result(
                        raw_repo=raw_repo,
                        canonical_repo=canonical_repo,
                        mart_repo=mart_repo,
                        normalizer=normalizer,
                        run_started_at_utc=run_started_at_utc,
                        args=args,
                        result=result,
                    )

                    total_success += 1
                    total_raw += raw_count
                    total_canonical += canonical_count
                    total_mart += mart_count
                    total_api_elapsed_sec += float(result.response.elapsed_sec or 0.0)
                    total_raw_insert_sec += timing.raw_insert_sec
                    total_normalize_sec += timing.normalize_sec
                    total_canonical_upsert_sec += timing.canonical_upsert_sec
                    total_mart_load_sec += timing.mart_load_sec
                    if telemetries > 0:
                        total_with_telemetry += 1
                        status_label = "OK"
                    else:
                        total_no_telemetry += 1
                        status_label = "NO_TELEMETRY"

                    print_result(result=result, status_label=status_label, raw_count=raw_count, canonical_count=canonical_count, mart_count=mart_count, telemetries=telemetries, timing=timing, profile_timing=args.profile_timing)

                except Exception as exc:
                    total_failed += 1
                    print(f"[FAIL] {item.internal_plant_code}/{item.serial_number}: {exc}")
                    if args.stop_on_error:
                        raise

                if args.sleep_seconds > 0:
                    time.sleep(args.sleep_seconds)
        else:
            print(f"parallel_mode    : enabled max_workers={args.max_workers}")
            with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
                future_to_item = {}
                for item in work_items:
                    print_work_item(item, prefix="[QUEUE]")
                    future = executor.submit(fetch_inverter_technical, item=item)
                    future_to_item[future] = item
                    if args.sleep_seconds > 0:
                        time.sleep(args.sleep_seconds)

                for future in as_completed(future_to_item):
                    item = future_to_item[future]
                    try:
                        result = future.result()
                        raw_count, canonical_count, mart_count, telemetries, timing = persist_inverter_technical_result(
                            raw_repo=raw_repo,
                            canonical_repo=canonical_repo,
                            mart_repo=mart_repo,
                            normalizer=normalizer,
                            run_started_at_utc=run_started_at_utc,
                            args=args,
                            result=result,
                        )

                        total_success += 1
                        total_raw += raw_count
                        total_canonical += canonical_count
                        total_mart += mart_count
                        total_api_elapsed_sec += float(result.response.elapsed_sec or 0.0)
                        total_raw_insert_sec += timing.raw_insert_sec
                        total_normalize_sec += timing.normalize_sec
                        total_canonical_upsert_sec += timing.canonical_upsert_sec
                        total_mart_load_sec += timing.mart_load_sec
                        if telemetries > 0:
                            total_with_telemetry += 1
                            status_label = "OK"
                        else:
                            total_no_telemetry += 1
                            status_label = "NO_TELEMETRY"

                        print_result(result=result, status_label=status_label, raw_count=raw_count, canonical_count=canonical_count, mart_count=mart_count, telemetries=telemetries, timing=timing, profile_timing=args.profile_timing)

                    except Exception as exc:
                        total_failed += 1
                        print(f"[FAIL] {item.internal_plant_code}/{item.serial_number}: {exc}")
                        if args.stop_on_error:
                            for pending_future in future_to_item:
                                if pending_future is not future:
                                    pending_future.cancel()
                            raise

        print("")
        print("=== Summary ===")
        print(f"success_inverters : {total_success}")
        print(f"with_telemetry    : {total_with_telemetry}")
        print(f"no_telemetry      : {total_no_telemetry}")
        print(f"failed_inverters  : {total_failed}")
        print(f"raw_calls         : {total_raw}")
        print(f"canonical_rows    : {total_canonical}")
        print(f"mart_rows         : {total_mart}")
        print("")
        print("=== Timing Summary ===")
        print(f"api_elapsed_sum_sec       : {total_api_elapsed_sec:.2f}")
        print(f"raw_insert_sum_sec        : {total_raw_insert_sec:.2f}")
        print(f"normalize_sum_sec         : {total_normalize_sec:.2f}")
        print(f"canonical_upsert_sum_sec  : {total_canonical_upsert_sec:.2f}")
        print(f"mart_load_sum_sec         : {total_mart_load_sec:.2f}")
        print(f"persist_sum_sec           : {(total_raw_insert_sec + total_normalize_sec + total_canonical_upsert_sec + total_mart_load_sec):.2f}")

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
    parser.add_argument("--sleep-seconds", type=float, default=0.0, help="Sequential mode: sleep between inverter API calls. Parallel mode: optional submission throttle. Default = 0.")
    parser.add_argument("--max-workers", type=int, default=2, help="Concurrent inverter API fetch workers. Use 1 for sequential. Default = 2, max = 3.")
    parser.add_argument("--request-timeout-sec", type=int, default=15, help="Per-inverter SolarEdge HTTP timeout in seconds. Default = 15.")
    parser.add_argument("--profile-timing", action="store_true", help="Print per-inverter DB timing breakdown and run-level timing summary.")
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
    if args.sleep_seconds < 0:
        raise ValueError("--sleep-seconds must be >= 0.")
    if args.max_workers < 1:
        raise ValueError("--max-workers must be >= 1.")
    if args.max_workers > 3:
        raise ValueError("--max-workers must be <= 3 to stay within the controlled SolarEdge API concurrency guardrail.")
    if args.request_timeout_sec < 3:
        raise ValueError("--request-timeout-sec must be >= 3.")
    if args.request_timeout_sec > 60:
        raise ValueError("--request-timeout-sec must be <= 60.")




def fetch_inverter_technical(*, item: InverterWorkItem) -> InverterFetchResult:
    """Fetch one inverter response without touching the shared DB connection."""
    client = SolarEdgeClient(api_key=item.api_key, timeout_sec=item.request_timeout_sec)
    request_started_at_utc = datetime.now(timezone.utc)
    response = client.get_inverter_technical_data(
        site_id=item.source_plant_code,
        serial_number=item.serial_number,
        start_time_local=fmt_dt(item.start_local),
        end_time_local=fmt_dt(item.end_local),
    )
    request_finished_at_utc = datetime.now(timezone.utc)
    return InverterFetchResult(
        item=item,
        response=response,
        request_started_at_utc=request_started_at_utc,
        request_finished_at_utc=request_finished_at_utc,
    )


def persist_inverter_technical_result(
    *,
    raw_repo: RawV2Repository,
    canonical_repo: CanonicalMetricRepository,
    mart_repo: SolarEdgeInverterMartRepository,
    normalizer: SolarEdgeInverterTechnicalNormalizer,
    run_started_at_utc: datetime,
    args: argparse.Namespace,
    result: InverterFetchResult,
) -> tuple[int, int, int, int, PersistTiming]:
    """Persist raw/canonical/mart rows in the main thread using the shared DB connection."""
    item = result.item
    response = result.response

    raw_insert_started = time.perf_counter()
    raw_id = raw_repo.insert_api_call_v2(
        {
            "source_system_code": SOURCE_SYSTEM,
            "endpoint_name": ENDPOINT_NAME,
            "endpoint_path": response.endpoint_path,
            "internal_plant_code": item.internal_plant_code,
            "source_plant_code": item.source_plant_code,
            "source_device_id": item.serial_number,
            "request_window_start_utc": item.start_utc,
            "request_window_end_utc": item.end_utc,
            "request_grain_sec": 300,
            # Never store api_key in DB/logs.
            "request_json": {
                "site_id": item.source_plant_code,
                "serial_number": item.serial_number,
                "startTime": fmt_dt(item.start_local),
                "endTime": fmt_dt(item.end_local),
                "nearline_mode": True,
                "nearline_window_mode": "frozen_once_per_run",
                "parallel_fetch_mode": args.max_workers > 1,
                "max_workers": args.max_workers,
                "request_timeout_sec": args.request_timeout_sec,
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
            "request_started_at_utc": result.request_started_at_utc,
            "request_finished_at_utc": result.request_finished_at_utc,
        }
    )
    raw_insert_sec = time.perf_counter() - raw_insert_started

    telemetries = telemetry_count(response.response_json)

    normalize_started = time.perf_counter()
    canonical_rows = normalizer.normalize(
        raw_id=raw_id,
        response_json=response.response_json,
        internal_plant_code=item.internal_plant_code,
        source_plant_code=item.source_plant_code,
        source_device_id=item.serial_number,
        source_device_name=item.inverter_name,
        timezone_name=item.timezone_name,
    )
    normalize_sec = time.perf_counter() - normalize_started

    canonical_upsert_started = time.perf_counter()
    canonical_count = canonical_repo.upsert_many(canonical_rows)
    canonical_upsert_sec = time.perf_counter() - canonical_upsert_started

    mart_load_started = time.perf_counter()
    mart_count = mart_repo.load_technical_5min(
        source_system_code=SOURCE_SYSTEM,
        internal_plant_code=item.internal_plant_code,
        source_plant_code=item.source_plant_code,
        source_device_id=item.serial_number,
        start_utc=item.start_utc,
        end_utc=item.end_utc,
    )
    mart_load_sec = time.perf_counter() - mart_load_started

    timing = PersistTiming(
        raw_insert_sec=raw_insert_sec,
        normalize_sec=normalize_sec,
        canonical_upsert_sec=canonical_upsert_sec,
        mart_load_sec=mart_load_sec,
    )

    return 1, canonical_count, mart_count, telemetries, timing


def print_work_item(item: InverterWorkItem, prefix: str = "") -> None:
    print("-" * 124)
    print(
        f"{prefix} #{item.idx}/{item.total} Plant={item.internal_plant_code} | site_id={item.source_plant_code} | "
        f"inverter={item.inverter_name} | serial={item.serial_number} | timezone={item.timezone_name}"
    )
    print(
        f"nearline_window local={fmt_dt(item.start_local)} -> {fmt_dt(item.end_local)} | "
        f"utc={item.start_utc} -> {item.end_utc}"
    )


def print_result(
    *,
    result: InverterFetchResult,
    status_label: str,
    raw_count: int,
    canonical_count: int,
    mart_count: int,
    telemetries: int,
    timing: PersistTiming,
    profile_timing: bool,
) -> None:
    item = result.item
    response = result.response
    print(
        f"[{status_label}] #{item.idx}/{item.total} {item.internal_plant_code}/{item.serial_number} "
        f"http_status={response.http_status} elapsed_sec={response.elapsed_sec:.2f} "
        f"telemetries={telemetries} raw_calls={raw_count} "
        f"canonical_rows={canonical_count} mart_rows={mart_count}"
    )
    if profile_timing:
        print(
            f"[TIMING] #{item.idx}/{item.total} {item.internal_plant_code}/{item.serial_number} "
            f"raw_insert={timing.raw_insert_sec:.3f}s "
            f"normalize={timing.normalize_sec:.3f}s "
            f"canonical_upsert={timing.canonical_upsert_sec:.3f}s "
            f"mart_load={timing.mart_load_sec:.3f}s "
            f"persist_total={timing.total_sec:.3f}s"
        )

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

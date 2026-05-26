from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

# Allow running both:
#   python -m scripts.run_solaredge_pilot_ingest
# and:
#   python scripts\run_solaredge_pilot_ingest.py
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
from src.solaredge.canonical_normalizer import SolarEdgeCanonicalNormalizer


SOURCE_SYSTEM = "SOLAREDGE"


def main():
    args = parse_args()

    app_config = ConfigLoader().load_app_config()
    conn = create_connection(app_config["database"]["connection_string"])

    source_repo = SourceMappingRepository(conn)
    raw_repo = RawV2Repository(conn)
    metric_repo = MetricMappingRepository(conn)
    canonical_repo = CanonicalMetricRepository(conn)
    mart_repo = SolarPlantMartRepository(conn)
    checkpoint_repo = SolarEdgeCheckpointRepository(conn)

    try:
        plant_map = source_repo.get_one_active_plant_map(
            source_system_code=SOURCE_SYSTEM,
            source_plant_code=args.site_id,
        )

        if not plant_map:
            raise RuntimeError(
                f"No active plant mapping found for SOLAREDGE site_id={args.site_id}. "
                "Please insert dbo.dim_plant_source_map first."
            )

        internal_plant_code = plant_map["internal_plant_code"]
        source_plant_code = plant_map["source_plant_code"]
        timezone_name = plant_map.get("timezone_name") or args.timezone

        # ------------------------------------------------------------
        # Resolve ingestion window first.
        # Important:
        # - dry-run must not resolve API key
        # - dry-run must not create SolarEdgeClient
        # - dry-run must not call API / write DB
        # ------------------------------------------------------------
        if args.auto_window:
            if args.endpoint == "both":
                raise RuntimeError(
                    "--auto-window does not support --endpoint both in this step. "
                    "Run sitePower and energyDetails separately first."
                )

            window = resolve_auto_window(
                checkpoint_repo=checkpoint_repo,
                source_plant_code=source_plant_code,
                endpoint_name=args.endpoint,
                timezone_name=timezone_name,
                window_minutes=args.window_minutes,
                lag_minutes=args.lag_minutes,
                bootstrap_start_local=args.bootstrap_start_local,
            )

            print_auto_window_plan(
                internal_plant_code=internal_plant_code,
                source_plant_code=source_plant_code,
                timezone_name=timezone_name,
                endpoint_name=args.endpoint,
                window=window,
            )

            if args.dry_run:
                print("")
                print("[DRY-RUN] No API call executed. No DB write executed.")
                return

            if window["status"] != "READY":
                print("")
                print("[SKIP] Window is not due yet. No API call executed.")
                return

            start_local = window["start_local"]
            end_local = window["end_local"]
            start_utc = window["start_utc"]
            end_utc = window["end_utc"]

        else:
            if not args.start_local or not args.end_local:
                raise RuntimeError(
                    "Manual mode requires --start-local and --end-local. "
                    "Or use --auto-window."
                )

            start_local = args.start_local
            end_local = args.end_local
            start_utc = parse_local_to_utc_naive(start_local, timezone_name)
            end_utc = parse_local_to_utc_naive(end_local, timezone_name)

        if not start_local or not end_local:
            raise RuntimeError(
                f"Resolved window is invalid. start_local={start_local}, end_local={end_local}"
            )

        # ------------------------------------------------------------
        # Resolve API key only after dry-run / not-due checks.
        # DB-first resolver; environment variable fallback.
        # ------------------------------------------------------------
        credential_resolver = SolarEdgeCredentialResolver(conn=conn)
        api_key = credential_resolver.get_api_key(plant_map.get("api_key_secret_name"))
        client = SolarEdgeClient(api_key=api_key)

        print("=== SolarEdge Pilot Ingestion ===")
        print(f"site_id={source_plant_code}")
        print(f"internal_plant_code={internal_plant_code}")
        print(f"timezone={timezone_name}")
        print(f"start_local={start_local}")
        print(f"end_local={end_local}")
        print(f"start_utc={start_utc}")
        print(f"end_utc={end_utc}")
        print("")

        if args.endpoint in ("sitePower", "both"):
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

        if args.endpoint in ("energyDetails", "both"):
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

        print("")
        print("[OK] SolarEdge pilot ingestion completed")

    finally:
        conn.close()


def run_site_power(
    *,
    client: SolarEdgeClient,
    raw_repo: RawV2Repository,
    metric_repo: MetricMappingRepository,
    canonical_repo: CanonicalMetricRepository,
    mart_repo: SolarPlantMartRepository,
    checkpoint_repo: SolarEdgeCheckpointRepository,
    internal_plant_code: str,
    source_plant_code: str,
    timezone_name: str,
    start_local: str,
    end_local: str,
    start_utc: datetime,
    end_utc: datetime,
):
    endpoint_name = "sitePower"

    if not start_local or not end_local:
        raise RuntimeError(
            f"{endpoint_name} requires start_local and end_local. "
            f"Got start_local={start_local}, end_local={end_local}"
        )

    print(f"--- Running {endpoint_name} ---")

    request_started_at_utc = datetime.now(timezone.utc)

    response = client.get_site_power(
        site_id=source_plant_code,
        start_time_local=start_local,
        end_time_local=end_local,
    )

    request_finished_at_utc = datetime.now(timezone.utc)

    raw_id = raw_repo.insert_api_call_v2(
        {
            "source_system_code": SOURCE_SYSTEM,
            "endpoint_name": endpoint_name,
            "endpoint_path": response.endpoint_path,

            "internal_plant_code": internal_plant_code,
            "source_plant_code": source_plant_code,
            "source_device_id": None,

            "request_window_start_utc": start_utc,
            "request_window_end_utc": end_utc,
            "request_grain_sec": 900,

            # Do not store api_key in DB
            "request_json": {
                "site_id": source_plant_code,
                "startTime": start_local,
                "endTime": end_local,
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

    mapping_lookup = metric_repo.build_mapping_lookup(
        source_system_code=SOURCE_SYSTEM,
        endpoint_name=endpoint_name,
    )

    if not mapping_lookup:
        raise RuntimeError("No enabled metric mapping found for SOLAREDGE/sitePower")

    normalizer = SolarEdgeCanonicalNormalizer(mapping_lookup)

    canonical_rows = normalizer.normalize(
        raw_id=raw_id,
        endpoint_name=endpoint_name,
        response_json=response.response_json,
        internal_plant_code=internal_plant_code,
        source_plant_code=source_plant_code,
        timezone_name=timezone_name,
    )

    canonical_count = canonical_repo.upsert_many(canonical_rows)

    mart_count = mart_repo.load_power_15min(
        source_system_code=SOURCE_SYSTEM,
        internal_plant_code=internal_plant_code,
        source_plant_code=source_plant_code,
        start_utc=start_utc,
        end_utc=end_utc,
    )

    checkpoint_rows = checkpoint_repo.mark_success(
        internal_plant_code=internal_plant_code,
        source_plant_code=source_plant_code,
        endpoint_name=endpoint_name,
        start_local=start_local,
        end_local=end_local,
        start_utc=start_utc,
        end_utc=end_utc,
        raw_id=raw_id,
    )

    print(f"[OK] {endpoint_name}: raw_id={raw_id}")
    print(f"[OK] {endpoint_name}: canonical_rows={canonical_count}")
    print(f"[OK] {endpoint_name}: mart_power_rows={mart_count}")
    print(f"[OK] {endpoint_name}: checkpoint_rows={checkpoint_rows}")


def run_energy_details(
    *,
    client: SolarEdgeClient,
    raw_repo: RawV2Repository,
    metric_repo: MetricMappingRepository,
    canonical_repo: CanonicalMetricRepository,
    mart_repo: SolarPlantMartRepository,
    checkpoint_repo: SolarEdgeCheckpointRepository,
    internal_plant_code: str,
    source_plant_code: str,
    timezone_name: str,
    start_local: str,
    end_local: str,
    start_utc: datetime,
    end_utc: datetime,
    meters: str,
):
    endpoint_name = "energyDetails"

    if not start_local or not end_local:
        raise RuntimeError(
            f"{endpoint_name} requires start_local and end_local. "
            f"Got start_local={start_local}, end_local={end_local}"
        )

    print(f"--- Running {endpoint_name} ---")

    request_started_at_utc = datetime.now(timezone.utc)

    response = client.get_energy_details(
        site_id=source_plant_code,
        start_time_local=start_local,
        end_time_local=end_local,
        time_unit="QUARTER_OF_AN_HOUR",
        meters=meters,
    )

    request_finished_at_utc = datetime.now(timezone.utc)

    raw_id = raw_repo.insert_api_call_v2(
        {
            "source_system_code": SOURCE_SYSTEM,
            "endpoint_name": endpoint_name,
            "endpoint_path": response.endpoint_path,

            "internal_plant_code": internal_plant_code,
            "source_plant_code": source_plant_code,
            "source_device_id": None,

            "request_window_start_utc": start_utc,
            "request_window_end_utc": end_utc,
            "request_grain_sec": 900,

            # Do not store api_key in DB
            "request_json": {
                "site_id": source_plant_code,
                "startTime": start_local,
                "endTime": end_local,
                "timeUnit": "QUARTER_OF_AN_HOUR",
                "meters": meters,
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

    mapping_lookup = metric_repo.build_mapping_lookup(
        source_system_code=SOURCE_SYSTEM,
        endpoint_name=endpoint_name,
    )

    if not mapping_lookup:
        raise RuntimeError("No enabled metric mapping found for SOLAREDGE/energyDetails")

    normalizer = SolarEdgeCanonicalNormalizer(mapping_lookup)

    canonical_rows = normalizer.normalize(
        raw_id=raw_id,
        endpoint_name=endpoint_name,
        response_json=response.response_json,
        internal_plant_code=internal_plant_code,
        source_plant_code=source_plant_code,
        timezone_name=timezone_name,
    )

    canonical_count = canonical_repo.upsert_many(canonical_rows)

    mart_count = mart_repo.load_energy_15min(
        source_system_code=SOURCE_SYSTEM,
        internal_plant_code=internal_plant_code,
        source_plant_code=source_plant_code,
        start_utc=start_utc,
        end_utc=end_utc,
    )

    checkpoint_rows = checkpoint_repo.mark_success(
        internal_plant_code=internal_plant_code,
        source_plant_code=source_plant_code,
        endpoint_name=endpoint_name,
        start_local=start_local,
        end_local=end_local,
        start_utc=start_utc,
        end_utc=end_utc,
        raw_id=raw_id,
    )

    print(f"[OK] {endpoint_name}: raw_id={raw_id}")
    print(f"[OK] {endpoint_name}: canonical_rows={canonical_count}")
    print(f"[OK] {endpoint_name}: mart_energy_rows={mart_count}")
    print(f"[OK] {endpoint_name}: checkpoint_rows={checkpoint_rows}")


def resolve_auto_window(
    *,
    checkpoint_repo: SolarEdgeCheckpointRepository,
    source_plant_code: str,
    endpoint_name: str,
    timezone_name: str,
    window_minutes: int,
    lag_minutes: int,
    bootstrap_start_local: str | None,
) -> dict:
    if window_minutes <= 0:
        raise RuntimeError("--window-minutes must be greater than 0")

    if lag_minutes < 0:
        raise RuntimeError("--lag-minutes must be greater than or equal to 0")

    tz = ZoneInfo(timezone_name)

    checkpoint = checkpoint_repo.get_checkpoint(
        source_plant_code=source_plant_code,
        endpoint_name=endpoint_name,
        source_system_code=SOURCE_SYSTEM,
    )

    if checkpoint and checkpoint.get("last_success_end_local"):
        next_start_local = ensure_local_datetime(
            checkpoint["last_success_end_local"],
            timezone_name=timezone_name,
        )
        start_reason = "checkpoint.last_success_end_local"

    elif bootstrap_start_local:
        next_start_local = parse_local_with_tz(
            bootstrap_start_local,
            timezone_name=timezone_name,
        )
        start_reason = "bootstrap-start-local"

    else:
        raise RuntimeError(
            f"No checkpoint found for source_plant_code={source_plant_code}, "
            f"endpoint_name={endpoint_name}, and --bootstrap-start-local not provided."
        )

    now_local = datetime.now(tz)
    available_end_local = floor_to_15min(
        now_local - timedelta(minutes=lag_minutes)
    )

    proposed_end_local = next_start_local + timedelta(minutes=window_minutes)
    next_end_local = min(proposed_end_local, available_end_local)

    status = "READY" if next_end_local > next_start_local else "NOT_DUE"

    return {
        "status": status,
        "start_reason": start_reason,
        "checkpoint": checkpoint,
        "start_local": format_local(next_start_local),
        "end_local": format_local(next_end_local),
        "start_utc": to_utc_naive(next_start_local),
        "end_utc": to_utc_naive(next_end_local),
        "available_end_local": format_local(available_end_local),
    }


def print_auto_window_plan(
    *,
    internal_plant_code: str,
    source_plant_code: str,
    timezone_name: str,
    endpoint_name: str,
    window: dict,
):
    checkpoint = window.get("checkpoint") or {}

    print("")
    print("=== SolarEdge Auto Window Plan ===")
    print(f"status                  : {window['status']}")
    print(f"internal_plant_code     : {internal_plant_code}")
    print(f"site_id                 : {source_plant_code}")
    print(f"endpoint_name           : {endpoint_name}")
    print(f"timezone                : {timezone_name}")
    print(f"start_reason            : {window['start_reason']}")
    print(f"available_end_local     : {window['available_end_local']}")
    print("")
    print("--- checkpoint ---")
    print(f"last_status             : {checkpoint.get('last_status')}")
    print(f"last_raw_id             : {checkpoint.get('last_raw_id')}")
    print(f"last_success_start_local: {checkpoint.get('last_success_start_local')}")
    print(f"last_success_end_local  : {checkpoint.get('last_success_end_local')}")
    print(f"last_success_start_utc  : {checkpoint.get('last_success_start_utc')}")
    print(f"last_success_end_utc    : {checkpoint.get('last_success_end_utc')}")
    print("")
    print("--- next window ---")
    print(f"next_start_local        : {window['start_local']}")
    print(f"next_end_local          : {window['end_local']}")
    print(f"next_start_utc          : {window['start_utc']}")
    print(f"next_end_utc            : {window['end_utc']}")


def parse_local_to_utc_naive(date_text: str, timezone_name: str) -> datetime:
    local_dt = parse_local_with_tz(date_text, timezone_name)
    return to_utc_naive(local_dt)


def parse_local_with_tz(date_text: str, timezone_name: str) -> datetime:
    local_tz = ZoneInfo(timezone_name)
    local_dt = datetime.strptime(date_text, "%Y-%m-%d %H:%M:%S")
    return local_dt.replace(tzinfo=local_tz)


def ensure_local_datetime(value, timezone_name: str) -> datetime:
    local_tz = ZoneInfo(timezone_name)

    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=local_tz)
        return value.astimezone(local_tz)

    if isinstance(value, str):
        return parse_local_with_tz(value, timezone_name)

    raise RuntimeError(f"Unsupported datetime value type: {type(value).__name__}")


def floor_to_15min(dt: datetime) -> datetime:
    minute = (dt.minute // 15) * 15
    return dt.replace(minute=minute, second=0, microsecond=0)


def to_utc_naive(local_dt: datetime) -> datetime:
    return local_dt.astimezone(timezone.utc).replace(tzinfo=None)


def format_local(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def parse_args():
    parser = argparse.ArgumentParser(
        description="SolarEdge pilot ingestion: API -> raw.api_call_v2 -> canonical norm -> mart"
    )

    parser.add_argument(
        "--site-id",
        required=True,
        help="SolarEdge siteId",
    )

    parser.add_argument(
        "--start-local",
        required=False,
        default=None,
        help='Local site start time, format "YYYY-MM-DD HH:MM:SS"',
    )

    parser.add_argument(
        "--end-local",
        required=False,
        default=None,
        help='Local site end time, format "YYYY-MM-DD HH:MM:SS"',
    )

    parser.add_argument(
        "--timezone",
        default="Asia/Bangkok",
        help="Site timezone. Used only if dbo.dim_plant_source_map.timezone_name is NULL.",
    )

    parser.add_argument(
        "--endpoint",
        choices=["sitePower", "energyDetails", "both"],
        default="both",
    )

    parser.add_argument(
        "--meters",
        default="PRODUCTION,FEEDIN,PURCHASED,SELFCONSUMPTION",
        help="Comma-separated SolarEdge energyDetails meters",
    )

    parser.add_argument(
        "--auto-window",
        action="store_true",
        help="Calculate next ingestion window from ctl.solaredge_ingest_checkpoint.",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print auto-window plan only. No API call. No DB write.",
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
        help='Used only when checkpoint does not exist. Format "YYYY-MM-DD HH:MM:SS".',
    )

    return parser.parse_args()


if __name__ == "__main__":
    main()
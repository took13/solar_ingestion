from __future__ import annotations

import argparse
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

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

    credential_resolver = SolarEdgeCredentialResolver()
    api_key = credential_resolver.get_api_key(plant_map.get("api_key_secret_name"))

    start_utc = parse_local_to_utc_naive(args.start_local, timezone_name)
    end_utc = parse_local_to_utc_naive(args.end_local, timezone_name)

    client = SolarEdgeClient(api_key=api_key)

    print("=== SolarEdge Pilot Ingestion ===")
    print(f"site_id={source_plant_code}")
    print(f"internal_plant_code={internal_plant_code}")
    print(f"timezone={timezone_name}")
    print(f"start_local={args.start_local}")
    print(f"end_local={args.end_local}")
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
            start_local=args.start_local,
            end_local=args.end_local,
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
            start_local=args.start_local,
            end_local=args.end_local,
            start_utc=start_utc,
            end_utc=end_utc,
            meters=args.meters,
        )

    conn.close()

    print("")
    print("[OK] SolarEdge pilot ingestion completed")


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


def parse_local_to_utc_naive(date_text: str, timezone_name: str) -> datetime:
    local_tz = ZoneInfo(timezone_name)
    local_dt = datetime.strptime(date_text, "%Y-%m-%d %H:%M:%S")
    local_dt = local_dt.replace(tzinfo=local_tz)

    return local_dt.astimezone(timezone.utc).replace(tzinfo=None)


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
        required=True,
        help='Local site start time, format "YYYY-MM-DD HH:MM:SS"',
    )

    parser.add_argument(
        "--end-local",
        required=True,
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

    return parser.parse_args()


if __name__ == "__main__":
    main()
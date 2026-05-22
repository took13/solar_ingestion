from __future__ import annotations

from datetime import datetime, timezone

from src.config_loader import ConfigLoader
from src.db.connection import create_connection
from src.db.repositories.canonical_metric_repo import CanonicalMetricRepository
from src.db.repositories.metric_mapping_repo import MetricMappingRepository
from src.db.repositories.raw_v2_repo import RawV2Repository
from src.solaredge.canonical_normalizer import SolarEdgeCanonicalNormalizer


def main():
    app_config = ConfigLoader().load_app_config()
    conn = create_connection(app_config["database"]["connection_string"])

    raw_repo = RawV2Repository(conn)
    metric_repo = MetricMappingRepository(conn)
    canonical_repo = CanonicalMetricRepository(conn)

    internal_plant_code = "SMOKE_PLANT"
    source_plant_code = "SMOKE_SITE"
    timezone_name = "Asia/Bangkok"

    run_site_power(
        raw_repo=raw_repo,
        metric_repo=metric_repo,
        canonical_repo=canonical_repo,
        internal_plant_code=internal_plant_code,
        source_plant_code=source_plant_code,
        timezone_name=timezone_name,
    )

    run_energy_details(
        raw_repo=raw_repo,
        metric_repo=metric_repo,
        canonical_repo=canonical_repo,
        internal_plant_code=internal_plant_code,
        source_plant_code=source_plant_code,
        timezone_name=timezone_name,
    )

    print("[OK] SolarEdge offline normalizer smoke test completed")

    conn.close()


def run_site_power(
    *,
    raw_repo: RawV2Repository,
    metric_repo: MetricMappingRepository,
    canonical_repo: CanonicalMetricRepository,
    internal_plant_code: str,
    source_plant_code: str,
    timezone_name: str,
):
    endpoint_name = "sitePower"

    response_json = {
        "power": {
            "timeUnit": "QUARTER_OF_AN_HOUR",
            "unit": "W",
            "values": [
                {"date": "2026-05-21 00:00:00", "value": 100000.0},
                {"date": "2026-05-21 00:15:00", "value": 120000.0},
                {"date": "2026-05-21 00:30:00", "value": None},
                {"date": "2026-05-21 00:45:00", "value": 90000.0},
            ],
        }
    }

    started = datetime.now(timezone.utc)

    raw_id = raw_repo.insert_api_call_v2(
        {
            "source_system_code": "SOLAREDGE",
            "endpoint_name": endpoint_name,
            "endpoint_path": f"/site/{source_plant_code}/power",

            "internal_plant_code": internal_plant_code,
            "source_plant_code": source_plant_code,
            "source_device_id": None,

            "request_window_start_utc": datetime(2026, 5, 20, 17, 0, 0, tzinfo=timezone.utc),
            "request_window_end_utc": datetime(2026, 5, 20, 18, 0, 0, tzinfo=timezone.utc),
            "request_grain_sec": 900,

            "request_json": {
                "purpose": "SolarEdge offline normalizer smoke test",
                "endpoint": endpoint_name,
            },
            "response_json": response_json,

            "http_status": 200,
            "api_success_flag": True,
            "fail_code": None,
            "fail_message": None,

            "request_started_at_utc": started,
            "request_finished_at_utc": datetime.now(timezone.utc),
        }
    )

    mapping_lookup = metric_repo.build_mapping_lookup(
        source_system_code="SOLAREDGE",
        endpoint_name=endpoint_name,
    )

    normalizer = SolarEdgeCanonicalNormalizer(mapping_lookup)

    rows = normalizer.normalize(
        raw_id=raw_id,
        endpoint_name=endpoint_name,
        response_json=response_json,
        internal_plant_code=internal_plant_code,
        source_plant_code=source_plant_code,
        timezone_name=timezone_name,
    )

    affected = canonical_repo.upsert_many(rows)

    print(f"[OK] {endpoint_name}: raw_id={raw_id}, canonical_rows={affected}")


def run_energy_details(
    *,
    raw_repo: RawV2Repository,
    metric_repo: MetricMappingRepository,
    canonical_repo: CanonicalMetricRepository,
    internal_plant_code: str,
    source_plant_code: str,
    timezone_name: str,
):
    endpoint_name = "energyDetails"

    response_json = {
        "energyDetails": {
            "timeUnit": "QUARTER_OF_AN_HOUR",
            "unit": "Wh",
            "meters": [
                {
                    "type": "Production",
                    "values": [
                        {"date": "2026-05-21 00:00:00", "value": 1000.0},
                        {"date": "2026-05-21 00:15:00", "value": 1500.0},
                    ],
                },
                {
                    "type": "FeedIn",
                    "values": [
                        {"date": "2026-05-21 00:00:00", "value": 800.0},
                        {"date": "2026-05-21 00:15:00", "value": 1100.0},
                    ],
                },
                {
                    "type": "Purchased",
                    "values": [
                        {"date": "2026-05-21 00:00:00", "value": 200.0},
                        {"date": "2026-05-21 00:15:00", "value": 100.0},
                    ],
                },
                {
                    "type": "SelfConsumption",
                    "values": [
                        {"date": "2026-05-21 00:00:00", "value": 300.0},
                        {"date": "2026-05-21 00:15:00", "value": None},
                    ],
                },
            ],
        }
    }

    started = datetime.now(timezone.utc)

    raw_id = raw_repo.insert_api_call_v2(
        {
            "source_system_code": "SOLAREDGE",
            "endpoint_name": endpoint_name,
            "endpoint_path": f"/site/{source_plant_code}/energyDetails",

            "internal_plant_code": internal_plant_code,
            "source_plant_code": source_plant_code,
            "source_device_id": None,

            "request_window_start_utc": datetime(2026, 5, 20, 17, 0, 0, tzinfo=timezone.utc),
            "request_window_end_utc": datetime(2026, 5, 20, 18, 0, 0, tzinfo=timezone.utc),
            "request_grain_sec": 900,

            "request_json": {
                "purpose": "SolarEdge offline normalizer smoke test",
                "endpoint": endpoint_name,
            },
            "response_json": response_json,

            "http_status": 200,
            "api_success_flag": True,
            "fail_code": None,
            "fail_message": None,

            "request_started_at_utc": started,
            "request_finished_at_utc": datetime.now(timezone.utc),
        }
    )

    mapping_lookup = metric_repo.build_mapping_lookup(
        source_system_code="SOLAREDGE",
        endpoint_name=endpoint_name,
    )

    normalizer = SolarEdgeCanonicalNormalizer(mapping_lookup)

    rows = normalizer.normalize(
        raw_id=raw_id,
        endpoint_name=endpoint_name,
        response_json=response_json,
        internal_plant_code=internal_plant_code,
        source_plant_code=source_plant_code,
        timezone_name=timezone_name,
    )

    affected = canonical_repo.upsert_many(rows)

    print(f"[OK] {endpoint_name}: raw_id={raw_id}, canonical_rows={affected}")


if __name__ == "__main__":
    main()
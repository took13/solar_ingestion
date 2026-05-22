from __future__ import annotations

from datetime import datetime, timezone

from src.config_loader import ConfigLoader
from src.db.connection import create_connection
from src.db.repositories.canonical_metric_repo import CanonicalMetricRepository
from src.db.repositories.raw_v2_repo import RawV2Repository


def main():
    app_config = ConfigLoader().load_app_config()
    conn = create_connection(app_config["database"]["connection_string"])

    raw_repo = RawV2Repository(conn)
    canonical_repo = CanonicalMetricRepository(conn)

    started = datetime.now(timezone.utc)

    raw_id = raw_repo.insert_api_call_v2(
        {
            "source_system_code": "SOLAREDGE",
            "endpoint_name": "CANONICAL_SMOKE_TEST",
            "endpoint_path": "/site/SMOKE_SITE/power",

            "internal_plant_code": "SMOKE_PLANT",
            "source_plant_code": "SMOKE_SITE",
            "source_device_id": None,

            "request_window_start_utc": datetime(2026, 5, 21, 0, 0, 0, tzinfo=timezone.utc),
            "request_window_end_utc": datetime(2026, 5, 21, 1, 0, 0, tzinfo=timezone.utc),
            "request_grain_sec": 900,

            "request_json": {
                "purpose": "canonical smoke test",
                "endpoint": "sitePower",
            },
            "response_json": {
                "power": {
                    "timeUnit": "QUARTER_OF_AN_HOUR",
                    "unit": "W",
                    "values": [
                        {
                            "date": "2026-05-21 00:00:00",
                            "value": 123450.0,
                        }
                    ],
                }
            },

            "http_status": 200,
            "api_success_flag": True,
            "fail_code": None,
            "fail_message": None,

            "request_started_at_utc": started,
            "request_finished_at_utc": datetime.now(timezone.utc),
        }
    )

    rows = [
        {
            "raw_id": raw_id,
            "source_system_code": "SOLAREDGE",
            "internal_plant_code": "SMOKE_PLANT",
            "source_plant_code": "SMOKE_SITE",

            "device_scope": "PLANT",
            "source_device_id": None,
            "source_device_name": "Smoke Site",

            "collect_time_utc": datetime(2026, 5, 21, 0, 0, 0, tzinfo=timezone.utc),
            "time_grain_sec": 900,

            "source_metric_name": "power",
            "canonical_metric_code": "active_power_w",

            "metric_value_num": 123450.0,
            "unit_code": "W",
            "quality_code": "OK",
        },
        {
            "raw_id": raw_id,
            "source_system_code": "SOLAREDGE",
            "internal_plant_code": "SMOKE_PLANT",
            "source_plant_code": "SMOKE_SITE",

            "device_scope": "PLANT",
            "source_device_id": None,
            "source_device_name": "Smoke Site",

            "collect_time_utc": datetime(2026, 5, 21, 0, 0, 0, tzinfo=timezone.utc),
            "time_grain_sec": 900,

            "source_metric_name": "power",
            "canonical_metric_code": "active_power_kw",

            "metric_value_num": 123.45,
            "unit_code": "kW",
            "quality_code": "OK",
        },
    ]

    affected = canonical_repo.upsert_many(rows)

    print(f"[OK] inserted raw_id={raw_id}")
    print(f"[OK] upserted canonical rows={affected}")

    conn.close()


if __name__ == "__main__":
    main()
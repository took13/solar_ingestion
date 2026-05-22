from __future__ import annotations

from datetime import datetime, timezone

from src.config_loader import ConfigLoader
from src.db.connection import create_connection
from src.db.repositories.raw_v2_repo import RawV2Repository


def main():
    app_config = ConfigLoader().load_app_config()
    conn = create_connection(app_config["database"]["connection_string"])

    repo = RawV2Repository(conn)

    started = datetime.now(timezone.utc)

    raw_id = repo.insert_api_call_v2(
        {
            "source_system_code": "SOLAREDGE",
            "endpoint_name": "SMOKE_TEST",
            "endpoint_path": "/smoke-test",

            "internal_plant_code": "SMOKE_PLANT",
            "source_plant_code": "SMOKE_SITE",
            "source_device_id": None,

            "request_window_start_utc": datetime(2026, 5, 21, 0, 0, 0, tzinfo=timezone.utc),
            "request_window_end_utc": datetime(2026, 5, 21, 1, 0, 0, tzinfo=timezone.utc),
            "request_grain_sec": 900,

            "request_json": {
                "purpose": "raw.api_call_v2 smoke test",
                "note": "safe test row; can be deleted",
            },
            "response_json": {
                "success": True,
                "data": [
                    {
                        "date": "2026-05-21 00:00:00",
                        "value": 123.45,
                    }
                ],
            },

            "http_status": 200,
            "api_success_flag": True,
            "fail_code": None,
            "fail_message": None,

            "request_started_at_utc": started,
            "request_finished_at_utc": datetime.now(timezone.utc),
        }
    )

    print(f"[OK] inserted raw.api_call_v2 smoke test raw_id={raw_id}")

    conn.close()


if __name__ == "__main__":
    main()
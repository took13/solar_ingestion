from __future__ import annotations

from datetime import datetime, timezone

from src.config_loader import ConfigLoader
from src.db.connection import create_connection
from src.db.repositories.canonical_metric_repo import CanonicalMetricRepository
from src.db.repositories.raw_v2_repo import RawV2Repository
from src.db.repositories.solar_plant_mart_repo import SolarPlantMartRepository


def main():
    app_config = ConfigLoader().load_app_config()
    conn = create_connection(app_config["database"]["connection_string"])

    raw_repo = RawV2Repository(conn)
    canonical_repo = CanonicalMetricRepository(conn)
    mart_repo = SolarPlantMartRepository(conn)

    internal_plant_code = "SMOKE_PLANT_MART"
    source_plant_code = "SMOKE_SITE_MART"

    start_utc = datetime(2026, 5, 20, 17, 0, 0, tzinfo=timezone.utc)
    end_utc = datetime(2026, 5, 20, 18, 0, 0, tzinfo=timezone.utc)

    raw_id = insert_raw(
        raw_repo=raw_repo,
        internal_plant_code=internal_plant_code,
        source_plant_code=source_plant_code,
        start_utc=start_utc,
        end_utc=end_utc,
    )

    insert_canonical_rows(
        canonical_repo=canonical_repo,
        raw_id=raw_id,
        internal_plant_code=internal_plant_code,
        source_plant_code=source_plant_code,
    )

    power_rows = mart_repo.load_power_15min(
        source_system_code="SOLAREDGE",
        internal_plant_code=internal_plant_code,
        source_plant_code=source_plant_code,
        start_utc=start_utc,
        end_utc=end_utc,
    )

    energy_rows = mart_repo.load_energy_15min(
        source_system_code="SOLAREDGE",
        internal_plant_code=internal_plant_code,
        source_plant_code=source_plant_code,
        start_utc=start_utc,
        end_utc=end_utc,
    )

    print(f"[OK] raw_id={raw_id}")
    print(f"[OK] mart power affected rows={power_rows}")
    print(f"[OK] mart energy affected rows={energy_rows}")

    print_mart_counts(
        conn=conn,
        internal_plant_code=internal_plant_code,
        source_plant_code=source_plant_code,
    )

    conn.close()


def insert_raw(
    *,
    raw_repo: RawV2Repository,
    internal_plant_code: str,
    source_plant_code: str,
    start_utc: datetime,
    end_utc: datetime,
) -> int:
    started = datetime.now(timezone.utc)

    return raw_repo.insert_api_call_v2(
        {
            "source_system_code": "SOLAREDGE",
            "endpoint_name": "MART_SMOKE_TEST",
            "endpoint_path": f"/site/{source_plant_code}/mart-smoke",

            "internal_plant_code": internal_plant_code,
            "source_plant_code": source_plant_code,
            "source_device_id": None,

            "request_window_start_utc": start_utc,
            "request_window_end_utc": end_utc,
            "request_grain_sec": 900,

            "request_json": {
                "purpose": "mart loader smoke test",
            },
            "response_json": {
                "success": True,
                "note": "canonical rows are inserted separately for smoke test",
            },

            "http_status": 200,
            "api_success_flag": True,
            "fail_code": None,
            "fail_message": None,

            "request_started_at_utc": started,
            "request_finished_at_utc": datetime.now(timezone.utc),
        }
    )


def insert_canonical_rows(
    *,
    canonical_repo: CanonicalMetricRepository,
    raw_id: int,
    internal_plant_code: str,
    source_plant_code: str,
) -> None:
    rows = [
        # 17:00 UTC = 00:00 Asia/Bangkok
        {
            "raw_id": raw_id,
            "source_system_code": "SOLAREDGE",
            "internal_plant_code": internal_plant_code,
            "source_plant_code": source_plant_code,
            "device_scope": "PLANT",
            "source_device_id": None,
            "source_device_name": "Smoke Mart Site",
            "collect_time_utc": datetime(2026, 5, 20, 17, 0, 0, tzinfo=timezone.utc),
            "time_grain_sec": 900,
            "source_metric_name": "power",
            "canonical_metric_code": "active_power_kw",
            "metric_value_num": 100.0,
            "unit_code": "kW",
            "quality_code": "OK",
        },
        {
            "raw_id": raw_id,
            "source_system_code": "SOLAREDGE",
            "internal_plant_code": internal_plant_code,
            "source_plant_code": source_plant_code,
            "device_scope": "PLANT",
            "source_device_id": None,
            "source_device_name": "Smoke Mart Site",
            "collect_time_utc": datetime(2026, 5, 20, 17, 15, 0, tzinfo=timezone.utc),
            "time_grain_sec": 900,
            "source_metric_name": "power",
            "canonical_metric_code": "active_power_kw",
            "metric_value_num": 120.0,
            "unit_code": "kW",
            "quality_code": "OK",
        },

        # Energy rows
        {
            "raw_id": raw_id,
            "source_system_code": "SOLAREDGE",
            "internal_plant_code": internal_plant_code,
            "source_plant_code": source_plant_code,
            "device_scope": "PLANT",
            "source_device_id": None,
            "source_device_name": "Smoke Mart Site",
            "collect_time_utc": datetime(2026, 5, 20, 17, 0, 0, tzinfo=timezone.utc),
            "time_grain_sec": 900,
            "source_metric_name": "Production",
            "canonical_metric_code": "production_energy_kwh",
            "metric_value_num": 1.00,
            "unit_code": "kWh",
            "quality_code": "OK",
        },
        {
            "raw_id": raw_id,
            "source_system_code": "SOLAREDGE",
            "internal_plant_code": internal_plant_code,
            "source_plant_code": source_plant_code,
            "device_scope": "PLANT",
            "source_device_id": None,
            "source_device_name": "Smoke Mart Site",
            "collect_time_utc": datetime(2026, 5, 20, 17, 0, 0, tzinfo=timezone.utc),
            "time_grain_sec": 900,
            "source_metric_name": "FeedIn",
            "canonical_metric_code": "feed_in_energy_kwh",
            "metric_value_num": 0.80,
            "unit_code": "kWh",
            "quality_code": "OK",
        },
        {
            "raw_id": raw_id,
            "source_system_code": "SOLAREDGE",
            "internal_plant_code": internal_plant_code,
            "source_plant_code": source_plant_code,
            "device_scope": "PLANT",
            "source_device_id": None,
            "source_device_name": "Smoke Mart Site",
            "collect_time_utc": datetime(2026, 5, 20, 17, 15, 0, tzinfo=timezone.utc),
            "time_grain_sec": 900,
            "source_metric_name": "Production",
            "canonical_metric_code": "production_energy_kwh",
            "metric_value_num": 1.50,
            "unit_code": "kWh",
            "quality_code": "OK",
        },
        {
            "raw_id": raw_id,
            "source_system_code": "SOLAREDGE",
            "internal_plant_code": internal_plant_code,
            "source_plant_code": source_plant_code,
            "device_scope": "PLANT",
            "source_device_id": None,
            "source_device_name": "Smoke Mart Site",
            "collect_time_utc": datetime(2026, 5, 20, 17, 15, 0, tzinfo=timezone.utc),
            "time_grain_sec": 900,
            "source_metric_name": "SelfConsumption",
            "canonical_metric_code": "self_consumption_energy_kwh",
            "metric_value_num": 0.40,
            "unit_code": "kWh",
            "quality_code": "OK",
        },
    ]

    affected = canonical_repo.upsert_many(rows)
    print(f"[OK] canonical rows upserted={affected}")


def print_mart_counts(
    *,
    conn,
    internal_plant_code: str,
    source_plant_code: str,
) -> None:
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT COUNT(*) AS row_count
        FROM mart.fact_solar_plant_power_15min
        WHERE internal_plant_code = ?
          AND source_system_code = 'SOLAREDGE'
          AND source_plant_code = ?;
        """,
        (internal_plant_code, source_plant_code),
    )
    power_count = int(cursor.fetchone()[0])

    cursor.execute(
        """
        SELECT COUNT(*) AS row_count
        FROM mart.fact_solar_plant_energy_15min
        WHERE internal_plant_code = ?
          AND source_system_code = 'SOLAREDGE'
          AND source_plant_code = ?;
        """,
        (internal_plant_code, source_plant_code),
    )
    energy_count = int(cursor.fetchone()[0])

    print(f"[OK] mart power total rows for smoke site={power_count}")
    print(f"[OK] mart energy total rows for smoke site={energy_count}")


if __name__ == "__main__":
    main()
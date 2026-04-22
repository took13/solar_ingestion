from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.main import build_app


def floor_to_5min(dt: datetime) -> datetime:
    minute = (dt.minute // 5) * 5
    return dt.replace(minute=minute, second=0, microsecond=0)


def main():
    app = build_app()

    end_utc = floor_to_5min(datetime.now(timezone.utc) - timedelta(minutes=10))
    start_utc = end_utc - timedelta(minutes=5)

    app.run_job_with_override_window(
        job_name="inverter_history_nearline",
        override_start_utc=start_utc,
        override_end_utc=end_utc,
    )


if __name__ == "__main__":
    main()
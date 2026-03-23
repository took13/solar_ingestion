from __future__ import annotations

import argparse
from datetime import datetime, timezone, timedelta

from src.main import build_app


def parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--job", required=True, help="Job name without .yaml, e.g. dev_history_backfill")
    parser.add_argument("--start", required=True, help="UTC ISO format, e.g. 2026-03-22T00:00:00Z")
    parser.add_argument("--end", required=True, help="UTC ISO format, e.g. 2026-03-23T00:00:00Z")
    parser.add_argument("--chunk-minutes", type=int, default=60, help="Chunk size for each backfill run")
    args = parser.parse_args()

    start_utc = parse_utc(args.start)
    end_utc = parse_utc(args.end)

    if start_utc >= end_utc:
        raise ValueError("start must be earlier than end")

    if args.chunk_minutes <= 0:
        raise ValueError("chunk-minutes must be > 0")

    app = build_app()

    current_start = start_utc
    while current_start < end_utc:
        current_end = min(current_start + timedelta(minutes=args.chunk_minutes), end_utc)

        print(f"[BACKFILL] Running chunk: {current_start.isoformat()} -> {current_end.isoformat()}")

        app.run_job_with_override_window(
            job_name=args.job,
            override_start_utc=current_start,
            override_end_utc=current_end,
        )

        current_start = current_end


if __name__ == "__main__":
    main()
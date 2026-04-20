from datetime import datetime, timezone, timedelta
import argparse

from src.main import build_app
from src.egress.egress_repo import EgressRepository
from src.egress.egress_client import EgressClient
from src.egress.payload_builder import PayloadBuilder
from src.egress.egress_service import EgressService


def parse_utc(value: str):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True, help="UTC ISO, e.g. 2024-12-31T17:00:00Z")
    parser.add_argument("--end", required=True, help="UTC ISO, e.g. 2025-12-31T17:00:00Z")
    parser.add_argument("--chunk-days", type=int, default=1, help="Backfill chunk size in days")
    args = parser.parse_args()

    start_utc = parse_utc(args.start)
    end_utc = parse_utc(args.end)

    if start_utc >= end_utc:
        raise ValueError("start must be earlier than end")

    if args.chunk_days <= 0:
        raise ValueError("chunk-days must be > 0")

    app = build_app()
    repo = EgressRepository(app.conn)
    client = EgressClient()
    payload_builder = PayloadBuilder()
    service = EgressService(repo, client, payload_builder)

    current_start = start_utc
    chunk_no = 0

    while current_start < end_utc:
        chunk_no += 1
        current_end = min(current_start + timedelta(days=args.chunk_days), end_utc)

        print(
            f"[EGRESS_BACKFILL] chunk={chunk_no} "
            f"{current_start.isoformat()} -> {current_end.isoformat()}"
        )

        service.run_backfill(start_utc=current_start, end_utc=current_end)
        current_start = current_end

    print("[EGRESS_BACKFILL] completed")


if __name__ == "__main__":
    main()
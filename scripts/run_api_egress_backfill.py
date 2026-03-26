from datetime import datetime, timezone
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
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    args = parser.parse_args()

    start_utc = parse_utc(args.start)
    end_utc = parse_utc(args.end)

    app = build_app()
    repo = EgressRepository(app.conn)
    client = EgressClient()
    payload_builder = PayloadBuilder()
    service = EgressService(repo, client, payload_builder)
    service.run_backfill(start_utc=start_utc, end_utc=end_utc)


if __name__ == "__main__":
    main()
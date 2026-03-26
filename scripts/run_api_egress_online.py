from src.main import build_app
from src.egress.egress_repo import EgressRepository
from src.egress.egress_client import EgressClient
from src.egress.payload_builder import PayloadBuilder
from src.egress.egress_service import EgressService


def main():
    app = build_app()
    repo = EgressRepository(app.conn)
    client = EgressClient()
    payload_builder = PayloadBuilder()
    service = EgressService(repo, client, payload_builder)
    service.run_online(lookback_minutes=30)


if __name__ == "__main__":
    main()
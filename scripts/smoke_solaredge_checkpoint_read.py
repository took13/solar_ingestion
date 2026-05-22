from __future__ import annotations

from src.config_loader import ConfigLoader
from src.db.connection import create_connection
from src.db.repositories.solaredge_checkpoint_repo import SolarEdgeCheckpointRepository


def main():
    app_config = ConfigLoader().load_app_config()
    conn = create_connection(app_config["database"]["connection_string"])

    repo = SolarEdgeCheckpointRepository(conn)
    checkpoints = repo.list_checkpoints()

    if not checkpoints:
        print("[WARN] No SOLAREDGE checkpoints found")
        conn.close()
        return

    print("=== SolarEdge Checkpoints ===")

    for row in checkpoints:
        print(
            f"- {row['internal_plant_code']} "
            f"site_id={row['source_plant_code']} "
            f"endpoint={row['endpoint_name']} "
            f"status={row['last_status']} "
            f"last_end_local={row['last_success_end_local']} "
            f"secret={row.get('api_key_secret_name')}"
        )

    print("")
    print(f"[OK] checkpoint_count={len(checkpoints)}")

    conn.close()


if __name__ == "__main__":
    main()
from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from src.config_loader import ConfigLoader
from src.db.connection import create_connection
from src.db.repositories.source_credential_repo import SourceCredentialRepository


def mask_secret(value: str | None) -> str | None:
    if value is None:
        return None

    if len(value) <= 8:
        return "*" * len(value)

    return f"{value[:4]}********{value[-4:]}"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Smoke test reading generic source credential from DB. Secret is masked."
    )

    parser.add_argument(
        "--source-system-code",
        required=True,
        help="Example: SOLAREDGE",
    )

    parser.add_argument(
        "--credential-name",
        required=True,
        help="Example: SOLAREDGE_API_KEY_GC5",
    )

    parser.add_argument(
        "--credential-type",
        default=None,
        help="Optional. Example: API_KEY",
    )

    args = parser.parse_args()

    app_config = ConfigLoader().load_app_config()
    conn = create_connection(app_config["database"]["connection_string"])

    try:
        repo = SourceCredentialRepository(conn)

        credential = repo.get_active_credential(
            source_system_code=args.source_system_code,
            credential_name=args.credential_name,
            credential_type=args.credential_type,
        )

        if not credential:
            raise RuntimeError(
                "Credential not found or inactive. "
                f"source_system_code={args.source_system_code}, "
                f"credential_name={args.credential_name}"
            )

        print("")
        print("=== Source Credential Smoke Read ===")
        print(f"credential_id         : {credential.get('credential_id')}")
        print(f"source_system_code    : {credential.get('source_system_code')}")
        print(f"credential_name       : {credential.get('credential_name')}")
        print(f"credential_type       : {credential.get('credential_type')}")
        print(f"username              : {credential.get('username')}")
        print(f"secret_value_masked   : {mask_secret(credential.get('secret_value'))}")
        print(f"token_value_masked    : {mask_secret(credential.get('token_value'))}")
        print(f"is_active             : {credential.get('is_active')}")
        print(f"last_rotated_at_utc   : {credential.get('last_rotated_at_utc')}")
        print(f"updated_at_utc        : {credential.get('updated_at_utc')}")
        print("")
        print("[OK] Credential read completed. Secret was not printed.")
        print("")

        return 0

    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
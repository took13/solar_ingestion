from __future__ import annotations

import os
from typing import Any

from src.db.repositories.source_credential_repo import SourceCredentialRepository


class SolarEdgeCredentialResolver:
    """
    Resolve SolarEdge API key.

    Resolution order:
    1) DB generic credential store: sec.source_credential
    2) Windows Environment Variable fallback

    DB mapping:
        dbo.dim_plant_source_map.api_key_secret_name
        -> sec.source_credential.credential_name
        -> secret_value

    Security:
    - Do not print API key
    - Do not log API key
    - Do not store API key in raw.api_call_v2 request_json
    """

    def __init__(self, conn: Any | None = None):
        self.conn = conn

    def get_api_key(self, api_key_secret_name: str | None) -> str:
        if not api_key_secret_name:
            raise RuntimeError(
                "api_key_secret_name is missing. "
                "Please set dbo.dim_plant_source_map.api_key_secret_name "
                "for this SolarEdge plant."
            )

        # 1) DB-first resolver
        if self.conn is not None:
            try:
                repo = SourceCredentialRepository(self.conn)
                api_key = repo.get_secret_value(
                    source_system_code="SOLAREDGE",
                    credential_name=api_key_secret_name,
                    credential_type="API_KEY",
                )

                # Optional audit: update last_used_at_utc
                repo.mark_last_used(
                    source_system_code="SOLAREDGE",
                    credential_name=api_key_secret_name,
                )

                return api_key

            except Exception as db_exc:
                # Do not expose secret. Fall back to env var for backward compatibility.
                env_api_key = os.getenv(api_key_secret_name)
                if env_api_key:
                    return env_api_key

                raise RuntimeError(
                    f"SolarEdge API key not found in DB credential store or environment variable. "
                    f"credential_name={api_key_secret_name}. "
                    f"DB error={type(db_exc).__name__}: {db_exc}"
                ) from db_exc

        # 2) Env Var fallback for backward compatibility
        api_key = os.getenv(api_key_secret_name)

        if not api_key:
            raise RuntimeError(
                f"SolarEdge API key not found. "
                f"credential_name={api_key_secret_name}. "
                "Expected DB credential store sec.source_credential or Windows Environment Variable."
            )

        return api_key
from __future__ import annotations

import os


class SolarEdgeCredentialResolver:
    """
    Resolve SolarEdge API key from environment variable name.

    DB stores only the environment variable name, for example:
        SOLAREDGE_API_KEY_GC5

    Actual API key must stay outside source code and database.
    """

    def get_api_key(self, api_key_secret_name: str | None) -> str:
        if not api_key_secret_name:
            raise RuntimeError(
                "api_key_secret_name is missing. "
                "Please set dbo.dim_plant_source_map.api_key_secret_name "
                "for this SolarEdge plant."
            )

        api_key = os.getenv(api_key_secret_name)

        if not api_key:
            raise RuntimeError(
                f"Environment variable '{api_key_secret_name}' is not set. "
                "Do not put the API key in source code, Git, or database."
            )

        return api_key
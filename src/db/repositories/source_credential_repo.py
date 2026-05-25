from __future__ import annotations

from typing import Any


class SourceCredentialRepository:
    """
    Generic credential repository for sec.source_credential.

    Purpose:
    - Support multiple source systems, not only SolarEdge
    - Examples:
        HUAWEI      + GPSC_PI_01              + USER_PASSWORD
        SOLAREDGE  + SOLAREDGE_API_KEY_GC5   + API_KEY
        ENSERVE    + ENSERVE_INGEST_TOKEN     + BEARER_TOKEN

    Security:
    - Do not print secret_value
    - Do not log secret_value
    - DB should expose masked view for validation
    """

    def __init__(self, conn):
        self.conn = conn

    def get_active_credential(
        self,
        *,
        source_system_code: str,
        credential_name: str,
        credential_type: str | None = None,
    ) -> dict[str, Any] | None:
        sql = """
            SELECT TOP (1)
                credential_id,
                source_system_code,
                credential_name,
                credential_type,
                username,
                secret_value,
                token_value,
                token_expires_at_utc,
                is_active,
                last_used_at_utc,
                last_rotated_at_utc,
                notes,
                created_at_utc,
                updated_at_utc
            FROM sec.source_credential
            WHERE source_system_code = ?
              AND credential_name = ?
              AND is_active = 1
        """

        params: list[Any] = [source_system_code, credential_name]

        if credential_type:
            sql += " AND credential_type = ?"
            params.append(credential_type)

        cursor = self.conn.cursor()
        cursor.execute(sql, params)

        rows = self._rows_to_dicts(cursor)
        return rows[0] if rows else None

    def get_secret_value(
        self,
        *,
        source_system_code: str,
        credential_name: str,
        credential_type: str | None = None,
    ) -> str:
        credential = self.get_active_credential(
            source_system_code=source_system_code,
            credential_name=credential_name,
            credential_type=credential_type,
        )

        if not credential:
            type_message = f", credential_type={credential_type}" if credential_type else ""
            raise RuntimeError(
                f"Active credential not found: "
                f"source_system_code={source_system_code}, "
                f"credential_name={credential_name}"
                f"{type_message}"
            )

        secret_value = credential.get("secret_value")

        if not secret_value:
            raise RuntimeError(
                f"Credential secret_value is empty: "
                f"source_system_code={source_system_code}, "
                f"credential_name={credential_name}"
            )

        return str(secret_value)

    def mark_last_used(
        self,
        *,
        source_system_code: str,
        credential_name: str,
    ) -> int:
        cursor = self.conn.cursor()
        cursor.execute(
            """
            UPDATE sec.source_credential
            SET
                last_used_at_utc = SYSUTCDATETIME(),
                updated_at_utc = SYSUTCDATETIME()
            WHERE source_system_code = ?
              AND credential_name = ?
              AND is_active = 1;
            """,
            (
                source_system_code,
                credential_name,
            ),
        )

        affected = cursor.rowcount
        self.conn.commit()
        return affected

    def _rows_to_dicts(self, cursor) -> list[dict[str, Any]]:
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
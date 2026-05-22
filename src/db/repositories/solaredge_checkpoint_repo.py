from __future__ import annotations

from typing import Any


class SolarEdgeCheckpointRepository:
    """
    Repository สำหรับ ctl.solaredge_ingest_checkpoint

    Step นี้ใช้ read-only ก่อน
    ยังไม่ update checkpoint จาก Python
    """

    def __init__(self, conn):
        self.conn = conn

    def list_checkpoints(
        self,
        source_system_code: str = "SOLAREDGE",
        active_only: bool = True,
    ) -> list[dict[str, Any]]:
        sql = """
            SELECT
                c.checkpoint_id,
                c.source_system_code,
                c.internal_plant_code,
                c.source_plant_code,
                m.source_plant_name,
                m.timezone_name,
                m.api_key_secret_name,
                c.endpoint_name,
                c.last_success_start_local,
                c.last_success_end_local,
                c.last_success_start_utc,
                c.last_success_end_utc,
                c.last_raw_id,
                c.last_status,
                c.consecutive_failures,
                c.last_error_message,
                c.updated_at_utc
            FROM ctl.solaredge_ingest_checkpoint c
            LEFT JOIN dbo.dim_plant_source_map m
                ON  m.source_system_code = c.source_system_code
                AND m.source_plant_code = c.source_plant_code
            WHERE c.source_system_code = ?
        """

        params: list[Any] = [source_system_code]

        if active_only:
            sql += " AND ISNULL(m.is_active, 1) = 1"

        sql += """
            ORDER BY
                c.internal_plant_code,
                c.endpoint_name
        """

        cursor = self.conn.cursor()
        cursor.execute(sql, params)

        return self._rows_to_dicts(cursor)

    def get_checkpoint(
        self,
        *,
        source_plant_code: str,
        endpoint_name: str,
        source_system_code: str = "SOLAREDGE",
    ) -> dict[str, Any] | None:
        sql = """
            SELECT
                checkpoint_id,
                source_system_code,
                internal_plant_code,
                source_plant_code,
                endpoint_name,
                last_success_start_local,
                last_success_end_local,
                last_success_start_utc,
                last_success_end_utc,
                last_raw_id,
                last_status,
                consecutive_failures,
                last_error_message,
                updated_at_utc
            FROM ctl.solaredge_ingest_checkpoint
            WHERE source_system_code = ?
              AND source_plant_code = ?
              AND endpoint_name = ?
        """

        cursor = self.conn.cursor()
        cursor.execute(
            sql,
            (
                source_system_code,
                source_plant_code,
                endpoint_name,
            ),
        )

        rows = self._rows_to_dicts(cursor)
        return rows[0] if rows else None

    def _rows_to_dicts(self, cursor) -> list[dict[str, Any]]:
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    def mark_success(
        self,
        *,
        internal_plant_code: str,
        source_plant_code: str,
        endpoint_name: str,
        start_local,
        end_local,
        start_utc,
        end_utc,
        raw_id: int,
        source_system_code: str = "SOLAREDGE",
    ) -> int:
        """
        Update checkpoint หลัง endpoint ingest สำเร็จ

        Safety:
        - ไม่ให้ checkpoint ถอยหลัง ถ้า end_utc เก่ากว่า last_success_end_utc เดิม
        """

        cursor = self.conn.cursor()

        cursor.execute(
            """
            UPDATE ctl.solaredge_ingest_checkpoint
            SET
                internal_plant_code = ?,
                last_success_start_local = ?,
                last_success_end_local = ?,
                last_success_start_utc = ?,
                last_success_end_utc = ?,
                last_raw_id = ?,
                last_status = 'SUCCESS',
                consecutive_failures = 0,
                last_error_message = NULL,
                updated_at_utc = SYSUTCDATETIME()
            WHERE source_system_code = ?
                AND source_plant_code = ?
                AND endpoint_name = ?
                AND (
                    last_success_end_utc IS NULL
                    OR ? >= last_success_end_utc
                    );
            """,
            (
                internal_plant_code,
                start_local,
                end_local,
                start_utc,
                end_utc,
                raw_id,
                source_system_code,
                source_plant_code,
                endpoint_name,
                end_utc,
            ),
        )

        affected = cursor.rowcount
        self.conn.commit()
        return affected
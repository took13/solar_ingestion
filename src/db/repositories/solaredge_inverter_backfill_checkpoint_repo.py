from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


class SolarEdgeInverterBackfillCheckpointRepository:
    """Checkpoint repository for SolarEdge inverter technical controlled backfill."""

    def __init__(self, conn):
        self.conn = conn

    def ensure_checkpoint(
        self,
        *,
        source_system_code: str,
        internal_plant_code: str,
        source_plant_code: str,
        source_device_id: str,
        source_device_name: str | None,
        endpoint_name: str,
        requested_start_local: datetime,
        requested_end_local: datetime,
    ) -> dict[str, Any]:
        cursor = self.conn.cursor()
        cursor.execute(
            """
            MERGE ctl.solaredge_inverter_backfill_checkpoint AS tgt
            USING
            (
                SELECT
                    ? AS source_system_code,
                    ? AS internal_plant_code,
                    ? AS source_plant_code,
                    ? AS source_device_id,
                    ? AS source_device_name,
                    ? AS endpoint_name,
                    ? AS requested_start_local,
                    ? AS requested_end_local
            ) AS src
            ON  tgt.source_system_code = src.source_system_code
            AND tgt.internal_plant_code = src.internal_plant_code
            AND tgt.source_plant_code = src.source_plant_code
            AND tgt.source_device_id = src.source_device_id
            AND tgt.endpoint_name = src.endpoint_name
            WHEN MATCHED THEN
                UPDATE SET
                    tgt.source_device_name = COALESCE(src.source_device_name, tgt.source_device_name),
                    tgt.requested_start_local =
                        CASE
                            WHEN tgt.requested_start_local IS NULL OR src.requested_start_local < tgt.requested_start_local
                            THEN src.requested_start_local ELSE tgt.requested_start_local
                        END,
                    tgt.requested_end_local =
                        CASE
                            WHEN tgt.requested_end_local IS NULL OR src.requested_end_local > tgt.requested_end_local
                            THEN src.requested_end_local ELSE tgt.requested_end_local
                        END,
                    tgt.updated_at_utc = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN
                INSERT
                (
                    source_system_code,
                    internal_plant_code,
                    source_plant_code,
                    source_device_id,
                    source_device_name,
                    endpoint_name,
                    requested_start_local,
                    requested_end_local
                )
                VALUES
                (
                    src.source_system_code,
                    src.internal_plant_code,
                    src.source_plant_code,
                    src.source_device_id,
                    src.source_device_name,
                    src.endpoint_name,
                    src.requested_start_local,
                    src.requested_end_local
                );
            """,
            (
                source_system_code,
                internal_plant_code,
                source_plant_code,
                source_device_id,
                source_device_name,
                endpoint_name,
                requested_start_local,
                requested_end_local,
            ),
        )
        self.conn.commit()
        return self.get_checkpoint(
            source_system_code=source_system_code,
            internal_plant_code=internal_plant_code,
            source_plant_code=source_plant_code,
            source_device_id=source_device_id,
            endpoint_name=endpoint_name,
        )

    def get_checkpoint(
        self,
        *,
        source_system_code: str,
        internal_plant_code: str,
        source_plant_code: str,
        source_device_id: str,
        endpoint_name: str,
    ) -> dict[str, Any]:
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT
                checkpoint_id,
                source_system_code,
                internal_plant_code,
                source_plant_code,
                source_device_id,
                source_device_name,
                endpoint_name,
                requested_start_local,
                requested_end_local,
                last_success_start_local,
                last_success_end_local,
                last_success_start_utc,
                last_success_end_utc,
                last_raw_id,
                last_status,
                consecutive_failures,
                total_success_windows,
                total_failed_windows,
                last_error_message,
                inserted_at_utc,
                updated_at_utc
            FROM ctl.solaredge_inverter_backfill_checkpoint
            WHERE source_system_code = ?
              AND internal_plant_code = ?
              AND source_plant_code = ?
              AND source_device_id = ?
              AND endpoint_name = ?;
            """,
            (
                source_system_code,
                internal_plant_code,
                source_plant_code,
                source_device_id,
                endpoint_name,
            ),
        )
        row = cursor.fetchone()
        if not row:
            raise RuntimeError(
                "Missing SolarEdge inverter backfill checkpoint after ensure_checkpoint()"
            )
        return self._row_to_dict(cursor, row)

    def mark_success(
        self,
        *,
        source_system_code: str,
        internal_plant_code: str,
        source_plant_code: str,
        source_device_id: str,
        endpoint_name: str,
        start_local: datetime,
        end_local: datetime,
        start_utc: datetime,
        end_utc: datetime,
        raw_id: int,
    ) -> None:
        cursor = self.conn.cursor()
        cursor.execute(
            """
            UPDATE ctl.solaredge_inverter_backfill_checkpoint
            SET
                last_success_start_local = ?,
                last_success_end_local = ?,
                last_success_start_utc = ?,
                last_success_end_utc = ?,
                last_raw_id = ?,
                last_status = 'SUCCESS',
                consecutive_failures = 0,
                total_success_windows = total_success_windows + 1,
                last_error_message = NULL,
                updated_at_utc = SYSUTCDATETIME()
            WHERE source_system_code = ?
              AND internal_plant_code = ?
              AND source_plant_code = ?
              AND source_device_id = ?
              AND endpoint_name = ?
              AND (last_success_end_utc IS NULL OR ? >= last_success_end_utc);
            """,
            (
                start_local,
                end_local,
                start_utc,
                end_utc,
                raw_id,
                source_system_code,
                internal_plant_code,
                source_plant_code,
                source_device_id,
                endpoint_name,
                end_utc,
            ),
        )
        self.conn.commit()

    def mark_failure(
        self,
        *,
        source_system_code: str,
        internal_plant_code: str,
        source_plant_code: str,
        source_device_id: str,
        endpoint_name: str,
        error_message: str,
    ) -> None:
        cursor = self.conn.cursor()
        cursor.execute(
            """
            UPDATE ctl.solaredge_inverter_backfill_checkpoint
            SET
                last_status = 'FAILED',
                consecutive_failures = consecutive_failures + 1,
                total_failed_windows = total_failed_windows + 1,
                last_error_message = LEFT(?, 1000),
                updated_at_utc = SYSUTCDATETIME()
            WHERE source_system_code = ?
              AND internal_plant_code = ?
              AND source_plant_code = ?
              AND source_device_id = ?
              AND endpoint_name = ?;
            """,
            (
                error_message,
                source_system_code,
                internal_plant_code,
                source_plant_code,
                source_device_id,
                endpoint_name,
            ),
        )
        self.conn.commit()

    def _row_to_dict(self, cursor, row) -> dict[str, Any]:
        columns = [col[0] for col in cursor.description]
        return dict(zip(columns, row))

    def _ensure_utc_naive(self, dt):
        if dt is None:
            return None
        if isinstance(dt, str):
            return dt
        if isinstance(dt, datetime):
            if dt.tzinfo is None:
                return dt
            return dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt

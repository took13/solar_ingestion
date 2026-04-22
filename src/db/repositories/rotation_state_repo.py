from __future__ import annotations


class RotationStateRepository:
    def __init__(self, conn):
        self.conn = conn

    def get_state(self, target_id: int) -> dict | None:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT
                target_id,
                last_device_offset,
                fleet_size,
                current_cycle_no,
                last_cycle_started_utc,
                last_cycle_completed_utc,
                last_run_id,
                updated_at_utc
            FROM ctl.ingest_rotation_state
            WHERE target_id = ?
        """, (target_id,))
        row = cursor.fetchone()
        if not row:
            return None

        return {
            "target_id": row.target_id,
            "last_device_offset": row.last_device_offset,
            "fleet_size": row.fleet_size,
            "current_cycle_no": row.current_cycle_no,
            "last_cycle_started_utc": row.last_cycle_started_utc,
            "last_cycle_completed_utc": row.last_cycle_completed_utc,
            "last_run_id": row.last_run_id,
            "updated_at_utc": row.updated_at_utc,
        }

    def upsert_state(
        self,
        target_id: int,
        last_device_offset: int,
        fleet_size: int,
        run_id: int | None = None,
    ) -> None:
        cursor = self.conn.cursor()
        cursor.execute("""
            IF EXISTS (
                SELECT 1
                FROM ctl.ingest_rotation_state
                WHERE target_id = ?
            )
            BEGIN
                UPDATE ctl.ingest_rotation_state
                SET
                    last_device_offset = ?,
                    fleet_size = ?,
                    last_run_id = ?,
                    updated_at_utc = SYSUTCDATETIME()
                WHERE target_id = ?
            END
            ELSE
            BEGIN
                INSERT INTO ctl.ingest_rotation_state (
                    target_id,
                    last_device_offset,
                    fleet_size,
                    last_run_id,
                    updated_at_utc
                )
                VALUES (?, ?, ?, ?, SYSUTCDATETIME())
            END
        """, (
            target_id,
            last_device_offset,
            fleet_size,
            run_id,
            target_id,
            target_id,
            last_device_offset,
            fleet_size,
            run_id,
        ))
        self.conn.commit()
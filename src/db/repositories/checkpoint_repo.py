from __future__ import annotations


class CheckpointRepository:
    def __init__(self, conn):
        self.conn = conn

    def get_checkpoint(self, job_id: int, account_id: int, plant_code: str, dev_type_id: int) -> dict | None:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT
                checkpoint_id,
                job_id,
                account_id,
                plant_code,
                dev_type_id,
                last_success_end_utc,
                last_attempt_end_utc,
                last_run_id,
                last_status,
                last_batch_hash,
                consecutive_failures,
                cooldown_until_utc,
                last_error_code,
                last_error_message,
                updated_at_utc
            FROM ctl.ingest_checkpoint
            WHERE job_id = ?
              AND account_id = ?
              AND plant_code = ?
              AND dev_type_id = ?
        """, (job_id, account_id, plant_code, dev_type_id))

        row = cursor.fetchone()
        if not row:
            return None

        return {
            "checkpoint_id": row.checkpoint_id,
            "job_id": row.job_id,
            "account_id": row.account_id,
            "plant_code": row.plant_code,
            "dev_type_id": row.dev_type_id,
            "last_success_end_utc": row.last_success_end_utc,
            "last_attempt_end_utc": row.last_attempt_end_utc,
            "last_run_id": row.last_run_id,
            "last_status": row.last_status,
            "last_batch_hash": row.last_batch_hash,
            "consecutive_failures": row.consecutive_failures,
            "cooldown_until_utc": row.cooldown_until_utc,
            "last_error_code": row.last_error_code,
            "last_error_message": row.last_error_message,
            "updated_at_utc": row.updated_at_utc,
        }

    def upsert_checkpoint(
        self,
        target: dict,
        run_id: int,
        status: str,
        last_success_end_utc,
        last_attempt_end_utc,
        error_code,
        error_message,
        consecutive_failures_reset: bool,
    ) -> None:
        cursor = self.conn.cursor()

        cursor.execute("""
            IF EXISTS (
                SELECT 1
                FROM ctl.ingest_checkpoint
                WHERE job_id = ?
                  AND account_id = ?
                  AND plant_code = ?
                  AND dev_type_id = ?
            )
            BEGIN
                UPDATE ctl.ingest_checkpoint
                SET
                    last_success_end_utc =
                        COALESCE(?, last_success_end_utc),
                    last_attempt_end_utc = ?,
                    last_run_id = ?,
                    last_status = ?,
                    consecutive_failures =
                        CASE
                            WHEN ? = 1 THEN 0
                            ELSE ISNULL(consecutive_failures, 0) + CASE WHEN ? IN ('FAILED','PARTIAL') THEN 1 ELSE 0 END
                        END,
                    last_error_code = ?,
                    last_error_message = ?,
                    updated_at_utc = SYSUTCDATETIME()
                WHERE job_id = ?
                  AND account_id = ?
                  AND plant_code = ?
                  AND dev_type_id = ?
            END
            ELSE
            BEGIN
                INSERT INTO ctl.ingest_checkpoint (
                    job_id,
                    account_id,
                    plant_code,
                    dev_type_id,
                    last_success_end_utc,
                    last_attempt_end_utc,
                    last_run_id,
                    last_status,
                    consecutive_failures,
                    last_error_code,
                    last_error_message,
                    updated_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME())
            END
        """, (
            target["job_id"],
            target["account_id"],
            target["plant_code"],
            target["dev_type_id"],

            last_success_end_utc,
            last_attempt_end_utc,
            run_id,
            status,
            1 if consecutive_failures_reset else 0,
            status,
            error_code,
            error_message,
            target["job_id"],
            target["account_id"],
            target["plant_code"],
            target["dev_type_id"],

            target["job_id"],
            target["account_id"],
            target["plant_code"],
            target["dev_type_id"],
            last_success_end_utc,
            last_attempt_end_utc,
            run_id,
            status,
            0 if consecutive_failures_reset else (1 if status in ("FAILED", "PARTIAL") else 0),
            error_code,
            error_message,
        ))

        self.conn.commit()
from __future__ import annotations


class BatchAuditRepository:
    def __init__(self, conn):
        self.conn = conn

    def insert(self, row: dict):
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO ctl.ingest_batch_audit (
                run_id,
                job_id,
                account_id,
                plant_code,
                dev_type_id,
                batch_no,
                batch_hash,
                window_start_utc,
                window_end_utc,
                expected_device_count,
                actual_device_count,
                raw_id,
                status,
                fail_code,
                message,
                created_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME())
        """, (
            row["run_id"],
            row["job_id"],
            row["account_id"],
            row["plant_code"],
            row["dev_type_id"],
            row["batch_no"],
            row["batch_hash"],
            row["window_start_utc"],
            row["window_end_utc"],
            row["expected_device_count"],
            row.get("actual_device_count"),
            row.get("raw_id"),
            row["status"],
            row.get("fail_code"),
            row.get("message"),
        ))
        self.conn.commit()

    def log_batch(
        self,
        run_id: int,
        target_id: int,
        batch_no: int,
        batch_size: int,
        status: str,
        window: dict | None = None,
        message: str | None = None,
        fail_code: int | None = None,
        raw_id: int | None = None,
    ) -> None:
        cursor = self.conn.cursor()

        cursor.execute("""
            SELECT
                t.job_id,
                t.account_id,
                t.plant_code,
                t.dev_type_id
            FROM ctl.ingest_target t
            WHERE t.target_id = ?
        """, (target_id,))
        t = cursor.fetchone()

        if not t:
            raise ValueError(f"Target not found for audit logging: target_id={target_id}")

        if window is not None:
            window_start_utc = window["start_utc"]
            window_end_utc = window["end_utc"]
        else:
            # fallback กันพังสำหรับ plant realtime หรือ target-failed logging
            cursor.execute("SELECT SYSUTCDATETIME()")
            now_utc = cursor.fetchone()[0]
            window_start_utc = now_utc
            window_end_utc = now_utc

        batch_hash = f"target:{target_id}:batch:{batch_no}"

        cursor.execute("""
            INSERT INTO ctl.ingest_batch_audit (
                run_id,
                job_id,
                account_id,
                plant_code,
                dev_type_id,
                batch_no,
                batch_hash,
                window_start_utc,
                window_end_utc,
                expected_device_count,
                actual_device_count,
                raw_id,
                status,
                fail_code,
                message,
                created_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME())
        """, (
            run_id,
            t.job_id,
            t.account_id,
            t.plant_code,
            t.dev_type_id,
            batch_no,
            batch_hash,
            window_start_utc,
            window_end_utc,
            batch_size,
            batch_size if status == "SUCCESS" else None,
            raw_id,
            status,
            fail_code,
            message,
        ))
        self.conn.commit()
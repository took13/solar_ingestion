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
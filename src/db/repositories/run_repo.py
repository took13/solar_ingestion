from __future__ import annotations


class RunRepository:
    def __init__(self, conn):
        self.conn = conn

    def get_job_by_name(self, job_name: str) -> dict | None:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT job_id, job_name, api_name, is_enabled, description
            FROM ctl.ingest_job
            WHERE job_name = ?
        """, (job_name,))
        row = cursor.fetchone()
        if not row:
            return None

        return {
            "job_id": row.job_id,
            "job_name": row.job_name,
            "api_name": row.api_name,
            "is_enabled": row.is_enabled,
            "description": row.description,
        }

    def create_job_if_missing(self, job_name: str, api_name: str, description: str | None = None) -> dict:
        job = self.get_job_by_name(job_name)
        if job:
            return job

        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO ctl.ingest_job (job_name, api_name, is_enabled, description)
            OUTPUT INSERTED.job_id
            VALUES (?, ?, 1, ?)
        """, (job_name, api_name, description))
        job_id = cursor.fetchone()[0]
        self.conn.commit()

        return {
            "job_id": job_id,
            "job_name": job_name,
            "api_name": api_name,
            "is_enabled": 1,
            "description": description,
        }

    def start_run(self, job_id: int, run_type: str, triggered_by: str | None) -> int:
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO ctl.ingest_run (job_id, run_type, status, triggered_by, started_at_utc)
            OUTPUT INSERTED.run_id
            VALUES (?, ?, 'RUNNING', ?, SYSUTCDATETIME())
        """, (job_id, run_type, triggered_by))
        run_id = cursor.fetchone()[0]
        self.conn.commit()
        return run_id

    def finish_run(self, run_id: int, status: str, message: str | None = None):
        cursor = self.conn.cursor()
        cursor.execute("""
            UPDATE ctl.ingest_run
            SET status = ?,
                ended_at_utc = SYSUTCDATETIME(),
                message = ?
            WHERE run_id = ?
        """, (status, message, run_id))
        self.conn.commit()
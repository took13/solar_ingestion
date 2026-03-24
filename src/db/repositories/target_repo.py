class TargetRepository:
    def __init__(self, conn):
        self.conn = conn

    def get_targets_by_job_name(self, job_name: str) -> list[dict]:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT
                t.target_id,
                t.job_id,
                j.job_name,
                t.account_id,
                t.plant_code,
                t.dev_type_id,
                t.is_enabled,
                t.priority_no,
                t.batch_size,
                t.lag_minutes,
                t.overlap_minutes,
                t.max_window_minutes,
                t.bootstrap_start_utc
            FROM ctl.ingest_target t
            INNER JOIN ctl.ingest_job j
                ON j.job_id = t.job_id
            WHERE j.job_name = ?
              AND j.is_enabled = 1
              AND t.is_enabled = 1
            ORDER BY
                t.priority_no,
                t.plant_code,
                t.dev_type_id
        """, (job_name,))

        rows = cursor.fetchall()

        return [
            {
                "target_id": r.target_id,
                "job_id": r.job_id,
                "job_name": r.job_name,
                "account_id": r.account_id,
                "plant_code": r.plant_code,
                "dev_type_id": r.dev_type_id,
                "is_enabled": r.is_enabled,
                "priority_no": r.priority_no,
                "batch_size": r.batch_size,
                "lag_minutes": r.lag_minutes,
                "overlap_minutes": r.overlap_minutes,
                "max_window_minutes": r.max_window_minutes,
                "bootstrap_start_utc": r.bootstrap_start_utc,
            }
            for r in rows
        ]
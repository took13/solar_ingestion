from __future__ import annotations


class TargetRepository:
    def __init__(self, conn):
        self.conn = conn

    def get_active_targets_by_job(self, job_id: int, wave_group=None) -> list[dict]:
        cursor = self.conn.cursor()

        where_wave = ""
        params = [job_id]

        if wave_group:
            where_wave = " AND UPPER(COALESCE(t.wave_group, '')) = ?"
            params.append(wave_group.upper())

        cursor.execute(f"""
            SELECT
                t.target_id,
                t.job_id,
                t.account_id,
                t.plant_code,
                t.dev_type_id,
                t.is_enabled,
                t.priority_no,
                t.batch_size,
                t.lag_minutes,
                t.overlap_minutes,
                t.max_window_minutes,
                t.bootstrap_start_utc,
                t.notes,
                t.created_at_utc,
                t.updated_at_utc,
                t.endpoint_name,
                t.service_class,
                t.requested_batch_size,
                t.max_batches_per_run,
                t.rotation_enabled,
                t.min_cycle_minutes,
                t.schedule_every_minutes,
                t.priority_weight,
                t.hard_window_mode,
                t.wave_group
            FROM ctl.ingest_target t
            WHERE t.job_id = ?
            AND t.is_enabled = 1
            {where_wave}
            ORDER BY
                COALESCE(t.priority_weight, t.priority_no, 999999),
                t.priority_no,
                t.target_id
        """, tuple(params))

        rows = cursor.fetchall()

        return [
            {
                "target_id": r.target_id,
                "job_id": r.job_id,
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
                "notes": r.notes,
                "created_at_utc": r.created_at_utc,
                "updated_at_utc": r.updated_at_utc,
                "endpoint_name": r.endpoint_name,
                "service_class": r.service_class,
                "requested_batch_size": r.requested_batch_size,
                "max_batches_per_run": r.max_batches_per_run,
                "rotation_enabled": r.rotation_enabled,
                "min_cycle_minutes": r.min_cycle_minutes,
                "schedule_every_minutes": r.schedule_every_minutes,
                "priority_weight": r.priority_weight,
                "hard_window_mode": r.hard_window_mode,
                "wave_group": r.wave_group,
            }
            for r in rows
        ]

    def get_targets_by_job_name(self, job_name: str, wave_group=None) -> list[dict]:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT TOP 1 job_id
            FROM ctl.ingest_job
            WHERE job_name = ?
            AND is_enabled = 1
        """, (job_name,))

        row = cursor.fetchone()
        if not row:
            return []

        return self.get_active_targets_by_job(row.job_id, wave_group=wave_group)

        def get_targets_by_job_name(self, job_name: str) -> list[dict]:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT TOP 1 job_id
                FROM ctl.ingest_job
                WHERE job_name = ?
                AND is_enabled = 1
            """, (job_name,))
            row = cursor.fetchone()
            if not row:
                return []
            return self.get_active_targets_by_job(row.job_id)
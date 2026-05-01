class EnserveRepository:
    def __init__(self, conn):
        self.conn = conn

    def get_enabled_targets(self):
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT
                egress_target_id,
                plant_code,
                target_name,
                endpoint_url,
                auth_token,
                http_method,
                batch_record_limit,
                timeout_seconds,
                retry_max_attempts,
                retry_backoff_seconds
            FROM ops.api_egress_target
            WHERE is_enabled = 1
              AND target_name IN ('enserve_gc5_realtime', 'enserve_polyplex_realtime')
            ORDER BY egress_target_id;
        """)
        return self._rows_to_dicts(cursor)

    def get_checkpoint(self, egress_target_id: int, plant_code: str):
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT
                egress_target_id,
                plant_code,
                last_success_end_utc,
                last_attempt_end_utc,
                last_status,
                last_error_message,
                updated_at_utc
            FROM ops.api_egress_checkpoint
            WHERE egress_target_id = ?
              AND plant_code = ?;
        """, (egress_target_id, plant_code))
        row = cursor.fetchone()
        if not row:
            return None

        cols = [c[0] for c in cursor.description]
        return dict(zip(cols, row))

    def get_rows_to_send(self, plant_code: str, start_utc, end_utc, limit: int):
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT TOP (?)
                plant_code,
                collect_time_utc,
                power_kw,
                number_inverter,
                irradiance_wm2,
                temperature_c
            FROM mart.vw_enserve_realtime
            WHERE plant_code = ?
              AND collect_time_utc > ?
              AND collect_time_utc <= ?
              AND power_kw IS NOT NULL
              AND number_inverter IS NOT NULL
            ORDER BY collect_time_utc;
        """, (limit, plant_code, start_utc, end_utc))
        return self._rows_to_dicts(cursor)

    def update_checkpoint_success(self, egress_target_id: int, plant_code: str, last_success_end_utc):
        cursor = self.conn.cursor()
        cursor.execute("""
            UPDATE ops.api_egress_checkpoint
            SET
                last_success_end_utc = ?,
                last_attempt_end_utc = ?,
                last_status = 'SUCCESS',
                last_error_message = NULL,
                updated_at_utc = SYSUTCDATETIME()
            WHERE egress_target_id = ?
              AND plant_code = ?;
        """, (last_success_end_utc, last_success_end_utc, egress_target_id, plant_code))
        self.conn.commit()

    def update_checkpoint_failed(self, egress_target_id: int, plant_code: str, attempt_end_utc, error_message: str):
        cursor = self.conn.cursor()
        cursor.execute("""
            UPDATE ops.api_egress_checkpoint
            SET
                last_attempt_end_utc = ?,
                last_status = 'FAILED',
                last_error_message = ?,
                updated_at_utc = SYSUTCDATETIME()
            WHERE egress_target_id = ?
              AND plant_code = ?;
        """, (attempt_end_utc, error_message[:2000], egress_target_id, plant_code))
        self.conn.commit()

    def log_request(
        self,
        egress_target_id: int,
        plant_code: str,
        request_started_at_utc,
        request_finished_at_utc,
        http_status,
        success_flag: int,
        request_body: str,
        response_body: str,
        error_message: str | None,
        records_count: int,
    ):
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO ops.api_egress_log (
                egress_target_id,
                plant_code,
                request_started_at_utc,
                request_finished_at_utc,
                http_status,
                success_flag,
                request_body,
                response_body,
                error_message,
                records_count
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """, (
            egress_target_id,
            plant_code,
            request_started_at_utc,
            request_finished_at_utc,
            http_status,
            success_flag,
            request_body,
            response_body,
            error_message[:2000] if error_message else None,
            records_count,
        ))
        self.conn.commit()

    def _rows_to_dicts(self, cursor):
        cols = [c[0] for c in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]
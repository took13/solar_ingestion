class EgressRepository:
    def __init__(self, conn):
        self.conn = conn

    def get_enabled_targets(self) -> list[dict]:
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
            ORDER BY plant_code
        """)
        rows = cursor.fetchall()
        return [
            {
                "egress_target_id": r.egress_target_id,
                "plant_code": r.plant_code,
                "target_name": r.target_name,
                "endpoint_url": r.endpoint_url,
                "auth_token": r.auth_token,
                "http_method": r.http_method,
                "batch_record_limit": r.batch_record_limit,
                "timeout_seconds": r.timeout_seconds,
                "retry_max_attempts": r.retry_max_attempts,
                "retry_backoff_seconds": r.retry_backoff_seconds,
            }
            for r in rows
        ]

    def get_checkpoint(self, egress_target_id: int) -> dict | None:
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
        """, (egress_target_id,))
        r = cursor.fetchone()
        if not r:
            return None
        return {
            "egress_target_id": r.egress_target_id,
            "plant_code": r.plant_code,
            "last_success_end_utc": r.last_success_end_utc,
            "last_attempt_end_utc": r.last_attempt_end_utc,
            "last_status": r.last_status,
            "last_error_message": r.last_error_message,
            "updated_at_utc": r.updated_at_utc,
        }

    def upsert_checkpoint(
        self,
        egress_target_id: int,
        plant_code: str,
        last_success_end_utc,
        last_attempt_end_utc,
        last_status: str,
        last_error_message: str | None,
    ):
        cursor = self.conn.cursor()
        cursor.execute("""
            IF EXISTS (SELECT 1 FROM ops.api_egress_checkpoint WHERE egress_target_id = ?)
            BEGIN
                UPDATE ops.api_egress_checkpoint
                SET
                    last_success_end_utc = COALESCE(?, last_success_end_utc),
                    last_attempt_end_utc = ?,
                    last_status = ?,
                    last_error_message = ?,
                    updated_at_utc = SYSUTCDATETIME()
                WHERE egress_target_id = ?
            END
            ELSE
            BEGIN
                INSERT INTO ops.api_egress_checkpoint (
                    egress_target_id, plant_code, last_success_end_utc,
                    last_attempt_end_utc, last_status, last_error_message, updated_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, SYSUTCDATETIME())
            END
        """, (
            egress_target_id,
            last_success_end_utc,
            last_attempt_end_utc,
            last_status,
            last_error_message,
            egress_target_id,

            egress_target_id,
            plant_code,
            last_success_end_utc,
            last_attempt_end_utc,
            last_status,
            last_error_message,
        ))
        self.conn.commit()

    def start_run(self, run_mode: str, triggered_by: str | None = None) -> int:
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO ops.api_egress_run (run_mode, triggered_by, status, started_at_utc)
            OUTPUT INSERTED.egress_run_id
            VALUES (?, ?, 'RUNNING', SYSUTCDATETIME())
        """, (run_mode, triggered_by))
        run_id = cursor.fetchone()[0]
        self.conn.commit()
        return run_id

    def finish_run(self, egress_run_id: int, status: str, message: str | None = None):
        cursor = self.conn.cursor()
        cursor.execute("""
            UPDATE ops.api_egress_run
            SET status = ?, ended_at_utc = SYSUTCDATETIME(), message = ?
            WHERE egress_run_id = ?
        """, (status, message, egress_run_id))
        self.conn.commit()

    def insert_log(self, row: dict):
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO ops.api_egress_log (
                egress_run_id, egress_target_id, plant_code,
                window_start_utc, window_end_utc,
                record_count, request_json, response_text,
                http_status, status, error_message,
                request_started_at_utc, request_finished_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row["egress_run_id"],
            row["egress_target_id"],
            row["plant_code"],
            row["window_start_utc"],
            row["window_end_utc"],
            row["record_count"],
            row.get("request_json"),
            row.get("response_text"),
            row.get("http_status"),
            row["status"],
            row.get("error_message"),
            row["request_started_at_utc"],
            row.get("request_finished_at_utc"),
        ))
        self.conn.commit()

    def get_payload_rows(self, plant_code: str, start_utc, end_utc, record_limit: int) -> list[dict]:
        cursor = self.conn.cursor()
        cursor.execute("""
            WITH src AS (
                SELECT
                    plant_code,
                    collect_time_utc,

                    -- รวม active_power จาก inverter ทุกตัว แล้วแปลงเป็น kW
                    SUM(CASE
                            WHEN dev_type_id = 1
                             AND metric_name = 'active_power'
                            THEN metric_value_num
                        END) AS power_kw,

                    -- irradiance จาก EMI
                    MAX(CASE
                            WHEN dev_type_id = 10
                             AND metric_name = 'radiant_line'
                            THEN metric_value_num
                        END) AS irradiance_wm2,

                    -- temperature จาก EMI
                    AVG(CASE
                            WHEN dev_type_id = 10
                             AND metric_name = 'temperature'
                            THEN metric_value_num
                        END) AS temperature_c
                FROM norm.device_metric_long
                WHERE plant_code = ?
                  AND collect_time_utc >= ?
                  AND collect_time_utc < ?
                  AND (
                        (dev_type_id = 1 AND metric_name = 'active_power')
                     OR (dev_type_id = 10 AND metric_name = 'radiant_line')
                     OR (dev_type_id = 10 AND metric_name = 'temperature')
                  )
                GROUP BY plant_code, collect_time_utc
            )
            SELECT TOP (?)
                collect_time_utc,
                CAST(power_kw AS DECIMAL(18,3)) AS power_kw,
                CAST(irradiance_wm2 AS DECIMAL(18,3)) AS irradiance_wm2,
                CAST(temperature_c AS DECIMAL(18,3)) AS temperature_c
            FROM src
            ORDER BY collect_time_utc
        """, (plant_code, start_utc, end_utc, record_limit))

        rows = cursor.fetchall()
        return [
            {
                "collect_time_utc": r.collect_time_utc,
                "power_kw": float(r.power_kw) if r.power_kw is not None else None,
                "irradiance_wm2": float(r.irradiance_wm2) if r.irradiance_wm2 is not None else None,
                "temperature_c": float(r.temperature_c) if r.temperature_c is not None else None,
            }
            for r in rows
        ]
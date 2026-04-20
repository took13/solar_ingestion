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
        inverter_fields = self.get_inverter_fields(plant_code)
        timestamps = self.get_timestamps(plant_code, start_utc, end_utc, record_limit)

        if not timestamps:
            return []

        emi_map = self.get_emi_rows(plant_code, start_utc, end_utc)
        inverter_map = self.get_inverter_rows(plant_code, start_utc, end_utc)

        rows = []
        for ts in timestamps:
            row = {
                "collect_time_utc": ts,
                "irradiance_wm2": None,
                "temperature_c": None,
            }

            # default ทุก inverter เป็น -99
            for field_name in inverter_fields:
                row[field_name] = -99

            emi = emi_map.get(ts)
            if emi:
                row["irradiance_wm2"] = emi.get("irradiance_wm2")
                row["temperature_c"] = emi.get("temperature_c")

            inv_values = inverter_map.get(ts, {})
            for field_name, value in inv_values.items():
                row[field_name] = value if value is not None else -99

            rows.append(row)

        return rows

    def get_inverter_fields(self, plant_code: str) -> list[str]:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT inverter_field_name
            FROM ops.api_egress_inverter_map
            WHERE plant_code = ?
              AND is_enabled = 1
            ORDER BY inverter_field_name
        """, (plant_code,))
        return [r.inverter_field_name for r in cursor.fetchall()]

    def get_timestamps(self, plant_code: str, start_utc, end_utc, record_limit: int):
        cursor = self.conn.cursor()
        cursor.execute("""
            WITH ts AS (
                SELECT collect_time_utc
                FROM norm.device_metric_long
                WHERE plant_code = ?
                  AND collect_time_utc >= ?
                  AND collect_time_utc < ?
                  AND (
                        (dev_type_id = 1 AND metric_name = 'active_power')
                     OR (dev_type_id = 10 AND metric_name = 'radiant_line')
                     OR (dev_type_id = 10 AND metric_name = 'temperature')
                  )
            )
            SELECT TOP (?)
                collect_time_utc
            FROM ts
            GROUP BY collect_time_utc
            ORDER BY collect_time_utc
        """, (plant_code, start_utc, end_utc, record_limit))
        return [r.collect_time_utc for r in cursor.fetchall()]

    def get_emi_rows(self, plant_code: str, start_utc, end_utc) -> dict:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT
                collect_time_utc,
                MAX(CASE WHEN metric_name = 'radiant_line' THEN metric_value_num END) AS irradiance_wm2,
                AVG(CASE WHEN metric_name = 'temperature' THEN metric_value_num END) AS temperature_c
            FROM norm.device_metric_long
            WHERE plant_code = ?
              AND dev_type_id = 10
              AND metric_name IN ('radiant_line', 'temperature')
              AND collect_time_utc >= ?
              AND collect_time_utc < ?
            GROUP BY collect_time_utc
            ORDER BY collect_time_utc
        """, (plant_code, start_utc, end_utc))
        rows = cursor.fetchall()

        out = {}
        for r in rows:
            out[r.collect_time_utc] = {
                "irradiance_wm2": float(r.irradiance_wm2) if r.irradiance_wm2 is not None else None,
                "temperature_c": float(r.temperature_c) if r.temperature_c is not None else None,
            }
        return out

    def get_inverter_rows(self, plant_code: str, start_utc, end_utc) -> dict:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT
                dml.collect_time_utc,
                m.inverter_field_name,
                dml.metric_value_num / 1000.0 AS inverter_kw
            FROM norm.device_metric_long dml
            INNER JOIN ops.api_egress_inverter_map m
                ON m.plant_code = dml.plant_code
               AND m.dev_id = dml.dev_id
               AND m.is_enabled = 1
            WHERE dml.plant_code = ?
              AND dml.dev_type_id = 1
              AND dml.metric_name = 'active_power'
              AND dml.collect_time_utc >= ?
              AND dml.collect_time_utc < ?
            ORDER BY dml.collect_time_utc, m.inverter_field_name
        """, (plant_code, start_utc, end_utc))
        rows = cursor.fetchall()

        out = {}
        for r in rows:
            ts = r.collect_time_utc
            out.setdefault(ts, {})
            out[ts][r.inverter_field_name] = float(r.inverter_kw) if r.inverter_kw is not None else -99
        return out
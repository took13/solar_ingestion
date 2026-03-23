class RawRepository:
    def __init__(self, conn):
        self.conn = conn

    def insert_api_call(self, row: dict) -> int:
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO raw.api_call (
                run_id, job_id, account_id, plant_id, plant_code, dev_type_id,
                api_family, api_name, endpoint_path, request_method,
                request_window_start_utc, request_window_end_utc,
                request_window_start_local, request_window_end_local,
                batch_no, batch_hash, device_count,
                request_json, response_json, response_size_bytes,
                http_status, api_success_flag, fail_code, fail_message,
                request_started_at_utc, request_finished_at_utc
            )
            OUTPUT INSERTED.raw_id
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row.get("run_id"),
            row.get("job_id"),
            row["account_id"],
            row.get("plant_id"),
            row["plant_code"],
            row["dev_type_id"],
            row["api_family"],
            row["api_name"],
            row["endpoint_path"],
            row["request_method"],
            row.get("request_window_start_utc"),
            row.get("request_window_end_utc"),
            row.get("request_window_start_local"),
            row.get("request_window_end_local"),
            row["batch_no"],
            row["batch_hash"],
            row["device_count"],
            row.get("request_json"),
            row.get("response_json"),
            row.get("response_size_bytes"),
            row.get("http_status"),
            row.get("api_success_flag"),
            row.get("fail_code"),
            row.get("fail_message"),
            row["request_started_at_utc"],
            row.get("request_finished_at_utc"),
        ))
        raw_id = cursor.fetchone()[0]
        self.conn.commit()
        return raw_id
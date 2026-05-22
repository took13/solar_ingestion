from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any


class RawV2Repository:
    """
    Repository สำหรับ raw.api_call_v2

    ใช้สำหรับ multi-source ingestion เช่น:
    - Huawei FusionSolar future flow
    - SolarEdge pilot flow

    Design:
    - ไม่กระทบ raw.api_call เดิม
    - ไม่ผูกกับ ctl.ingest_job เดิม
    - ใช้สำหรับ pilot / new architecture เท่านั้น
    """

    def __init__(self, conn):
        self.conn = conn

    def insert_api_call_v2(self, row: dict[str, Any]) -> int:
        request_json = self._to_json(row.get("request_json"))
        response_json = self._to_json(row.get("response_json"))

        response_size_bytes = (
            len(response_json.encode("utf-8"))
            if response_json is not None
            else None
        )

        response_hash = (
            hashlib.sha256(response_json.encode("utf-8")).digest()
            if response_json is not None
            else None
        )

        cursor = self.conn.cursor()

        cursor.execute(
            """
            INSERT INTO raw.api_call_v2
            (
                source_system_code,
                endpoint_name,
                endpoint_path,

                internal_plant_code,
                source_plant_code,
                source_device_id,

                request_window_start_utc,
                request_window_end_utc,
                request_grain_sec,

                request_json,
                response_json,
                response_size_bytes,

                http_status,
                api_success_flag,
                fail_code,
                fail_message,

                request_started_at_utc,
                request_finished_at_utc,

                response_hash
            )
            OUTPUT INSERTED.raw_id
            VALUES
            (
                ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?,
                ?
            )
            """,
            (
                row["source_system_code"],
                row["endpoint_name"],
                row.get("endpoint_path"),

                row.get("internal_plant_code"),
                row.get("source_plant_code"),
                row.get("source_device_id"),

                self._ensure_utc_naive(row.get("request_window_start_utc")),
                self._ensure_utc_naive(row.get("request_window_end_utc")),
                row.get("request_grain_sec"),

                request_json,
                response_json,
                response_size_bytes,

                row.get("http_status"),
                1 if row.get("api_success_flag") else 0,
                row.get("fail_code"),
                row.get("fail_message"),

                self._ensure_utc_naive(row.get("request_started_at_utc")),
                self._ensure_utc_naive(row.get("request_finished_at_utc")),

                response_hash,
            ),
        )

        raw_id = int(cursor.fetchone()[0])
        self.conn.commit()
        return raw_id

    def _to_json(self, value: Any) -> str | None:
        if value is None:
            return None

        if isinstance(value, str):
            return value

        return json.dumps(value, ensure_ascii=False, default=str)

    def _ensure_utc_naive(self, dt):
        if dt is None:
            return None

        if isinstance(dt, str):
            return dt

        if isinstance(dt, datetime):
            if dt.tzinfo is None:
                return dt

            return dt.astimezone(timezone.utc).replace(tzinfo=None)

        return dt
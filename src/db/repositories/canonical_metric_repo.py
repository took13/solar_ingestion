from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any


class CanonicalMetricRepository:
    """
    Repository สำหรับ norm.canonical_metric_selected

    ใช้เก็บ metric ที่ผ่านการ map เป็น canonical metric แล้ว
    แบบ selected metrics เท่านั้น เพื่อไม่ให้เกิด all-metric explosion
    """

    def __init__(self, conn):
        self.conn = conn

    def upsert_many(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0

        affected = 0
        cursor = self.conn.cursor()

        for row in rows:
            affected += self.upsert_one(row, cursor=cursor, commit=False)

        self.conn.commit()
        return affected

    def upsert_one(self, row: dict[str, Any], cursor=None, commit: bool = True) -> int:
        own_cursor = cursor is None
        if own_cursor:
            cursor = self.conn.cursor()

        source_device_id = row.get("source_device_id")

        cursor.execute(
            """
            UPDATE norm.canonical_metric_selected
            SET
                raw_id = ?,
                internal_plant_code = ?,
                source_device_name = ?,
                time_grain_sec = ?,
                source_metric_name = ?,
                metric_value_num = ?,
                unit_code = ?,
                quality_code = ?
            WHERE source_system_code = ?
              AND source_plant_code = ?
              AND device_scope = ?
              AND ISNULL(source_device_id, '') = ISNULL(?, '')
              AND collect_time_utc = ?
              AND canonical_metric_code = ?;
            """,
            (
                row["raw_id"],
                row["internal_plant_code"],
                row.get("source_device_name"),
                row["time_grain_sec"],
                row["source_metric_name"],
                self._to_decimal_or_none(row.get("metric_value_num")),
                row.get("unit_code"),
                row.get("quality_code", "OK"),

                row["source_system_code"],
                row["source_plant_code"],
                row["device_scope"],
                source_device_id,
                self._ensure_utc_naive(row["collect_time_utc"]),
                row["canonical_metric_code"],
            ),
        )

        if cursor.rowcount == 0:
            cursor.execute(
                """
                INSERT INTO norm.canonical_metric_selected
                (
                    raw_id,
                    source_system_code,
                    internal_plant_code,
                    source_plant_code,

                    device_scope,
                    source_device_id,
                    source_device_name,

                    collect_time_utc,
                    time_grain_sec,

                    source_metric_name,
                    canonical_metric_code,

                    metric_value_num,
                    unit_code,
                    quality_code
                )
                VALUES
                (
                    ?, ?, ?, ?,
                    ?, ?, ?,
                    ?, ?,
                    ?, ?,
                    ?, ?, ?
                );
                """,
                (
                    row["raw_id"],
                    row["source_system_code"],
                    row["internal_plant_code"],
                    row["source_plant_code"],

                    row["device_scope"],
                    source_device_id,
                    row.get("source_device_name"),

                    self._ensure_utc_naive(row["collect_time_utc"]),
                    row["time_grain_sec"],

                    row["source_metric_name"],
                    row["canonical_metric_code"],

                    self._to_decimal_or_none(row.get("metric_value_num")),
                    row.get("unit_code"),
                    row.get("quality_code", "OK"),
                ),
            )

        if commit:
            self.conn.commit()

        return 1

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

    def _to_decimal_or_none(self, value):
        if value is None:
            return None

        if isinstance(value, Decimal):
            return value

        return Decimal(str(value))
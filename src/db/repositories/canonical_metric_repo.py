from __future__ import annotations

import json
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

        # SolarEdge inverterTechnicalData can produce ~11k canonical rows per
        # nearline cycle. The legacy row-by-row UPDATE/INSERT path is correct
        # but too slow for scheduler use. Use one JSON payload + set-based MERGE
        # to keep the public repository contract unchanged.
        return self.upsert_many_json(rows)

    def upsert_many_json(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0

        stage_rows = [self._to_stage_row(row) for row in self._dedupe_rows(rows)]
        payload = json.dumps(stage_rows, ensure_ascii=False, separators=(",", ":"), default=str)

        cursor = self.conn.cursor()
        cursor.execute(
            """
            SET NOCOUNT ON;
            SET XACT_ABORT ON;

            DECLARE @payload nvarchar(max) = ?;

            IF OBJECT_ID('tempdb..#canonical_metric_selected_stage') IS NOT NULL
                DROP TABLE #canonical_metric_selected_stage;

            CREATE TABLE #canonical_metric_selected_stage
            (
                raw_id bigint NOT NULL,
                source_system_code varchar(50) NOT NULL,
                internal_plant_code nvarchar(100) NOT NULL,
                source_plant_code nvarchar(100) NOT NULL,

                device_scope varchar(50) NOT NULL,
                source_device_id nvarchar(100) NULL,
                source_device_name nvarchar(255) NULL,

                collect_time_utc datetime2(0) NOT NULL,
                time_grain_sec int NOT NULL,

                source_metric_name nvarchar(200) NOT NULL,
                canonical_metric_code varchar(100) NOT NULL,

                metric_value_num decimal(38, 12) NULL,
                unit_code varchar(50) NULL,
                quality_code varchar(50) NOT NULL
            );

            INSERT INTO #canonical_metric_selected_stage
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
            SELECT
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
            FROM OPENJSON(@payload)
            WITH
            (
                raw_id bigint '$.raw_id',
                source_system_code varchar(50) '$.source_system_code',
                internal_plant_code nvarchar(100) '$.internal_plant_code',
                source_plant_code nvarchar(100) '$.source_plant_code',
                device_scope varchar(50) '$.device_scope',
                source_device_id nvarchar(100) '$.source_device_id',
                source_device_name nvarchar(255) '$.source_device_name',
                collect_time_utc datetime2(0) '$.collect_time_utc',
                time_grain_sec int '$.time_grain_sec',
                source_metric_name nvarchar(200) '$.source_metric_name',
                canonical_metric_code varchar(100) '$.canonical_metric_code',
                metric_value_num decimal(38, 12) '$.metric_value_num',
                unit_code varchar(50) '$.unit_code',
                quality_code varchar(50) '$.quality_code'
            );

            CREATE INDEX IX_stage_canonical_key
            ON #canonical_metric_selected_stage
            (
                source_system_code,
                source_plant_code,
                device_scope,
                source_device_id,
                collect_time_utc,
                canonical_metric_code
            );

            DECLARE @MergeActions TABLE
            (
                action_name nvarchar(20) NOT NULL
            );

            MERGE norm.canonical_metric_selected WITH (HOLDLOCK) AS tgt
            USING #canonical_metric_selected_stage AS src
            ON  tgt.source_system_code = src.source_system_code
            AND tgt.source_plant_code = src.source_plant_code
            AND tgt.device_scope = src.device_scope
            AND ISNULL(tgt.source_device_id, '') = ISNULL(src.source_device_id, '')
            AND tgt.collect_time_utc = src.collect_time_utc
            AND tgt.canonical_metric_code = src.canonical_metric_code

            WHEN MATCHED THEN
                UPDATE SET
                    tgt.raw_id = src.raw_id,
                    tgt.internal_plant_code = src.internal_plant_code,
                    tgt.source_device_name = src.source_device_name,
                    tgt.time_grain_sec = src.time_grain_sec,
                    tgt.source_metric_name = src.source_metric_name,
                    tgt.metric_value_num = src.metric_value_num,
                    tgt.unit_code = src.unit_code,
                    tgt.quality_code = src.quality_code

            WHEN NOT MATCHED THEN
                INSERT
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
                    src.raw_id,
                    src.source_system_code,
                    src.internal_plant_code,
                    src.source_plant_code,
                    src.device_scope,
                    src.source_device_id,
                    src.source_device_name,
                    src.collect_time_utc,
                    src.time_grain_sec,
                    src.source_metric_name,
                    src.canonical_metric_code,
                    src.metric_value_num,
                    src.unit_code,
                    src.quality_code
                )

            OUTPUT $action INTO @MergeActions;

            SELECT COUNT(*) AS affected_rows
            FROM @MergeActions;

            DROP TABLE #canonical_metric_selected_stage;
            """,
            payload,
        )

        affected = int(cursor.fetchone()[0])
        self.conn.commit()
        return affected

    def upsert_many_rowwise(self, rows: list[dict[str, Any]]) -> int:
        """Legacy row-by-row path kept for emergency troubleshooting."""
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

    def _dedupe_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Deduplicate stage rows by the canonical target key; keep the last row."""
        deduped: dict[tuple[Any, ...], dict[str, Any]] = {}
        for row in rows:
            source_device_id = row.get("source_device_id") or ""
            collect_time_utc = self._ensure_utc_naive(row["collect_time_utc"])
            key = (
                row["source_system_code"],
                row["source_plant_code"],
                row["device_scope"],
                source_device_id,
                collect_time_utc,
                row["canonical_metric_code"],
            )
            deduped[key] = row
        return list(deduped.values())

    def _to_stage_row(self, row: dict[str, Any]) -> dict[str, Any]:
        metric_value = self._to_decimal_or_none(row.get("metric_value_num"))
        return {
            "raw_id": int(row["raw_id"]),
            "source_system_code": row["source_system_code"],
            "internal_plant_code": row["internal_plant_code"],
            "source_plant_code": row["source_plant_code"],
            "device_scope": row["device_scope"],
            "source_device_id": row.get("source_device_id"),
            "source_device_name": row.get("source_device_name"),
            "collect_time_utc": self._format_datetime2_0(self._ensure_utc_naive(row["collect_time_utc"])),
            "time_grain_sec": int(row["time_grain_sec"]),
            "source_metric_name": row["source_metric_name"],
            "canonical_metric_code": row["canonical_metric_code"],
            "metric_value_num": None if metric_value is None else str(metric_value),
            "unit_code": row.get("unit_code"),
            "quality_code": row.get("quality_code", "OK"),
        }

    def _format_datetime2_0(self, dt: Any) -> Any:
        if isinstance(dt, datetime):
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        return dt

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

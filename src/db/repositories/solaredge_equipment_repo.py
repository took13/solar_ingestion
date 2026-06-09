from __future__ import annotations

import json
from typing import Any


class SolarEdgeEquipmentRepository:
    """
    Repository for dbo.dim_solaredge_equipment.

    Purpose:
    - Store SolarEdge equipment master data, especially inverter serial numbers.
    - Keep this lane separate from Huawei dbo.dim_device until mapping semantics are proven.
    """

    def __init__(self, conn):
        self.conn = conn

    def upsert_many(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0

        cursor = self.conn.cursor()
        affected = 0

        for row in rows:
            affected += self.upsert_one(row, cursor=cursor, commit=False)

        self.conn.commit()
        return affected

    def upsert_one(self, row: dict[str, Any], cursor=None, commit: bool = True) -> int:
        own_cursor = cursor is None
        if own_cursor:
            cursor = self.conn.cursor()

        raw_payload_json = self._to_json(row.get("raw_payload"))

        cursor.execute(
            """
            MERGE dbo.dim_solaredge_equipment AS tgt
            USING
            (
                SELECT
                    ? AS source_system_code,
                    ? AS internal_plant_code,
                    ? AS source_plant_code,
                    ? AS equipment_type,
                    ? AS source_device_id,
                    ? AS source_device_name,
                    ? AS manufacturer,
                    ? AS model,
                    ? AS firmware_version,
                    ? AS communication_method,
                    ? AS connected_optimizers,
                    ? AS last_raw_id,
                    ? AS raw_payload_json
            ) AS src
            ON  tgt.source_system_code = src.source_system_code
            AND tgt.source_plant_code = src.source_plant_code
            AND tgt.equipment_type = src.equipment_type
            AND tgt.source_device_id = src.source_device_id

            WHEN MATCHED THEN
                UPDATE SET
                    tgt.internal_plant_code = src.internal_plant_code,
                    tgt.source_device_name = src.source_device_name,
                    tgt.manufacturer = src.manufacturer,
                    tgt.model = src.model,
                    tgt.firmware_version = src.firmware_version,
                    tgt.communication_method = src.communication_method,
                    tgt.connected_optimizers = src.connected_optimizers,
                    tgt.is_active = 1,
                    tgt.last_seen_utc = SYSUTCDATETIME(),
                    tgt.last_raw_id = src.last_raw_id,
                    tgt.raw_payload_json = src.raw_payload_json,
                    tgt.updated_at_utc = SYSUTCDATETIME()

            WHEN NOT MATCHED THEN
                INSERT
                (
                    source_system_code,
                    internal_plant_code,
                    source_plant_code,
                    equipment_type,
                    source_device_id,
                    source_device_name,
                    manufacturer,
                    model,
                    firmware_version,
                    communication_method,
                    connected_optimizers,
                    last_raw_id,
                    raw_payload_json
                )
                VALUES
                (
                    src.source_system_code,
                    src.internal_plant_code,
                    src.source_plant_code,
                    src.equipment_type,
                    src.source_device_id,
                    src.source_device_name,
                    src.manufacturer,
                    src.model,
                    src.firmware_version,
                    src.communication_method,
                    src.connected_optimizers,
                    src.last_raw_id,
                    src.raw_payload_json
                );
            """,
            (
                row["source_system_code"],
                row["internal_plant_code"],
                row["source_plant_code"],
                row["equipment_type"],
                row["source_device_id"],
                row.get("source_device_name"),
                row.get("manufacturer"),
                row.get("model"),
                row.get("firmware_version"),
                row.get("communication_method"),
                row.get("connected_optimizers"),
                row.get("last_raw_id"),
                raw_payload_json,
            ),
        )

        if commit:
            self.conn.commit()

        return 1

    def list_active_inverters(
        self,
        *,
        source_system_code: str = "SOLAREDGE",
        internal_plant_code: str | None = None,
        source_plant_code: str | None = None,
    ) -> list[dict[str, Any]]:
        sql = """
            SELECT
                source_system_code,
                internal_plant_code,
                source_plant_code,
                serial_number,
                inverter_name,
                manufacturer,
                model,
                firmware_version,
                connected_optimizers,
                last_seen_utc,
                last_raw_id
            FROM dbo.vw_solaredge_active_inverter
            WHERE source_system_code = ?
        """
        params: list[Any] = [source_system_code]

        if internal_plant_code:
            sql += " AND internal_plant_code = ?"
            params.append(internal_plant_code)

        if source_plant_code:
            sql += " AND source_plant_code = ?"
            params.append(source_plant_code)

        sql += " ORDER BY internal_plant_code, serial_number"

        cursor = self.conn.cursor()
        cursor.execute(sql, params)
        return self._rows_to_dicts(cursor)

    def _to_json(self, value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, str):
            return value
        return json.dumps(value, ensure_ascii=False, default=str)

    def _rows_to_dicts(self, cursor) -> list[dict[str, Any]]:
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

from __future__ import annotations

from typing import Any


class SourceMappingRepository:
    """
    อ่าน plant mapping ระหว่าง internal plant กับ source plant

    ตัวอย่าง:
    - source_system_code = SOLAREDGE
    - source_plant_code = SolarEdge siteId
    - internal_plant_code = plant code กลางของเรา
    """

    def __init__(self, conn):
        self.conn = conn

    def get_active_plant_maps(
        self,
        source_system_code: str,
        source_plant_code: str | None = None,
    ) -> list[dict[str, Any]]:
        sql = """
            SELECT
                internal_plant_code,
                source_system_code,
                source_plant_code,
                source_plant_name,
                timezone_name,
                latitude,
                longitude,
                capacity_kwp,
                api_key_secret_name,
                is_active
            FROM dbo.dim_plant_source_map
            WHERE source_system_code = ?
            AND is_active = 1
        """

        params: list[Any] = [source_system_code]

        if source_plant_code:
            sql += " AND source_plant_code = ?"
            params.append(source_plant_code)

        sql += " ORDER BY internal_plant_code, source_plant_code"

        cursor = self.conn.cursor()
        cursor.execute(sql, params)

        return self._rows_to_dicts(cursor)

    def get_one_active_plant_map(
        self,
        source_system_code: str,
        source_plant_code: str,
    ) -> dict[str, Any] | None:
        rows = self.get_active_plant_maps(
            source_system_code=source_system_code,
            source_plant_code=source_plant_code,
        )

        if not rows:
            return None

        return rows[0]

    def _rows_to_dicts(self, cursor) -> list[dict[str, Any]]:
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
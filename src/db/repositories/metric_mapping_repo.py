from __future__ import annotations

from typing import Any


class MetricMappingRepository:
    """
    อ่าน metric mapping สำหรับ canonical selected norm

    ใช้เพื่อแปลง:
    source metric name → canonical metric code + unit + multiplier
    """

    def __init__(self, conn):
        self.conn = conn

    def get_enabled_mappings(
        self,
        source_system_code: str,
        endpoint_name: str | None = None,
    ) -> list[dict[str, Any]]:
        sql = """
            SELECT
                source_system_code,
                endpoint_name,
                source_device_scope,
                source_metric_name,
                canonical_metric_code,
                canonical_unit_code,
                multiplier_to_canonical,
                target_mart,
                target_pi,
                target_rca,
                target_report,
                retention_level
            FROM norm.metric_mapping
            WHERE source_system_code = ?
              AND is_enabled = 1
        """

        params: list[Any] = [source_system_code]

        if endpoint_name:
            sql += " AND endpoint_name = ?"
            params.append(endpoint_name)

        sql += """
            ORDER BY
                endpoint_name,
                source_device_scope,
                source_metric_name,
                canonical_metric_code
        """

        cursor = self.conn.cursor()
        cursor.execute(sql, params)

        return self._rows_to_dicts(cursor)

    def build_mapping_lookup(
        self,
        source_system_code: str,
        endpoint_name: str,
    ) -> dict[tuple[str, str], list[dict[str, Any]]]:
        """
        คืนค่า lookup:
        (source_device_scope, source_metric_name) -> list[mapping]

        ใช้ list เพราะ source metric เดียวกันอาจ map ได้หลาย canonical code
        เช่น SolarEdge power:
        - active_power_w
        - active_power_kw
        """

        rows = self.get_enabled_mappings(
            source_system_code=source_system_code,
            endpoint_name=endpoint_name,
        )

        lookup: dict[tuple[str, str], list[dict[str, Any]]] = {}

        for row in rows:
            key = (
                str(row["source_device_scope"]),
                str(row["source_metric_name"]),
            )
            lookup.setdefault(key, []).append(row)

        return lookup

    def _rows_to_dicts(self, cursor) -> list[dict[str, Any]]:
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
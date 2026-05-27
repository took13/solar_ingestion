from __future__ import annotations

from typing import Any


class MetricWhitelistRepository:
    """
    Read-only repository for norm.metric_whitelist.

    Purpose:
    - Used by Huawei generic normalizer in a later milestone
    - This milestone is read-only smoke validation only
    - Do not write/modify whitelist from Python in this step

    Expected table key:
        source_system_code, source_api, dev_type_id, metric_name
    """

    def __init__(self, conn):
        self.conn = conn

    def list_enabled_metrics(
        self,
        *,
        source_system_code: str = "HUAWEI",
        source_api: str | None = None,
        dev_type_id: int | None = None,
    ) -> list[dict[str, Any]]:
        sql = """
            SELECT
                source_system_code,
                source_api,
                dev_type_id,
                metric_name,
                is_enabled,
                keep_null,
                keep_raw_text,
                target_layer,
                use_case,
                retention_level,
                min_keep_days,
                created_at_utc,
                updated_at_utc
            FROM norm.metric_whitelist
            WHERE source_system_code = ?
              AND is_enabled = 1
        """

        params: list[Any] = [source_system_code]

        if source_api is not None:
            sql += " AND source_api = ?"
            params.append(source_api)

        if dev_type_id is not None:
            sql += " AND dev_type_id = ?"
            params.append(dev_type_id)

        sql += """
            ORDER BY
                source_api,
                dev_type_id,
                metric_name
        """

        cursor = self.conn.cursor()
        cursor.execute(sql, params)
        return self._rows_to_dicts(cursor)

    def get_enabled_metric_set(
        self,
        *,
        source_system_code: str = "HUAWEI",
        source_api: str,
        dev_type_id: int,
    ) -> set[str]:
        rows = self.list_enabled_metrics(
            source_system_code=source_system_code,
            source_api=source_api,
            dev_type_id=dev_type_id,
        )
        return {str(row["metric_name"]) for row in rows}

    def get_enabled_metric_rules(
        self,
        *,
        source_system_code: str = "HUAWEI",
        source_api: str,
        dev_type_id: int,
    ) -> dict[str, dict[str, Any]]:
        rows = self.list_enabled_metrics(
            source_system_code=source_system_code,
            source_api=source_api,
            dev_type_id=dev_type_id,
        )
        return {str(row["metric_name"]): row for row in rows}

    def summarize_enabled_metrics(
        self,
        *,
        source_system_code: str = "HUAWEI",
    ) -> list[dict[str, Any]]:
        sql = """
            SELECT
                source_system_code,
                source_api,
                dev_type_id,
                COUNT(*) AS enabled_metric_count,
                SUM(CASE WHEN keep_null = 1 THEN 1 ELSE 0 END) AS keep_null_count,
                SUM(CASE WHEN keep_raw_text = 1 THEN 1 ELSE 0 END) AS keep_raw_text_count
            FROM norm.metric_whitelist
            WHERE source_system_code = ?
              AND is_enabled = 1
            GROUP BY
                source_system_code,
                source_api,
                dev_type_id
            ORDER BY
                source_api,
                dev_type_id;
        """

        cursor = self.conn.cursor()
        cursor.execute(sql, (source_system_code,))
        return self._rows_to_dicts(cursor)

    def list_high_volume_metrics_enabled(
        self,
        *,
        source_system_code: str = "HUAWEI",
    ) -> list[dict[str, Any]]:
        """
        Guardrail:
        PV string and MPPT metrics should not be enabled in baseline restart.
        They may be allowed later only with explicit RCA-only design.
        """
        sql = """
            SELECT
                source_system_code,
                source_api,
                dev_type_id,
                metric_name,
                target_layer,
                use_case,
                retention_level,
                min_keep_days,
                updated_at_utc
            FROM norm.metric_whitelist
            WHERE source_system_code = ?
              AND is_enabled = 1
              AND (
                    metric_name LIKE 'pv%[_]u'
                 OR metric_name LIKE 'pv%[_]i'
                 OR metric_name LIKE 'mppt%'
              )
            ORDER BY
                source_api,
                dev_type_id,
                metric_name;
        """

        cursor = self.conn.cursor()
        cursor.execute(sql, (source_system_code,))
        return self._rows_to_dicts(cursor)

    def _rows_to_dicts(self, cursor) -> list[dict[str, Any]]:
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
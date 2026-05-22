from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


class SolarPlantMartRepository:
    """
    Load canonical selected metrics into mart fact tables.

    Source:
        norm.canonical_metric_selected

    Target:
        mart.fact_solar_plant_power_15min
        mart.fact_solar_plant_energy_15min

    Design:
    - mart เป็น primary query layer
    - canonical norm เป็น selected integration layer
    - ไม่แตะ Huawei legacy pipeline
    """

    def __init__(self, conn):
        self.conn = conn

    def load_power_15min(
        self,
        *,
        source_system_code: str,
        internal_plant_code: str | None = None,
        source_plant_code: str | None = None,
        start_utc: datetime | str | None = None,
        end_utc: datetime | str | None = None,
    ) -> int:
        cursor = self.conn.cursor()

        start_utc = self._ensure_utc_naive(start_utc)
        end_utc = self._ensure_utc_naive(end_utc)

        cursor.execute(
            """
            SET NOCOUNT ON;

            DECLARE @MergeActions TABLE
            (
                action_name nvarchar(20)
            );

            MERGE mart.fact_solar_plant_power_15min AS tgt
            USING
            (
                SELECT
                    c.internal_plant_code,
                    c.source_system_code,
                    c.source_plant_code,
                    c.collect_time_utc,

                    MAX(CASE
                        WHEN c.canonical_metric_code = 'active_power_kw'
                        THEN c.metric_value_num
                    END) AS active_power_kw,

                    MAX(CASE
                        WHEN c.canonical_metric_code = 'active_power_kw'
                        THEN c.raw_id
                    END) AS raw_id
                FROM norm.canonical_metric_selected c
                WHERE c.source_system_code = ?
                  AND c.device_scope = 'PLANT'
                  AND c.canonical_metric_code = 'active_power_kw'
                  AND (? IS NULL OR c.internal_plant_code = ?)
                  AND (? IS NULL OR c.source_plant_code = ?)
                  AND (? IS NULL OR c.collect_time_utc >= ?)
                  AND (? IS NULL OR c.collect_time_utc < ?)
                GROUP BY
                    c.internal_plant_code,
                    c.source_system_code,
                    c.source_plant_code,
                    c.collect_time_utc
            ) AS src
            ON  tgt.internal_plant_code = src.internal_plant_code
            AND tgt.source_system_code = src.source_system_code
            AND tgt.collect_time_utc = src.collect_time_utc

            WHEN MATCHED THEN
                UPDATE SET
                    tgt.source_plant_code = src.source_plant_code,
                    tgt.active_power_kw = src.active_power_kw,
                    tgt.active_power_source_unit = 'kW',
                    tgt.raw_id = src.raw_id,
                    tgt.data_quality_status =
                        CASE
                            WHEN src.active_power_kw IS NULL THEN 'NO_VALUE'
                            WHEN src.active_power_kw < 0 THEN 'NEGATIVE_VALUE'
                            ELSE 'OK'
                        END,
                    tgt.updated_at_utc = SYSUTCDATETIME()

            WHEN NOT MATCHED THEN
                INSERT
                (
                    internal_plant_code,
                    source_system_code,
                    source_plant_code,
                    collect_time_utc,
                    active_power_kw,
                    active_power_source_unit,
                    raw_id,
                    data_quality_status
                )
                VALUES
                (
                    src.internal_plant_code,
                    src.source_system_code,
                    src.source_plant_code,
                    src.collect_time_utc,
                    src.active_power_kw,
                    'kW',
                    src.raw_id,
                    CASE
                        WHEN src.active_power_kw IS NULL THEN 'NO_VALUE'
                        WHEN src.active_power_kw < 0 THEN 'NEGATIVE_VALUE'
                        ELSE 'OK'
                    END
                )

            OUTPUT $action INTO @MergeActions;

            SELECT COUNT(*) AS affected_rows
            FROM @MergeActions;
            """,
            (
                source_system_code,
                internal_plant_code,
                internal_plant_code,
                source_plant_code,
                source_plant_code,
                start_utc,
                start_utc,
                end_utc,
                end_utc,
            ),
        )

        affected_rows = int(cursor.fetchone()[0])
        self.conn.commit()
        return affected_rows

    def load_energy_15min(
        self,
        *,
        source_system_code: str,
        internal_plant_code: str | None = None,
        source_plant_code: str | None = None,
        start_utc: datetime | str | None = None,
        end_utc: datetime | str | None = None,
    ) -> int:
        cursor = self.conn.cursor()

        start_utc = self._ensure_utc_naive(start_utc)
        end_utc = self._ensure_utc_naive(end_utc)

        cursor.execute(
            """
            SET NOCOUNT ON;

            DECLARE @MergeActions TABLE
            (
                action_name nvarchar(20)
            );

            MERGE mart.fact_solar_plant_energy_15min AS tgt
            USING
            (
                SELECT
                    c.internal_plant_code,
                    c.source_system_code,
                    c.source_plant_code,
                    c.collect_time_utc,

                    MAX(CASE
                        WHEN c.canonical_metric_code = 'production_energy_kwh'
                        THEN c.metric_value_num
                    END) AS production_energy_kwh,

                    MAX(CASE
                        WHEN c.canonical_metric_code = 'feed_in_energy_kwh'
                        THEN c.metric_value_num
                    END) AS feed_in_energy_kwh,

                    MAX(CASE
                        WHEN c.canonical_metric_code = 'purchased_energy_kwh'
                        THEN c.metric_value_num
                    END) AS purchased_energy_kwh,

                    MAX(CASE
                        WHEN c.canonical_metric_code = 'self_consumption_energy_kwh'
                        THEN c.metric_value_num
                    END) AS self_consumption_energy_kwh,

                    MAX(c.raw_id) AS raw_id
                FROM norm.canonical_metric_selected c
                WHERE c.source_system_code = ?
                  AND c.device_scope = 'PLANT'
                  AND c.canonical_metric_code IN
                  (
                      'production_energy_kwh',
                      'feed_in_energy_kwh',
                      'purchased_energy_kwh',
                      'self_consumption_energy_kwh'
                  )
                  AND (? IS NULL OR c.internal_plant_code = ?)
                  AND (? IS NULL OR c.source_plant_code = ?)
                  AND (? IS NULL OR c.collect_time_utc >= ?)
                  AND (? IS NULL OR c.collect_time_utc < ?)
                GROUP BY
                    c.internal_plant_code,
                    c.source_system_code,
                    c.source_plant_code,
                    c.collect_time_utc
            ) AS src
            ON  tgt.internal_plant_code = src.internal_plant_code
            AND tgt.source_system_code = src.source_system_code
            AND tgt.collect_time_utc = src.collect_time_utc

            WHEN MATCHED THEN
                UPDATE SET
                    tgt.source_plant_code = src.source_plant_code,
                    tgt.production_energy_kwh = src.production_energy_kwh,
                    tgt.feed_in_energy_kwh = src.feed_in_energy_kwh,
                    tgt.purchased_energy_kwh = src.purchased_energy_kwh,
                    tgt.self_consumption_energy_kwh = src.self_consumption_energy_kwh,
                    tgt.raw_id = src.raw_id,
                    tgt.data_quality_status =
                        CASE
                            WHEN src.production_energy_kwh IS NULL
                             AND src.feed_in_energy_kwh IS NULL
                             AND src.purchased_energy_kwh IS NULL
                             AND src.self_consumption_energy_kwh IS NULL
                            THEN 'NO_VALUE'
                            ELSE 'OK'
                        END,
                    tgt.updated_at_utc = SYSUTCDATETIME()

            WHEN NOT MATCHED THEN
                INSERT
                (
                    internal_plant_code,
                    source_system_code,
                    source_plant_code,
                    collect_time_utc,
                    production_energy_kwh,
                    feed_in_energy_kwh,
                    purchased_energy_kwh,
                    self_consumption_energy_kwh,
                    raw_id,
                    data_quality_status
                )
                VALUES
                (
                    src.internal_plant_code,
                    src.source_system_code,
                    src.source_plant_code,
                    src.collect_time_utc,
                    src.production_energy_kwh,
                    src.feed_in_energy_kwh,
                    src.purchased_energy_kwh,
                    src.self_consumption_energy_kwh,
                    src.raw_id,
                    CASE
                        WHEN src.production_energy_kwh IS NULL
                         AND src.feed_in_energy_kwh IS NULL
                         AND src.purchased_energy_kwh IS NULL
                         AND src.self_consumption_energy_kwh IS NULL
                        THEN 'NO_VALUE'
                        ELSE 'OK'
                    END
                )

            OUTPUT $action INTO @MergeActions;

            SELECT COUNT(*) AS affected_rows
            FROM @MergeActions;
            """,
            (
                source_system_code,
                internal_plant_code,
                internal_plant_code,
                source_plant_code,
                source_plant_code,
                start_utc,
                start_utc,
                end_utc,
                end_utc,
            ),
        )

        affected_rows = int(cursor.fetchone()[0])
        self.conn.commit()
        return affected_rows

    def _ensure_utc_naive(self, dt: Any):
        if dt is None:
            return None

        if isinstance(dt, str):
            return dt

        if isinstance(dt, datetime):
            if dt.tzinfo is None:
                return dt

            return dt.astimezone(timezone.utc).replace(tzinfo=None)

        return dt
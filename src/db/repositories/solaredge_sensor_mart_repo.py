from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


class SolarEdgeSensorMartRepository:
    """Load SolarEdge sensor canonical metrics into mart.fact_solaredge_sensor_5min."""

    def __init__(self, conn):
        self.conn = conn

    def load_sensor_5min(
        self,
        *,
        source_system_code: str,
        internal_plant_code: str | None = None,
        source_plant_code: str | None = None,
        source_device_id: str | None = None,
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

            MERGE mart.fact_solaredge_sensor_5min AS tgt
            USING
            (
                SELECT
                    c.internal_plant_code,
                    c.source_system_code,
                    c.source_plant_code,
                    c.source_device_id,
                    MAX(c.source_device_name) AS source_device_name,
                    c.collect_time_utc,
                    DATEADD(hour, 7, c.collect_time_utc) AS collect_time_local,

                    MAX(CASE WHEN c.canonical_metric_code = 'global_horizontal_irradiance_wm2' THEN c.metric_value_num END) AS global_horizontal_irradiance_wm2,
                    MAX(CASE WHEN c.canonical_metric_code = 'plane_of_array_irradiance_wm2' THEN c.metric_value_num END) AS plane_of_array_irradiance_wm2,
                    MAX(CASE WHEN c.canonical_metric_code = 'direct_irradiance_wm2' THEN c.metric_value_num END) AS direct_irradiance_wm2,
                    MAX(CASE WHEN c.canonical_metric_code = 'diffuse_horizontal_irradiance_wm2' THEN c.metric_value_num END) AS diffuse_horizontal_irradiance_wm2,
                    MAX(CASE WHEN c.canonical_metric_code = 'ambient_temperature_c' THEN c.metric_value_num END) AS ambient_temperature_c,
                    MAX(CASE WHEN c.canonical_metric_code = 'module_temperature_c' THEN c.metric_value_num END) AS module_temperature_c,
                    MAX(CASE WHEN c.canonical_metric_code = 'wind_speed_raw' THEN c.metric_value_num END) AS wind_speed_raw,
                    MAX(c.raw_id) AS raw_id
                FROM norm.canonical_metric_selected c
                WHERE c.source_system_code = ?
                  AND c.device_scope = 'SENSOR'
                  AND c.canonical_metric_code IN
                  (
                      'global_horizontal_irradiance_wm2',
                      'plane_of_array_irradiance_wm2',
                      'direct_irradiance_wm2',
                      'diffuse_horizontal_irradiance_wm2',
                      'ambient_temperature_c',
                      'module_temperature_c',
                      'wind_speed_raw'
                  )
                  AND (? IS NULL OR c.internal_plant_code = ?)
                  AND (? IS NULL OR c.source_plant_code = ?)
                  AND (? IS NULL OR c.source_device_id = ?)
                  AND (? IS NULL OR c.collect_time_utc >= ?)
                  AND (? IS NULL OR c.collect_time_utc < ?)
                GROUP BY
                    c.internal_plant_code,
                    c.source_system_code,
                    c.source_plant_code,
                    c.source_device_id,
                    c.collect_time_utc
            ) AS src
            ON  tgt.internal_plant_code = src.internal_plant_code
            AND tgt.source_system_code = src.source_system_code
            AND tgt.source_device_id = src.source_device_id
            AND tgt.collect_time_utc = src.collect_time_utc

            WHEN MATCHED THEN
                UPDATE SET
                    tgt.source_plant_code = src.source_plant_code,
                    tgt.source_device_name = src.source_device_name,
                    tgt.collect_time_local = src.collect_time_local,
                    tgt.global_horizontal_irradiance_wm2 = src.global_horizontal_irradiance_wm2,
                    tgt.plane_of_array_irradiance_wm2 = src.plane_of_array_irradiance_wm2,
                    tgt.direct_irradiance_wm2 = src.direct_irradiance_wm2,
                    tgt.diffuse_horizontal_irradiance_wm2 = src.diffuse_horizontal_irradiance_wm2,
                    tgt.irradiance_wm2_best_effort = COALESCE(
                        src.plane_of_array_irradiance_wm2,
                        src.global_horizontal_irradiance_wm2,
                        src.direct_irradiance_wm2,
                        src.diffuse_horizontal_irradiance_wm2
                    ),
                    tgt.ambient_temperature_c = src.ambient_temperature_c,
                    tgt.module_temperature_c = src.module_temperature_c,
                    tgt.wind_speed_raw = src.wind_speed_raw,
                    tgt.raw_id = src.raw_id,
                    tgt.data_quality_status =
                        CASE
                            WHEN src.global_horizontal_irradiance_wm2 IS NULL
                             AND src.plane_of_array_irradiance_wm2 IS NULL
                             AND src.direct_irradiance_wm2 IS NULL
                             AND src.diffuse_horizontal_irradiance_wm2 IS NULL
                             AND src.ambient_temperature_c IS NULL
                             AND src.module_temperature_c IS NULL
                             AND src.wind_speed_raw IS NULL
                            THEN 'NO_VALUE'
                            WHEN src.global_horizontal_irradiance_wm2 IS NULL
                             AND src.plane_of_array_irradiance_wm2 IS NULL
                             AND src.direct_irradiance_wm2 IS NULL
                             AND src.diffuse_horizontal_irradiance_wm2 IS NULL
                            THEN 'NO_IRRADIANCE'
                            ELSE 'OK'
                        END,
                    tgt.updated_at_utc = SYSUTCDATETIME()

            WHEN NOT MATCHED THEN
                INSERT
                (
                    internal_plant_code,
                    source_system_code,
                    source_plant_code,
                    source_device_id,
                    source_device_name,
                    collect_time_utc,
                    collect_time_local,
                    global_horizontal_irradiance_wm2,
                    plane_of_array_irradiance_wm2,
                    direct_irradiance_wm2,
                    diffuse_horizontal_irradiance_wm2,
                    irradiance_wm2_best_effort,
                    ambient_temperature_c,
                    module_temperature_c,
                    wind_speed_raw,
                    raw_id,
                    data_quality_status
                )
                VALUES
                (
                    src.internal_plant_code,
                    src.source_system_code,
                    src.source_plant_code,
                    src.source_device_id,
                    src.source_device_name,
                    src.collect_time_utc,
                    src.collect_time_local,
                    src.global_horizontal_irradiance_wm2,
                    src.plane_of_array_irradiance_wm2,
                    src.direct_irradiance_wm2,
                    src.diffuse_horizontal_irradiance_wm2,
                    COALESCE(
                        src.plane_of_array_irradiance_wm2,
                        src.global_horizontal_irradiance_wm2,
                        src.direct_irradiance_wm2,
                        src.diffuse_horizontal_irradiance_wm2
                    ),
                    src.ambient_temperature_c,
                    src.module_temperature_c,
                    src.wind_speed_raw,
                    src.raw_id,
                    CASE
                        WHEN src.global_horizontal_irradiance_wm2 IS NULL
                         AND src.plane_of_array_irradiance_wm2 IS NULL
                         AND src.direct_irradiance_wm2 IS NULL
                         AND src.diffuse_horizontal_irradiance_wm2 IS NULL
                         AND src.ambient_temperature_c IS NULL
                         AND src.module_temperature_c IS NULL
                         AND src.wind_speed_raw IS NULL
                        THEN 'NO_VALUE'
                        WHEN src.global_horizontal_irradiance_wm2 IS NULL
                         AND src.plane_of_array_irradiance_wm2 IS NULL
                         AND src.direct_irradiance_wm2 IS NULL
                         AND src.diffuse_horizontal_irradiance_wm2 IS NULL
                        THEN 'NO_IRRADIANCE'
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
                source_device_id,
                source_device_id,
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

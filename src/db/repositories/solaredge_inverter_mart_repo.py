from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


class SolarEdgeInverterMartRepository:
    """Load SolarEdge inverter technical canonical metrics into mart.fact_solaredge_inverter_technical_5min."""

    def __init__(self, conn):
        self.conn = conn

    def load_technical_5min(
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

            MERGE mart.fact_solaredge_inverter_technical_5min AS tgt
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

                    MAX(CASE WHEN c.canonical_metric_code = 'inverter_active_power_kw' THEN c.metric_value_num END) AS active_power_kw,
                    MAX(CASE WHEN c.canonical_metric_code = 'inverter_total_energy_kwh' THEN c.metric_value_num END) AS total_energy_kwh,
                    MAX(CASE WHEN c.canonical_metric_code = 'inverter_dc_voltage_v' THEN c.metric_value_num END) AS dc_voltage_v,
                    MAX(CASE WHEN c.canonical_metric_code = 'inverter_temperature_c' THEN c.metric_value_num END) AS temperature_c,
                    MAX(CASE WHEN c.canonical_metric_code = 'inverter_power_limit_pct' THEN c.metric_value_num END) AS power_limit_pct,
                    MAX(c.raw_id) AS raw_id
                FROM norm.canonical_metric_selected c
                WHERE c.source_system_code = ?
                  AND c.device_scope = 'INVERTER'
                  AND c.canonical_metric_code IN
                  (
                      'inverter_active_power_kw',
                      'inverter_total_energy_kwh',
                      'inverter_dc_voltage_v',
                      'inverter_temperature_c',
                      'inverter_power_limit_pct'
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
                    tgt.active_power_kw = src.active_power_kw,
                    tgt.total_energy_kwh = src.total_energy_kwh,
                    tgt.dc_voltage_v = src.dc_voltage_v,
                    tgt.temperature_c = src.temperature_c,
                    tgt.power_limit_pct = src.power_limit_pct,
                    tgt.raw_id = src.raw_id,
                    tgt.data_quality_status =
                        CASE
                            WHEN src.active_power_kw IS NULL AND src.total_energy_kwh IS NULL THEN 'NO_VALUE'
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
                    active_power_kw,
                    total_energy_kwh,
                    dc_voltage_v,
                    temperature_c,
                    power_limit_pct,
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
                    src.active_power_kw,
                    src.total_energy_kwh,
                    src.dc_voltage_v,
                    src.temperature_c,
                    src.power_limit_pct,
                    src.raw_id,
                    CASE
                        WHEN src.active_power_kw IS NULL AND src.total_energy_kwh IS NULL THEN 'NO_VALUE'
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

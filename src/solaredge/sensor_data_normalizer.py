from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo


class SolarEdgeSensorDataNormalizer:
    """
    Normalize SolarEdge Sensor Data response into norm.canonical_metric_selected rows.

    Observed endpoint response shape from M6 probe:
        {
          "siteSensors": {
            "data": [
              {
                "connectedTo": "Gateway 1",
                "count": 24,
                "telemetries": [
                  {
                    "date": "2026-06-08 10:03:07",
                    "ambientTemperature": 34.0781,
                    "moduleTemperature": 49.3309
                  }
                ]
              },
              {
                "connectedTo": "Gateway 2",
                "count": 25,
                "telemetries": [
                  {
                    "date": "2026-06-08 10:03:00",
                    "windSpeed": 0.197881,
                    "globalHorizontalIrradiance": 321.5
                  }
                ]
              }
            ]
          }
        }

    Important design decisions:
    - SolarEdge sensor timestamp is local site time.
    - Sensor timestamps are offset from exact 5-minute boundary, for example
      10:03:07, 10:08:07, 10:13:07. We bucket to 5-minute bucket start,
      aligned with the inverter technical lane.
    - Missing values are skipped, not converted to 0.
    - M6 proved inventory can miss some irradiance capability, so this normalizer
      trusts telemetry measurement keys rather than inventory only.
    """

    SOURCE_SYSTEM = "SOLAREDGE"
    DEVICE_SCOPE = "SENSOR"
    TIME_GRAIN_SEC = 300

    SELECTED_KEYS: tuple[str, ...] = (
        "globalHorizontalIrradiance",
        "planeOfArrayIrradiance",
        "directIrradiance",
        "diffuseHorizontalIrradiance",
        "ambientTemperature",
        "moduleTemperature",
        "windSpeed",
    )

    def __init__(self, mapping_lookup: dict[tuple[str, str], list[dict[str, Any]]]):
        self.mapping_lookup = mapping_lookup

    def normalize(
        self,
        *,
        raw_id: int,
        response_json: dict[str, Any],
        internal_plant_code: str,
        source_plant_code: str,
        timezone_name: str = "Asia/Bangkok",
    ) -> list[dict[str, Any]]:
        groups = self._extract_groups(response_json)
        rows: list[dict[str, Any]] = []

        for group_index, group in enumerate(groups, start=1):
            if not isinstance(group, dict):
                continue

            connected_to = (
                group.get("connectedTo")
                or group.get("connected_to")
                or group.get("gateway")
                or f"SENSOR_GROUP_{group_index}"
            )
            source_device_id = str(connected_to)
            source_device_name = str(connected_to)

            telemetries = group.get("telemetries") or []
            for telemetry in telemetries:
                if not isinstance(telemetry, dict):
                    continue

                raw_date_text = (
                    telemetry.get("date")
                    or telemetry.get("time")
                    or telemetry.get("timestamp")
                )
                if not raw_date_text:
                    continue

                collect_time_utc = self._parse_local_time_to_bucket_utc(
                    str(raw_date_text),
                    timezone_name,
                )

                for source_metric_name in self.SELECTED_KEYS:
                    source_value = telemetry.get(source_metric_name)
                    if source_value is None:
                        continue

                    mappings = self.mapping_lookup.get(
                        (self.DEVICE_SCOPE, source_metric_name.strip().upper()),
                        [],
                    )
                    if not mappings:
                        continue

                    for mapping in mappings:
                        rows.append(
                            self._build_row(
                                raw_id=raw_id,
                                internal_plant_code=internal_plant_code,
                                source_plant_code=source_plant_code,
                                source_device_id=source_device_id,
                                source_device_name=source_device_name,
                                collect_time_utc=collect_time_utc,
                                source_metric_name=source_metric_name,
                                source_value=source_value,
                                mapping=mapping,
                            )
                        )

        return rows

    def _extract_groups(self, response_json: dict[str, Any]) -> list[dict[str, Any]]:
        root = (
            response_json.get("siteSensors")
            or response_json.get("SiteSensors")
            or response_json
        )
        if not isinstance(root, dict):
            return []

        data = root.get("data") or root.get("list") or []
        if not isinstance(data, list):
            return []

        return data

    def _build_row(
        self,
        *,
        raw_id: int,
        internal_plant_code: str,
        source_plant_code: str,
        source_device_id: str,
        source_device_name: str | None,
        collect_time_utc: datetime,
        source_metric_name: str,
        source_value: Any,
        mapping: dict[str, Any],
    ) -> dict[str, Any]:
        value = Decimal(str(source_value))
        multiplier = Decimal(str(mapping["multiplier_to_canonical"]))
        canonical_value = value * multiplier

        return {
            "raw_id": raw_id,
            "source_system_code": self.SOURCE_SYSTEM,
            "internal_plant_code": internal_plant_code,
            "source_plant_code": source_plant_code,
            "device_scope": self.DEVICE_SCOPE,
            "source_device_id": source_device_id,
            "source_device_name": source_device_name,
            "collect_time_utc": collect_time_utc,
            "time_grain_sec": self.TIME_GRAIN_SEC,
            "source_metric_name": source_metric_name,
            "canonical_metric_code": mapping["canonical_metric_code"],
            "metric_value_num": canonical_value,
            "unit_code": mapping["canonical_unit_code"],
            "quality_code": "OK",
        }

    def _parse_local_time_to_bucket_utc(
        self,
        date_text: str,
        timezone_name: str,
    ) -> datetime:
        local_tz = ZoneInfo(timezone_name)
        local_dt = datetime.strptime(date_text, "%Y-%m-%d %H:%M:%S")
        bucket_minute = (local_dt.minute // 5) * 5
        bucket_dt = local_dt.replace(minute=bucket_minute, second=0, microsecond=0)
        bucket_dt = bucket_dt.replace(tzinfo=local_tz)
        return bucket_dt.astimezone(timezone.utc).replace(tzinfo=None)

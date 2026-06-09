from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo


class SolarEdgeInverterTechnicalNormalizer:
    """
    Normalize SolarEdge Inverter Technical Data response into norm.canonical_metric_selected rows.

    Endpoint response shape observed from API probe:
        {
          "data": {
            "count": 12,
            "telemetries": [
              {
                "date": "2026-06-08 10:03:13",
                "totalActivePower": 27738.291,
                "dcVoltage": 750.487,
                "totalEnergy": 316690050.0,
                "temperature": 52.990116,
                "powerLimit": 100.0,
                "L1Data": {"activePower": ...},
                "L2Data": {...},
                "L3Data": {...}
              }
            ]
          }
        }

    Important design decisions:
    - SolarEdge telemetry timestamp is local site time.
    - Inverter technical timestamps are not exactly aligned to 5-minute boundary
      (for example 10:03:13, 10:08:13, ...). We bucket them to 5-minute bucket start.
    - Missing numeric value is skipped, not converted to 0.
    - Only selected metrics defined in norm.metric_mapping are normalized.
    """

    SOURCE_SYSTEM = "SOLAREDGE"
    DEVICE_SCOPE = "INVERTER"
    TIME_GRAIN_SEC = 300

    SELECTED_PATHS: dict[str, tuple[str, ...]] = {
        "totalActivePower": ("totalActivePower",),
        "totalEnergy": ("totalEnergy",),
        "dcVoltage": ("dcVoltage",),
        "temperature": ("temperature",),
        "powerLimit": ("powerLimit",),
        "groundFaultResistance": ("groundFaultResistance",),
        "vL1To2": ("vL1To2",),
        "vL2To3": ("vL2To3",),
        "vL3To1": ("vL3To1",),
        "L1Data.acCurrent": ("L1Data", "acCurrent"),
        "L2Data.acCurrent": ("L2Data", "acCurrent"),
        "L3Data.acCurrent": ("L3Data", "acCurrent"),
        "L1Data.acVoltage": ("L1Data", "acVoltage"),
        "L2Data.acVoltage": ("L2Data", "acVoltage"),
        "L3Data.acVoltage": ("L3Data", "acVoltage"),
        "L1Data.acFrequency": ("L1Data", "acFrequency"),
        "L2Data.acFrequency": ("L2Data", "acFrequency"),
        "L3Data.acFrequency": ("L3Data", "acFrequency"),
        "L1Data.activePower": ("L1Data", "activePower"),
        "L2Data.activePower": ("L2Data", "activePower"),
        "L3Data.activePower": ("L3Data", "activePower"),
        "L1Data.reactivePower": ("L1Data", "reactivePower"),
        "L2Data.reactivePower": ("L2Data", "reactivePower"),
        "L3Data.reactivePower": ("L3Data", "reactivePower"),
        "L1Data.apparentPower": ("L1Data", "apparentPower"),
        "L2Data.apparentPower": ("L2Data", "apparentPower"),
        "L3Data.apparentPower": ("L3Data", "apparentPower"),
        "L1Data.cosPhi": ("L1Data", "cosPhi"),
        "L2Data.cosPhi": ("L2Data", "cosPhi"),
        "L3Data.cosPhi": ("L3Data", "cosPhi"),
    }

    def __init__(self, mapping_lookup: dict[tuple[str, str], list[dict[str, Any]]]):
        self.mapping_lookup = mapping_lookup

    def normalize(
        self,
        *,
        raw_id: int,
        response_json: dict[str, Any],
        internal_plant_code: str,
        source_plant_code: str,
        source_device_id: str,
        source_device_name: str | None,
        timezone_name: str = "Asia/Bangkok",
    ) -> list[dict[str, Any]]:
        data = response_json.get("data") or {}
        telemetries = data.get("telemetries") or []

        rows: list[dict[str, Any]] = []

        for telemetry in telemetries:
            raw_date_text = telemetry.get("date")
            if not raw_date_text:
                continue

            collect_time_utc = self._parse_local_time_to_bucket_utc(
                raw_date_text,
                timezone_name,
            )

            for source_metric_name, path in self.SELECTED_PATHS.items():
                source_value = self._get_path(telemetry, path)
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

    def _get_path(self, obj: dict[str, Any], path: tuple[str, ...]) -> Any:
        current: Any = obj
        for part in path:
            if not isinstance(current, dict):
                return None
            current = current.get(part)
            if current is None:
                return None
        return current

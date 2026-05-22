from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo


class SolarEdgeCanonicalNormalizer:
    """
    แปลง SolarEdge response JSON เป็น norm.canonical_metric_selected rows

    Supported pilot endpoints:
    - sitePower
      Response shape:
      {
        "power": {
          "timeUnit": "QUARTER_OF_AN_HOUR",
          "unit": "W",
          "values": [{"date": "...", "value": 123.45}]
        }
      }

    - energyDetails
      Response shape:
      {
        "energyDetails": {
          "timeUnit": "QUARTER_OF_AN_HOUR",
          "unit": "Wh",
          "meters": [
            {
              "type": "Production",
              "values": [{"date": "...", "value": 1000}]
            }
          ]
        }
      }

    Important:
    - SolarEdge timestamp เป็น local site time
    - เราจะแปลงเป็น UTC ก่อน insert canonical
    - skip NULL values เพื่อป้องกัน storage โตโดยไม่จำเป็น
    - ใช้ norm.metric_mapping เป็นตัวกำหนด selected metrics เท่านั้น
    """

    SOURCE_SYSTEM = "SOLAREDGE"

    def __init__(self, mapping_lookup: dict[tuple[str, str], list[dict[str, Any]]]):
        self.mapping_lookup = mapping_lookup

    def normalize(
        self,
        *,
        raw_id: int,
        endpoint_name: str,
        response_json: dict[str, Any],
        internal_plant_code: str,
        source_plant_code: str,
        timezone_name: str = "Asia/Bangkok",
    ) -> list[dict[str, Any]]:
        if endpoint_name == "sitePower":
            return self._normalize_site_power(
                raw_id=raw_id,
                response_json=response_json,
                internal_plant_code=internal_plant_code,
                source_plant_code=source_plant_code,
                timezone_name=timezone_name,
            )

        if endpoint_name == "energyDetails":
            return self._normalize_energy_details(
                raw_id=raw_id,
                response_json=response_json,
                internal_plant_code=internal_plant_code,
                source_plant_code=source_plant_code,
                timezone_name=timezone_name,
            )

        raise ValueError(f"Unsupported SolarEdge endpoint_name={endpoint_name}")

    def _normalize_site_power(
        self,
        *,
        raw_id: int,
        response_json: dict[str, Any],
        internal_plant_code: str,
        source_plant_code: str,
        timezone_name: str,
    ) -> list[dict[str, Any]]:
        power = response_json.get("power") or {}
        values = power.get("values") or []

        rows: list[dict[str, Any]] = []

        for item in values:
            source_metric_name = "power"
            source_value = item.get("value")

            if source_value is None:
                continue

            collect_time_utc = self._parse_solaredge_local_time_to_utc(
                item.get("date"),
                timezone_name,
            )

            mappings = self.mapping_lookup.get(("PLANT", source_metric_name), [])

            for mapping in mappings:
                rows.append(
                    self._build_row(
                        raw_id=raw_id,
                        internal_plant_code=internal_plant_code,
                        source_plant_code=source_plant_code,
                        device_scope="PLANT",
                        source_device_id=None,
                        source_device_name=None,
                        collect_time_utc=collect_time_utc,
                        time_grain_sec=900,
                        source_metric_name=source_metric_name,
                        source_value=source_value,
                        mapping=mapping,
                    )
                )

        return rows

    def _normalize_energy_details(
        self,
        *,
        raw_id: int,
        response_json: dict[str, Any],
        internal_plant_code: str,
        source_plant_code: str,
        timezone_name: str,
    ) -> list[dict[str, Any]]:
        energy_details = response_json.get("energyDetails") or {}
        meters = energy_details.get("meters") or []

        rows: list[dict[str, Any]] = []

        for meter in meters:
            source_metric_name = meter.get("type")

            if not source_metric_name:
                continue

            values = meter.get("values") or []
            mappings = self.mapping_lookup.get(("PLANT", source_metric_name), [])

            if not mappings:
                continue

            for item in values:
                source_value = item.get("value")

                if source_value is None:
                    continue

                collect_time_utc = self._parse_solaredge_local_time_to_utc(
                    item.get("date"),
                    timezone_name,
                )

                for mapping in mappings:
                    rows.append(
                        self._build_row(
                            raw_id=raw_id,
                            internal_plant_code=internal_plant_code,
                            source_plant_code=source_plant_code,
                            device_scope="PLANT",
                            source_device_id=None,
                            source_device_name=None,
                            collect_time_utc=collect_time_utc,
                            time_grain_sec=900,
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
        device_scope: str,
        source_device_id: str | None,
        source_device_name: str | None,
        collect_time_utc: datetime,
        time_grain_sec: int,
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

            "device_scope": device_scope,
            "source_device_id": source_device_id,
            "source_device_name": source_device_name,

            "collect_time_utc": collect_time_utc,
            "time_grain_sec": time_grain_sec,

            "source_metric_name": source_metric_name,
            "canonical_metric_code": mapping["canonical_metric_code"],

            "metric_value_num": canonical_value,
            "unit_code": mapping["canonical_unit_code"],
            "quality_code": "OK",
        }

    def _parse_solaredge_local_time_to_utc(
        self,
        date_text: str | None,
        timezone_name: str,
    ) -> datetime:
        if not date_text:
            raise ValueError("SolarEdge date is empty")

        local_tz = ZoneInfo(timezone_name)

        # SolarEdge common format: YYYY-MM-DD HH:MM:SS
        local_dt = datetime.strptime(date_text, "%Y-%m-%d %H:%M:%S")
        local_dt = local_dt.replace(tzinfo=local_tz)

        return local_dt.astimezone(timezone.utc).replace(tzinfo=None)
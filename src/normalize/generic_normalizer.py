from typing import Any, Dict, List, Optional
from src.domain.enums import ValueType
from src.domain.time_utils import epoch_ms_to_utc


class GenericNormalizer:
    def normalize(
        self,
        response_body: Dict[str, Any],
        raw_id: int,
        plant_code: str,
        plant_id: Optional[int],
        dev_type_id: int,
        source_api: str,
    ) -> List[dict]:
        rows: List[dict] = []
        data = response_body.get("data") or []

        for record in data:
            collect_ms = record.get("collectTime")
            if collect_ms is None:
                continue

            dev_id = record.get("devId")
            dev_dn = record.get("devDn")
            metric_map = record.get("dataItemMap") or record.get("dataItems") or {}

            collect_time_utc = epoch_ms_to_utc(collect_ms)

            if dev_id is None and dev_dn and "NE=" in dev_dn:
                dev_id = int(dev_dn.split("NE=")[-1])

            if dev_id is None:
                continue

            for metric_name, value in metric_map.items():
                parsed = self._parse_value(value)
                rows.append({
                    "raw_id": raw_id,
                    "plant_id": plant_id,
                    "plant_code": plant_code,
                    "dev_type_id": dev_type_id,
                    "dev_id": int(dev_id),
                    "dev_dn": dev_dn,
                    "collect_time_utc": collect_time_utc,
                    "collect_time_local": None,
                    "metric_name": metric_name,
                    "value_type": parsed["value_type"],
                    "metric_value_num": parsed["metric_value_num"],
                    "metric_value_text": parsed["metric_value_text"],
                    "metric_value_bool": parsed["metric_value_bool"],
                    "metric_value_raw_text": parsed["metric_value_raw_text"],
                    "source_api": source_api,
                })
        return rows

    def _parse_value(self, value: Any) -> dict:
        if value is None:
            return {
                "value_type": ValueType.NULL.value,
                "metric_value_num": None,
                "metric_value_text": None,
                "metric_value_bool": None,
                "metric_value_raw_text": None,
            }

        if isinstance(value, bool):
            return {
                "value_type": ValueType.BOOL.value,
                "metric_value_num": None,
                "metric_value_text": None,
                "metric_value_bool": value,
                "metric_value_raw_text": str(value),
            }

        if isinstance(value, (int, float)):
            return {
                "value_type": ValueType.NUMBER.value,
                "metric_value_num": float(value),
                "metric_value_text": None,
                "metric_value_bool": None,
                "metric_value_raw_text": str(value),
            }

        return {
            "value_type": ValueType.TEXT.value,
            "metric_value_num": None,
            "metric_value_text": str(value),
            "metric_value_bool": None,
            "metric_value_raw_text": str(value),
        }
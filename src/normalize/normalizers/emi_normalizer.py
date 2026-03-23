from typing import Dict, List
from src.normalize.base_typed_normalizer import BaseTypedNormalizer
from src.domain.time_utils import epoch_ms_to_utc


class EmiNormalizer(BaseTypedNormalizer):
    def normalize(self, response_body: Dict, raw_id: int, plant_code: str) -> List[dict]:
        rows: List[dict] = []

        for rec in response_body.get("data", []):
            m = rec.get("dataItemMap") or {}
            collect_ms = rec.get("collectTime")
            dev_id = rec.get("devId")

            if collect_ms is None or dev_id is None:
                continue

            rows.append({
                "plant_code": plant_code,
                "dev_id": int(dev_id),
                "collect_time_utc": epoch_ms_to_utc(collect_ms),
                "collect_time_local": None,
                "temperature_c": m.get("temperature"),
                "wind_speed_ms": m.get("wind_speed"),
                "wind_direction_deg": m.get("wind_direction"),
                "pv_temperature_c": m.get("pv_temperature"),
                "radiant_line_wm2": m.get("radiant_line"),
                "radiant_total_kwhm2": m.get("radiant_total"),
                "horiz_radiant_line_wm2": m.get("horiz_radiant_line"),
                "horiz_radiant_total_kwhm2": m.get("horiz_radiant_total"),
                "raw_id": raw_id,
            })
        return rows
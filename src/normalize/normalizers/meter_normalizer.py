from typing import Dict, List
from src.normalize.base_typed_normalizer import BaseTypedNormalizer
from src.domain.time_utils import epoch_ms_to_utc


class MeterNormalizer(BaseTypedNormalizer):
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
                "active_power_w": m.get("active_power"),
                "reactive_power_var": m.get("reactive_power"),
                "power_factor": m.get("power_factor"),
                "total_apparent_power_kva": m.get("total_apparent_power"),
                "grid_frequency_hz": m.get("grid_frequency"),
                "a_i": m.get("a_i"),
                "b_i": m.get("b_i"),
                "c_i": m.get("c_i"),
                "a_u": m.get("a_u"),
                "b_u": m.get("b_u"),
                "c_u": m.get("c_u"),
                "ab_u": m.get("ab_u"),
                "bc_u": m.get("bc_u"),
                "ca_u": m.get("ca_u"),
                "active_cap": m.get("active_cap"),
                "reverse_active_cap": m.get("reverse_active_cap"),
                "forward_reactive_cap": m.get("forward_reactive_cap"),
                "reverse_reactive_cap": m.get("reverse_reactive_cap"),
                "active_power_a": m.get("active_power_a"),
                "active_power_b": m.get("active_power_b"),
                "active_power_c": m.get("active_power_c"),
                "raw_id": raw_id,
            })
        return rows
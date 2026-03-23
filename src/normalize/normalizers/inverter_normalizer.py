from typing import Dict, List
from datetime import datetime, timezone
from src.normalize.base_typed_normalizer import BaseTypedNormalizer
from src.domain.time_utils import epoch_ms_to_utc


class InverterNormalizer(BaseTypedNormalizer):
    def normalize(self, response_body: Dict, raw_id: int, plant_code: str) -> List[dict]:
        rows: List[dict] = []

        for rec in response_body.get("data", []):
            m = rec.get("dataItemMap") or rec.get("dataItems") or {}
            collect_ms = rec.get("collectTime")
            dev_id = rec.get("devId")

            if collect_ms is None or dev_id is None:
                continue

            rows.append({
                "plant_code": plant_code,
                "dev_id": int(dev_id),
                "collect_time_utc": epoch_ms_to_utc(collect_ms),
                "collect_time_local": None,
                "active_power_kw": m.get("active_power"),
                "reactive_power_kvar": m.get("reactive_power"),
                "power_factor": m.get("power_factor"),
                "efficiency_pct": m.get("efficiency"),
                "temperature_c": m.get("temperature"),
                "day_cap_kwh": m.get("day_cap"),
                "total_cap_kwh": m.get("total_cap"),
                "inverter_state_code": m.get("inverter_state"),
                "elec_freq_hz": m.get("elec_freq"),
                "a_i": m.get("a_i"),
                "b_i": m.get("b_i"),
                "c_i": m.get("c_i"),
                "a_u": m.get("a_u"),
                "b_u": m.get("b_u"),
                "c_u": m.get("c_u"),
                "ab_u": m.get("ab_u"),
                "bc_u": m.get("bc_u"),
                "ca_u": m.get("ca_u"),
                "mppt_power_kw": m.get("mppt_power"),
                "mppt_total_cap_kwh": self._sum_mppt_caps(m),
                "open_time_utc": self._parse_epoch_seconds(m.get("open_time")),
                "close_time_utc": self._parse_epoch_seconds(m.get("close_time")),
                "raw_id": raw_id,
            })
        return rows

    def _sum_mppt_caps(self, metric_map: Dict):
        keys = [k for k in metric_map.keys() if k.startswith("mppt_") and k.endswith("_cap")]
        vals = [metric_map[k] for k in keys if metric_map.get(k) is not None]
        return sum(vals) if vals else None

    def _parse_epoch_seconds(self, value):
        if value is None:
            return None
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
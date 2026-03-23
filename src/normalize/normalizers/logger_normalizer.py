from typing import Dict, List
from src.normalize.base_typed_normalizer import BaseTypedNormalizer
from src.domain.time_utils import epoch_ms_to_utc


class LoggerNormalizer(BaseTypedNormalizer):
    def normalize(self, response_body: Dict, raw_id: int, plant_code: str) -> List[dict]:
        rows: List[dict] = []

        for rec in response_body.get("data", []):
            m = rec.get("dataItems") or rec.get("dataItemMap") or {}
            collect_ms = rec.get("collectTime")
            dev_id = rec.get("devId")

            if dev_id is None:
                dev_dn = rec.get("devDn")
                if dev_dn and "NE=" in dev_dn:
                    dev_id = int(dev_dn.split("NE=")[-1])

            if collect_ms is None or dev_id is None:
                continue

            rows.append({
                "plant_code": plant_code,
                "dev_id": int(dev_id),
                "collect_time_utc": epoch_ms_to_utc(collect_ms),
                "collect_time_local": None,
                "total_yield_kwh": m.get("total_yield"),
                "total_power_consumption_kwh": m.get("total_power_consumption"),
                "total_supply_from_grid_kwh": m.get("total_supply_from_grid"),
                "total_feed_in_to_grid_kwh": m.get("total_feed_in_to_grid"),
                "ac_total_charge_energy_kwh": m.get("ac_total_charge_energy"),
                "ac_total_discharge_energy_kwh": m.get("ac_total_discharge_energy"),
                "total_charge_kwh": m.get("total_charge"),
                "total_discharge_kwh": m.get("total_discharge"),
                "raw_id": raw_id,
            })
        return rows
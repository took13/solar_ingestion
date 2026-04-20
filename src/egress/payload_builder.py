from datetime import timezone


class PayloadBuilder:
    def build(self, rows: list[dict]) -> dict:
        records = []

        for r in rows:
            data = {
                "irradiance_wm2": -99 if r.get("irradiance_wm2") is None else r.get("irradiance_wm2"),
                "temperature_c": -99 if r.get("temperature_c") is None else r.get("temperature_c"),
            }

            for k, v in r.items():
                if k.startswith("inverter"):
                    data[k] = -99 if v is None else v

            records.append({
                "timestamp": self._to_iso_utc(r["collect_time_utc"]),
                "data": data
            })

        return {"records": records}

    def _to_iso_utc(self, dt) -> str:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
from datetime import timezone


class EnservePayloadBuilder:
    def build(self, rows: list[dict]) -> dict:
        records = []

        for r in rows:
            if r.get("power_kw") is None or r.get("number_inverter") is None:
                continue

            data = {
                "power_kw": float(r["power_kw"]),
                "number_inverter": int(r["number_inverter"]),
                "irradiance_wm2": float(r.get("irradiance_wm2") or 0.0),
                "temperature_c": float(r.get("temperature_c") or 0.0),
            }

            records.append({
                "timestamp": self._format_utc_z(r["collect_time_utc"]),
                "data": data,
            })

        return {"records": records}

    def _format_utc_z(self, dt) -> str:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)

        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
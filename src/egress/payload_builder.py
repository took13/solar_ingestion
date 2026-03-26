from datetime import timezone


class PayloadBuilder:
    def build(self, rows: list[dict]) -> dict:
        return {
            "records": [
                {
                    "timestamp": self._to_iso_utc(r["collect_time_utc"]),
                    "data": {
                        "power_kw": r["power_kw"],
                        "irradiance_wm2": r["irradiance_wm2"],
                        "temperature_c": r["temperature_c"],
                    }
                }
                for r in rows
            ]
        }

    def _to_iso_utc(self, dt) -> str:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
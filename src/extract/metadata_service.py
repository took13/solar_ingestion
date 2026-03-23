from __future__ import annotations


class MetadataService:
    def __init__(self, metadata_repo):
        self.metadata_repo = metadata_repo

    def build_targets_from_job_config(self, job: dict, job_config: dict) -> list[dict]:
        targets = []

        for t in job_config.get("targets", []):
            if not t.get("enabled", True):
                continue

            plant_code = t["plant_code"]
            account_id = t.get("account_id")

            if account_id is None:
                resolved = self.metadata_repo.resolve_account_for_plant(plant_code)
                if not resolved:
                    raise ValueError(f"Cannot resolve account for plant_code={plant_code}")
                account_id = resolved["account_id"]

            plant = self.metadata_repo.get_plant(plant_code)
            if not plant:
                raise ValueError(f"Plant not found in dim_plant: {plant_code}")

            targets.append({
                "job_id": job["job_id"],
                "job_name": job["job_name"],
                "account_id": account_id,
                "plant_id": plant["plant_id"],
                "plant_code": plant_code,
                "dev_type_id": t["dev_type_id"],
                "batch_size": t["batch_size"],
                "lag_minutes": t.get("lag_minutes", 15),
                "overlap_minutes": t.get("overlap_minutes", 10),
                "max_window_minutes": t.get("max_window_minutes", 60),
                "bootstrap_start_utc": self._parse_dt(t.get("bootstrap_start_utc")),
                "enabled": t.get("enabled", True),
            })

        return targets

    def _parse_dt(self, value):
        if value is None:
            return None
        from datetime import datetime
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
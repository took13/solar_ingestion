from __future__ import annotations


class MetadataService:
    def __init__(self, metadata_repo):
        self.metadata_repo = metadata_repo

    def enrich_targets_from_db(self, targets: list[dict]) -> list[dict]:
        enriched = []

        for target in targets:
            endpoint_name = target.get("endpoint_name")
            dev_type_id = target.get("dev_type_id")
            plant_code = target.get("plant_code")

            # account-level targets
            if plant_code == "__ACCOUNT__":
                target["plant"] = None
                enriched.append(target)
                continue

            # backward-compatible special case
            if endpoint_name == "getStationRealKpi" and dev_type_id == -1:
                target["plant"] = None
                enriched.append(target)
                continue

            plant = self.metadata_repo.get_plant(plant_code)
            if not plant:
                raise ValueError(f"Plant not found in dim_plant: {plant_code}")

            target["plant"] = plant
            enriched.append(target)

        return enriched
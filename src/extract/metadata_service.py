class MetadataService:
    def __init__(self, metadata_repo):
        self.metadata_repo = metadata_repo

    def enrich_targets_from_db(self, targets: list[dict]) -> list[dict]:
        enriched_targets = []

        for target in targets:
            plant = self.metadata_repo.get_plant(target["plant_code"])
            if not plant:
                raise ValueError(f"Plant not found in dim_plant: {target['plant_code']}")

            enriched = dict(target)
            enriched["plant_id"] = plant["plant_id"]
            enriched_targets.append(enriched)

        return enriched_targets
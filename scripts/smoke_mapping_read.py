from __future__ import annotations

from src.config_loader import ConfigLoader
from src.db.connection import create_connection
from src.db.repositories.metric_mapping_repo import MetricMappingRepository
from src.db.repositories.source_mapping_repo import SourceMappingRepository


def main():
    app_config = ConfigLoader().load_app_config()
    conn = create_connection(app_config["database"]["connection_string"])

    source_repo = SourceMappingRepository(conn)
    metric_repo = MetricMappingRepository(conn)

    print("=== Active SolarEdge plant mappings ===")
    plant_maps = source_repo.get_active_plant_maps("SOLAREDGE")

    if not plant_maps:
        print("[WARN] No active SOLAREDGE plant mapping found in dbo.dim_plant_source_map")
    else:
        for row in plant_maps[:10]:
            print(
                f"- internal={row['internal_plant_code']} "
                f"siteId={row['source_plant_code']} "
                f"name={row['source_plant_name']} "
                f"tz={row['timezone_name']}"
            )

    print("")
    print("=== Enabled SolarEdge metric mappings: sitePower ===")
    site_power_mappings = metric_repo.get_enabled_mappings(
        source_system_code="SOLAREDGE",
        endpoint_name="sitePower",
    )

    if not site_power_mappings:
        print("[WARN] No enabled metric mapping found for SOLAREDGE/sitePower")
    else:
        for row in site_power_mappings:
            print(
                f"- {row['source_device_scope']}.{row['source_metric_name']} "
                f"→ {row['canonical_metric_code']} "
                f"unit={row['canonical_unit_code']} "
                f"multiplier={row['multiplier_to_canonical']}"
            )

    print("")
    print("=== Enabled SolarEdge metric mappings: energyDetails ===")
    energy_mappings = metric_repo.get_enabled_mappings(
        source_system_code="SOLAREDGE",
        endpoint_name="energyDetails",
    )

    if not energy_mappings:
        print("[WARN] No enabled metric mapping found for SOLAREDGE/energyDetails")
    else:
        for row in energy_mappings:
            print(
                f"- {row['source_device_scope']}.{row['source_metric_name']} "
                f"→ {row['canonical_metric_code']} "
                f"unit={row['canonical_unit_code']} "
                f"multiplier={row['multiplier_to_canonical']}"
            )

    print("")
    print("[OK] mapping repository smoke test completed")

    conn.close()


if __name__ == "__main__":
    main()
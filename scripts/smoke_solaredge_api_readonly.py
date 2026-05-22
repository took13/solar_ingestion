from __future__ import annotations

import argparse
import os
from typing import Any

from src.solaredge.client import SolarEdgeClient


def main():
    args = parse_args()

    api_key = os.getenv("SOLAREDGE_API_KEY")
    if not api_key:
        raise RuntimeError(
            "Missing SOLAREDGE_API_KEY environment variable. "
            "Set it in PowerShell before running this script."
        )

    client = SolarEdgeClient(api_key=api_key)

    if args.endpoint in ("sitePower", "both"):
        power_response = client.get_site_power(
            site_id=args.site_id,
            start_time_local=args.start_local,
            end_time_local=args.end_local,
        )

        print("")
        print("=== sitePower ===")
        print_response_summary(power_response.response_json)
        print(f"http_status={power_response.http_status}")
        print(f"elapsed_sec={power_response.elapsed_sec:.2f}")

    if args.endpoint in ("energyDetails", "both"):
        energy_response = client.get_energy_details(
            site_id=args.site_id,
            start_time_local=args.start_local,
            end_time_local=args.end_local,
            time_unit=args.time_unit,
            meters=args.meters,
        )

        print("")
        print("=== energyDetails ===")
        print_response_summary(energy_response.response_json)
        print(f"http_status={energy_response.http_status}")
        print(f"elapsed_sec={energy_response.elapsed_sec:.2f}")

    print("")
    print("[OK] SolarEdge API read-only smoke test completed")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Read-only SolarEdge API smoke test. Does not write to DB."
    )

    parser.add_argument(
        "--site-id",
        required=True,
        help="SolarEdge siteId",
    )

    parser.add_argument(
        "--start-local",
        required=True,
        help='Local site start time, format "YYYY-MM-DD HH:MM:SS"',
    )

    parser.add_argument(
        "--end-local",
        required=True,
        help='Local site end time, format "YYYY-MM-DD HH:MM:SS"',
    )

    parser.add_argument(
        "--endpoint",
        choices=["sitePower", "energyDetails", "both"],
        default="both",
    )

    parser.add_argument(
        "--time-unit",
        default="QUARTER_OF_AN_HOUR",
        help="SolarEdge energyDetails timeUnit",
    )

    parser.add_argument(
        "--meters",
        default="Production,FeedIn,Purchased,SelfConsumption",
        help="Comma-separated meters for energyDetails",
    )

    return parser.parse_args()


def print_response_summary(payload: dict[str, Any]) -> None:
    if "power" in payload:
        power = payload.get("power") or {}
        values = power.get("values") or []

        print(f"root=power")
        print(f"timeUnit={power.get('timeUnit')}")
        print(f"unit={power.get('unit')}")
        print(f"value_count={len(values)}")
        print_sample_values(values)
        return

    if "energyDetails" in payload:
        energy_details = payload.get("energyDetails") or {}
        meters = energy_details.get("meters") or []

        print(f"root=energyDetails")
        print(f"timeUnit={energy_details.get('timeUnit')}")
        print(f"unit={energy_details.get('unit')}")
        print(f"meter_count={len(meters)}")

        for meter in meters:
            meter_type = meter.get("type")
            values = meter.get("values") or []
            print(f"- meter={meter_type}, value_count={len(values)}")
            print_sample_values(values)
        return

    print("[WARN] Unknown response shape")
    print(f"top_level_keys={list(payload.keys())}")


def print_sample_values(values: list[dict[str, Any]], limit: int = 5) -> None:
    for item in values[:limit]:
        print(f"  {item.get('date')} = {item.get('value')}")


if __name__ == "__main__":
    main()
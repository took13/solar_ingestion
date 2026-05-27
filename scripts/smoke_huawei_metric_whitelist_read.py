from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from src.config_loader import ConfigLoader
from src.db.connection import create_connection
from src.db.repositories.metric_whitelist_repo import MetricWhitelistRepository


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Smoke test reading Huawei metric whitelist from DB."
    )

    parser.add_argument(
        "--source-system-code",
        default="HUAWEI",
        help="Default: HUAWEI",
    )

    parser.add_argument(
        "--source-api",
        default=None,
        help="Optional. Example: getDevHistoryKpi",
    )

    parser.add_argument(
        "--dev-type-id",
        type=int,
        default=None,
        help="Optional. Example: 1",
    )

    parser.add_argument(
        "--allow-high-volume",
        action="store_true",
        help=(
            "Allow enabled pv*_u / pv*_i / mppt* metrics. "
            "Do not use this for baseline production restart."
        ),
    )

    args = parser.parse_args()

    app_config = ConfigLoader().load_app_config()
    conn = create_connection(app_config["database"]["connection_string"])

    try:
        repo = MetricWhitelistRepository(conn)

        print("")
        print("=== Huawei Metric Whitelist Smoke Read ===")
        print(f"source_system_code : {args.source_system_code}")
        print(f"source_api         : {args.source_api or '(all)'}")
        print(f"dev_type_id        : {args.dev_type_id if args.dev_type_id is not None else '(all)'}")
        print("")

        summary_rows = repo.summarize_enabled_metrics(
            source_system_code=args.source_system_code
        )

        if not summary_rows:
            raise RuntimeError(
                f"No enabled metrics found for source_system_code={args.source_system_code}"
            )

        print("Enabled metric summary:")
        for row in summary_rows:
            print(
                "  "
                f"{row['source_api']} "
                f"dev_type={row['dev_type_id']} "
                f"enabled={row['enabled_metric_count']} "
                f"keep_null={row['keep_null_count']} "
                f"keep_raw_text={row['keep_raw_text_count']}"
            )

        print("")

        metric_rows = repo.list_enabled_metrics(
            source_system_code=args.source_system_code,
            source_api=args.source_api,
            dev_type_id=args.dev_type_id,
        )

        print(f"Matched enabled metric rows: {len(metric_rows)}")
        for row in metric_rows[:100]:
            print(
                "  "
                f"{row['source_api']} "
                f"dev_type={row['dev_type_id']} "
                f"metric={row['metric_name']} "
                f"target_layer={row['target_layer']} "
                f"retention={row['retention_level']} "
                f"keep_null={row['keep_null']} "
                f"keep_raw_text={row['keep_raw_text']}"
            )

        if len(metric_rows) > 100:
            print(f"  ... truncated. Total rows = {len(metric_rows)}")

        print("")

        high_volume_rows = repo.list_high_volume_metrics_enabled(
            source_system_code=args.source_system_code
        )

        if high_volume_rows and not args.allow_high_volume:
            print("[FAIL] High-volume PV string / MPPT metrics are enabled:")
            for row in high_volume_rows[:100]:
                print(
                    "  "
                    f"{row['source_api']} "
                    f"dev_type={row['dev_type_id']} "
                    f"metric={row['metric_name']} "
                    f"target_layer={row['target_layer']} "
                    f"retention={row['retention_level']}"
                )

            print("")
            raise RuntimeError(
                "High-volume metrics are enabled. "
                "Disable pv*_u / pv*_i / mppt* before Huawei history/backfill restart, "
                "or rerun with --allow-high-volume only for explicit RCA testing."
            )

        print("[OK] Metric whitelist read completed.")
        print("[OK] No high-volume PV string / MPPT metrics enabled for baseline.")
        print("")

        return 0

    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
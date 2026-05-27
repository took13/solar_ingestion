from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from src.main import build_app
from src.db.repositories.metric_whitelist_repo import MetricWhitelistRepository
from src.normalize_jobs.generic_normalize_job import GenericNormalizeJob


def main() -> int:
    args = parse_args()

    app = build_app()

    default_limit = app.app_config.get("pipeline", {}).get("generic_metrics", {}).get(
        "pending_limit",
        100,
    )

    chunk_size = app.app_config.get("pipeline", {}).get("generic_metrics", {}).get(
        "normalize_chunk_size",
        5000,
    )

    limit = args.limit_raw if args.limit_raw is not None else default_limit

    if args.raw_id is not None and args.limit_raw is not None:
        raise RuntimeError("Use either --raw-id or --limit-raw, not both.")

    print("")
    print("=== Huawei Generic Normalize Runner ===")
    print(f"mode              : {'DRY-RUN' if args.dry_run else 'REAL-RUN'}")
    print(f"raw_id            : {args.raw_id if args.raw_id is not None else '(pending)'}")
    print(f"limit             : {limit}")
    print(f"chunk_size        : {chunk_size}")
    print(f"require_whitelist : {not args.allow_no_whitelist}")
    print("")

    job = GenericNormalizeJob(
        conn=app.conn,
        metadata_repo=app.metadata_repo,
        chunk_size=chunk_size,
        metric_whitelist_repo=MetricWhitelistRepository(app.conn),
        require_whitelist=not args.allow_no_whitelist,
    )

    job.run(
        limit=limit,
        raw_id=args.raw_id,
        dry_run=args.dry_run,
    )

    return 0


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run Huawei generic normalization with metric whitelist guard."
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and whitelist-filter only. No DB writes and no status updates.",
    )

    parser.add_argument(
        "--raw-id",
        type=int,
        default=None,
        help="Run one specific raw.api_call.raw_id.",
    )

    parser.add_argument(
        "--limit-raw",
        type=int,
        default=None,
        help="Limit pending raw rows. Default comes from config.",
    )

    parser.add_argument(
        "--allow-no-whitelist",
        action="store_true",
        help=(
            "Dangerous. Allows normalization without whitelist rules. "
            "Do not use for Huawei production restart."
        ),
    )

    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(main())
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

from src.main import build_app

PROC_NAME = "ops.usp_load_enserve_energy_avg_adhoc_stage"


def parse_utc(value: str) -> datetime:
    v = value.strip().replace("Z", "+00:00")
    dt = datetime.fromisoformat(v)
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    from_utc = parse_utc(args.start)
    to_utc = parse_utc(args.end)

    if to_utc <= from_utc:
        raise ValueError("--end must be greater than --start")

    print("[STAGE] Enserve energy-average stage loader")
    print(f"[STAGE] FromUtc = {from_utc}")
    print(f"[STAGE] ToUtc   = {to_utc}")
    print(f"[STAGE] Proc    = {PROC_NAME}")

    if args.dry_run:
        print("[STAGE] DRY-RUN only. No DB write.")
        return 0

    app = build_app()
    conn = app.conn
    cursor = conn.cursor()

    try:
        cursor.execute(
            f"EXEC {PROC_NAME} @FromUtc=?, @ToUtc=?",
            from_utc,
            to_utc,
        )
        conn.commit()
        print("[STAGE] Done")
        return 0
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[STAGE][FAILED] {exc}")
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
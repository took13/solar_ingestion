from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.main import build_app


def main() -> int:
    app = build_app()
    cur = app.conn.cursor()

    print("=== DB CHECK ===")
    cur.execute("SELECT DB_NAME() AS db_name, @@SERVERNAME AS server_name")
    r = cur.fetchone()
    print(f"server={r.server_name} db={r.db_name}")

    print("\n=== RAW TARGET CHECK FROM APP CONNECTION ===")
    cur.execute("""
        SELECT
            j.job_name,
            t.target_id,
            t.account_id,
            t.plant_code,
            t.dev_type_id,
            t.endpoint_name,
            t.service_class,
            t.is_enabled
        FROM ctl.ingest_target t
        JOIN ctl.ingest_job j
            ON j.job_id = t.job_id
        WHERE j.job_name = 'inverter_realtime_online'
        ORDER BY t.is_enabled DESC, t.target_id;
    """)

    rows = cur.fetchall()
    for x in rows:
        print(
            x.job_name,
            x.target_id,
            x.account_id,
            x.plant_code,
            x.dev_type_id,
            x.endpoint_name,
            x.service_class,
            x.is_enabled,
        )

    print(f"\ntotal_rows={len(rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
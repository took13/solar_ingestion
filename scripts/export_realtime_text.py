from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

from src.main import build_app


DEFAULT_OUTPUT_DIR = Path("exports") / "realtime_text"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export Solar realtime monitoring text file from mart.vw_export_realtime_text"
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Output directory for exported text files. Example: Z:\\SolarRealtime or \\\\server\\share\\solar_data",
    )
    parser.add_argument(
        "--include-late",
        action="store_true",
        help="Include rows with data_status = LATE. Default exports only FRESH rows.",
    )
    parser.add_argument(
        "--include-missing",
        action="store_true",
        help="Include MISSING/STALE rows as blank values. Not recommended for production.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview rows without writing file.",
    )
    parser.add_argument(
        "--print-lines",
        action="store_true",
        help="Print export lines to console.",
    )
    return parser.parse_args()


def build_status_filter(include_late: bool, include_missing: bool) -> tuple[str, list[str]]:
    statuses = ["FRESH"]

    if include_late:
        statuses.append("LATE")

    if include_missing:
        statuses.extend(["MISSING", "STALE"])

    placeholders = ",".join("?" for _ in statuses)
    return placeholders, statuses


def main() -> int:
    args = parse_args()

    output_dir = Path(args.output_dir)

    app = build_app()
    conn = app.conn
    cursor = conn.cursor()

    placeholders, statuses = build_status_filter(
        include_late=args.include_late,
        include_missing=args.include_missing,
    )

    sql = f"""
        SELECT
            plant_code,
            tag_name,
            timestamp_text,
            value_text,
            export_line,
            data_status,
            source_age_minute
        FROM mart.vw_export_realtime_text
        WHERE data_status IN ({placeholders})
          AND (
                value_text IS NOT NULL
                OR data_status IN ('MISSING', 'STALE')
              )
        ORDER BY plant_code, tag_name;
    """

    cursor.execute(sql, statuses)
    rows = cursor.fetchall()

    if not rows:
        print("[EXPORT][WARN] No rows returned from mart.vw_export_realtime_text")
        return 2

    export_lines: list[str] = []

    fresh_count = 0
    late_count = 0
    missing_count = 0
    stale_count = 0

    for r in rows:
        status = str(r.data_status)

        if status == "FRESH":
            fresh_count += 1
        elif status == "LATE":
            late_count += 1
        elif status == "MISSING":
            missing_count += 1
        elif status == "STALE":
            stale_count += 1

        if r.export_line is not None:
            export_lines.append(str(r.export_line))
        else:
            value_text = "" if r.value_text is None else str(r.value_text)
            export_lines.append(f"{r.tag_name},{r.timestamp_text},{value_text}")

    if args.print_lines or args.dry_run:
        print("[EXPORT] Preview:")
        for line in export_lines:
            print(line)

    print(
        "[EXPORT] "
        f"row_count={len(export_lines)} "
        f"fresh={fresh_count} "
        f"late={late_count} "
        f"missing={missing_count} "
        f"stale={stale_count}"
    )
    print(f"[EXPORT] output_dir={output_dir}")

    if args.dry_run:
        print("[EXPORT] Dry run only. No file written.")
        return 0

    output_dir.mkdir(parents=True, exist_ok=True)

    file_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"solar_realtime_{file_ts}.csv"
    temp_file = output_file.with_suffix(".tmp")

    with temp_file.open("w", encoding="utf-8", newline="") as f:
        for line in export_lines:
            f.write(line + "\r\n")

    temp_file.replace(output_file)

    print(f"[EXPORT] wrote {len(export_lines)} rows -> {output_file}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
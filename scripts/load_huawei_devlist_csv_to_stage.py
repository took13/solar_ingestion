from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from typing import Dict, List, Optional

from src.main import build_app


STAGE_TABLE = "stg.huawei_dev_list_latest"


CREATE_SQL = """
IF SCHEMA_ID('stg') IS NULL
    EXEC('CREATE SCHEMA stg');

IF OBJECT_ID('stg.huawei_dev_list_latest', 'U') IS NULL
BEGIN
    CREATE TABLE stg.huawei_dev_list_latest (
        snapshot_utc datetime2(0) NOT NULL DEFAULT SYSUTCDATETIME(),
        stationCode nvarchar(100) NOT NULL,
        devTypeId int NOT NULL,
        id nvarchar(100) NOT NULL,
        devDn nvarchar(100) NULL,
        devName nvarchar(255) NULL,
        esnCode nvarchar(255) NULL,
        invType nvarchar(255) NULL,
        latitude float NULL,
        longitude float NULL,
        model nvarchar(255) NULL,
        optimizerNumber nvarchar(100) NULL,
        softwareVersion nvarchar(255) NULL,
        source_file nvarchar(500) NULL,
        inserted_at_utc datetime2(0) NOT NULL DEFAULT SYSUTCDATETIME()
    );
END;
"""


INSERT_SQL = """
INSERT INTO stg.huawei_dev_list_latest (
    stationCode,
    devTypeId,
    id,
    devDn,
    devName,
    esnCode,
    invType,
    latitude,
    longitude,
    model,
    optimizerNumber,
    softwareVersion,
    source_file
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""


def clean(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    v = str(value).strip()
    return v if v != "" else None


def to_int(value: Optional[str]) -> Optional[int]:
    v = clean(value)
    if v is None:
        return None
    return int(float(v))


def to_float(value: Optional[str]) -> Optional[float]:
    v = clean(value)
    if v is None:
        return None
    return float(v)


def normalize_headers(row: Dict[str, str]) -> Dict[str, str]:
    return {str(k).strip(): v for k, v in row.items() if k is not None}


def get_value(row: Dict[str, str], *names: str) -> Optional[str]:
    for name in names:
        if name in row:
            return row.get(name)
    return None


def detect_dialect(file_path: Path) -> csv.Dialect:
    sample = file_path.read_text(encoding="utf-8-sig", errors="replace")[:4096]
    try:
        return csv.Sniffer().sniff(sample, delimiters=",\t;|")
    except csv.Error:
        class DefaultDialect(csv.excel):
            delimiter = ","
        return DefaultDialect


def read_rows(file_path: Path, force_station_code: Optional[str]) -> List[tuple]:
    dialect = detect_dialect(file_path)

    rows: List[tuple] = []

    with file_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, dialect=dialect)

        for raw in reader:
            r = normalize_headers(raw)

            station_code = clean(
                force_station_code
                or get_value(r, "stationCode", "station_code", "plant_code")
            )

            dev_type_id = to_int(get_value(r, "devTypeId", "dev_type_id", "devTypeID"))
            dev_id = clean(get_value(r, "id", "dev_id", "devId"))

            if not station_code or dev_type_id is None or not dev_id:
                print(f"[SKIP] Missing required field: {r}")
                continue

            rows.append(
                (
                    station_code,
                    dev_type_id,
                    dev_id,
                    clean(get_value(r, "devDn", "dev_dn")),
                    clean(get_value(r, "devName", "dev_name")),
                    clean(get_value(r, "esnCode", "esn_code")),
                    clean(get_value(r, "invType", "inv_type")),
                    to_float(get_value(r, "latitude")),
                    to_float(get_value(r, "longitude")),
                    clean(get_value(r, "model")),
                    clean(get_value(r, "optimizerNumber", "optimizer_number")),
                    clean(get_value(r, "softwareVersion", "software_version")),
                    str(file_path),
                )
            )

    return rows


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Load Huawei getDevList CSV/TSV into stg.huawei_dev_list_latest"
    )
    parser.add_argument("--file", required=True, help="CSV/TSV file path")
    parser.add_argument(
        "--station-code",
        help="Force stationCode/plant_code if the file does not contain stationCode",
    )
    parser.add_argument(
        "--truncate-all",
        action="store_true",
        help="TRUNCATE the whole staging table before loading",
    )
    parser.add_argument(
        "--replace-station",
        action="store_true",
        help="Delete existing staging rows for stationCode(s) in this file before loading",
    )

    args = parser.parse_args()

    file_path = Path(args.file)
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    rows = read_rows(file_path, args.station_code)

    if not rows:
        print("[LOAD] No valid rows found")
        return 1

    station_codes = sorted({r[0] for r in rows})

    print("[LOAD] Huawei getDevList stage loader")
    print(f"[LOAD] file={file_path}")
    print(f"[LOAD] rows={len(rows)}")
    print(f"[LOAD] station_codes={station_codes}")

    app = build_app()
    conn = app.conn
    cur = conn.cursor()

    try:
        cur.execute(CREATE_SQL)
        conn.commit()

        if args.truncate_all:
            print("[LOAD] TRUNCATE all stage rows")
            cur.execute("TRUNCATE TABLE stg.huawei_dev_list_latest;")
            conn.commit()

        if args.replace_station:
            print(f"[LOAD] Delete stage rows for station_codes={station_codes}")
            for plant_code in station_codes:
                cur.execute(
                    "DELETE FROM stg.huawei_dev_list_latest WHERE stationCode = ?;",
                    plant_code,
                )
            conn.commit()

        try:
            cur.fast_executemany = True
        except Exception:
            pass

        cur.executemany(INSERT_SQL, rows)
        conn.commit()

        print("[LOAD] Done")
        return 0

    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[LOAD][FAILED] {exc}")
        raise

    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
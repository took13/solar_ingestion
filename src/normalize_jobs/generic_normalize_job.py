from __future__ import annotations

import json
import time
from typing import Any

from src.normalize.generic_normalizer import GenericNormalizer
from src.normalize_jobs.stage_writer import StageWriter
from src.normalize_jobs.upsert_service import UpsertService
from src.normalize_jobs.status_service import StatusService
from src.db.repositories.metric_whitelist_repo import MetricWhitelistRepository


class GenericNormalizeJob:
    def __init__(
        self,
        conn,
        metadata_repo,
        chunk_size: int = 5000,
        metric_whitelist_repo: MetricWhitelistRepository | None = None,
        require_whitelist: bool = True,
    ):
        self.conn = conn
        self.metadata_repo = metadata_repo
        self.chunk_size = chunk_size
        self.metric_whitelist_repo = metric_whitelist_repo or MetricWhitelistRepository(conn)
        self.require_whitelist = require_whitelist

        self.normalizer = GenericNormalizer()
        self.stage_writer = StageWriter(conn)
        self.upsert_service = UpsertService(conn)
        self.status_service = StatusService(conn)

    def run(
        self,
        limit: int = 100,
        *,
        raw_id: int | None = None,
        dry_run: bool = False,
    ):
        rows = self._get_raw_rows(limit=limit, raw_id=raw_id)

        mode = "DRY-RUN" if dry_run else "REAL-RUN"
        print(f"[NORM] mode = {mode}")
        print(f"[NORM] selected raw rows = {len(rows)}")

        total_output_rows = 0
        total_blocked_metrics = 0
        total_skipped_nulls = 0
        total_failed = 0

        for row in rows:
            current_raw_id = row["raw_id"]

            print("")
            print(
                f"[NORM] processing raw_id={current_raw_id} "
                f"api={row['api_name']} "
                f"plant={row['plant_code']} "
                f"devType={row['dev_type_id']}"
            )

            try:
                response_body = json.loads(row["response_json"]) if row["response_json"] else {}

                whitelist_rules = self.metric_whitelist_repo.get_enabled_metric_rules(
                    source_system_code="HUAWEI",
                    source_api=row["api_name"],
                    dev_type_id=row["dev_type_id"],
                )

                print(
                    f"[NORM] raw_id={current_raw_id} "
                    f"whitelist_rules={len(whitelist_rules)}"
                )

                result = self.normalizer.normalize(
                    response_body=response_body,
                    raw_id=current_raw_id,
                    plant_code=row["plant_code"],
                    plant_id=row["plant_id"],
                    dev_type_id=row["dev_type_id"],
                    source_api=row["api_name"],
                    whitelist_rules=whitelist_rules,
                    require_whitelist=self.require_whitelist,
                )

                generic_rows = result["rows"]
                stats = result["stats"]

                self._print_stats(stats)

                total_output_rows += stats["output_row_count"]
                total_blocked_metrics += stats["blocked_metric_count"]
                total_skipped_nulls += stats["skipped_null_count"]

                # ---------------------------------------------------------------------
                # SAFETY FIX:
                # getDevRealKpi response contains devId/sn but does NOT contain
                # stationCode per device record.
                #
                # raw.api_call.plant_code can be:
                #   - "__ACCOUNT__"
                #   - comma-separated plant list
                #   - only the first plant in a multi-plant device batch
                #
                # Therefore, for device realtime API, plant_code must be resolved
                # from dbo.dim_device by dev_id before inserting to norm.device_metric_long.
                # ---------------------------------------------------------------------
                if row["api_name"] == "getDevRealKpi" and generic_rows:
                    self._apply_device_plant_lookup(
                        raw_id=current_raw_id,
                        rows=generic_rows,
                    )

                if not generic_rows:
                    if dry_run:
                        print(f"[NORM][DRY-RUN] raw_id={current_raw_id} no rows. No status update.")
                    else:
                        self.status_service.mark_success(
                            raw_id=current_raw_id,
                            generic_row_count=0,
                        )
                        print(f"[NORM] raw_id={current_raw_id} no rows")
                    continue

                deduped_rows = self._dedup_rows(generic_rows)
                catalog_rows = self._build_catalog_rows(deduped_rows)

                print(
                    f"[NORM] raw_id={current_raw_id} "
                    f"output_rows={len(generic_rows)} "
                    f"deduped_rows={len(deduped_rows)} "
                    f"catalog_rows={len(catalog_rows)}"
                )

                if dry_run:
                    print(
                        f"[NORM][DRY-RUN] raw_id={current_raw_id} "
                        f"would_insert_metric_rows={len(deduped_rows)} "
                        f"would_upsert_catalog_rows={len(catalog_rows)}"
                    )
                    continue

                for idx, chunk in enumerate(
                    self._chunk_list(deduped_rows, self.chunk_size),
                    start=1,
                ):
                    load_id = self.stage_writer.next_load_id()
                    chunk_catalog_rows = self._build_catalog_rows(chunk)

                    print(
                        f"[NORM] raw_id={current_raw_id} chunk={idx} "
                        f"metric_rows={len(chunk)} "
                        f"catalog_rows={len(chunk_catalog_rows)}"
                    )

                    self.stage_writer.insert_metric_rows(load_id=load_id, rows=chunk)
                    self.stage_writer.insert_metric_catalog_rows(load_id=load_id, rows=chunk_catalog_rows)

                    self.upsert_service.upsert_device_metric_long(load_id=load_id)
                    self.upsert_service.upsert_metric_catalog(load_id=load_id)
                    self.stage_writer.cleanup(load_id=load_id)

                self.status_service.mark_success(
                    raw_id=current_raw_id,
                    generic_row_count=len(deduped_rows),
                )

                print(f"[NORM] raw_id={current_raw_id} success rows={len(deduped_rows)}")

            except Exception as e:
                total_failed += 1

                if dry_run:
                    print(f"[NORM][DRY-RUN][FAILED] raw_id={current_raw_id} error={e}")
                else:
                    self.status_service.mark_failed(
                        raw_id=current_raw_id,
                        error_message=str(e),
                    )
                    print(f"[NORM][FAILED] raw_id={current_raw_id} error={e}")

            time.sleep(0.2)

        print("")
        print("=== Generic Normalize Summary ===")
        print(f"mode                  : {mode}")
        print(f"raw_rows              : {len(rows)}")
        print(f"total_output_rows     : {total_output_rows}")
        print(f"total_blocked_metrics : {total_blocked_metrics}")
        print(f"total_skipped_nulls   : {total_skipped_nulls}")
        print(f"total_failed          : {total_failed}")
        print("")

        if total_failed > 0:
            raise RuntimeError(f"Generic normalization completed with failures: {total_failed}")

    def _get_raw_rows(self, limit: int, raw_id: int | None = None):
        if raw_id is not None:
            return self._get_raw_by_id(raw_id=raw_id)

        return self._get_pending_raw(limit=limit)

    def _get_raw_by_id(self, raw_id: int):
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT
                r.raw_id,
                r.plant_id,
                r.plant_code,
                r.dev_type_id,
                r.api_name,
                r.response_json
            FROM raw.api_call r
            WHERE r.raw_id = ?
              AND r.api_success_flag = 1
              AND r.fail_code = 0
              AND r.dev_type_id > 0
              AND r.api_name IN ('getDevRealKpi', 'getDevHistoryKpi')
        """, (raw_id,))

        rows = cursor.fetchall()
        return [self._raw_row_to_dict(r) for r in rows]

    def _get_pending_raw(self, limit: int):
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT TOP (?)
                r.raw_id,
                r.plant_id,
                r.plant_code,
                r.dev_type_id,
                r.api_name,
                r.response_json
            FROM raw.api_call r
            LEFT JOIN norm.raw_normalization_status s
                ON s.raw_id = r.raw_id
            WHERE r.api_success_flag = 1
              AND r.fail_code = 0
              AND r.dev_type_id > 0
              AND r.api_name IN ('getDevRealKpi', 'getDevHistoryKpi')
              AND (s.raw_id IS NULL OR s.generic_status IN ('PENDING', 'FAILED'))
            ORDER BY r.raw_id
        """, (limit,))

        rows = cursor.fetchall()
        return [self._raw_row_to_dict(r) for r in rows]

    def _raw_row_to_dict(self, r) -> dict[str, Any]:
        return {
            "raw_id": r.raw_id,
            "plant_id": r.plant_id,
            "plant_code": r.plant_code,
            "dev_type_id": r.dev_type_id,
            "api_name": r.api_name,
            "response_json": r.response_json,
        }

    def _apply_device_plant_lookup(self, raw_id: int, rows: list[dict]) -> None:
        dev_ids = {
            int(r["dev_id"])
            for r in rows
            if r.get("dev_id") is not None
        }

        if not dev_ids:
            print(f"[WARN] raw_id={raw_id} getDevRealKpi has no dev_id in parsed rows")
            return

        device_lookup = self._get_device_lookup(dev_ids)
        missing_dev_ids = set()

        for r in rows:
            dev_id = r.get("dev_id")

            if dev_id is None:
                continue

            dev_id = int(dev_id)
            info = device_lookup.get(dev_id)

            if info:
                r["plant_code"] = info["plant_code"]
                r["plant_id"] = info["plant_id"]
            else:
                missing_dev_ids.add(dev_id)

        if missing_dev_ids:
            print(
                f"[WARN] raw_id={raw_id} getDevRealKpi has "
                f"{len(missing_dev_ids)} dev_id(s) not found in dbo.dim_device: "
                f"{sorted(list(missing_dev_ids))[:10]}"
            )

    def _get_device_lookup(self, dev_ids: set[int]) -> dict[int, dict]:
        if not dev_ids:
            return {}

        placeholders = ",".join("?" for _ in dev_ids)

        sql = f"""
            SELECT
                d.dev_id,
                d.plant_code,
                p.plant_id
            FROM dbo.dim_device d
            LEFT JOIN dbo.dim_plant p
                ON p.plant_code = d.plant_code
            WHERE d.dev_id IN ({placeholders})
        """

        cursor = self.conn.cursor()
        cursor.execute(sql, tuple(dev_ids))

        lookup = {}
        for r in cursor.fetchall():
            lookup[int(r.dev_id)] = {
                "plant_code": r.plant_code,
                "plant_id": r.plant_id,
            }

        return lookup

    def _dedup_rows(self, rows: list[dict]) -> list[dict]:
        seen = {}

        for r in rows:
            key = (
                r["plant_code"],
                r["dev_type_id"],
                r["dev_id"],
                r["collect_time_utc"],
                r["metric_name"],
            )
            seen[key] = r

        return list(seen.values())

    def _build_catalog_rows(self, rows: list[dict]) -> list[dict]:
        seen = {}

        for r in rows:
            key = (r["dev_type_id"], r["metric_name"])

            if key not in seen:
                sample_value = r["metric_value_raw_text"]

                if sample_value is None and r["metric_value_num"] is not None:
                    sample_value = str(r["metric_value_num"])
                elif sample_value is None and r["metric_value_bool"] is not None:
                    sample_value = str(r["metric_value_bool"])
                elif sample_value is None and r["metric_value_text"] is not None:
                    sample_value = str(r["metric_value_text"])

                seen[key] = {
                    "dev_type_id": r["dev_type_id"],
                    "metric_name": r["metric_name"],
                    "observed_data_type": r["value_type"],
                    "sample_value": sample_value,
                    "raw_id": r["raw_id"],
                }

        return list(seen.values())

    def _chunk_list(self, rows: list[dict], chunk_size: int):
        for i in range(0, len(rows), chunk_size):
            yield rows[i:i + chunk_size]

    def _print_stats(self, stats: dict[str, Any]) -> None:
        print(
            f"[NORM] raw_id={stats['raw_id']} stats "
            f"records={stats['record_count']} "
            f"parsed_metrics={stats['parsed_metric_count']} "
            f"allowed_metrics={stats['allowed_metric_count']} "
            f"blocked_metrics={stats['blocked_metric_count']} "
            f"skipped_nulls={stats['skipped_null_count']} "
            f"raw_text_suppressed={stats['raw_text_suppressed_count']} "
            f"output_rows={stats['output_row_count']}"
        )
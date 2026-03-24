from __future__ import annotations

from typing import Iterable


class MetricRepository:
    def __init__(self, conn, metric_catalog_repo=None, logger=None):
        self.conn = conn
        self.metric_catalog_repo = metric_catalog_repo
        self.logger = logger

    def upsert_generic_metrics(
        self,
        rows: list[dict],
        chunk_size: int = 1000,
        enable_catalog_upsert: bool = False,
        use_merge: bool = False,
    ) -> None:
        """
        Production-grade bulk upsert for generic metrics.

        Strategy:
        - Dedupe in Python by natural key
        - Bulk insert to temp table
        - Upsert target table in batch (UPDATE + INSERT by default)
        - Optional distinct catalog upsert
        - Single commit at the end
        """
        if not rows:
            self._log("info", "[METRIC] no generic rows to upsert")
            return

        deduped_rows = self._dedupe_rows(rows)

        self._log(
            "info",
            f"[METRIC] incoming_rows={len(rows)} deduped_rows={len(deduped_rows)} chunk_size={chunk_size}"
        )

        original_autocommit = getattr(self.conn, "autocommit", False)
        self.conn.autocommit = False

        try:
            for chunk_no, chunk in enumerate(self._chunked(deduped_rows, chunk_size), start=1):
                self._upsert_chunk(chunk=chunk, chunk_no=chunk_no, use_merge=use_merge)

            if enable_catalog_upsert and self.metric_catalog_repo:
                self._upsert_metric_catalog_distinct(deduped_rows)

            self.conn.commit()
            self._log("info", f"[METRIC] upsert completed rows={len(deduped_rows)}")

        except Exception:
            self.conn.rollback()
            self._log("exception", "[METRIC] upsert_generic_metrics failed; rolled back")
            raise
        finally:
            self.conn.autocommit = original_autocommit

    def _upsert_chunk(self, chunk: list[dict], chunk_no: int, use_merge: bool) -> None:
        cursor = self.conn.cursor()
        cursor.fast_executemany = True

        self._create_temp_table(cursor)

        insert_stage_sql = """
        INSERT INTO #stage_device_metric_long (
            raw_id,
            plant_id,
            plant_code,
            dev_type_id,
            dev_id,
            dev_dn,
            collect_time_utc,
            collect_time_local,
            metric_name,
            value_type,
            metric_value_num,
            metric_value_text,
            metric_value_bool,
            metric_value_raw_text,
            source_api
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        params = [
            (
                r.get("raw_id"),
                r.get("plant_id"),
                r.get("plant_code"),
                r.get("dev_type_id"),
                str(r.get("dev_id")) if r.get("dev_id") is not None else None,
                r.get("dev_dn"),
                r.get("collect_time_utc"),
                r.get("collect_time_local"),
                r.get("metric_name"),
                r.get("value_type"),
                r.get("metric_value_num"),
                r.get("metric_value_text"),
                r.get("metric_value_bool"),
                r.get("metric_value_raw_text"),
                r.get("source_api"),
            )
            for r in chunk
        ]

        cursor.executemany(insert_stage_sql, params)

        if use_merge:
            cursor.execute(self._merge_sql())
        else:
            cursor.execute(self._update_existing_sql())
            cursor.execute(self._insert_new_sql())

        cursor.execute("DROP TABLE #stage_device_metric_long")

        self._log("info", f"[METRIC] chunk={chunk_no} upserted rows={len(chunk)}")

    def _create_temp_table(self, cursor) -> None:
        cursor.execute("""
        IF OBJECT_ID('tempdb..#stage_device_metric_long') IS NOT NULL
            DROP TABLE #stage_device_metric_long;

        CREATE TABLE #stage_device_metric_long (
            raw_id BIGINT NULL,
            plant_id BIGINT NULL,
            plant_code NVARCHAR(64) NOT NULL,
            dev_type_id INT NOT NULL,
            dev_id NVARCHAR(64) NOT NULL,
            dev_dn NVARCHAR(255) NULL,
            collect_time_utc DATETIME2(0) NOT NULL,
            collect_time_local DATETIME2(0) NULL,
            metric_name NVARCHAR(128) NOT NULL,
            value_type NVARCHAR(32) NULL,
            metric_value_num FLOAT NULL,
            metric_value_text NVARCHAR(4000) NULL,
            metric_value_bool BIT NULL,
            metric_value_raw_text NVARCHAR(4000) NULL,
            source_api NVARCHAR(128) NULL
        );
        """)

    @staticmethod
    def _update_existing_sql() -> str:
        return """
        UPDATE tgt
        SET
            tgt.raw_id = src.raw_id,
            tgt.plant_id = src.plant_id,
            tgt.dev_dn = src.dev_dn,
            tgt.collect_time_local = src.collect_time_local,
            tgt.value_type = src.value_type,
            tgt.metric_value_num = src.metric_value_num,
            tgt.metric_value_text = src.metric_value_text,
            tgt.metric_value_bool = src.metric_value_bool,
            tgt.metric_value_raw_text = src.metric_value_raw_text,
            tgt.source_api = src.source_api
        FROM norm.device_metric_long AS tgt
        INNER JOIN #stage_device_metric_long AS src
            ON tgt.plant_code = src.plant_code
           AND tgt.dev_type_id = src.dev_type_id
           AND tgt.dev_id = src.dev_id
           AND tgt.collect_time_utc = src.collect_time_utc
           AND tgt.metric_name = src.metric_name;
        """

    @staticmethod
    def _insert_new_sql() -> str:
        return """
        INSERT INTO norm.device_metric_long (
            raw_id,
            plant_id,
            plant_code,
            dev_type_id,
            dev_id,
            dev_dn,
            collect_time_utc,
            collect_time_local,
            metric_name,
            value_type,
            metric_value_num,
            metric_value_text,
            metric_value_bool,
            metric_value_raw_text,
            source_api
        )
        SELECT
            src.raw_id,
            src.plant_id,
            src.plant_code,
            src.dev_type_id,
            src.dev_id,
            src.dev_dn,
            src.collect_time_utc,
            src.collect_time_local,
            src.metric_name,
            src.value_type,
            src.metric_value_num,
            src.metric_value_text,
            src.metric_value_bool,
            src.metric_value_raw_text,
            src.source_api
        FROM #stage_device_metric_long AS src
        LEFT JOIN norm.device_metric_long AS tgt
            ON tgt.plant_code = src.plant_code
           AND tgt.dev_type_id = src.dev_type_id
           AND tgt.dev_id = src.dev_id
           AND tgt.collect_time_utc = src.collect_time_utc
           AND tgt.metric_name = src.metric_name
        WHERE tgt.plant_code IS NULL;
        """

    @staticmethod
    def _merge_sql() -> str:
        return """
        MERGE norm.device_metric_long AS tgt
        USING #stage_device_metric_long AS src
        ON  tgt.plant_code = src.plant_code
        AND tgt.dev_type_id = src.dev_type_id
        AND tgt.dev_id = src.dev_id
        AND tgt.collect_time_utc = src.collect_time_utc
        AND tgt.metric_name = src.metric_name

        WHEN MATCHED THEN
            UPDATE SET
                tgt.raw_id = src.raw_id,
                tgt.plant_id = src.plant_id,
                tgt.dev_dn = src.dev_dn,
                tgt.collect_time_local = src.collect_time_local,
                tgt.value_type = src.value_type,
                tgt.metric_value_num = src.metric_value_num,
                tgt.metric_value_text = src.metric_value_text,
                tgt.metric_value_bool = src.metric_value_bool,
                tgt.metric_value_raw_text = src.metric_value_raw_text,
                tgt.source_api = src.source_api

        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                raw_id, plant_id, plant_code, dev_type_id, dev_id, dev_dn,
                collect_time_utc, collect_time_local,
                metric_name, value_type,
                metric_value_num, metric_value_text, metric_value_bool, metric_value_raw_text,
                source_api
            )
            VALUES (
                src.raw_id, src.plant_id, src.plant_code, src.dev_type_id, src.dev_id, src.dev_dn,
                src.collect_time_utc, src.collect_time_local,
                src.metric_name, src.value_type,
                src.metric_value_num, src.metric_value_text, src.metric_value_bool, src.metric_value_raw_text,
                src.source_api
            );
        """

    def _upsert_metric_catalog_distinct(self, rows: list[dict]) -> None:
        """
        Distinct upsert only once per (dev_type_id, metric_name, value_type).
        This avoids catalog write per metric row.
        """
        seen = set()

        for r in rows:
            key = (
                r.get("dev_type_id"),
                r.get("metric_name"),
                r.get("value_type"),
            )
            if key in seen:
                continue
            seen.add(key)

            self.metric_catalog_repo.upsert_observation(
                dev_type_id=r.get("dev_type_id"),
                metric_name=r.get("metric_name"),
                observed_data_type=r.get("value_type"),
                sample_value=r.get("metric_value_raw_text"),
                raw_id=r.get("raw_id"),
            )

        self._log("info", f"[METRIC] metric_catalog_distinct_upserts={len(seen)}")

    @staticmethod
    def _dedupe_rows(rows: list[dict]) -> list[dict]:
        """
        Deduplicate by natural key.
        Keep last occurrence in the same batch.
        """
        latest_by_key = {}

        for r in rows:
            key = (
                r.get("plant_code"),
                r.get("dev_type_id"),
                str(r.get("dev_id")) if r.get("dev_id") is not None else None,
                r.get("collect_time_utc"),
                r.get("metric_name"),
            )
            latest_by_key[key] = r

        return list(latest_by_key.values())

    @staticmethod
    def _chunked(rows: list[dict], chunk_size: int) -> Iterable[list[dict]]:
        for i in range(0, len(rows), chunk_size):
            yield rows[i:i + chunk_size]

    def _log(self, level: str, message: str) -> None:
        if not self.logger:
            return
        log_fn = getattr(self.logger, level, None)
        if callable(log_fn):
            log_fn(message)
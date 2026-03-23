from __future__ import annotations


class MetricCatalogRepository:
    def __init__(self, conn):
        self.conn = conn

    def upsert_observation(
        self,
        dev_type_id: int,
        metric_name: str,
        observed_data_type: str,
        sample_value: str | None,
        raw_id: int,
    ):
        cursor = self.conn.cursor()
        cursor.execute("""
            IF EXISTS (
                SELECT 1
                FROM norm.metric_catalog
                WHERE dev_type_id = ?
                  AND metric_name = ?
            )
            BEGIN
                UPDATE norm.metric_catalog
                SET observed_data_type = ?,
                    last_seen_at_utc = SYSUTCDATETIME(),
                    sample_value = COALESCE(?, sample_value),
                    last_seen_raw_id = ?
                WHERE dev_type_id = ?
                  AND metric_name = ?
            END
            ELSE
            BEGIN
                INSERT INTO norm.metric_catalog (
                    dev_type_id,
                    metric_name,
                    observed_data_type,
                    first_seen_at_utc,
                    last_seen_at_utc,
                    sample_value,
                    first_seen_raw_id,
                    last_seen_raw_id,
                    is_active
                )
                VALUES (?, ?, ?, SYSUTCDATETIME(), SYSUTCDATETIME(), ?, ?, ?, 1)
            END
        """, (
            dev_type_id,
            metric_name,
            observed_data_type,
            sample_value,
            raw_id,
            dev_type_id,
            metric_name,

            dev_type_id,
            metric_name,
            observed_data_type,
            sample_value,
            raw_id,
            raw_id,
        ))
        self.conn.commit()
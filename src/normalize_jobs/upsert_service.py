class UpsertService:
    def __init__(self, conn):
        self.conn = conn

    def upsert_device_metric_long(self, load_id: int):
        cursor = self.conn.cursor()

        cursor.execute("""
            UPDATE tgt
            SET
                tgt.raw_id = stg.raw_id,
                tgt.value_type = stg.value_type,
                tgt.metric_value_num = stg.metric_value_num,
                tgt.metric_value_text = stg.metric_value_text,
                tgt.metric_value_bool = stg.metric_value_bool,
                tgt.metric_value_raw_text = stg.metric_value_raw_text,
                tgt.source_api = stg.source_api
            FROM norm.device_metric_long tgt
            JOIN stage.device_metric_long_load stg
                ON tgt.plant_code = stg.plant_code
               AND tgt.dev_type_id = stg.dev_type_id
               AND tgt.dev_id = stg.dev_id
               AND tgt.collect_time_utc = stg.collect_time_utc
               AND tgt.metric_name = stg.metric_name
            WHERE stg.load_id = ?
        """, (load_id,))

        cursor.execute("""
            INSERT INTO norm.device_metric_long (
                raw_id, plant_id, plant_code, dev_type_id, dev_id, dev_dn,
                collect_time_utc, collect_time_local, metric_name, value_type,
                metric_value_num, metric_value_text, metric_value_bool,
                metric_value_raw_text, source_api
            )
            SELECT
                stg.raw_id, stg.plant_id, stg.plant_code, stg.dev_type_id, stg.dev_id, stg.dev_dn,
                stg.collect_time_utc, stg.collect_time_local, stg.metric_name, stg.value_type,
                stg.metric_value_num, stg.metric_value_text, stg.metric_value_bool,
                stg.metric_value_raw_text, stg.source_api
            FROM stage.device_metric_long_load stg
            LEFT JOIN norm.device_metric_long tgt
                ON tgt.plant_code = stg.plant_code
               AND tgt.dev_type_id = stg.dev_type_id
               AND tgt.dev_id = stg.dev_id
               AND tgt.collect_time_utc = stg.collect_time_utc
               AND tgt.metric_name = stg.metric_name
            WHERE stg.load_id = ?
              AND tgt.metric_row_id IS NULL
        """, (load_id,))

        self.conn.commit()

    def upsert_metric_catalog(self, load_id: int):
        cursor = self.conn.cursor()

        cursor.execute("""
            UPDATE mc
            SET
                mc.observed_data_type = stg.observed_data_type,
                mc.last_seen_at_utc = SYSUTCDATETIME(),
                mc.sample_value = COALESCE(stg.sample_value, mc.sample_value),
                mc.last_seen_raw_id = stg.raw_id
            FROM norm.metric_catalog mc
            JOIN stage.metric_catalog_load stg
                ON mc.dev_type_id = stg.dev_type_id
               AND mc.metric_name = stg.metric_name
            WHERE stg.load_id = ?
        """, (load_id,))

        cursor.execute("""
            INSERT INTO norm.metric_catalog (
                dev_type_id, metric_name, observed_data_type,
                first_seen_at_utc, last_seen_at_utc,
                sample_value, first_seen_raw_id, last_seen_raw_id, is_active
            )
            SELECT
                stg.dev_type_id, stg.metric_name, stg.observed_data_type,
                SYSUTCDATETIME(), SYSUTCDATETIME(),
                stg.sample_value, stg.raw_id, stg.raw_id, 1
            FROM stage.metric_catalog_load stg
            LEFT JOIN norm.metric_catalog mc
                ON mc.dev_type_id = stg.dev_type_id
               AND mc.metric_name = stg.metric_name
            WHERE stg.load_id = ?
              AND mc.metric_catalog_id IS NULL
        """, (load_id,))

        self.conn.commit()
class MetricRepository:
    def __init__(self, conn, metric_catalog_repo=None):
        self.conn = conn
        self.metric_catalog_repo = metric_catalog_repo

    def upsert_generic_metrics(self, rows: list[dict]):
        cursor = self.conn.cursor()

        for r in rows:
            cursor.execute("""
                IF EXISTS (
                    SELECT 1
                    FROM norm.device_metric_long
                    WHERE plant_code = ?
                      AND dev_type_id = ?
                      AND dev_id = ?
                      AND collect_time_utc = ?
                      AND metric_name = ?
                )
                BEGIN
                    UPDATE norm.device_metric_long
                    SET raw_id = ?,
                        value_type = ?,
                        metric_value_num = ?,
                        metric_value_text = ?,
                        metric_value_bool = ?,
                        metric_value_raw_text = ?,
                        source_api = ?
                    WHERE plant_code = ?
                      AND dev_type_id = ?
                      AND dev_id = ?
                      AND collect_time_utc = ?
                      AND metric_name = ?
                END
                ELSE
                BEGIN
                    INSERT INTO norm.device_metric_long (
                        raw_id, plant_id, plant_code, dev_type_id, dev_id, dev_dn,
                        collect_time_utc, collect_time_local,
                        metric_name, value_type,
                        metric_value_num, metric_value_text, metric_value_bool, metric_value_raw_text,
                        source_api
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                END
            """, (
                r["plant_code"], r["dev_type_id"], r["dev_id"], r["collect_time_utc"], r["metric_name"],
                r["raw_id"], r["value_type"], r["metric_value_num"], r["metric_value_text"], r["metric_value_bool"], r["metric_value_raw_text"], r["source_api"],
                r["plant_code"], r["dev_type_id"], r["dev_id"], r["collect_time_utc"], r["metric_name"],

                r["raw_id"], r["plant_id"], r["plant_code"], r["dev_type_id"], r["dev_id"], r["dev_dn"],
                r["collect_time_utc"], r["collect_time_local"],
                r["metric_name"], r["value_type"],
                r["metric_value_num"], r["metric_value_text"], r["metric_value_bool"], r["metric_value_raw_text"],
                r["source_api"],
            ))

            if self.metric_catalog_repo:
                sample_value = r["metric_value_raw_text"]
                self.metric_catalog_repo.upsert_observation(
                    dev_type_id=r["dev_type_id"],
                    metric_name=r["metric_name"],
                    observed_data_type=r["value_type"],
                    sample_value=sample_value,
                    raw_id=r["raw_id"],
                )

        self.conn.commit()
import time


class StageWriter:
    def __init__(self, conn):
        self.conn = conn

    def next_load_id(self) -> int:
        return int(time.time() * 1000)

    def insert_metric_rows(self, load_id: int, rows: list[dict]):
        if not rows:
            return

        cursor = self.conn.cursor()
        cursor.fast_executemany = True

        sql = """
            INSERT INTO stage.device_metric_long_load (
                load_id, raw_id, plant_id, plant_code, dev_type_id, dev_id, dev_dn,
                collect_time_utc, collect_time_local, metric_name, value_type,
                metric_value_num, metric_value_text, metric_value_bool,
                metric_value_raw_text, source_api
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        params = [
            (
                load_id,
                r["raw_id"],
                r["plant_id"],
                r["plant_code"],
                r["dev_type_id"],
                r["dev_id"],
                r["dev_dn"],
                r["collect_time_utc"],
                r["collect_time_local"],
                r["metric_name"],
                r["value_type"],
                r["metric_value_num"],
                r["metric_value_text"],
                r["metric_value_bool"],
                r["metric_value_raw_text"],
                r["source_api"],
            )
            for r in rows
        ]

        cursor.executemany(sql, params)
        self.conn.commit()

    def insert_metric_catalog_rows(self, load_id: int, rows: list[dict]):
        if not rows:
            return

        cursor = self.conn.cursor()
        cursor.fast_executemany = True

        sql = """
            INSERT INTO stage.metric_catalog_load (
                load_id, dev_type_id, metric_name, observed_data_type, sample_value, raw_id
            )
            VALUES (?, ?, ?, ?, ?, ?)
        """

        params = [
            (
                load_id,
                r["dev_type_id"],
                r["metric_name"],
                r["observed_data_type"],
                r["sample_value"],
                r["raw_id"],
            )
            for r in rows
        ]

        cursor.executemany(sql, params)
        self.conn.commit()

    def cleanup(self, load_id: int):
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM stage.metric_catalog_load WHERE load_id = ?", (load_id,))
        cursor.execute("DELETE FROM stage.device_metric_long_load WHERE load_id = ?", (load_id,))
        self.conn.commit()
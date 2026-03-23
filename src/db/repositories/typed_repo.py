class TypedRepository:
    TABLE_MAP = {
        1: "mart.fact_dev_inverter_5min",
        10: "mart.fact_dev_emi_5min",
        17: "mart.fact_dev_meter_5min",
        63: "mart.fact_dev_logger_5min",
    }

    KEY_MAP = {
        1: ("dev_id", "collect_time_utc"),
        10: ("dev_id", "collect_time_utc"),
        17: ("dev_id", "collect_time_utc"),
        63: ("dev_id", "collect_time_utc"),
    }

    def __init__(self, conn):
        self.conn = conn

    def upsert(self, dev_type_id: int, rows: list[dict]):
        if not rows:
            return

        table = self.TABLE_MAP.get(dev_type_id)
        if not table:
            return

        cursor = self.conn.cursor()

        for row in rows:
            key_dev_id = row["dev_id"]
            key_collect_time = row["collect_time_utc"]

            columns = list(row.keys())
            update_cols = [c for c in columns if c not in ("dev_id", "collect_time_utc")]

            set_clause = ", ".join([f"{c} = ?" for c in update_cols])
            insert_cols = ", ".join(columns)
            insert_q = ", ".join(["?"] * len(columns))

            sql = f"""
                IF EXISTS (
                    SELECT 1 FROM {table}
                    WHERE dev_id = ? AND collect_time_utc = ?
                )
                BEGIN
                    UPDATE {table}
                    SET {set_clause}
                    WHERE dev_id = ? AND collect_time_utc = ?
                END
                ELSE
                BEGIN
                    INSERT INTO {table} ({insert_cols})
                    VALUES ({insert_q})
                END
            """

            params = [key_dev_id, key_collect_time]
            params.extend([row[c] for c in update_cols])
            params.extend([key_dev_id, key_collect_time])
            params.extend([row[c] for c in columns])

            cursor.execute(sql, params)

        self.conn.commit()
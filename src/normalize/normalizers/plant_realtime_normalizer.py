from __future__ import annotations

import json
from datetime import datetime, timezone
from zoneinfo import ZoneInfo


class PlantRealtimeNormalizer:
    def __init__(self, conn):
        self.conn = conn

    def run(self):
        rows = self._get_pending_raw_rows()
        total = 0

        for row in rows:
            raw_id = row["raw_id"]
            response_json = row["response_json"]

            try:
                payload = json.loads(response_json) if response_json else {}
            except Exception:
                continue

            if not payload or not payload.get("success"):
                continue

            data = payload.get("data") or []
            inserted = 0

            for plant_obj in data:
                plant_code = plant_obj.get("stationCode")
                metric_map = plant_obj.get("dataItemMap") or {}

                collect_time_utc = self._resolve_collect_time_utc(row, payload)
                collect_time_local = self._to_local(collect_time_utc)

                for metric_name, metric_value in metric_map.items():
                    self._insert_metric(
                        plant_code=plant_code,
                        period_type="REALTIME",
                        period_start_utc=collect_time_utc,
                        period_end_utc=collect_time_utc,
                        period_start_local=collect_time_local,
                        period_end_local=collect_time_local,
                        metric_name=metric_name,
                        metric_value=metric_value,
                        source_api="getStationRealKpi",
                        raw_id=raw_id,
                    )
                    inserted += 1

            self.conn.commit()
            total += inserted
            print(f"[NORMALIZE] raw_id={raw_id} inserted={inserted}")

        print(f"[NORMALIZE] DONE total_metrics={total}")

    def _get_pending_raw_rows(self):
        sql = """
        SELECT r.raw_id, r.response_json, r.request_finished_at_utc
        FROM raw.api_call r
        WHERE r.api_name = 'getStationRealKpi'
          AND r.api_success_flag = 1
          AND NOT EXISTS
          (
              SELECT 1
              FROM norm.plant_metric_long n
              WHERE n.raw_id = r.raw_id
          )
        ORDER BY r.raw_id
        """
        cur = self.conn.cursor()
        cur.execute(sql)
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, x)) for x in cur.fetchall()]

    def _resolve_collect_time_utc(self, raw_row, payload):
        dt = raw_row.get("request_finished_at_utc")

        if isinstance(dt, datetime):
            if dt.tzinfo is not None:
                return dt.astimezone(timezone.utc).replace(tzinfo=None)
            return dt

        current_time_ms = ((payload.get("params") or {}).get("currentTime"))
        if current_time_ms:
            return datetime.fromtimestamp(
                current_time_ms / 1000, tz=timezone.utc
            ).replace(tzinfo=None)

        return datetime.utcnow()

    def _to_local(self, utc_dt):
        if utc_dt is None:
            return None
        return utc_dt.replace(tzinfo=ZoneInfo("UTC")) \
                     .astimezone(ZoneInfo("Asia/Bangkok")) \
                     .replace(tzinfo=None)

    def _insert_metric(
        self,
        *,
        plant_code,
        period_type,
        period_start_utc,
        period_end_utc,
        period_start_local,
        period_end_local,
        metric_name,
        metric_value,
        source_api,
        raw_id,
    ):
        value_type = self._value_type(metric_value)

        metric_value_num = None
        metric_value_text = None
        metric_value_bool = None
        metric_value_raw_text = None if metric_value is None else str(metric_value)

        if value_type == "NUMBER":
            metric_value_num = float(metric_value)
        elif value_type == "BOOLEAN":
            metric_value_bool = 1 if bool(metric_value) else 0
        else:
            metric_value_text = None if metric_value is None else str(metric_value)

        sql = """
        INSERT INTO norm.plant_metric_long
        (
            plant_code,
            period_type,
            period_start_utc,
            period_end_utc,
            period_start_local,
            period_end_local,
            metric_name,
            value_type,
            metric_value_num,
            metric_value_text,
            metric_value_bool,
            metric_value_raw_text,
            source_api,
            raw_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        cur = self.conn.cursor()
        cur.execute(
            sql,
            (
                plant_code,
                period_type,
                period_start_utc,
                period_end_utc,
                period_start_local,
                period_end_local,
                metric_name,
                value_type,
                metric_value_num,
                metric_value_text,
                metric_value_bool,
                metric_value_raw_text,
                source_api,
                raw_id,
            ),
        )

    def _value_type(self, value):
        if value is None:
            return "TEXT"
        if isinstance(value, bool):
            return "BOOLEAN"
        try:
            float(value)
            return "NUMBER"
        except Exception:
            return "TEXT"
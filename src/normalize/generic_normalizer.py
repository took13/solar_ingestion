from __future__ import annotations

from typing import Any, Dict, List, Optional

from src.domain.enums import ValueType
from src.domain.time_utils import epoch_ms_to_utc


class GenericNormalizer:
    """
    Generic Huawei device normalizer.

    Safety behavior:
    - Requires whitelist rules per source_api + dev_type_id
    - Blocks metrics not in whitelist
    - Skips NULL values unless keep_null=1
    - Suppresses metric_value_raw_text unless keep_raw_text=1

    This prevents all-metric long normalization from exploding norm.device_metric_long.
    """

    def normalize(
        self,
        response_body: Dict[str, Any],
        raw_id: int,
        plant_code: str,
        plant_id: Optional[int],
        dev_type_id: int,
        source_api: str,
        whitelist_rules: Optional[dict[str, dict[str, Any]]] = None,
        require_whitelist: bool = True,
    ) -> dict[str, Any]:
        rows: List[dict] = []

        stats = {
            "raw_id": raw_id,
            "source_api": source_api,
            "dev_type_id": dev_type_id,
            "record_count": 0,
            "parsed_metric_count": 0,
            "allowed_metric_count": 0,
            "blocked_metric_count": 0,
            "skipped_null_count": 0,
            "raw_text_suppressed_count": 0,
            "skipped_no_collect_time_count": 0,
            "skipped_bad_collect_time_count": 0,
            "skipped_no_dev_id_count": 0,
            "output_row_count": 0,
        }

        whitelist_rules = whitelist_rules or {}

        if require_whitelist and not whitelist_rules:
            raise RuntimeError(
                f"No enabled whitelist rules found for source_api={source_api}, "
                f"dev_type_id={dev_type_id}. Refusing to normalize all metrics."
            )

        data = response_body.get("data") or []
        params = response_body.get("params") or {}
        response_current_ms = params.get("currentTime")

        for record in data:
            stats["record_count"] += 1

            # getDevHistoryKpi has collectTime per record.
            # getDevRealKpi usually has no collectTime, so use params.currentTime.
            collect_ms = record.get("collectTime") or response_current_ms
            if collect_ms is None:
                stats["skipped_no_collect_time_count"] += 1
                continue

            try:
                collect_time_utc = epoch_ms_to_utc(int(collect_ms))
            except Exception:
                stats["skipped_bad_collect_time_count"] += 1
                continue

            dev_id = record.get("devId") or record.get("id")
            dev_dn = record.get("devDn") or record.get("dn") or record.get("sn")
            metric_map = record.get("dataItemMap") or record.get("dataItems") or {}

            if dev_id is None and dev_dn and "NE=" in str(dev_dn):
                try:
                    dev_id = int(str(dev_dn).split("NE=")[-1])
                except Exception:
                    dev_id = None

            if dev_id is None:
                stats["skipped_no_dev_id_count"] += 1
                continue

            for metric_name, value in metric_map.items():
                metric_name = str(metric_name)
                stats["parsed_metric_count"] += 1

                rule = whitelist_rules.get(metric_name)

                if require_whitelist and rule is None:
                    stats["blocked_metric_count"] += 1
                    continue

                keep_null = bool(rule.get("keep_null", False)) if rule else False
                keep_raw_text = bool(rule.get("keep_raw_text", False)) if rule else False

                parsed = self._parse_value(value)

                if parsed["value_type"] == ValueType.NULL.value and not keep_null:
                    stats["skipped_null_count"] += 1
                    continue

                if not keep_raw_text and parsed["metric_value_raw_text"] is not None:
                    parsed["metric_value_raw_text"] = None
                    stats["raw_text_suppressed_count"] += 1

                stats["allowed_metric_count"] += 1

                rows.append({
                    "raw_id": raw_id,
                    "plant_id": plant_id,
                    "plant_code": plant_code,
                    "dev_type_id": dev_type_id,
                    "dev_id": int(dev_id),
                    "dev_dn": dev_dn,
                    "collect_time_utc": collect_time_utc,
                    "collect_time_local": None,
                    "metric_name": metric_name,
                    "value_type": parsed["value_type"],
                    "metric_value_num": parsed["metric_value_num"],
                    "metric_value_text": parsed["metric_value_text"],
                    "metric_value_bool": parsed["metric_value_bool"],
                    "metric_value_raw_text": parsed["metric_value_raw_text"],
                    "source_api": source_api,
                })

        stats["output_row_count"] = len(rows)

        return {
            "rows": rows,
            "stats": stats,
        }

    def _parse_value(self, value: Any) -> dict:
        if value is None:
            return {
                "value_type": ValueType.NULL.value,
                "metric_value_num": None,
                "metric_value_text": None,
                "metric_value_bool": None,
                "metric_value_raw_text": None,
            }

        if isinstance(value, bool):
            return {
                "value_type": ValueType.BOOL.value,
                "metric_value_num": None,
                "metric_value_text": None,
                "metric_value_bool": value,
                "metric_value_raw_text": str(value),
            }

        if isinstance(value, (int, float)):
            return {
                "value_type": ValueType.NUMBER.value,
                "metric_value_num": float(value),
                "metric_value_text": None,
                "metric_value_bool": None,
                "metric_value_raw_text": str(value),
            }

        text = str(value).strip()

        try:
            return {
                "value_type": ValueType.NUMBER.value,
                "metric_value_num": float(text),
                "metric_value_text": None,
                "metric_value_bool": None,
                "metric_value_raw_text": text,
            }
        except Exception:
            pass

        if text.lower() in ("true", "false"):
            return {
                "value_type": ValueType.BOOL.value,
                "metric_value_num": None,
                "metric_value_text": None,
                "metric_value_bool": text.lower() == "true",
                "metric_value_raw_text": text,
            }

        return {
            "value_type": ValueType.TEXT.value,
            "metric_value_num": None,
            "metric_value_text": text,
            "metric_value_bool": None,
            "metric_value_raw_text": text,
        }
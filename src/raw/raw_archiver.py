from __future__ import annotations

import json
from pathlib import Path
from hashlib import sha256
from datetime import datetime, timezone


class RawArchiver:
    def __init__(self, raw_root: str):
        self.raw_root = Path(raw_root)

    def archive(
        self,
        plant_code: str,
        dev_type_id: int,
        batch_hash: str,
        batch_no: int,
        request_payload: dict,
        response_payload: dict,
    ) -> dict:
        window_start_dt = self._extract_window_start_dt(request_payload)
        folder_date = window_start_dt.strftime("%Y-%m-%d") if window_start_dt else "unknown_date"

        safe_plant_code = self._sanitize_for_path(plant_code)
        folder = self.raw_root / folder_date / safe_plant_code / f"devtype_{dev_type_id}"
        folder.mkdir(parents=True, exist_ok=True)

        request_text = json.dumps(request_payload, ensure_ascii=False, indent=2)
        response_text = json.dumps(response_payload, ensure_ascii=False, indent=2)

        time_range_text = self._build_time_range_text(request_payload)
        short_hash = batch_hash[:8]
        batch_text = f"batch{batch_no:03d}"

        base_name = f"{time_range_text}_dev{dev_type_id}_{batch_text}_{short_hash}"

        request_file = folder / f"{base_name}_request.json"
        response_file = folder / f"{base_name}_response.json"

        request_file.write_text(request_text, encoding="utf-8")
        response_file.write_text(response_text, encoding="utf-8")

        return {
            "request_file_path": str(request_file),
            "response_file_path": str(response_file),
            "request_sha256": sha256(request_text.encode("utf-8")).hexdigest(),
            "response_sha256": sha256(response_text.encode("utf-8")).hexdigest(),
            "request_size_bytes": len(request_text.encode("utf-8")),
            "response_size_bytes": len(response_text.encode("utf-8")),
            "folder_date": folder_date,
        }

    def _extract_window_start_dt(self, request_payload: dict) -> datetime | None:
        start_ms = request_payload.get("startTime")
        if start_ms is None:
            return None
        return datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)

    def _build_time_range_text(self, request_payload: dict) -> str:
        start_ms = request_payload.get("startTime")
        end_ms = request_payload.get("endTime")

        if start_ms is None or end_ms is None:
            return "no_window"

        start_dt = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
        end_dt = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc)

        if start_dt.date() == end_dt.date():
            return f"{start_dt.strftime('%Y%m%d_%H%M%S')}_{end_dt.strftime('%H%M%S')}"

        return f"{start_dt.strftime('%Y%m%d_%H%M%S')}_{end_dt.strftime('%Y%m%d_%H%M%S')}"

    def _sanitize_for_path(self, value: str) -> str:
        if not value:
            return "unknown"

        safe = value.strip()
        replacements = {
            "\\": "_",
            "/": "_",
            ":": "_",
            "*": "_",
            "?": "_",
            "\"": "_",
            "<": "_",
            ">": "_",
            "|": "_",
            "=": "-",
            " ": "_",
        }
        for old, new in replacements.items():
            safe = safe.replace(old, new)

        return safe
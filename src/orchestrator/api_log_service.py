from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any


class ApiLogService:
    def __init__(self, raw_repo, raw_archiver=None):
        self.raw_repo = raw_repo
        self.raw_archiver = raw_archiver

    def log_api_call(
        self,
        *,
        run_id: int,
        job_id: int,
        account_id: int,
        plant_code: str,
        dev_type_id: int,
        api_family: str,
        api_name: str,
        endpoint_path: str,
        request_method: str,
        batch_no: int,
        device_count: int,
        request_payload: dict[str, Any],
        response: dict[str, Any] | None,
        request_started_at_utc: datetime,
        request_finished_at_utc: datetime,
        request_window_start_utc=None,
        request_window_end_utc=None,
        request_window_start_local=None,
        request_window_end_local=None,
        plant_id=None,
        fail_message: str | None = None,
    ) -> int:
        request_json = json.dumps(request_payload, ensure_ascii=False)
        response_json = json.dumps(response, ensure_ascii=False) if response is not None else None

        batch_hash = self._make_batch_hash(
            api_name=api_name,
            plant_code=plant_code,
            dev_type_id=dev_type_id,
            request_payload=request_payload,
            request_window_start_utc=request_window_start_utc,
            request_window_end_utc=request_window_end_utc,
            batch_no=batch_no,
        )

        response_size_bytes = len(response_json.encode("utf-8")) if response_json else None

        http_status = None
        api_success_flag = 0
        fail_code = None
        api_message = fail_message

        if response is not None:
            http_status = response.get("http_status")
            api_success_flag = 1 if response.get("success") else 0
            fail_code = response.get("fail_code")
            if api_message is None:
                api_message = response.get("message")

        raw_id = self.raw_repo.insert_api_call(
            {
                "run_id": run_id,
                "job_id": job_id,
                "account_id": account_id,
                "plant_id": plant_id,
                "plant_code": plant_code,
                "dev_type_id": dev_type_id,
                "api_family": api_family,
                "api_name": api_name,
                "endpoint_path": endpoint_path,
                "request_method": request_method,
                "request_window_start_utc": request_window_start_utc,
                "request_window_end_utc": request_window_end_utc,
                "request_window_start_local": request_window_start_local,
                "request_window_end_local": request_window_end_local,
                "batch_no": batch_no,
                "batch_hash": batch_hash,
                "device_count": device_count,
                "request_json": request_json,
                "response_json": response_json,
                "response_size_bytes": response_size_bytes,
                "http_status": http_status,
                "api_success_flag": api_success_flag,
                "fail_code": fail_code,
                "fail_message": api_message,
                "request_started_at_utc": self._ensure_utc_naive(request_started_at_utc),
                "request_finished_at_utc": self._ensure_utc_naive(request_finished_at_utc),
            }
        )

        if self.raw_archiver is not None:
            try:
                self.raw_archiver.archive_response(
                    api_name=api_name,
                    plant_code=plant_code,
                    dev_type_id=dev_type_id,
                    batch_no=batch_no,
                    request_started_at_utc=request_started_at_utc,
                    payload={
                        "request": request_payload,
                        "response": response,
                        "raw_id": raw_id,
                    },
                )
            except Exception as e:
                print(f"[WARN] Raw archive failed for {api_name}: {e}")

        return raw_id

    def _make_batch_hash(
        self,
        *,
        api_name: str,
        plant_code: str,
        dev_type_id: int,
        request_payload: dict[str, Any],
        request_window_start_utc,
        request_window_end_utc,
        batch_no: int,
    ) -> str:
        raw = json.dumps(
            {
                "api_name": api_name,
                "plant_code": plant_code,
                "dev_type_id": dev_type_id,
                "request_payload": request_payload,
                "request_window_start_utc": str(request_window_start_utc) if request_window_start_utc else None,
                "request_window_end_utc": str(request_window_end_utc) if request_window_end_utc else None,
                "batch_no": batch_no,
            },
            sort_keys=True,
            ensure_ascii=False,
        )
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _ensure_utc_naive(self, dt: datetime | None):
        if dt is None:
            return None
        if dt.tzinfo is None:
            return dt
        return dt.astimezone(timezone.utc).replace(tzinfo=None)

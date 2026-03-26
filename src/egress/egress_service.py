import json
from datetime import timedelta

from src.domain.time_utils import utc_now


class EgressService:
    def __init__(self, repo, client, payload_builder):
        self.repo = repo
        self.client = client
        self.payload_builder = payload_builder

    def run_online(self, lookback_minutes: int = 30):
        run_id = self.repo.start_run(run_mode="ONLINE", triggered_by="scheduler")
        any_failed = False
        now_utc = utc_now()

        targets = self.repo.get_enabled_targets()
        print(f"[EGRESS] online targets={len(targets)}")

        for target in targets:
            checkpoint = self.repo.get_checkpoint(target["egress_target_id"])

            if checkpoint and checkpoint.get("last_success_end_utc"):
                start_utc = checkpoint["last_success_end_utc"]
            else:
                start_utc = now_utc - timedelta(minutes=lookback_minutes)

            end_utc = now_utc

            ok = self._run_target(run_id, target, start_utc, end_utc)
            if not ok:
                any_failed = True

        self.repo.finish_run(run_id, status="PARTIAL" if any_failed else "SUCCESS", message=None)

    def run_backfill(self, start_utc, end_utc):
        run_id = self.repo.start_run(run_mode="BACKFILL", triggered_by="manual")
        any_failed = False

        targets = self.repo.get_enabled_targets()
        print(f"[EGRESS] backfill targets={len(targets)}")

        for target in targets:
            ok = self._run_target(run_id, target, start_utc, end_utc)
            if not ok:
                any_failed = True

        self.repo.finish_run(run_id, status="PARTIAL" if any_failed else "SUCCESS", message=None)

    def _run_target(self, run_id: int, target: dict, start_utc, end_utc) -> bool:
        request_started_at = utc_now()

        try:
            rows = self.repo.get_payload_rows(
                plant_code=target["plant_code"],
                start_utc=start_utc,
                end_utc=end_utc,
                record_limit=target["batch_record_limit"],
            )

            print(
                f"[EGRESS] plant={target['plant_code']} window={start_utc.isoformat()} -> {end_utc.isoformat()} rows={len(rows)}"
            )

            if not rows:
                self.repo.upsert_checkpoint(
                    egress_target_id=target["egress_target_id"],
                    plant_code=target["plant_code"],
                    last_success_end_utc=end_utc,
                    last_attempt_end_utc=end_utc,
                    last_status="SUCCESS",
                    last_error_message=None,
                )
                return True

            payload = self.payload_builder.build(rows)

            response = self.client.post_json(
                endpoint_url=target["endpoint_url"],
                auth_token=target["auth_token"],
                payload=payload,
                timeout_seconds=target["timeout_seconds"],
                max_attempts=target["retry_max_attempts"],
                backoff_seconds=target["retry_backoff_seconds"],
            )

            ok = 200 <= response.status_code < 300

            self.repo.insert_log({
                "egress_run_id": run_id,
                "egress_target_id": target["egress_target_id"],
                "plant_code": target["plant_code"],
                "window_start_utc": start_utc,
                "window_end_utc": end_utc,
                "record_count": len(rows),
                "request_json": json.dumps(payload, ensure_ascii=False),
                "response_text": response.text,
                "http_status": response.status_code,
                "status": "SUCCESS" if ok else "FAILED",
                "error_message": None if ok else response.text[:2000],
                "request_started_at_utc": request_started_at,
                "request_finished_at_utc": utc_now(),
            })

            self.repo.upsert_checkpoint(
                egress_target_id=target["egress_target_id"],
                plant_code=target["plant_code"],
                last_success_end_utc=end_utc if ok else None,
                last_attempt_end_utc=end_utc,
                last_status="SUCCESS" if ok else "FAILED",
                last_error_message=None if ok else response.text[:2000],
            )

            return ok

        except Exception as e:
            self.repo.insert_log({
                "egress_run_id": run_id,
                "egress_target_id": target["egress_target_id"],
                "plant_code": target["plant_code"],
                "window_start_utc": start_utc,
                "window_end_utc": end_utc,
                "record_count": 0,
                "request_json": None,
                "response_text": None,
                "http_status": None,
                "status": "FAILED",
                "error_message": str(e)[:2000],
                "request_started_at_utc": request_started_at,
                "request_finished_at_utc": utc_now(),
            })

            self.repo.upsert_checkpoint(
                egress_target_id=target["egress_target_id"],
                plant_code=target["plant_code"],
                last_success_end_utc=None,
                last_attempt_end_utc=end_utc,
                last_status="FAILED",
                last_error_message=str(e)[:2000],
            )
            return False
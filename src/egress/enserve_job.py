import json
import time
from datetime import datetime, timezone

from src.egress.enserve_client import EnserveClient
from src.egress.enserve_payload_builder import EnservePayloadBuilder
from src.egress.enserve_repository import EnserveRepository


class EnserveEgressJob:
    def __init__(self, conn):
        self.repo = EnserveRepository(conn)
        self.client = EnserveClient()
        self.builder = EnservePayloadBuilder()

    def run(self):
        targets = self.repo.get_enabled_targets()

        if not targets:
            print("[EGRESS] No enabled Enserve targets")
            return

        for target in targets:
            self._run_target(target)

    def _run_target(self, target: dict):
        egress_target_id = target["egress_target_id"]
        plant_code = target["plant_code"]

        checkpoint = self.repo.get_checkpoint(egress_target_id, plant_code)
        if not checkpoint:
            print(f"[EGRESS] Missing checkpoint target={egress_target_id}, plant={plant_code}")
            return

        start_utc = checkpoint["last_success_end_utc"]
        end_utc = datetime.now(timezone.utc).replace(tzinfo=None)

        rows = self.repo.get_rows_to_send(
            plant_code=plant_code,
            start_utc=start_utc,
            end_utc=end_utc,
            limit=target["batch_record_limit"],
        )

        if not rows:
            print(f"[EGRESS] No rows to send plant={plant_code}")
            return

        payload = self.builder.build(rows)
        record_count = len(payload["records"])

        if record_count == 0:
            print(f"[EGRESS] No valid rows after payload build plant={plant_code}")
            return

        request_body = json.dumps(payload, ensure_ascii=False)
        max_attempts = int(target.get("retry_max_attempts") or 3)
        backoff_seconds = int(target.get("retry_backoff_seconds") or 60)

        for attempt in range(1, max_attempts + 1):
            request_started = datetime.utcnow()
            http_status = None
            response_body = None
            error_message = None

            try:
                response = self.client.post_batch(
                    endpoint_url=target["endpoint_url"],
                    token=target["auth_token"],
                    payload=payload,
                    timeout_seconds=int(target.get("timeout_seconds") or 60),
                )

                http_status = response.status_code
                response_body = response.text

                if 200 <= response.status_code < 300:
                    last_sent_time = max(r["collect_time_utc"] for r in rows)

                    self.repo.log_request(
                        egress_target_id=egress_target_id,
                        plant_code=plant_code,
                        request_started_at_utc=request_started,
                        request_finished_at_utc=datetime.utcnow(),
                        http_status=http_status,
                        success_flag=1,
                        request_body=request_body,
                        response_body=response_body,
                        error_message=None,
                        records_count=record_count,
                    )

                    self.repo.update_checkpoint_success(
                        egress_target_id=egress_target_id,
                        plant_code=plant_code,
                        last_success_end_utc=last_sent_time,
                    )

                    print(f"[EGRESS] SUCCESS plant={plant_code}, records={record_count}")
                    return

                error_message = f"HTTP {response.status_code}: {response.text}"

                self.repo.log_request(
                    egress_target_id=egress_target_id,
                    plant_code=plant_code,
                    request_started_at_utc=request_started,
                    request_finished_at_utc=datetime.utcnow(),
                    http_status=http_status,
                    success_flag=0,
                    request_body=request_body,
                    response_body=response_body,
                    error_message=error_message,
                    records_count=record_count,
                )

                if response.status_code in (400, 401, 403, 422):
                    self.repo.update_checkpoint_failed(
                        egress_target_id,
                        plant_code,
                        max(r["collect_time_utc"] for r in rows),
                        error_message,
                    )
                    print(f"[EGRESS] NON-RETRYABLE {error_message}")
                    return

                if response.status_code == 429:
                    sleep_seconds = 60
                else:
                    sleep_seconds = backoff_seconds * attempt

                print(f"[EGRESS] RETRY attempt={attempt}, plant={plant_code}, sleep={sleep_seconds}s")
                time.sleep(sleep_seconds)

            except Exception as ex:
                error_message = str(ex)

                self.repo.log_request(
                    egress_target_id=egress_target_id,
                    plant_code=plant_code,
                    request_started_at_utc=request_started,
                    request_finished_at_utc=datetime.utcnow(),
                    http_status=http_status,
                    success_flag=0,
                    request_body=request_body,
                    response_body=response_body,
                    error_message=error_message,
                    records_count=record_count,
                )

                print(f"[EGRESS] EXCEPTION attempt={attempt}, plant={plant_code}: {error_message}")
                time.sleep(backoff_seconds * attempt)

        self.repo.update_checkpoint_failed(
            egress_target_id,
            plant_code,
            max(r["collect_time_utc"] for r in rows),
            error_message or "Max retry exceeded",
        )
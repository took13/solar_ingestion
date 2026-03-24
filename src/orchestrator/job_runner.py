from __future__ import annotations

import json
import time

from src.domain.hash_utils import build_batch_hash
from src.domain.time_utils import utc_now, fmt_local
from src.normalize.generic_normalizer import GenericNormalizer
from src.normalize.typed_dispatcher import TypedDispatcher


class JobRunner:
    def __init__(
        self,
        metadata_repo,
        checkpoint_repo,
        run_repo,
        raw_repo,
        metric_repo,
        typed_repo,
        batch_audit_repo,
        checkpoint_service,
        batch_planner,
        window_planner,
        client,
        raw_archiver,
        retry_policy,
        batch_delay_seconds: int = 3,
        generic_metrics_enabled: bool = False,
    ):
        self.metadata_repo = metadata_repo
        self.checkpoint_repo = checkpoint_repo
        self.run_repo = run_repo
        self.raw_repo = raw_repo
        self.metric_repo = metric_repo
        self.typed_repo = typed_repo
        self.batch_audit_repo = batch_audit_repo
        self.checkpoint_service = checkpoint_service
        self.batch_planner = batch_planner
        self.window_planner = window_planner
        self.client = client
        self.raw_archiver = raw_archiver
        self.retry_policy = retry_policy
        self.batch_delay_seconds = batch_delay_seconds
        self.generic_metrics_enabled = generic_metrics_enabled

        self.generic_normalizer = GenericNormalizer()
        self.typed_dispatcher = TypedDispatcher()

    def run_targets(self, job: dict, targets: list[dict]):
        run_id = self.run_repo.start_run(
            job_id=job["job_id"],
            run_type="manual",
            triggered_by="user",
        )

        print(f"[RUN] Started run_id={run_id} for job={job['job_name']} at {fmt_local(utc_now())}")

        any_failed = False

        for target in targets:
            print(
                f"[TARGET] plant={target['plant_code']} devType={target['dev_type_id']} "
                f"account_id={target['account_id']} batch_size={target['batch_size']}"
            )

            devices = self.metadata_repo.get_devices(
                plant_code=target["plant_code"],
                dev_type_id=target["dev_type_id"]
            )

            if not devices:
                print(
                    f"[TARGET] No devices found for plant={target['plant_code']} "
                    f"devType={target['dev_type_id']}"
                )
                self.checkpoint_service.mark_no_devices(target, run_id)
                continue

            checkpoint = self.checkpoint_repo.get_checkpoint(
                job_id=job["job_id"],
                account_id=target["account_id"],
                plant_code=target["plant_code"],
                dev_type_id=target["dev_type_id"]
            )

            window = self.window_planner.compute_window(checkpoint=checkpoint, target=target)
            if window is None:
                print(
                    f"[TARGET] Skip plant={target['plant_code']} devType={target['dev_type_id']} "
                    f"(no runnable window)"
                )
                self.checkpoint_service.mark_skipped(target, run_id, "No runnable window")
                continue

            print(
                f"[WINDOW] plant={target['plant_code']} devType={target['dev_type_id']} "
                f"{fmt_local(window['start_utc'])} -> {fmt_local(window['end_utc'])}"
            )

            batches = self.batch_planner.plan(devices, target["batch_size"])
            print(
                f"[TARGET] plant={target['plant_code']} devType={target['dev_type_id']} "
                f"devices={len(devices)} batches={len(batches)}"
            )

            target_failed = False

            archive_partition_mode = "window_date" if (
                target.get("override_start_utc") and target.get("override_end_utc")
            ) else "run_date"

            for batch_no, batch in enumerate(batches, start=1):
                dev_ids = [x["dev_id"] for x in batch]

                print(
                    f"[BATCH] plant={target['plant_code']} devType={target['dev_type_id']} "
                    f"batch={batch_no}/{len(batches)} devices_in_batch={len(dev_ids)} "
                    f"window_start={fmt_local(window['start_utc'])} "
                    f"window_end={fmt_local(window['end_utc'])}"
                )

                batch_hash = build_batch_hash(
                    account_id=target["account_id"],
                    plant_code=target["plant_code"],
                    dev_type_id=target["dev_type_id"],
                    api_name=job["api_name"],
                    dev_ids=dev_ids,
                    window_start_utc=window["start_utc"].isoformat(),
                    window_end_utc=window["end_utc"].isoformat(),
                )

                request_payload = {
                    "devTypeId": target["dev_type_id"],
                    "devIds": ",".join(str(x) for x in dev_ids),
                    "startTime": window["start_ms"],
                    "endTime": window["end_ms"],
                }

                request_started_at = utc_now()

                try:
                    result = self.retry_policy.execute(
                        self.client.get_dev_history_kpi,
                        dev_type_id=target["dev_type_id"],
                        dev_ids=dev_ids,
                        start_time_ms=window["start_ms"],
                        end_time_ms=window["end_ms"],
                    )
                except Exception as e:
                    target_failed = True
                    any_failed = True

                    print(
                        f"[BATCH][FAILED] plant={target['plant_code']} devType={target['dev_type_id']} "
                        f"batch={batch_no} error={str(e)}"
                    )

                    self.batch_audit_repo.insert({
                        "run_id": run_id,
                        "job_id": job["job_id"],
                        "account_id": target["account_id"],
                        "plant_code": target["plant_code"],
                        "dev_type_id": target["dev_type_id"],
                        "batch_no": batch_no,
                        "batch_hash": batch_hash,
                        "window_start_utc": window["start_utc"],
                        "window_end_utc": window["end_utc"],
                        "expected_device_count": len(dev_ids),
                        "actual_device_count": None,
                        "raw_id": None,
                        "status": "FAILED",
                        "fail_code": None,
                        "message": str(e),
                    })
                    continue

                archive = self.raw_archiver.archive(
                    plant_code=target["plant_code"],
                    dev_type_id=target["dev_type_id"],
                    batch_hash=batch_hash,
                    batch_no=batch_no,
                    request_payload=request_payload,
                    response_payload=result.body,
                    archive_partition_mode=archive_partition_mode,
                )

                raw_id = self.raw_repo.insert_api_call({
                    "run_id": run_id,
                    "job_id": job["job_id"],
                    "account_id": target["account_id"],
                    "plant_id": target.get("plant_id"),
                    "plant_code": target["plant_code"],
                    "dev_type_id": target["dev_type_id"],
                    "api_family": "thirdData",
                    "api_name": job["api_name"],
                    "endpoint_path": "/thirdData/getDevHistoryKpi",
                    "request_method": "POST",
                    "request_window_start_utc": window["start_utc"],
                    "request_window_end_utc": window["end_utc"],
                    "request_window_start_local": None,
                    "request_window_end_local": None,
                    "batch_no": batch_no,
                    "batch_hash": batch_hash,
                    "device_count": len(dev_ids),
                    "request_json": json.dumps(request_payload, ensure_ascii=False),
                    "response_json": json.dumps(result.body, ensure_ascii=False),
                    "response_size_bytes": archive["response_size_bytes"],
                    "http_status": result.http_status,
                    "api_success_flag": result.success,
                    "fail_code": result.fail_code,
                    "fail_message": result.message,
                    "request_started_at_utc": request_started_at,
                    "request_finished_at_utc": utc_now(),
                })

                self.batch_audit_repo.insert({
                    "run_id": run_id,
                    "job_id": job["job_id"],
                    "account_id": target["account_id"],
                    "plant_code": target["plant_code"],
                    "dev_type_id": target["dev_type_id"],
                    "batch_no": batch_no,
                    "batch_hash": batch_hash,
                    "window_start_utc": window["start_utc"],
                    "window_end_utc": window["end_utc"],
                    "expected_device_count": len(dev_ids),
                    "actual_device_count": len(result.body.get("data", [])),
                    "raw_id": raw_id,
                    "status": "SUCCESS" if result.success else "FAILED",
                    "fail_code": result.fail_code,
                    "message": result.message,
                })

                print(
                    f"[BATCH][DONE] plant={target['plant_code']} devType={target['dev_type_id']} "
                    f"batch={batch_no} raw_id={raw_id} api_success={result.success} "
                    f"records={len(result.body.get('data', []))} "
                    f"saved_to={archive['folder_date']}"
                )

                if self.generic_metrics_enabled:
                    generic_rows = self.generic_normalizer.normalize(
                        response_body=result.body,
                        raw_id=raw_id,
                        plant_code=target["plant_code"],
                        plant_id=target.get("plant_id"),
                        dev_type_id=target["dev_type_id"],
                        source_api=job["api_name"],
                    )
                    self.metric_repo.upsert_generic_metrics(generic_rows)

                    typed_rows = self.typed_dispatcher.normalize(
                        dev_type_id=target["dev_type_id"],
                        response_body=result.body,
                        raw_id=raw_id,
                        plant_code=target["plant_code"],
                    )
                    self.typed_repo.upsert(target["dev_type_id"], typed_rows)
                else:
                    print("[BATCH] generic_metrics disabled -> raw-only path")

                if self.batch_delay_seconds > 0:
                    print(f"[BATCH] sleeping {self.batch_delay_seconds} sec")
                    time.sleep(self.batch_delay_seconds)

            if target_failed:
                print(
                    f"[TARGET][PARTIAL] plant={target['plant_code']} devType={target['dev_type_id']}"
                )
                self.checkpoint_service.mark_partial(target, run_id, window)
            else:
                print(
                    f"[TARGET][SUCCESS] plant={target['plant_code']} devType={target['dev_type_id']} "
                    f"up_to={fmt_local(window['end_utc'])}"
                )
                self.checkpoint_service.mark_success(target, run_id, window)

        self.run_repo.finish_run(
            run_id=run_id,
            status="PARTIAL" if any_failed else "SUCCESS",
            message=None if not any_failed else "Some targets or batches failed.",
        )

        print(
            f"[RUN] Finished run_id={run_id} status={'PARTIAL' if any_failed else 'SUCCESS'} "
            f"at {fmt_local(utc_now())}"
        )
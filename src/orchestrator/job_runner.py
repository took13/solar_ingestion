from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.api.exceptions import HuaweiRateLimitError
from src.orchestrator.batch_planner import BatchPlanner
from src.orchestrator.rotation_planner import RotationPlanner


class JobRunner:
    def __init__(
        self,
        client,
        run_repo,
        checkpoint_repo,
        metadata_repo,
        checkpoint_service,
        batch_audit_repo,
        api_log_service,
        batch_planner: BatchPlanner,
        window_planner,
        retry_policy,
        rate_gate,
        rotation_state_repo=None,
        rotation_planner: RotationPlanner | None = None,
        account_backoff_policy: dict | None = None,
    ):
        self.client = client
        self.run_repo = run_repo
        self.checkpoint_repo = checkpoint_repo
        self.metadata_repo = metadata_repo
        self.checkpoint_service = checkpoint_service
        self.batch_audit_repo = batch_audit_repo
        self.api_log_service = api_log_service
        self.batch_planner = batch_planner
        self.window_planner = window_planner
        self.retry_policy = retry_policy
        self.rate_gate = rate_gate
        self.rotation_state_repo = rotation_state_repo
        self.rotation_planner = rotation_planner or RotationPlanner()
        self.account_backoff_policy = account_backoff_policy or {
            "first": 600,
            "second": 1800,
            "third": 3600,
        }
        self._rate_limit_events = 0

    def run_targets(self, job: dict, targets: list[dict]) -> int:
        run_id = self.run_repo.start_run(
            job_id=job["job_id"],
            run_type="manual",
            triggered_by="user",
        )

        any_failed = False
        collapsed_targets = self._collapse_targets(targets)

        for target in collapsed_targets:
            try:
                self._run_target(run_id, job, target)
            except Exception as e:
                any_failed = True
                self.batch_audit_repo.log_batch(
                    run_id=run_id,
                    target_id=target["target_id"],
                    batch_no=0,
                    batch_size=0,
                    status="TARGET_FAILED",
                    message=str(e),
                )

        self.run_repo.finish_run(
            run_id=run_id,
            status="FAILED" if any_failed else "SUCCESS",
        )
        return run_id

    def _collapse_targets(self, targets: list[dict]) -> list[dict]:
        collapsed: list[dict] = []
        seen_device_realtime: set[tuple[int, int]] = set()

        for target in targets:
            endpoint_name = target.get("endpoint_name")
            if endpoint_name == "getDevRealKpi":
                key = (target["account_id"], target["dev_type_id"])
                if key in seen_device_realtime:
                    continue
                seen_device_realtime.add(key)
                synthetic = dict(target)
                synthetic["plant_code"] = "__ACCOUNT__"
                synthetic["is_account_scope"] = True
                collapsed.append(synthetic)
            else:
                collapsed.append(target)

        return collapsed

    def _run_target(self, run_id: int, job: dict, target: dict) -> None:
        endpoint_name = target.get("endpoint_name") or job.get("api_name")
        service_class = (target.get("service_class") or "backfill").lower()

        print(
            f"[DEBUG] target_id={target['target_id']} "
            f"plant={target.get('plant_code')} "
            f"dev_type={target.get('dev_type_id')} "
            f"endpoint={endpoint_name} service_class={service_class}"
        )

        if endpoint_name == "getStationRealKpi":
            print("[DEBUG] enter plant realtime path")
            self._run_plant_realtime_target(run_id, target)
            return

        if endpoint_name == "getDevRealKpi" and target.get("is_account_scope"):
            devices = self.metadata_repo.get_devices_for_account_and_type(
                account_id=target["account_id"],
                dev_type_id=target["dev_type_id"],
            )
        else:
            devices = self.metadata_repo.get_devices(
                plant_code=target["plant_code"],
                dev_type_id=target["dev_type_id"],
            )

        print(f"[DEBUG] devices count = {len(devices)}")

        if not devices:
            self.checkpoint_service.mark_no_devices(target, run_id)
            return

        checkpoint = self.checkpoint_repo.get_checkpoint(
            job_id=target["job_id"],
            account_id=target["account_id"],
            plant_code=target["plant_code"],
            dev_type_id=target["dev_type_id"],
        )

        override_start_utc = target.get("override_start_utc")
        override_end_utc = target.get("override_end_utc")

        if endpoint_name == "getDevRealKpi":
            window = None
            print("[DEBUG] realtime device path -> window bypassed")
        elif override_start_utc and override_end_utc:
            window = {
                "start_utc": override_start_utc,
                "end_utc": override_end_utc,
                "start_ms": int(override_start_utc.timestamp() * 1000),
                "end_ms": int(override_end_utc.timestamp() * 1000),
            }
        else:
            window = self.window_planner.compute_window(checkpoint=checkpoint, target=target)

        if not window and endpoint_name != "getDevRealKpi":
            self.checkpoint_service.mark_skipped(target, run_id, "No runnable window")
            return

        if service_class == "nearline_rotating" and bool(target.get("rotation_enabled")):
            print("[DEBUG] enter rotating device path")
            self._run_rotating_device_target(run_id, target, endpoint_name, devices, window)
        else:
            print("[DEBUG] enter full device path")
            self._run_full_device_target(run_id, target, endpoint_name, devices, window)

    def _utcnow(self) -> datetime:
        return datetime.now(timezone.utc)

    def _plant_code_for_batch(self, target: dict, batch_items: list) -> str:
        if target.get("plant_code") and target.get("plant_code") != "__ACCOUNT__":
            return target["plant_code"]

        if not batch_items:
            return target.get("plant_code") or ""

        first = batch_items[0]
        if isinstance(first, dict):
            return first.get("plant_code") or (target.get("plant_code") or "")

        return ",".join(str(x) for x in batch_items)

    def _window_locals(self, window: dict | None):
        if not window:
            return None, None
        return window.get("start_local"), window.get("end_local")

    def _log_and_audit_success(self, *, run_id: int, target: dict, batch_no: int, batch_items: list, batch_size: int,
                               api_family: str, api_name: str, endpoint_path: str, request_payload: dict,
                               response: dict, started_at_utc: datetime, finished_at_utc: datetime,
                               window: dict | None) -> int:
        start_local, end_local = self._window_locals(window)
        raw_id = self.api_log_service.log_api_call(
            run_id=run_id,
            job_id=target["job_id"],
            account_id=target["account_id"],
            plant_code=self._plant_code_for_batch(target, batch_items),
            dev_type_id=target.get("dev_type_id") or 0,
            api_family=api_family,
            api_name=api_name,
            endpoint_path=endpoint_path,
            request_method="POST",
            batch_no=batch_no,
            device_count=batch_size,
            request_payload=request_payload,
            response=response,
            request_started_at_utc=started_at_utc,
            request_finished_at_utc=finished_at_utc,
            request_window_start_utc=window.get("start_utc") if window else None,
            request_window_end_utc=window.get("end_utc") if window else None,
            request_window_start_local=start_local,
            request_window_end_local=end_local,
        )
        self.batch_audit_repo.log_batch(
            run_id=run_id,
            target_id=target["target_id"],
            batch_no=batch_no,
            batch_size=batch_size,
            status="SUCCESS",
            window=window,
            message=None,
            raw_id=raw_id,
        )
        return raw_id

    def _log_and_audit_failure(self, *, run_id: int, target: dict, batch_no: int, batch_items: list, batch_size: int,
                               api_family: str, api_name: str, endpoint_path: str, request_payload: dict,
                               started_at_utc: datetime, finished_at_utc: datetime, window: dict | None,
                               exc: Exception, status: str) -> int:
        start_local, end_local = self._window_locals(window)
        raw_id = self.api_log_service.log_api_call(
            run_id=run_id,
            job_id=target["job_id"],
            account_id=target["account_id"],
            plant_code=self._plant_code_for_batch(target, batch_items),
            dev_type_id=target.get("dev_type_id") or 0,
            api_family=api_family,
            api_name=api_name,
            endpoint_path=endpoint_path,
            request_method="POST",
            batch_no=batch_no,
            device_count=batch_size,
            request_payload=request_payload,
            response=None,
            request_started_at_utc=started_at_utc,
            request_finished_at_utc=finished_at_utc,
            request_window_start_utc=window.get("start_utc") if window else None,
            request_window_end_utc=window.get("end_utc") if window else None,
            request_window_start_local=start_local,
            request_window_end_local=end_local,
            fail_message=str(exc),
        )
        self.batch_audit_repo.log_batch(
            run_id=run_id,
            target_id=target["target_id"],
            batch_no=batch_no,
            batch_size=batch_size,
            status=status,
            window=window,
            message=str(exc),
            raw_id=raw_id,
        )
        return raw_id

    def _run_plant_realtime_target(self, run_id: int, target: dict) -> None:
        plant_codes = self.metadata_repo.get_active_plants_for_account(target["account_id"])
        if not plant_codes:
            self.checkpoint_service.mark_skipped(target, run_id, "No active plants for account")
            return

        batches = self.batch_planner.split_items(items=plant_codes, endpoint_name="getStationRealKpi", requested_batch_size=100)
        max_batches = target.get("max_batches_per_run") or len(batches)
        selected_batches = batches[:max_batches]

        for batch_no, batch in enumerate(selected_batches, start=1):
            request_payload = {"stationCodes": ",".join(batch)}
            started_at_utc = self._utcnow()
            self.rate_gate.wait_until_allowed()
            try:
                response = self.retry_policy.execute(self.client.get_station_real_kpi, station_codes=batch)
                finished_at_utc = self._utcnow()
                self.rate_gate.mark_successful_call()
                self._log_and_audit_success(
                    run_id=run_id, target=target, batch_no=batch_no, batch_items=batch, batch_size=len(batch),
                    api_family="plant", api_name="getStationRealKpi", endpoint_path="/thirdData/getStationRealKpi",
                    request_payload=request_payload, response=response, started_at_utc=started_at_utc,
                    finished_at_utc=finished_at_utc, window=None,
                )
            except HuaweiRateLimitError as e:
                finished_at_utc = self._utcnow()
                self._apply_rate_limit_backoff(target["account_id"])
                self._log_and_audit_failure(
                    run_id=run_id, target=target, batch_no=batch_no, batch_items=batch, batch_size=len(batch),
                    api_family="plant", api_name="getStationRealKpi", endpoint_path="/thirdData/getStationRealKpi",
                    request_payload=request_payload, started_at_utc=started_at_utc, finished_at_utc=finished_at_utc,
                    window=None, exc=e, status="RATE_LIMITED",
                )
                self.checkpoint_service.mark_partial(target, run_id, None, str(e))
                return
            except Exception as e:
                finished_at_utc = self._utcnow()
                self._log_and_audit_failure(
                    run_id=run_id, target=target, batch_no=batch_no, batch_items=batch, batch_size=len(batch),
                    api_family="plant", api_name="getStationRealKpi", endpoint_path="/thirdData/getStationRealKpi",
                    request_payload=request_payload, started_at_utc=started_at_utc, finished_at_utc=finished_at_utc,
                    window=None, exc=e, status="FAILED",
                )
                raise

        self.checkpoint_service.mark_success(target, run_id, None)

    def _run_full_device_target(self, run_id: int, target: dict, endpoint_name: str, devices: list[dict], window: dict | None) -> None:
        requested_batch_size = target.get("requested_batch_size") or target.get("batch_size") or 10
        batches = self.batch_planner.split_items(items=devices, endpoint_name=endpoint_name, requested_batch_size=requested_batch_size)
        max_batches = target.get("max_batches_per_run") or len(batches)
        selected_batches = batches[:max_batches]
        target_failed = False
        all_batches_completed = len(selected_batches) == len(batches)

        for batch_no, batch in enumerate(selected_batches, start=1):
            dev_ids = [d["dev_id"] for d in batch]
            if endpoint_name == "getDevRealKpi":
                request_payload = {"devTypeId": target["dev_type_id"], "devIds": ",".join(str(x) for x in dev_ids)}
                api_name = "getDevRealKpi"
                endpoint_path = "/thirdData/getDevRealKpi"
            elif endpoint_name == "getDevHistoryKpi":
                request_payload = {
                    "devTypeId": target["dev_type_id"],
                    "devIds": ",".join(str(x) for x in dev_ids),
                    "startTime": window["start_ms"],
                    "endTime": window["end_ms"],
                }
                api_name = "getDevHistoryKpi"
                endpoint_path = "/thirdData/getDevHistoryKpi"
            else:
                raise ValueError(f"Unsupported endpoint_name: {endpoint_name}")

            print(f"[DEBUG] calling {endpoint_name} plant={target['plant_code']} dev_type={target['dev_type_id']} batch_no={batch_no} batch_size={len(dev_ids)}")
            started_at_utc = self._utcnow()
            self.rate_gate.wait_until_allowed()
            try:
                if endpoint_name == "getDevRealKpi":
                    response = self.retry_policy.execute(self.client.get_dev_real_kpi, dev_type_id=target["dev_type_id"], dev_ids=dev_ids)
                else:
                    response = self.retry_policy.execute(
                        self.client.get_dev_history_kpi,
                        dev_type_id=target["dev_type_id"],
                        dev_ids=dev_ids,
                        start_time_ms=window["start_ms"],
                        end_time_ms=window["end_ms"],
                    )
                finished_at_utc = self._utcnow()
                self.rate_gate.mark_successful_call()
                self._log_and_audit_success(
                    run_id=run_id, target=target, batch_no=batch_no, batch_items=batch, batch_size=len(dev_ids),
                    api_family="device", api_name=api_name, endpoint_path=endpoint_path, request_payload=request_payload,
                    response=response, started_at_utc=started_at_utc, finished_at_utc=finished_at_utc, window=window,
                )
            except HuaweiRateLimitError as e:
                finished_at_utc = self._utcnow()
                self._apply_rate_limit_backoff(target["account_id"])
                self._log_and_audit_failure(
                    run_id=run_id, target=target, batch_no=batch_no, batch_items=batch, batch_size=len(dev_ids),
                    api_family="device", api_name=api_name, endpoint_path=endpoint_path, request_payload=request_payload,
                    started_at_utc=started_at_utc, finished_at_utc=finished_at_utc, window=window, exc=e,
                    status="RATE_LIMITED",
                )
                target_failed = True
                break
            except Exception as e:
                finished_at_utc = self._utcnow()
                self._log_and_audit_failure(
                    run_id=run_id, target=target, batch_no=batch_no, batch_items=batch, batch_size=len(dev_ids),
                    api_family="device", api_name=api_name, endpoint_path=endpoint_path, request_payload=request_payload,
                    started_at_utc=started_at_utc, finished_at_utc=finished_at_utc, window=window, exc=e,
                    status="FAILED",
                )
                raise

        if target_failed:
            self.checkpoint_service.mark_partial(target, run_id, window, "Rate limited / partial execution")
            return
        if all_batches_completed:
            self.checkpoint_service.mark_success(target, run_id, window)
        else:
            self.checkpoint_service.mark_partial(target, run_id, window, "Subset executed under max_batches_per_run")

    def _run_rotating_device_target(self, run_id: int, target: dict, endpoint_name: str, devices: list[dict], window: dict) -> None:
        requested_batch_size = target.get("requested_batch_size") or target.get("batch_size") or 10
        effective_batch_size = self.batch_planner.effective_batch_size(endpoint_name, requested_batch_size)
        max_batches_per_run = target.get("max_batches_per_run") or 1
        state = self.rotation_state_repo.get_state(target["target_id"]) if self.rotation_state_repo else None
        last_offset = (state or {}).get("last_device_offset", 0)
        batches, next_offset = self.rotation_planner.select_rotating_batches(
            devices=devices, batch_size=effective_batch_size, max_batches_per_run=max_batches_per_run, last_offset=last_offset,
        )
        if not batches:
            self.checkpoint_service.mark_no_devices(target, run_id)
            return

        for batch_no, batch in enumerate(batches, start=1):
            dev_ids = [d["dev_id"] for d in batch]
            if endpoint_name != "getDevHistoryKpi":
                raise ValueError(f"nearline_rotating currently supports getDevHistoryKpi only, got {endpoint_name}")

            request_payload = {
                "devTypeId": target["dev_type_id"],
                "devIds": ",".join(str(x) for x in dev_ids),
                "startTime": window["start_ms"],
                "endTime": window["end_ms"],
            }
            started_at_utc = self._utcnow()
            self.rate_gate.wait_until_allowed()
            try:
                response = self.retry_policy.execute(
                    self.client.get_dev_history_kpi,
                    dev_type_id=target["dev_type_id"],
                    dev_ids=dev_ids,
                    start_time_ms=window["start_ms"],
                    end_time_ms=window["end_ms"],
                )
                finished_at_utc = self._utcnow()
                self.rate_gate.mark_successful_call()
                self._log_and_audit_success(
                    run_id=run_id, target=target, batch_no=batch_no, batch_items=batch, batch_size=len(dev_ids),
                    api_family="device", api_name="getDevHistoryKpi", endpoint_path="/thirdData/getDevHistoryKpi",
                    request_payload=request_payload, response=response, started_at_utc=started_at_utc,
                    finished_at_utc=finished_at_utc, window=window,
                )
            except HuaweiRateLimitError as e:
                finished_at_utc = self._utcnow()
                self._apply_rate_limit_backoff(target["account_id"])
                self._log_and_audit_failure(
                    run_id=run_id, target=target, batch_no=batch_no, batch_items=batch, batch_size=len(dev_ids),
                    api_family="device", api_name="getDevHistoryKpi", endpoint_path="/thirdData/getDevHistoryKpi",
                    request_payload=request_payload, started_at_utc=started_at_utc, finished_at_utc=finished_at_utc,
                    window=window, exc=e, status="RATE_LIMITED",
                )
                self.checkpoint_service.mark_partial(target, run_id, window, "Rotating subset partially executed due to rate limit")
                return
            except Exception as e:
                finished_at_utc = self._utcnow()
                self._log_and_audit_failure(
                    run_id=run_id, target=target, batch_no=batch_no, batch_items=batch, batch_size=len(dev_ids),
                    api_family="device", api_name="getDevHistoryKpi", endpoint_path="/thirdData/getDevHistoryKpi",
                    request_payload=request_payload, started_at_utc=started_at_utc, finished_at_utc=finished_at_utc,
                    window=window, exc=e, status="FAILED",
                )
                raise

        if self.rotation_state_repo:
            self.rotation_state_repo.upsert_state(
                target_id=target["target_id"],
                last_device_offset=next_offset,
                fleet_size=len(devices),
                run_id=run_id,
            )
        self.checkpoint_service.mark_partial(target, run_id, window, f"Rotating subset completed. Next offset={next_offset}")

    def _apply_rate_limit_backoff(self, account_id: int) -> None:
        self._rate_limit_events += 1
        if self._rate_limit_events <= 1:
            seconds = self.account_backoff_policy.get("first", 600)
        elif self._rate_limit_events == 2:
            seconds = self.account_backoff_policy.get("second", 1800)
        else:
            seconds = self.account_backoff_policy.get("third", 3600)
        self.rate_gate.apply_backoff(seconds)
        cooldown_until = self._utcnow() + timedelta(seconds=seconds)
        self.metadata_repo.set_account_interface_cooldown(account_id, cooldown_until)
        print(f"[DEBUG] account_id={account_id} rate-limited -> cooldown until {cooldown_until.isoformat()}")

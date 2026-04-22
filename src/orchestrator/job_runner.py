from __future__ import annotations

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
        self.batch_planner = batch_planner
        self.window_planner = window_planner
        self.retry_policy = retry_policy
        self.rate_gate = rate_gate
        self.rotation_state_repo = rotation_state_repo
        self.rotation_planner = rotation_planner or RotationPlanner()
        self.account_backoff_policy = account_backoff_policy or {
            "first": 120,
            "second": 300,
            "third": 900,
        }

    def run_targets(self, job: dict, targets: list[dict]) -> int:
        run_id = self.run_repo.start_run(
            job_id=job["job_id"],
            run_type="manual",
            triggered_by="user",
        )

        any_failed = False

        for target in targets:
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

    def _run_target(self, run_id: int, job: dict, target: dict) -> None:
        endpoint_name = target.get("endpoint_name") or job.get("api_name")
        service_class = (target.get("service_class") or "backfill").lower()

        if endpoint_name == "getStationRealKpi":
            self._run_plant_realtime_target(run_id, target)
            return

        devices = self.metadata_repo.get_devices(
            plant_code=target["plant_code"],
            dev_type_id=target["dev_type_id"],
        )

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

        if override_start_utc and override_end_utc:
            window = {
                "start_utc": override_start_utc,
                "end_utc": override_end_utc,
                "start_ms": int(override_start_utc.timestamp() * 1000),
                "end_ms": int(override_end_utc.timestamp() * 1000),
            }
        else:
            window = self.window_planner.compute_window(checkpoint=checkpoint, target=target)

        if not window:
            self.checkpoint_service.mark_skipped(target, run_id, "No runnable window")
            return

        if service_class == "nearline_rotating" and bool(target.get("rotation_enabled")):
            self._run_rotating_device_target(run_id, target, endpoint_name, devices, window)
        else:
            self._run_full_device_target(run_id, target, endpoint_name, devices, window)

    def _run_plant_realtime_target(self, run_id: int, target: dict) -> None:
        """
        Assumption:
        metadata_repo has method get_active_plants_for_account(account_id) -> list[str]
        """
        plant_codes = self.metadata_repo.get_active_plants_for_account(target["account_id"])
        if not plant_codes:
            self.checkpoint_service.mark_skipped(target, run_id, "No active plants for account")
            return

        batches = self.batch_planner.split_items(
            items=plant_codes,
            endpoint_name="getStationRealKpi",
            requested_batch_size=100,
        )

        max_batches = target.get("max_batches_per_run") or len(batches)
        selected_batches = batches[:max_batches]

        for batch_no, batch in enumerate(selected_batches, start=1):
            self.rate_gate.wait_until_allowed()
            try:
                self.retry_policy.execute(
                    self.client.get_station_real_kpi,
                    station_codes=batch,
                )
                self.rate_gate.mark_successful_call()
            except HuaweiRateLimitError as e:
                self._apply_rate_limit_backoff()
                self.batch_audit_repo.log_batch(
                    run_id=run_id,
                    target_id=target["target_id"],
                    batch_no=batch_no,
                    batch_size=len(batch),
                    status="RATE_LIMITED",
                    message=str(e),
                )
                self.checkpoint_service.mark_partial(target, run_id, None, str(e))
                return

            self.batch_audit_repo.log_batch(
                run_id=run_id,
                target_id=target["target_id"],
                batch_no=batch_no,
                batch_size=len(batch),
                status="SUCCESS",
                message=None,
            )

        self.checkpoint_service.mark_success(target, run_id, None)

    def _run_full_device_target(
        self,
        run_id: int,
        target: dict,
        endpoint_name: str,
        devices: list[dict],
        window: dict,
    ) -> None:
        requested_batch_size = (
            target.get("requested_batch_size")
            or target.get("batch_size")
            or 10
        )

        batches = self.batch_planner.split_items(
            items=devices,
            endpoint_name=endpoint_name,
            requested_batch_size=requested_batch_size,
        )

        max_batches = target.get("max_batches_per_run") or len(batches)
        selected_batches = batches[:max_batches]

        target_failed = False
        all_batches_completed = len(selected_batches) == len(batches)

        for batch_no, batch in enumerate(selected_batches, start=1):
            dev_ids = [d["dev_id"] for d in batch]

            self.rate_gate.wait_until_allowed()
            try:
                if endpoint_name == "getDevRealKpi":
                    self.retry_policy.execute(
                        self.client.get_dev_real_kpi,
                        dev_type_id=target["dev_type_id"],
                        dev_ids=dev_ids,
                    )
                elif endpoint_name == "getDevHistoryKpi":
                    self.retry_policy.execute(
                        self.client.get_dev_history_kpi,
                        dev_type_id=target["dev_type_id"],
                        dev_ids=dev_ids,
                        start_time_ms=window["start_ms"],
                        end_time_ms=window["end_ms"],
                    )
                else:
                    raise ValueError(f"Unsupported endpoint_name: {endpoint_name}")

                self.rate_gate.mark_successful_call()

            except HuaweiRateLimitError as e:
                self._apply_rate_limit_backoff()
                self.batch_audit_repo.log_batch(
                    run_id=run_id,
                    target_id=target["target_id"],
                    batch_no=batch_no,
                    batch_size=len(dev_ids),
                    status="RATE_LIMITED",
                    window=window,
                    message=str(e),
                )
                target_failed = True
                break

            self.batch_audit_repo.log_batch(
                run_id=run_id,
                target_id=target["target_id"],
                batch_no=batch_no,
                batch_size=len(dev_ids),
                status="SUCCESS",
                window=window,
                message=None,
            )

        if target_failed:
            self.checkpoint_service.mark_partial(
                target,
                run_id,
                window,
                "Rate limited / partial execution",
            )
            return

        if all_batches_completed:
            self.checkpoint_service.mark_success(target, run_id, window)
        else:
            self.checkpoint_service.mark_partial(
                target,
                run_id,
                window,
                "Subset executed under max_batches_per_run",
            )

    def _run_rotating_device_target(
        self,
        run_id: int,
        target: dict,
        endpoint_name: str,
        devices: list[dict],
        window: dict,
    ) -> None:
        requested_batch_size = (
            target.get("requested_batch_size")
            or target.get("batch_size")
            or 10
        )

        effective_batch_size = self.batch_planner.effective_batch_size(endpoint_name, requested_batch_size)
        max_batches_per_run = target.get("max_batches_per_run") or 1

        state = self.rotation_state_repo.get_state(target["target_id"]) if self.rotation_state_repo else None
        last_offset = (state or {}).get("last_device_offset", 0)

        batches, next_offset = self.rotation_planner.select_rotating_batches(
            devices=devices,
            batch_size=effective_batch_size,
            max_batches_per_run=max_batches_per_run,
            last_offset=last_offset,
        )

        if not batches:
            self.checkpoint_service.mark_no_devices(target, run_id)
            return

        for batch_no, batch in enumerate(batches, start=1):
            dev_ids = [d["dev_id"] for d in batch]

            self.rate_gate.wait_until_allowed()
            try:
                if endpoint_name != "getDevHistoryKpi":
                    raise ValueError(
                        f"nearline_rotating currently supports getDevHistoryKpi only, got {endpoint_name}"
                    )

                self.retry_policy.execute(
                    self.client.get_dev_history_kpi,
                    dev_type_id=target["dev_type_id"],
                    dev_ids=dev_ids,
                    start_time_ms=window["start_ms"],
                    end_time_ms=window["end_ms"],
                )
                self.rate_gate.mark_successful_call()

            except HuaweiRateLimitError as e:
                self._apply_rate_limit_backoff()
                self.batch_audit_repo.log_batch(
                    run_id=run_id,
                    target_id=target["target_id"],
                    batch_no=batch_no,
                    batch_size=len(dev_ids),
                    status="RATE_LIMITED",
                    window=window,
                    message=str(e),
                )
                self.checkpoint_service.mark_partial(
                    target,
                    run_id,
                    window,
                    "Rotating subset partially executed due to rate limit",
                )
                return

            self.batch_audit_repo.log_batch(
                run_id=run_id,
                target_id=target["target_id"],
                batch_no=batch_no,
                batch_size=len(dev_ids),
                status="SUCCESS",
                window=window,
                message=None,
            )

        if self.rotation_state_repo:
            self.rotation_state_repo.upsert_state(
                target_id=target["target_id"],
                last_device_offset=next_offset,
                fleet_size=len(devices),
                run_id=run_id,
            )

        self.checkpoint_service.mark_partial(
            target,
            run_id,
            window,
            f"Rotating subset completed. Next offset={next_offset}",
        )

    def _apply_rate_limit_backoff(self) -> None:
        self.rate_gate.apply_backoff(self.account_backoff_policy.get("first", 120))
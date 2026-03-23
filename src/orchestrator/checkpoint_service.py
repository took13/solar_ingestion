from src.domain.enums import BatchStatus


class CheckpointService:
    def __init__(self, checkpoint_repo):
        self.checkpoint_repo = checkpoint_repo

    def mark_success(self, target: dict, run_id: int, window: dict):
        self.checkpoint_repo.upsert_checkpoint(
            target=target,
            run_id=run_id,
            status=BatchStatus.SUCCESS.value,
            last_success_end_utc=window["end_utc"],
            last_attempt_end_utc=window["end_utc"],
            error_code=None,
            error_message=None,
            consecutive_failures_reset=True,
        )

    def mark_partial(self, target: dict, run_id: int, window: dict, message: str = None):
        self.checkpoint_repo.upsert_checkpoint(
            target=target,
            run_id=run_id,
            status=BatchStatus.PARTIAL.value,
            last_success_end_utc=None,
            last_attempt_end_utc=window["end_utc"],
            error_code=None,
            error_message=message,
            consecutive_failures_reset=False,
        )

    def mark_failed(self, target: dict, run_id: int, window: dict, error_code=None, error_message=None):
        self.checkpoint_repo.upsert_checkpoint(
            target=target,
            run_id=run_id,
            status=BatchStatus.FAILED.value,
            last_success_end_utc=None,
            last_attempt_end_utc=window["end_utc"] if window else None,
            error_code=error_code,
            error_message=error_message,
            consecutive_failures_reset=False,
        )

    def mark_skipped(self, target: dict, run_id: int, message: str):
        self.checkpoint_repo.upsert_checkpoint(
            target=target,
            run_id=run_id,
            status=BatchStatus.SKIPPED.value,
            last_success_end_utc=None,
            last_attempt_end_utc=None,
            error_code=None,
            error_message=message,
            consecutive_failures_reset=True,
        )

    def mark_no_devices(self, target: dict, run_id: int):
        self.checkpoint_repo.upsert_checkpoint(
            target=target,
            run_id=run_id,
            status=BatchStatus.NO_DEVICES.value,
            last_success_end_utc=None,
            last_attempt_end_utc=None,
            error_code=None,
            error_message="No devices found for target.",
            consecutive_failures_reset=True,
        )
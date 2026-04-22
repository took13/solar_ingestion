from __future__ import annotations

from src.config_loader import ConfigLoader
from src.db.connection import create_connection
from src.db.repositories.metadata_repo import MetadataRepository
from src.db.repositories.target_repo import TargetRepository
from src.db.repositories.checkpoint_repo import CheckpointRepository
from src.db.repositories.run_repo import RunRepository
from src.db.repositories.batch_audit_repo import BatchAuditRepository
from src.db.repositories.rotation_state_repo import RotationStateRepository
from src.extract.metadata_service import MetadataService
from src.orchestrator.batch_planner import BatchPlanner
from src.orchestrator.window_planner import WindowPlanner
from src.orchestrator.checkpoint_service import CheckpointService
from src.orchestrator.retry_policy import RetryPolicy
from src.orchestrator.job_runner import JobRunner
from src.orchestrator.account_rate_gate import AccountRateGate
from src.orchestrator.rotation_planner import RotationPlanner
from src.api.session_manager import SessionManager
from src.api.huawei_legacy_client import HuaweiLegacyClient
from src.domain.time_utils import utc_now, fmt_local


class Application:
    def __init__(self, app_config: dict):
        self.app_config = app_config
        self.conn = create_connection(app_config["database"]["connection_string"])

        self.metadata_repo = MetadataRepository(self.conn)
        self.target_repo = TargetRepository(self.conn)
        self.checkpoint_repo = CheckpointRepository(self.conn)
        self.run_repo = RunRepository(self.conn)
        self.batch_audit_repo = BatchAuditRepository(self.conn)
        self.rotation_state_repo = RotationStateRepository(self.conn)

        self.metadata_service = MetadataService(self.metadata_repo)
        self.batch_planner = BatchPlanner()
        self.window_planner = WindowPlanner()
        self.checkpoint_service = CheckpointService(self.checkpoint_repo)
        self.rotation_planner = RotationPlanner()

        self.retry_policy = RetryPolicy(
            max_attempts=app_config.get("retry", {}).get("max_attempts", 3),
            backoff_seconds=app_config.get("retry", {}).get("backoff_seconds", 10),
        )

    def run_job(self, job_name: str):
        print(f"[APP] Starting job from DB: {job_name}")

        job = self.run_repo.get_job_by_name(job_name)
        if not job:
            raise ValueError(f"Job not found in ctl.ingest_job: {job_name}")

        targets = self.target_repo.get_targets_by_job_name(job_name)
        if not targets:
            raise ValueError(f"No enabled targets found for job: {job_name}")

        targets = self.metadata_service.enrich_targets_from_db(targets)
        print(f"[APP] Loaded {len(targets)} enabled targets for job={job_name}")

        self._run_targets_grouped_by_account(job=job, targets=targets)

    def run_job_with_override_window(self, job_name: str, override_start_utc, override_end_utc):
        print(
            f"[APP] Starting override-window job from DB: {job_name} | "
            f"{override_start_utc.isoformat()} -> {override_end_utc.isoformat()}"
        )

        job = self.run_repo.get_job_by_name(job_name)
        if not job:
            raise ValueError(f"Job not found in ctl.ingest_job: {job_name}")

        targets = self.target_repo.get_targets_by_job_name(job_name)
        if not targets:
            raise ValueError(f"No enabled targets found for job: {job_name}")

        targets = self.metadata_service.enrich_targets_from_db(targets)

        for target in targets:
            target["override_start_utc"] = override_start_utc
            target["override_end_utc"] = override_end_utc

        print(f"[APP] Loaded {len(targets)} enabled targets for job={job_name} with override window")
        self._run_targets_grouped_by_account(job=job, targets=targets)

    def _run_targets_grouped_by_account(self, job: dict, targets: list[dict]):
        targets_by_account: dict[int, list[dict]] = {}

        for target in targets:
            targets_by_account.setdefault(target["account_id"], []).append(target)

        print(f"[APP] Grouped into {len(targets_by_account)} account bucket(s)")

        for account_id, account_targets in targets_by_account.items():
            account = self.metadata_repo.get_account_by_id(account_id)
            if not account:
                raise ValueError(f"Account not found or inactive: {account_id}")

            if not account.get("api_password"):
                raise ValueError(
                    f"Account {account['account_name']} does not have api_password in dbo.dim_api_account"
                )

            cooldown_until = account.get("interface_cooldown_until")
            now_utc = utc_now()

            if cooldown_until is not None:
                if cooldown_until.tzinfo is None:
                    cooldown_until = cooldown_until.replace(tzinfo=now_utc.tzinfo)

                if now_utc < cooldown_until:
                    print(
                        f"[APP] Account={account['account_name']} (id={account_id}) "
                        f"is in cooldown until {fmt_local(cooldown_until)} -> skip this round"
                    )
                    continue
                else:
                    self.metadata_repo.clear_account_interface_cooldown(account_id)
                    print(
                        f"[APP] Account={account['account_name']} (id={account_id}) "
                        f"cooldown expired -> cleared"
                    )

            print(
                f"[APP] Account={account['account_name']} (id={account_id}) "
                f"will run {len(account_targets)} target(s)"
            )

            session_manager = SessionManager(
                base_url=account["base_url"],
                username=account["username"],
                system_code=account["api_password"],
                timeout=self.app_config["api"]["timeout_seconds"],
            )

            client = HuaweiLegacyClient(
                session_manager=session_manager,
                base_url=account["base_url"],
                timeout=self.app_config["api"]["timeout_seconds"],
            )

            rate_gate = AccountRateGate(
                min_interval_seconds=self.app_config.get("api", {}).get("account_min_interval_seconds", 60)
            )

            runner = JobRunner(
                client=client,
                run_repo=self.run_repo,
                checkpoint_repo=self.checkpoint_repo,
                metadata_repo=self.metadata_repo,
                checkpoint_service=self.checkpoint_service,
                batch_audit_repo=self.batch_audit_repo,
                batch_planner=self.batch_planner,
                window_planner=self.window_planner,
                retry_policy=self.retry_policy,
                rate_gate=rate_gate,
                rotation_state_repo=self.rotation_state_repo,
                rotation_planner=self.rotation_planner,
                account_backoff_policy=self.app_config.get("scheduler", {}).get(
                    "rate_limit_backoff_seconds",
                    {
                        "first": 120,
                        "second": 300,
                        "third": 900,
                    },
                ),
            )

            runner.run_targets(job=job, targets=account_targets)


def build_app() -> Application:
    config_loader = ConfigLoader()
    app_config = config_loader.load_app_config()
    return Application(app_config)
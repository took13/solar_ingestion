from __future__ import annotations

from src.config_loader import ConfigLoader
from src.db.connection import create_connection
from src.db.repositories.metadata_repo import MetadataRepository
from src.db.repositories.target_repo import TargetRepository
from src.db.repositories.checkpoint_repo import CheckpointRepository
from src.db.repositories.run_repo import RunRepository
from src.db.repositories.raw_repo import RawRepository
from src.db.repositories.metric_repo import MetricRepository
from src.db.repositories.metric_catalog_repo import MetricCatalogRepository
from src.db.repositories.typed_repo import TypedRepository
from src.db.repositories.batch_audit_repo import BatchAuditRepository
from src.extract.metadata_service import MetadataService
from src.orchestrator.batch_planner import BatchPlanner
from src.orchestrator.window_planner import WindowPlanner
from src.orchestrator.checkpoint_service import CheckpointService
from src.orchestrator.retry_policy import RetryPolicy
from src.orchestrator.job_runner import JobRunner
from src.api.session_manager import SessionManager
from src.api.huawei_legacy_client import HuaweiLegacyClient
from src.raw.raw_archiver import RawArchiver


class Application:
    def __init__(self, app_config: dict):
        self.app_config = app_config
        self.conn = create_connection(app_config["database"]["connection_string"])

        self.metadata_repo = MetadataRepository(self.conn)
        self.target_repo = TargetRepository(self.conn)
        self.checkpoint_repo = CheckpointRepository(self.conn)
        self.run_repo = RunRepository(self.conn)
        self.raw_repo = RawRepository(self.conn)
        self.metric_catalog_repo = MetricCatalogRepository(self.conn)
        self.metric_repo = MetricRepository(
            self.conn,
            metric_catalog_repo=self.metric_catalog_repo
        )
        self.typed_repo = TypedRepository(self.conn)
        self.batch_audit_repo = BatchAuditRepository(self.conn)

        self.metadata_service = MetadataService(self.metadata_repo)
        self.batch_planner = BatchPlanner()
        self.window_planner = WindowPlanner()
        self.checkpoint_service = CheckpointService(self.checkpoint_repo)

        self.retry_policy = RetryPolicy(
            max_attempts=app_config.get("retry", {}).get("max_attempts", 3),
            backoff_seconds=app_config.get("retry", {}).get("backoff_seconds", 10),
        )

        self.raw_archiver = RawArchiver(app_config["storage"]["raw_root"])

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

            runner = JobRunner(
                metadata_repo=self.metadata_repo,
                checkpoint_repo=self.checkpoint_repo,
                run_repo=self.run_repo,
                raw_repo=self.raw_repo,
                metric_repo=self.metric_repo,
                typed_repo=self.typed_repo,
                batch_audit_repo=self.batch_audit_repo,
                checkpoint_service=self.checkpoint_service,
                batch_planner=self.batch_planner,
                window_planner=self.window_planner,
                client=client,
                raw_archiver=self.raw_archiver,
                retry_policy=self.retry_policy,
                batch_delay_seconds=self.app_config.get("api", {}).get("batch_delay_seconds", 3),
                generic_metrics_enabled=self.app_config.get("pipeline", {})
                    .get("generic_metrics", {})
                    .get("enabled", False),
            )

            runner.run_targets(job=job, targets=account_targets)


def build_app() -> Application:
    config_loader = ConfigLoader()
    app_config = config_loader.load_app_config()
    return Application(app_config)
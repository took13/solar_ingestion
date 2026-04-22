from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.config_loader import ConfigLoader
from src.api.session_manager import SessionManager
from src.api.huawei_legacy_client import HuaweiLegacyClient


def floor_to_5min(dt: datetime) -> datetime:
    minute = (dt.minute // 5) * 5
    return dt.replace(minute=minute, second=0, microsecond=0)


def main():
    cfg = ConfigLoader().load_app_config()

    base_url = cfg["test"]["base_url"]
    username = cfg["test"]["username"]
    api_password = cfg["test"]["api_password"]
    dev_type_id = cfg["test"]["dev_type_id"]
    dev_ids = cfg["test"]["dev_ids"]
    timeout = cfg.get("api", {}).get("timeout_seconds", 120)

    now_utc = datetime.now(timezone.utc)
    end_utc = floor_to_5min(now_utc - timedelta(minutes=10))
    start_utc = end_utc - timedelta(minutes=5)

    sm = SessionManager(
        base_url=base_url,
        username=username,
        system_code=api_password,
        timeout=timeout,
    )
    client = HuaweiLegacyClient(
        session_manager=sm,
        base_url=base_url,
        timeout=timeout,
    )

    result = client.get_dev_history_kpi(
        dev_type_id=dev_type_id,
        dev_ids=dev_ids,
        start_time_ms=int(start_utc.timestamp() * 1000),
        end_time_ms=int(end_utc.timestamp() * 1000),
    )
    print("[OK] getDevHistoryKpi success")
    print(result)


if __name__ == "__main__":
    main()
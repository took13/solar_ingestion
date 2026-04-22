from __future__ import annotations

from src.config_loader import ConfigLoader
from src.api.session_manager import SessionManager
from src.api.huawei_legacy_client import HuaweiLegacyClient


def main():
    cfg = ConfigLoader().load_app_config()

    base_url = cfg["test"]["base_url"]
    username = cfg["test"]["username"]
    api_password = cfg["test"]["api_password"]
    dev_type_id = cfg["test"]["dev_type_id"]
    dev_ids = cfg["test"]["dev_ids"]
    timeout = cfg.get("api", {}).get("timeout_seconds", 120)

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

    result = client.get_dev_real_kpi(
        dev_type_id=dev_type_id,
        dev_ids=dev_ids,
    )
    print("[OK] getDevRealKpi success")
    print(result)


if __name__ == "__main__":
    main()
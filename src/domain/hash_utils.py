import hashlib
import json


def stable_sha256(payload: dict) -> str:
    text = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def build_batch_hash(
    account_id: int,
    plant_code: str,
    dev_type_id: int,
    api_name: str,
    dev_ids: list[int],
    window_start_utc: str,
    window_end_utc: str,
) -> str:
    payload = {
        "account_id": account_id,
        "plant_code": plant_code,
        "dev_type_id": dev_type_id,
        "api_name": api_name,
        "dev_ids": sorted(dev_ids),
        "window_start_utc": window_start_utc,
        "window_end_utc": window_end_utc,
    }
    return stable_sha256(payload)
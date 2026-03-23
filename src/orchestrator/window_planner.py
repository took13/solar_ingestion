from datetime import timedelta
from src.domain.time_utils import utc_now


class WindowPlanner:
    def compute_window(self, checkpoint: dict, target: dict):
        now_utc = utc_now()
        end_utc = now_utc - timedelta(minutes=target["lag_minutes"])

        if checkpoint and checkpoint.get("last_success_end_utc"):
            start_utc = checkpoint["last_success_end_utc"] - timedelta(minutes=target["overlap_minutes"])
        else:
            start_utc = target["bootstrap_start_utc"]

        if start_utc is None or start_utc >= end_utc:
            return None

        max_window = timedelta(minutes=target["max_window_minutes"])
        if (end_utc - start_utc) > max_window:
            end_utc = start_utc + max_window

        return {
            "start_utc": start_utc,
            "end_utc": end_utc,
            "start_ms": int(start_utc.timestamp() * 1000),
            "end_ms": int(end_utc.timestamp() * 1000),
        }
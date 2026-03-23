from __future__ import annotations

from datetime import timedelta, timezone
from src.domain.time_utils import utc_now


class WindowPlanner:
    def _ensure_utc_aware(self, dt):
        if dt is None:
            return None
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    def compute_window(self, checkpoint: dict, target: dict):
        override_start = self._ensure_utc_aware(target.get("override_start_utc"))
        override_end = self._ensure_utc_aware(target.get("override_end_utc"))

        if override_start is not None and override_end is not None:
            if override_start >= override_end:
                return None

            return {
                "start_utc": override_start,
                "end_utc": override_end,
                "start_ms": int(override_start.timestamp() * 1000),
                "end_ms": int(override_end.timestamp() * 1000),
            }

        now_utc = self._ensure_utc_aware(utc_now())
        end_utc = now_utc - timedelta(minutes=target["lag_minutes"])

        if checkpoint and checkpoint.get("last_success_end_utc"):
            start_utc = self._ensure_utc_aware(checkpoint["last_success_end_utc"]) - timedelta(
                minutes=target["overlap_minutes"]
            )
        else:
            start_utc = self._ensure_utc_aware(target["bootstrap_start_utc"])

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
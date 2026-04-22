from __future__ import annotations

from datetime import datetime, timedelta, timezone


class WindowPlanner:
    """
    Window strategy:
    - hard_window_mode='slot'     -> fixed recent slot, good for online_full / nearline_rotating
    - hard_window_mode='rolling'  -> checkpoint driven rolling window
    - hard_window_mode='backfill' -> same as rolling for now, but semantically clearer
    """

    def _floor_to_5min(self, dt: datetime) -> datetime:
        minute = (dt.minute // 5) * 5
        return dt.replace(minute=minute, second=0, microsecond=0)

    def _normalize_dt(self, value):
        if value is None:
            return None
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc)
        raise TypeError(f"Unsupported datetime value: {type(value)}")

    def compute_window(self, checkpoint: dict | None, target: dict) -> dict | None:
        now_utc = datetime.now(timezone.utc)

        lag_minutes = target.get("lag_minutes") or 0
        overlap_minutes = target.get("overlap_minutes") or 0
        max_window_minutes = target.get("max_window_minutes") or 5
        bootstrap_start_utc = self._normalize_dt(target.get("bootstrap_start_utc"))
        hard_window_mode = (target.get("hard_window_mode") or "rolling").lower()

        effective_now = now_utc - timedelta(minutes=lag_minutes)

        if hard_window_mode == "slot":
            end_utc = self._floor_to_5min(effective_now)
            start_utc = end_utc - timedelta(minutes=max_window_minutes)

            if end_utc <= start_utc:
                return None

            return {
                "start_utc": start_utc,
                "end_utc": end_utc,
                "start_ms": int(start_utc.timestamp() * 1000),
                "end_ms": int(end_utc.timestamp() * 1000),
            }

        last_success_end_utc = None
        if checkpoint and checkpoint.get("last_success_end_utc"):
            last_success_end_utc = self._normalize_dt(checkpoint["last_success_end_utc"])

        if last_success_end_utc:
            start_utc = last_success_end_utc - timedelta(minutes=overlap_minutes)
        elif bootstrap_start_utc:
            start_utc = bootstrap_start_utc
        else:
            start_utc = effective_now - timedelta(minutes=max_window_minutes)

        end_utc = min(start_utc + timedelta(minutes=max_window_minutes), effective_now)

        if end_utc <= start_utc:
            return None

        return {
            "start_utc": start_utc,
            "end_utc": end_utc,
            "start_ms": int(start_utc.timestamp() * 1000),
            "end_ms": int(end_utc.timestamp() * 1000),
        }
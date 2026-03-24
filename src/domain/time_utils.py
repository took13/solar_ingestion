from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo


LOCAL_TZ = ZoneInfo("Asia/Bangkok")


def epoch_ms_to_utc(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_local(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(LOCAL_TZ)


def local_now() -> datetime:
    return datetime.now(LOCAL_TZ)


def fmt_local(dt: datetime | None, pattern: str = "%Y-%m-%d %H:%M:%S") -> str:
    local_dt = to_local(dt)
    if local_dt is None:
        return "None"
    return local_dt.strftime(pattern)


def fmt_local_compact(dt: datetime | None, pattern: str = "%Y%m%d_%H%M%S") -> str:
    local_dt = to_local(dt)
    if local_dt is None:
        return "None"
    return local_dt.strftime(pattern)
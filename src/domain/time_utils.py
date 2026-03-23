from datetime import datetime, timezone, timedelta


def epoch_ms_to_utc(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def utc_now():
    return datetime.now(timezone.utc)
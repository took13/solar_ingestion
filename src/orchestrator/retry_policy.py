from __future__ import annotations

import time


class RetryPolicy:
    def __init__(self, max_attempts: int = 3, backoff_seconds: int = 10):
        self.max_attempts = max_attempts
        self.backoff_seconds = backoff_seconds

    def execute(self, func, *args, **kwargs):
        last_exc = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_exc = e
                if attempt == self.max_attempts:
                    raise
                time.sleep(self.backoff_seconds * attempt)
        raise last_exc
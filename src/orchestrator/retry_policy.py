from __future__ import annotations

import time

import requests

from src.api.exceptions import (
    HuaweiLoginError,
    HuaweiRateLimitError,
    HuaweiUnauthorizedError,
)


class RetryPolicy:
    """
    Retry only transient network/unexpected exceptions.
    Do not retry rate-limit/auth problems inside the same loop.
    """

    def __init__(self, max_attempts: int = 3, backoff_seconds: int = 5):
        self.max_attempts = max(1, max_attempts)
        self.backoff_seconds = max(1, backoff_seconds)

    def execute(self, func, *args, **kwargs):
        last_exc = None

        for attempt in range(1, self.max_attempts + 1):
            try:
                return func(*args, **kwargs)

            except (HuaweiRateLimitError, HuaweiLoginError, HuaweiUnauthorizedError):
                raise

            except (requests.Timeout, requests.ConnectionError) as e:
                last_exc = e
                if attempt == self.max_attempts:
                    raise
                time.sleep(self.backoff_seconds * attempt)

            except Exception as e:
                last_exc = e
                if attempt == self.max_attempts:
                    raise
                time.sleep(self.backoff_seconds * attempt)

        raise last_exc
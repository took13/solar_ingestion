from __future__ import annotations

import time


class AccountRateGate:
    """
    Simple account-level pacing gate.
    One account should have one gate instance in one execution lane.
    """

    def __init__(self, min_interval_seconds: int = 60):
        self.min_interval_seconds = max(1, min_interval_seconds)
        self.next_allowed_epoch = 0.0

    def wait_until_allowed(self) -> None:
        now = time.time()
        if now < self.next_allowed_epoch:
            time.sleep(self.next_allowed_epoch - now)

    def mark_successful_call(self) -> None:
        self.next_allowed_epoch = time.time() + self.min_interval_seconds

    def apply_backoff(self, backoff_seconds: int) -> None:
        backoff_seconds = max(1, backoff_seconds)
        self.next_allowed_epoch = max(self.next_allowed_epoch, time.time() + backoff_seconds)
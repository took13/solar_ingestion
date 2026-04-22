from __future__ import annotations

from typing import Any


class BatchPlanner:
    """
    Endpoint-aware batching.

    - getStationRealKpi: max 100 plants/request
    - getDevRealKpi: max 100 devices/request
    - getDevHistoryKpi: max 10 devices/request
    """

    ENDPOINT_LIMITS = {
        "getStationRealKpi": 100,
        "getDevRealKpi": 100,
        "getDevHistoryKpi": 10,
    }

    def effective_batch_size(self, endpoint_name: str, requested_batch_size: int | None) -> int:
        endpoint_limit = self.ENDPOINT_LIMITS.get(endpoint_name, 1)
        requested = requested_batch_size or endpoint_limit
        return max(1, min(requested, endpoint_limit))

    def split_items(
        self,
        items: list[Any],
        endpoint_name: str,
        requested_batch_size: int | None,
    ) -> list[list[Any]]:
        batch_size = self.effective_batch_size(endpoint_name, requested_batch_size)
        return [items[i:i + batch_size] for i in range(0, len(items), batch_size)]
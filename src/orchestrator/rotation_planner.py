from __future__ import annotations


class RotationPlanner:
    """
    Select rotating subset of devices for nearline history jobs.
    """

    def select_rotating_batches(
        self,
        devices: list[dict],
        batch_size: int,
        max_batches_per_run: int,
        last_offset: int,
    ) -> tuple[list[list[dict]], int]:
        if not devices:
            return [], 0

        n = len(devices)
        batch_size = max(1, batch_size)
        max_batches_per_run = max(1, max_batches_per_run)

        take_count = min(n, batch_size * max_batches_per_run)

        ordered = devices[last_offset:] + devices[:last_offset]
        selected = ordered[:take_count]

        batches = [selected[i:i + batch_size] for i in range(0, len(selected), batch_size)]
        next_offset = (last_offset + len(selected)) % n

        return batches, next_offset
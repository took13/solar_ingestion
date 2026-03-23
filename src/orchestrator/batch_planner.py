class BatchPlanner:
    def plan(self, devices: list[dict], batch_size: int) -> list[list[dict]]:
        return [devices[i:i + batch_size] for i in range(0, len(devices), batch_size)]
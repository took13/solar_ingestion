from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import yaml


class ConfigLoader:
    def __init__(self, config_root: str = "config"):
        self.config_root = Path(config_root)

    def load_app_config(self) -> Dict[str, Any]:
        return self._load_yaml(self.config_root / "app.yaml")

    def load_job_config(self, job_name: str) -> Dict[str, Any]:
        return self._load_yaml(self.config_root / "jobs" / f"{job_name}.yaml")

    def load_mapping_config(self, mapping_name: str) -> Dict[str, Any]:
        return self._load_yaml(self.config_root / "mappings" / f"{mapping_name}.yaml")

    def _load_yaml(self, path: Path) -> Dict[str, Any]:
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")
        with path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
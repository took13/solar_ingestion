from abc import ABC, abstractmethod
from typing import Dict, List


class BaseTypedNormalizer(ABC):
    @abstractmethod
    def normalize(self, response_body: Dict, raw_id: int, plant_code: str) -> List[dict]:
        raise NotImplementedError
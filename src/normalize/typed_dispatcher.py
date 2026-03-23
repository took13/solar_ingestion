from src.normalize.normalizers.inverter_normalizer import InverterNormalizer
from src.normalize.normalizers.emi_normalizer import EmiNormalizer
from src.normalize.normalizers.meter_normalizer import MeterNormalizer
from src.normalize.normalizers.logger_normalizer import LoggerNormalizer


class TypedDispatcher:
    def __init__(self):
        self.handlers = {
            1: InverterNormalizer(),
            10: EmiNormalizer(),
            17: MeterNormalizer(),
            63: LoggerNormalizer(),
        }

    def normalize(self, dev_type_id: int, response_body: dict, raw_id: int, plant_code: str):
        handler = self.handlers.get(dev_type_id)
        if handler is None:
            return []
        return handler.normalize(response_body=response_body, raw_id=raw_id, plant_code=plant_code)
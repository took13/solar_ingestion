from enum import Enum


class BatchStatus(str, Enum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    PARTIAL = "PARTIAL"
    SKIPPED = "SKIPPED"
    NO_DEVICES = "NO_DEVICES"


class RunStatus(str, Enum):
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"


class ValueType(str, Enum):
    NUMBER = "number"
    TEXT = "text"
    BOOL = "bool"
    NULL = "null"
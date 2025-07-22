from abc import ABC
from datetime import datetime
from enum import Enum

from pydantic import BaseModel

CDF_FLOAT_FILLVAL = -1e31  # ISTP compliant FILLVAL for CDF_FLOAT


class CalibrationMethod(str, Enum):
    KEPKO = "Kepko"
    LEINWEBER = "Leinweber"
    IMAPLO_PIVOT = "IMAP-Lo Pivot Platform Interference"
    NOOP = "noop"
    SUM = "Sum of other calibrations"


class Sensor(str, Enum):
    MAGO = "MAGo"
    MAGI = "MAGi"


class ValueType(str, Enum):
    VECTOR = "vector"


class Mission(str, Enum):
    IMAP = "IMAP"


class CalibrationMetadata(BaseModel):
    dependencies: list[str]
    science: list[str]
    creation_timestamp: datetime
    comment: str | None = None


class Value(BaseModel, ABC):
    time: datetime
    value: list[float]
    magnitude: float | None = None


class CalibrationValue(Value):
    timedelta: float = 0
    quality_flag: int = 0
    quality_bitmask: int = 0


class ScienceValue(Value):
    range: int
    quality_flag: int | None = 0
    quality_bitmask: int | None = 0


class Validity(BaseModel):
    start: datetime
    end: datetime

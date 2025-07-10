from abc import ABC
from datetime import datetime
from enum import Enum

from pydantic import BaseModel

CDF_FLOAT_FILLVAL = -1e31  # ISTP compliant FILLVAL for CDF_FLOAT


class CalibrationMethod(Enum):
    def __init__(self, short_name: str, long_name: str) -> None:
        super().__init__()

        # Typer does not support Enums with tuple values,
        # so we need to overwrite the value with a string name
        self._value_ = short_name

        self.short_name = short_name
        self.long_name = long_name

    KEPKO = "kepko", "Kepko"
    LEINWEBER = "leinweber", "Leinweber"
    IMAPLO_PIVOT = "imaplo", "IMAP-Lo Pivot Platform Interference"
    GRADIOMETER = "gradiometer", "Gradiometer"
    NOOP = "noop", "noop"
    SUM = "sum", "Sum of other calibrations"


class Sensor(str, Enum):
    MAGO = "MAGo"
    MAGI = "MAGi"


class ValueType(str, Enum):
    VECTOR = "vector"
    INTERPOLATION_POINTS = "interpolation_points"


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

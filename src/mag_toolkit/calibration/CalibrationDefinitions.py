from abc import ABC
from enum import Enum
from typing import Optional

import numpy as np
from pydantic import BaseModel

CDF_FLOAT_FILLVAL = -1e31  # ISTP compliant FILLVAL for CDF_FLOAT


class ArbitraryTypesAllowedBaseModel(BaseModel):
    class Config:
        arbitrary_types_allowed = True


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


class CalibrationMetadata(ArbitraryTypesAllowedBaseModel):
    dependencies: list[str]
    science: list[str]
    creation_timestamp: np.datetime64
    comment: Optional[str] = None


class Value(ArbitraryTypesAllowedBaseModel, ABC):
    raw_time: int
    time: np.datetime64
    value: list[float]
    magnitude: Optional[float] = None


class CalibrationValue(Value):
    timedelta: float = 0
    quality_flag: int = 0
    quality_bitmask: int = 0


class ScienceValue(Value):
    range: int
    quality_flag: Optional[int] = 0
    quality_bitmask: Optional[int] = 0


class Validity(ArbitraryTypesAllowedBaseModel):
    start: np.datetime64
    end: np.datetime64

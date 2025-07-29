from abc import ABC
from enum import Enum
from typing import Annotated

import numpy as np
import pandas as pd
from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    PlainSerializer,
)

CDF_FLOAT_FILLVAL = -1e31  # ISTP compliant FILLVAL for CDF_FLOAT


class ArbitraryTypesAllowedBaseModel(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)


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


def convert_time(value: str) -> np.datetime64:
    return np.datetime64(value, "ns")


def convert_timedelta(value) -> np.timedelta64:
    return pd.to_timedelta(str(value) + "s").to_numpy()


def serialize_dt(dt: np.datetime64, _info):
    return np.datetime_as_string(dt)


class CalibrationMetadata(ArbitraryTypesAllowedBaseModel):
    dependencies: list[str]
    science: list[str]
    creation_timestamp: Annotated[
        np.datetime64,
        BeforeValidator(convert_time),
        PlainSerializer(serialize_dt, return_type=str),
    ]
    comment: str | None = None


class Value(ArbitraryTypesAllowedBaseModel, ABC):
    time: Annotated[
        np.datetime64,
        BeforeValidator(convert_time),
        PlainSerializer(serialize_dt, return_type=str),
    ]
    value: list[float]
    magnitude: float | None = None


class CalibrationValue(Value):
    timedelta: float = 0.0
    quality_flag: int = 0
    quality_bitmask: int = 0


class ScienceValue(Value):
    range: int
    quality_flag: int | None = 0
    quality_bitmask: int | None = 0


class Validity(ArbitraryTypesAllowedBaseModel):
    start: Annotated[
        np.datetime64,
        BeforeValidator(convert_time),
        PlainSerializer(serialize_dt, return_type=str),
    ]
    end: Annotated[
        np.datetime64,
        BeforeValidator(convert_time),
        PlainSerializer(serialize_dt, return_type=str),
    ]

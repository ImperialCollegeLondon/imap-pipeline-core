from abc import ABC
from enum import Enum
from pathlib import Path
from typing import Annotated

import numpy as np
from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    PlainSerializer,
)


class CONSTANTS:
    L2_SKELETON_CDF = "resource/l2_dsrf_skeleton.cdf"
    OFFSET_SKELETON_CDF = "resource/imap_mag_l2-norm-offsets_20250421_20250421_v001.cdf"
    CDF_FLOAT_FILLVAL = -1e31  # ISTP compliant FILLVAL for CDF_FLOAT

    class CDF_VARS:
        EPOCH = "epoch"
        VECTORS = "vectors"
        MAGNITUDE = "magnitude"
        RANGE = "range"
        OFFSETS = "offsets"
        TIMEDELTAS = "timedeltas"
        QUALITY_FLAG = "quality_flag"
        QUALITY_FLAGS = "quality_flags"
        QUALITY_BITMASK = "quality_bitmask"
        VALIDITY_START_DATETIME = "validity_start_datetime"
        VALIDITY_END_DATETIME = "validity_end_datetime"

    class CDF_COORDS:
        AXIS = "axis"
        X = "x"
        Y = "y"
        Z = "z"
        DIRECTION = "direction"

    class CDF_ATTRS:
        GENERATION_DATE = "Generation_date"
        DATA_VERSION = "Data_version"
        LOGICAL_FILE_ID = "Logical_file_id"
        MISSION_GROUP = "Mission_group"
        IS_MAGO = "is_mago"

    class CSV_VARS:
        EPOCH = "time"
        X = "x"
        Y = "y"
        Z = "z"
        OFFSET_X = "offset_x"
        OFFSET_Y = "offset_y"
        OFFSET_Z = "offset_z"
        MAGNITUDE = "magnitude"
        RANGE = "range"
        TIMEDELTA = "timedelta"
        QUALITY_FLAG = "quality_flag"
        QUALITY_BITMASK = "quality_bitmask"


class ArbitraryTypesAllowedBaseModel(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)


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

    @classmethod
    def from_string(cls, name: str) -> "CalibrationMethod":
        for method in cls:
            if method.short_name == name or method.long_name == name:
                return method

        raise ValueError(f"Unknown calibration method: {name}")


class Sensor(str, Enum):
    MAGO = "MAGo"
    MAGI = "MAGi"


class ValueType(str, Enum):
    VECTOR = "vector"
    INTERPOLATION_POINTS = "interpolation_points"


class Mission(str, Enum):
    IMAP = "IMAP"


def convert_time(value: str) -> np.datetime64:
    return np.datetime64(value, "ns")


def serialize_dt(dt: np.datetime64, _info):
    return np.datetime_as_string(dt)


class CalibrationMetadata(ArbitraryTypesAllowedBaseModel):
    data_filename: Path | None = None
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

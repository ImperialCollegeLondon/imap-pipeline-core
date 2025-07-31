from abc import ABC
from enum import Enum
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
        EPOCH = "epoch"
        X = "x"
        Y = "y"
        Z = "z"
        MAGNITUDE = "magnitude"
        RANGE = "range"
        QUALITY_FLAGS = "quality_flags"
        QUALITY_BITMASK = "quality_bitmask"


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

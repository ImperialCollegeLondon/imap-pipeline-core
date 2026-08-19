from abc import abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated

import yaml
from pydantic import BaseModel, Field, field_validator

from mag_toolkit.calibration.CalibrationDefinitions import (
    CalibrationMethod,
    CreateOffsets,
    DatastoreAccessMode,
)


class CalibrationConfig(BaseModel):
    cleanup_temp_files_after_run: Annotated[
        bool, Field(default=True, json_schema_extra={"position": 99})
    ] = True

    @classmethod
    @abstractmethod
    def get_method(cls) -> CalibrationMethod:
        pass

    @classmethod
    def from_file(cls, path: Path):
        with open(path) as fid:
            as_dict = yaml.safe_load(fid)
        model = cls(**as_dict)
        return model

    @classmethod
    def get_class(cls, method: CalibrationMethod) -> type["CalibrationConfig"]:
        for subclass in cls.__subclasses__():
            if subclass.get_method() == method:
                return subclass
        raise ValueError(f"No subclass found for method {method}")


class GradiometryConfig(CalibrationConfig):
    kappa: float = 0.0
    sc_interference_threshold: float = 0.0

    @classmethod
    def get_method(cls) -> CalibrationMethod:
        return CalibrationMethod.GRADIOMETER


class SetQualityAndNaNConfig(CalibrationConfig):
    csv_file: str

    @classmethod
    def get_method(cls) -> CalibrationMethod:
        return CalibrationMethod.SET_QUALITY_AND_NAN


@dataclass
class ScienceFileVersionConfig:
    major: int
    minor: int


class ScriptedL2CalibrationConfig(CalibrationConfig):
    calibration_matrix_version: int
    input_json_file: str
    datastore_access_mode: DatastoreAccessMode = DatastoreAccessMode.READ_DIRECTLY
    matlab_repo: str
    produce_report: bool = False
    write_offsets: CreateOffsets = CreateOffsets.AUTOMATIC
    layer_version_number_override: Annotated[
        ScienceFileVersionConfig | None, Field(default=None)
    ] = None

    @classmethod
    def get_method(cls) -> CalibrationMethod:
        return CalibrationMethod.SCRIPTED_L2_CALIBRATION

    @field_validator("input_json_file")
    @classmethod
    def _input_json_file_not_empty(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("input_json_file must be a non-empty path")
        return value

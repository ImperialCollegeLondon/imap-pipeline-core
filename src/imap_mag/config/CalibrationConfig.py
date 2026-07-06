from pathlib import Path

import yaml
from pydantic import BaseModel, field_validator


class GradiometryConfig(BaseModel):
    kappa: float = 0.0
    sc_interference_threshold: float = 0.0


class SetQualityAndNaNConfig(BaseModel):
    csv_file: str


class CalibrationConfig(BaseModel):
    gradiometer: GradiometryConfig = GradiometryConfig()
    set_quality_and_nan: SetQualityAndNaNConfig | None = None

    @classmethod
    def from_file(cls, path: Path):
        with open(path) as fid:
            as_dict = yaml.safe_load(fid)
        model = cls(**as_dict)
        return model


class ScriptedL2CalibrationConfig(CalibrationConfig):
    """Configuration for the scripted L2 calibration that calls the external
    MATLAB ``calibration.scripts.calibrate_l2_offsets`` pipeline.

    Attributes:
        calibration_matrix_version: Version number of the calibration matrices to
            load in MATLAB (passed as the ``calibration_matrix_version`` argument).
        input_json_file: Path (relative to the MATLAB calibration repository root)
            of the calibration input JSON file, passed as the
            ``calibration_configuration_file`` argument. Must be non-empty.
    """

    calibration_matrix_version: int
    input_json_file: str

    @field_validator("input_json_file")
    @classmethod
    def _input_json_file_not_empty(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("input_json_file must be a non-empty path")
        return value

from abc import abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Self

import yaml
from pydantic import BaseModel, Field, field_validator

from imap_mag.util.constants import CONSTANTS
from mag_toolkit.calibration.CalibrationDefinitions import (
    CalibrationMethod,
    ConfigureMidpoint,
    CreateOffsets,
    DatastoreAccessMode,
)
from prefect_server.durationUtils import parse_duration

if TYPE_CHECKING:
    # Type-only import: imap_mag.config imports this module, so a runtime
    # import would be circular.
    from imap_mag.config.AppSettings import AppSettings


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
        for subclass in cls._all_subclasses():
            if subclass.get_method() == method:
                return subclass
        raise ValueError(f"No subclass found for method {method}")

    @classmethod
    def _all_subclasses(c):
        for s in c.__subclasses__():
            yield s
            yield from s._all_subclasses()


class SetQualityAndNaNConfig(CalibrationConfig):
    csv_file: str

    @classmethod
    def get_method(cls) -> CalibrationMethod:
        return CalibrationMethod.SET_QUALITY_AND_NAN


@dataclass
class ScienceFileVersionConfig:
    major: int
    minor: int


class ScriptedCalibrationConfig(CalibrationConfig):
    matlab_repo: str | None = CONSTANTS.DEFAULT_MATLAB_REPO_PATH
    calibration_matrix_version: int
    input_json_file: str

    @classmethod
    def build_with_defaults_from_app_settings(cls, app_settings: "AppSettings") -> Self:
        return cls(
            calibration_matrix_version=app_settings.calibrate.scripted_l2_calibration_matrix_version,
            input_json_file=app_settings.calibrate.scripted_l2_input_json_file,
        )

    @field_validator("input_json_file")
    @classmethod
    def _input_json_file_not_empty(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("input_json_file must be a non-empty path")
        return value


class ScriptedLeinweberCalibrationConfig(ScriptedCalibrationConfig):
    duration: Annotated[
        str | None,
        Field(
            json_schema_extra={
                "title": "Duration",
                "description": "Width of the analysis window centred on the "
                "midpoint, e.g. '24h' or '1d12h'. Sub-hour parts are rounded to the "
                "nearest hour. Left unset, the flow uses the "
                "leinweber_duration_hours Prefect variable; imap-mag calibrate "
                "requires it to be set.",
            }
        ),
    ] = None
    configure_midpoint: Annotated[
        ConfigureMidpoint,
        Field(
            json_schema_extra={
                "title": "Configure midpoint",
                "description": "How the analysis midpoint is derived from the "
                "date: USE_DATE_AS_IS uses the date (and time) exactly as given; "
                "USE_MID_DAY uses noon on that day; "
                "USE_12_HRS_AFTER_PIVOT_PLATFORM_MOVEMENT (MATLAB's own default) "
                "uses 12 hours after the IMAP-Lo pivot-platform movement time of "
                "day.",
            }
        ),
    ] = ConfigureMidpoint.USE_12_HRS_AFTER_PIVOT_PLATFORM_MOVEMENT
    verify: Annotated[
        bool,
        Field(
            json_schema_extra={
                "title": "Verify",
                "description": "Re-run Leinweber with the generated offsets and "
                "record the highest per-axis residual (VerifyResidual) in the "
                "offsets file. Slower, but recommended.",
            }
        ),
    ] = True
    allow_pivot_platform_extrapolation: Annotated[
        bool,
        Field(
            json_schema_extra={
                "title": "Allow pivot-platform extrapolation",
                "description": "If False (default), a sensor/day is skipped "
                "rather than corrected with extrapolated IMAP-Lo pivot-platform "
                "angles when the available pivot-platform data doesn't cover the "
                "window.",
            }
        ),
    ] = False

    @field_validator("duration")
    @classmethod
    def _duration_is_parseable(cls, value: str | None) -> str | None:
        if value is not None:
            parse_duration(value)
        return value

    def get_duration_hours(self) -> int | None:
        """The window width in whole hours, or None if ``duration`` is unset."""
        if self.duration is None:
            return None
        return round(parse_duration(self.duration).total_seconds() / 3600)

    @classmethod
    def get_method(cls) -> CalibrationMethod:
        return CalibrationMethod.SCRIPTED_LEINWEBER_CALIBRATION


class ScriptedL2CalibrationConfig(ScriptedCalibrationConfig):
    datastore_access_mode: DatastoreAccessMode = DatastoreAccessMode.READ_DIRECTLY
    produce_report: bool = False
    write_offsets: CreateOffsets = CreateOffsets.AUTOMATIC
    layer_version_number_override: Annotated[
        ScienceFileVersionConfig | None, Field(default=None)
    ] = None

    @classmethod
    def get_method(cls) -> CalibrationMethod:
        return CalibrationMethod.SCRIPTED_L2_CALIBRATION

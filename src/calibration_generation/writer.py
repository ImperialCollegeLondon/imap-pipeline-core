"""Writing MAG calibration CDF files."""

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
from imap_processing.cdf.imap_cdf_manager import ImapCdfAttributes
from spacepy import pycdf

from calibration_generation.matrices import NUM_RANGES
from calibration_generation.offsets import AXIS_LABELS, SENSORS

CDF_CONFIG_FOLDER = Path(__file__).parent / "cdf"

GLOBAL_ATTRS_FILE = CDF_CONFIG_FOLDER / "imap_mag_calibration_global_cdf_attrs.yaml"
VARIABLE_ATTRS_FILE = CDF_CONFIG_FOLDER / "imap_mag_calibration_variable_cdf_attrs.yaml"

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Variable:
    """A single variable to write to a calibration CDF."""

    name: str
    value: Any
    cdf_type: Any | None = None
    record_varying: bool = True


@dataclass(frozen=True)
class CalibrationFile:
    """The contents of a calibration file, independent of how it is written."""

    logical_source: str
    version: int
    valid_start_date: datetime
    variables: list[Variable] = field(default_factory=list)

    @property
    def filename(self) -> str:
        """Name of the file, following the IMAP file naming convention."""
        return (
            f"{self.logical_source}_"
            f"{self.valid_start_date.strftime('%Y%m%d')}_"
            f"v{self.version:03d}.cdf"
        )


SCHEMA_WARNING_PREFIX = "Required schema"


@contextmanager
def _collect_schema_warnings(collected: list[str]) -> Iterator[None]:
    """
    Divert the CDF attribute manager's schema warnings into a list.

    The attribute manager logs one warning per ISTP attribute it cannot find,
    directly on the root logger. Reporting dozens of them per file drowns out
    everything else, so they are collected here and summarised by the caller.

    Args:
        collected: List that intercepted warning messages are appended to.
    """

    def collect(record: logging.LogRecord) -> bool:
        if record.getMessage().startswith(SCHEMA_WARNING_PREFIX):
            collected.append(record.getMessage())
            return False
        return True

    # Filters on the root logger only see records logged directly on it, so
    # logging from this package and its dependencies is unaffected.
    root_logger = logging.getLogger()
    root_logger.addFilter(collect)

    try:
        yield
    finally:
        root_logger.removeFilter(collect)


def get_cdf_attribute_manager() -> ImapCdfAttributes:
    """
    Build a CDF attribute manager holding the MAG calibration attributes.

    The IMAP-wide defaults come from imap_processing; the calibration specific
    global and variable attributes are layered on top of them.

    Returns:
        Attribute manager for MAG calibration products.
    """
    cdf_manager = ImapCdfAttributes()
    cdf_manager.load_global_attributes(GLOBAL_ATTRS_FILE)
    cdf_manager.load_variable_attributes(VARIABLE_ATTRS_FILE)
    return cdf_manager


def _write_global_attributes(
    cdf: pycdf.CDF, cdf_manager: ImapCdfAttributes, file: CalibrationFile
) -> None:
    """Write the global attributes for a calibration file, skipping empty ones."""
    cdf_manager.add_global_attribute("Data_version", f"v{file.version:03d}")
    cdf_manager.add_global_attribute("Logical_file_id", Path(file.filename).stem)
    cdf_manager.add_global_attribute(
        "Generation_date", datetime.now().strftime("%Y%m%d")
    )

    for name, value in cdf_manager.get_global_attributes(file.logical_source).items():
        if value is not None and value != "":
            cdf.attrs[name] = value


def _write_variable(
    cdf: pycdf.CDF, cdf_manager: ImapCdfAttributes, variable: Variable
) -> None:
    """Write a variable and its non-empty attributes to an open CDF."""
    cdf.new(
        variable.name,
        variable.value,
        recVary=variable.record_varying,
        type=variable.cdf_type,
    )

    attributes = cdf_manager.get_variable_attributes(variable.name)
    for name, value in attributes.items():
        if value is not None and value != "":
            cdf[variable.name].attrs[name] = value


def write_calibration_file(file: CalibrationFile, output_folder: Path) -> Path:
    """
    Write a calibration file to disk.

    Args:
        file: The calibration file contents to write.
        output_folder: Folder to write the file into. Created if it is missing.

    Returns:
        Path of the file written.

    Raises:
        FileExistsError: If a file of the same name is already in the folder.
    """
    output_folder.mkdir(parents=True, exist_ok=True)
    path = output_folder / file.filename

    if path.exists():
        raise FileExistsError(
            f"{path} already exists. Use a different version or date, or delete it."
        )

    schema_warnings: list[str] = []
    cdf_manager = get_cdf_attribute_manager()

    with _collect_schema_warnings(schema_warnings):
        with pycdf.CDF(str(path), "") as cdf:
            _write_global_attributes(cdf, cdf_manager, file)
            for variable in file.variables:
                _write_variable(cdf, cdf_manager, variable)

    for warning in sorted(set(schema_warnings)):
        logger.debug("CDF schema: %s", warning)

    if schema_warnings:
        logger.info(
            "%d CDF schema attributes were left unset (run with --verbose for detail)",
            len(set(schema_warnings)),
        )

    return path


def _shared_variables(
    valid_start_date: datetime,
    frame_transform_mago: np.ndarray,
    frame_transform_magi: np.ndarray,
) -> list[Variable]:
    """Build the variables common to every calibration product."""
    return [
        Variable("range", list(range(NUM_RANGES))),
        Variable(
            "valid_start_datetime",
            valid_start_date,
            cdf_type=pycdf.const.CDF_TIME_TT2000,
            record_varying=False,
        ),
        Variable("URFTOORFO", frame_transform_mago),
        Variable("URFTOORFI", frame_transform_magi),
    ]


def _sensor_variables() -> list[Variable]:
    """Build the axis and sensor label variables."""
    return [
        Variable("axis", [label.lower() for label in AXIS_LABELS]),
        Variable("sensor", list(SENSORS)),
    ]


def build_l2_file(
    version: int,
    valid_start_date: datetime,
    frame_transform_mago: np.ndarray,
    frame_transform_magi: np.ndarray,
) -> CalibrationFile:
    """
    Build the contents of an L2 calibration file.

    Args:
        version: File version, written as vNNN in the filename.
        valid_start_date: First date the calibration is valid for.
        frame_transform_mago: MAGo URF to ORF matrices, shape (3, 3, NUM_RANGES).
        frame_transform_magi: MAGi URF to ORF matrices, shape (3, 3, NUM_RANGES).

    Returns:
        The calibration file contents, ready to write.
    """
    return CalibrationFile(
        logical_source="imap_mag_l2-calibration",
        version=version,
        valid_start_date=valid_start_date,
        variables=_shared_variables(
            valid_start_date, frame_transform_mago, frame_transform_magi
        ),
    )


def build_ialirt_file(
    version: int,
    valid_start_date: datetime,
    offsets: np.ndarray,
    frame_transform_mago: np.ndarray,
    frame_transform_magi: np.ndarray,
    gradiometer_factor: float,
) -> CalibrationFile:
    """
    Build the contents of an I-ALiRT calibration file.

    Args:
        version: File version, written as vNNN in the filename.
        valid_start_date: First date the calibration is valid for.
        offsets: Sensor offsets, shape (sensor, range, axis).
        frame_transform_mago: MAGo URF to ORF matrices, shape (3, 3, NUM_RANGES).
        frame_transform_magi: MAGi URF to ORF matrices, shape (3, 3, NUM_RANGES).
        gradiometer_factor: Scalar gradiometer factor, stored as a diagonal matrix.

    Returns:
        The calibration file contents, ready to write.
    """
    return CalibrationFile(
        logical_source="imap_mag_ialirt-calibration",
        version=version,
        valid_start_date=valid_start_date,
        variables=[
            *_sensor_variables(),
            *_shared_variables(
                valid_start_date, frame_transform_mago, frame_transform_magi
            ),
            Variable(
                "gradiometer_factor",
                gradiometer_factor * np.eye(3),
                cdf_type=pycdf.const.CDF_DOUBLE,
                record_varying=False,
            ),
            Variable("offsets", offsets, cdf_type=pycdf.const.CDF_DOUBLE),
        ],
    )


def build_l1d_file(
    version: int,
    valid_start_date: datetime,
    offsets: np.ndarray,
    frame_transform_mago: np.ndarray,
    frame_transform_magi: np.ndarray,
    gradiometer_factor: float,
    spin_average_factor: float,
    number_of_spins: int,
    quality_flag_threshold: float,
) -> CalibrationFile:
    """
    Build the contents of an L1d calibration file.

    Args:
        version: File version, written as vNNN in the filename.
        valid_start_date: First date the calibration is valid for.
        offsets: Sensor offsets, shape (sensor, range, axis).
        frame_transform_mago: MAGo URF to ORF matrices, shape (3, 3, NUM_RANGES).
        frame_transform_magi: MAGi URF to ORF matrices, shape (3, 3, NUM_RANGES).
        gradiometer_factor: Scalar gradiometer factor, stored as a diagonal matrix.
        spin_average_factor: Fraction of the spin average offset to subtract.
        number_of_spins: Number of spins to average over.
        quality_flag_threshold: MAGo to MAGi delta above which data is flagged.

    Returns:
        The calibration file contents, ready to write.
    """
    return CalibrationFile(
        logical_source="imap_mag_l1d-calibration",
        version=version,
        valid_start_date=valid_start_date,
        variables=[
            *_sensor_variables(),
            *_shared_variables(
                valid_start_date, frame_transform_mago, frame_transform_magi
            ),
            Variable(
                "gradiometer_factor",
                gradiometer_factor * np.eye(3),
                cdf_type=pycdf.const.CDF_DOUBLE,
                record_varying=False,
            ),
            Variable("offsets", offsets, cdf_type=pycdf.const.CDF_DOUBLE),
            Variable(
                "spin_average_application_factor",
                spin_average_factor,
                cdf_type=pycdf.const.CDF_DOUBLE,
                record_varying=False,
            ),
            Variable(
                "number_of_spins",
                number_of_spins,
                cdf_type=pycdf.const.CDF_UINT4,
                record_varying=False,
            ),
            Variable(
                "quality_flag_threshold",
                quality_flag_threshold,
                cdf_type=pycdf.const.CDF_DOUBLE,
                record_varying=False,
            ),
        ],
    )

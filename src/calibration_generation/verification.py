"""Checking the contents of generated MAG calibration files."""

import re
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
from spacepy import pycdf

from calibration_generation.matrices import NUM_RANGES
from calibration_generation.offsets import OFFSETS_SHAPE, magnitude_warnings

FILENAME_PATTERN = re.compile(r"imap_mag_(?P<level>[\w-]+?)-calibration_")

FRAME_TRANSFORM_SHAPE = (3, 3, NUM_RANGES)

LEVELS_WITH_OFFSETS = frozenset({"l1d", "ialirt"})
"""Calibration levels that carry sensor offsets. L2 holds matrices only."""


@dataclass
class VerificationResult:
    """The outcome of checking a calibration file."""

    level: str
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    summary: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        """Whether the file passed every check."""
        return not self.errors


def get_calibration_level(path: Path) -> str:
    """
    Work out which calibration product a file holds from its name.

    Args:
        path: Path to a calibration file.

    Returns:
        The calibration level, for example "l2" or "ialirt".

    Raises:
        ValueError: If the filename is not a MAG calibration filename.
    """
    match = FILENAME_PATTERN.match(path.name)

    if match is None:
        raise ValueError(
            f"{path.name} is not a calibration filename. Expected "
            "imap_mag_<level>-calibration_YYYYMMDD_vNNN.cdf"
        )

    level = match.group("level")

    if level not in LEVELS_WITH_OFFSETS and level != "l2":
        raise ValueError(f"Unknown calibration level {level!r} in {path.name}")

    return level


def _check_frame_transforms(cdf: pycdf.CDF, result: VerificationResult) -> None:
    """Check that both sensors' frame transforms are present and well formed."""
    for sensor, variable in (("MAGo", "URFTOORFO"), ("MAGi", "URFTOORFI")):
        if variable not in cdf:
            result.errors.append(f"{variable} is missing")
            continue

        transform = np.asarray(cdf[variable][...])

        if transform.shape != FRAME_TRANSFORM_SHAPE:
            result.errors.append(
                f"{variable} has shape {transform.shape}, "
                f"expected {FRAME_TRANSFORM_SHAPE}"
            )
            continue

        if not np.all(np.isfinite(transform)):
            result.errors.append(f"{variable} contains non-finite values")
            continue

        for range_index in range(NUM_RANGES):
            determinant = float(np.linalg.det(transform[:, :, range_index]))
            if not np.isclose(determinant, 1.0, atol=0.01):
                result.warnings.append(
                    f"{variable} range {range_index} has determinant "
                    f"{determinant:.4f}, expected close to 1"
                )

        result.summary.append(
            f"{sensor} range {NUM_RANGES - 1} frame transform "
            f"({variable}):\n{transform[:, :, NUM_RANGES - 1]}"
        )


def _check_offsets(cdf: pycdf.CDF, result: VerificationResult) -> None:
    """Check that offsets are present, well formed and of plausible magnitudes."""
    if "offsets" not in cdf:
        result.errors.append("offsets is missing")
        return

    offsets = np.asarray(cdf["offsets"][...])

    if offsets.shape != OFFSETS_SHAPE:
        result.errors.append(
            f"offsets has shape {offsets.shape}, expected {OFFSETS_SHAPE}"
        )
        return

    if not np.all(np.isfinite(offsets)):
        result.errors.append("offsets contains non-finite values")
        return

    result.warnings.extend(magnitude_warnings(offsets))


def verify_calibration_file(path: Path) -> VerificationResult:
    """
    Check a calibration file's variables against what its level should contain.

    Args:
        path: Path to the calibration file to check.

    Returns:
        The errors, warnings and values for manual review that were found.

    Raises:
        ValueError: If the filename does not identify a calibration level.
        FileNotFoundError: If the file does not exist.
    """
    if not path.exists():
        raise FileNotFoundError(f"{path} does not exist")

    result = VerificationResult(level=get_calibration_level(path))

    with pycdf.CDF(str(path), readonly=True) as cdf:
        _check_frame_transforms(cdf, result)

        if result.level in LEVELS_WITH_OFFSETS:
            _check_offsets(cdf, result)

    return result

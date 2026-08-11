"""Tests for the calibration_generation.verification module."""

from datetime import datetime
from pathlib import Path

import numpy as np
import pytest
from spacepy import pycdf

from calibration_generation.matrices import NUM_RANGES, MatrixSet, get_frame_transforms
from calibration_generation.offsets import build_offsets, zero_offsets
from calibration_generation.verification import (
    get_calibration_level,
    verify_calibration_file,
)
from calibration_generation.writer import (
    build_ialirt_file,
    build_l1d_file,
    build_l2_file,
    write_calibration_file,
)

VALID_START_DATE = datetime(2026, 1, 2)


def write_l2(folder: Path, version: int = 1, **overrides) -> Path:
    mago, magi = get_frame_transforms(MatrixSet.LATEST)
    return write_calibration_file(
        build_l2_file(
            version=version,
            valid_start_date=VALID_START_DATE,
            frame_transform_mago=overrides.get("frame_transform_mago", mago),
            frame_transform_magi=overrides.get("frame_transform_magi", magi),
        ),
        folder,
    )


def write_ialirt(folder: Path, offsets: np.ndarray, version: int = 1) -> Path:
    mago, magi = get_frame_transforms(MatrixSet.LATEST)
    return write_calibration_file(
        build_ialirt_file(
            version=version,
            valid_start_date=VALID_START_DATE,
            offsets=offsets,
            frame_transform_mago=mago,
            frame_transform_magi=magi,
            gradiometer_factor=0.35,
        ),
        folder,
    )


class TestGetCalibrationLevel:
    @pytest.mark.parametrize("level", ["l2", "l1d", "ialirt"])
    def test_level_is_read_from_the_filename(self, level):
        path = Path(f"imap_mag_{level}-calibration_20260102_v001.cdf")

        assert get_calibration_level(path) == level

    def test_unknown_level_is_rejected(self):
        with pytest.raises(ValueError, match="Unknown calibration level 'l3'"):
            get_calibration_level(Path("imap_mag_l3-calibration_20260102_v001.cdf"))

    @pytest.mark.parametrize(
        "name",
        [
            "imap_mag_l2-norm-offsets_20260102_v001.cdf",
            "something_else.cdf",
            "imap_mag_l2_calibration_20260102_v001.cdf",
        ],
    )
    def test_other_filenames_are_rejected(self, name):
        with pytest.raises(ValueError, match="not a calibration filename"):
            get_calibration_level(Path(name))


class TestVerifyCalibrationFile:
    def test_a_generated_l2_file_passes(self, tmp_path):
        result = verify_calibration_file(write_l2(tmp_path))

        assert result.passed
        assert result.level == "l2"
        assert not result.warnings
        assert len(result.summary) == 2

    def test_a_generated_ialirt_file_with_offsets_passes(self, tmp_path):
        offsets = build_offsets(np.ones((NUM_RANGES, 3)), np.full((NUM_RANGES, 3), 2.0))

        result = verify_calibration_file(write_ialirt(tmp_path, offsets))

        assert result.passed
        assert not result.warnings

    def test_zero_offsets_pass_without_warnings(self, tmp_path):
        result = verify_calibration_file(write_ialirt(tmp_path, zero_offsets()))

        assert result.passed
        assert not result.warnings

    def test_suspicious_offset_magnitudes_warn_but_pass(self, tmp_path):
        offsets = build_offsets(np.full((NUM_RANGES, 3), 5.0), np.ones((NUM_RANGES, 3)))

        result = verify_calibration_file(write_ialirt(tmp_path, offsets))

        assert result.passed
        assert len(result.warnings) == NUM_RANGES

    def test_l2_files_are_not_checked_for_offsets(self, tmp_path):
        """L2 calibration holds frame transforms only."""
        result = verify_calibration_file(write_l2(tmp_path))

        assert result.passed
        assert not any("offsets" in error for error in result.errors)

    def test_missing_file_is_reported(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            verify_calibration_file(
                tmp_path / "imap_mag_l2-calibration_20260102_v001.cdf"
            )

    def test_missing_frame_transform_is_an_error(self, tmp_path):
        path = write_l2(tmp_path)
        with pycdf.CDF(str(path)) as cdf:
            cdf.readonly(False)
            del cdf["URFTOORFI"]

        result = verify_calibration_file(path)

        assert not result.passed
        assert "URFTOORFI is missing" in result.errors

    def test_wrong_frame_transform_shape_is_an_error(self, tmp_path):
        _, magi = get_frame_transforms(MatrixSet.LATEST)
        path = write_l2(tmp_path, frame_transform_magi=magi[:, :, :2])

        result = verify_calibration_file(path)

        assert not result.passed
        assert any("URFTOORFI has shape" in error for error in result.errors)

    def test_a_frame_transform_that_is_not_a_rotation_warns(self, tmp_path):
        squashed = get_frame_transforms(MatrixSet.IDENTITY)[0] * 2

        result = verify_calibration_file(
            write_l2(tmp_path, frame_transform_mago=squashed)
        )

        assert result.passed
        assert len(result.warnings) == NUM_RANGES
        assert "determinant" in result.warnings[0]

    def test_non_finite_frame_transform_is_an_error(self, tmp_path):
        broken = get_frame_transforms(MatrixSet.IDENTITY)[0].copy()
        broken[0, 0, 0] = np.nan

        result = verify_calibration_file(
            write_l2(tmp_path, frame_transform_mago=broken)
        )

        assert not result.passed
        assert "URFTOORFO contains non-finite values" in result.errors

    def test_non_finite_offsets_are_an_error(self, tmp_path):
        offsets = zero_offsets()
        offsets[0, 0, 0] = np.inf

        result = verify_calibration_file(write_ialirt(tmp_path, offsets))

        assert not result.passed
        assert "offsets contains non-finite values" in result.errors

    def test_missing_offsets_is_an_error(self, tmp_path):
        path = write_ialirt(tmp_path, zero_offsets())
        with pycdf.CDF(str(path)) as cdf:
            cdf.readonly(False)
            del cdf["offsets"]

        result = verify_calibration_file(path)

        assert not result.passed
        assert "offsets is missing" in result.errors

    def test_wrong_offsets_shape_is_an_error(self, tmp_path):
        mago, magi = get_frame_transforms(MatrixSet.LATEST)
        path = write_calibration_file(
            build_ialirt_file(
                version=1,
                valid_start_date=VALID_START_DATE,
                offsets=np.zeros((2, 3)),
                frame_transform_mago=mago,
                frame_transform_magi=magi,
                gradiometer_factor=0.35,
            ),
            tmp_path,
        )

        result = verify_calibration_file(path)

        assert not result.passed
        assert any("offsets has shape" in error for error in result.errors)

    def test_l1d_files_are_checked_for_offsets_too(self, tmp_path):
        mago, magi = get_frame_transforms(MatrixSet.LATEST)
        offsets = build_offsets(np.full((NUM_RANGES, 3), 5.0), np.ones((NUM_RANGES, 3)))
        path = write_calibration_file(
            build_l1d_file(
                version=1,
                valid_start_date=VALID_START_DATE,
                offsets=offsets,
                frame_transform_mago=mago,
                frame_transform_magi=magi,
                gradiometer_factor=0.35,
                spin_average_factor=1.0,
                number_of_spins=4,
                quality_flag_threshold=2.5,
            ),
            tmp_path,
        )

        result = verify_calibration_file(path)

        assert result.level == "l1d"
        assert len(result.warnings) == NUM_RANGES

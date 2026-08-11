"""Tests for the calibration_generation.writer module."""

import logging
from datetime import datetime
from unittest.mock import patch

import numpy as np
import pytest
from spacepy import pycdf

from calibration_generation import writer
from calibration_generation.matrices import NUM_RANGES, MatrixSet, get_frame_transforms
from calibration_generation.offsets import OFFSETS_SHAPE, build_offsets
from calibration_generation.writer import (
    CalibrationFile,
    Variable,
    build_ialirt_file,
    build_l1d_file,
    build_l2_file,
    get_cdf_attribute_manager,
    write_calibration_file,
)

VALID_START_DATE = datetime(2026, 1, 2)


@pytest.fixture
def frame_transforms() -> tuple[np.ndarray, np.ndarray]:
    return get_frame_transforms(MatrixSet.LATEST)


@pytest.fixture
def offsets() -> np.ndarray:
    return build_offsets(np.ones((NUM_RANGES, 3)), np.full((NUM_RANGES, 3), 2.0))


def l2_file(version: int = 1, **kwargs):
    mago, magi = get_frame_transforms(MatrixSet.LATEST)
    return build_l2_file(
        version=version,
        valid_start_date=kwargs.get("valid_start_date", VALID_START_DATE),
        frame_transform_mago=mago,
        frame_transform_magi=magi,
    )


class TestFilename:
    def test_follows_the_imap_naming_convention(self):
        assert (
            l2_file(version=7).filename == "imap_mag_l2-calibration_20260102_v007.cdf"
        )

    def test_version_is_zero_padded_to_three_digits(self):
        assert "_v012.cdf" in l2_file(version=12).filename

    def test_each_level_has_its_own_logical_source(self, frame_transforms, offsets):
        mago, magi = frame_transforms

        ialirt = build_ialirt_file(
            version=1,
            valid_start_date=VALID_START_DATE,
            offsets=offsets,
            frame_transform_mago=mago,
            frame_transform_magi=magi,
            gradiometer_factor=0.5,
        )
        l1d = build_l1d_file(
            version=1,
            valid_start_date=VALID_START_DATE,
            offsets=offsets,
            frame_transform_mago=mago,
            frame_transform_magi=magi,
            gradiometer_factor=0.5,
            spin_average_factor=1.0,
            number_of_spins=4,
            quality_flag_threshold=2.5,
        )

        assert ialirt.filename.startswith("imap_mag_ialirt-calibration_")
        assert l1d.filename.startswith("imap_mag_l1d-calibration_")


class TestGetCdfAttributeManager:
    def test_calibration_global_attributes_are_layered_on_imap_defaults(self):
        cdf_manager = get_cdf_attribute_manager()

        attributes = cdf_manager.get_global_attributes("imap_mag_l2-calibration")

        assert attributes["Logical_source"] == "imap_mag_l2-calibration"
        assert attributes["Descriptor"] == "MAG>Magnetometer"
        # Provided by imap_processing, not by this package.
        assert attributes["Mission_group"] == "IMAP"

    @pytest.mark.parametrize(
        "logical_source",
        [
            "imap_mag_l2-calibration",
            "imap_mag_l1d-calibration",
            "imap_mag_ialirt-calibration",
        ],
    )
    def test_every_product_has_a_description(self, logical_source):
        attributes = get_cdf_attribute_manager().get_global_attributes(logical_source)

        assert attributes["Logical_source_description"]
        assert attributes["Data_type"]
        assert attributes["TEXT"]


class TestWriteCalibrationFile:
    def test_writes_the_file_and_returns_its_path(self, tmp_path):
        path = write_calibration_file(l2_file(), tmp_path)

        assert path == tmp_path / "imap_mag_l2-calibration_20260102_v001.cdf"
        assert path.exists()

    def test_creates_a_missing_output_folder(self, tmp_path):
        path = write_calibration_file(l2_file(), tmp_path / "new" / "folder")

        assert path.exists()

    def test_refuses_to_overwrite_an_existing_file(self, tmp_path):
        write_calibration_file(l2_file(), tmp_path)

        with pytest.raises(FileExistsError, match="already exists"):
            write_calibration_file(l2_file(), tmp_path)

    def test_global_attributes_identify_the_file(self, tmp_path):
        path = write_calibration_file(l2_file(version=3), tmp_path)

        with pycdf.CDF(str(path), readonly=True) as cdf:
            assert str(cdf.attrs["Data_version"]) == "v003"
            assert (
                str(cdf.attrs["Logical_file_id"])
                == "imap_mag_l2-calibration_20260102_v003"
            )
            assert str(cdf.attrs["Generation_date"])
            assert str(cdf.attrs["Logical_source"]) == "imap_mag_l2-calibration"

    def test_empty_attributes_are_not_written(self, tmp_path):
        """The attribute manager returns "" for attributes it cannot resolve."""
        path = write_calibration_file(l2_file(), tmp_path)

        with pycdf.CDF(str(path), readonly=True) as cdf:
            for variable in cdf:
                for name, value in cdf[variable].attrs.items():
                    assert value != "", f"{variable}.{name} was written as empty"

    def test_schema_warnings_are_not_logged_at_warning_level(self, tmp_path, caplog):
        with caplog.at_level(logging.WARNING):
            write_calibration_file(l2_file(), tmp_path)

        assert not [
            record
            for record in caplog.records
            if record.getMessage().startswith("Required schema")
        ]

    def test_unrelated_warnings_logged_during_a_write_get_through(
        self, tmp_path, caplog
    ):
        original = writer._write_variable

        def noisy_write(*args, **kwargs):
            logging.getLogger().warning("something else happened")
            return original(*args, **kwargs)

        with caplog.at_level(logging.WARNING):
            with patch.object(writer, "_write_variable", noisy_write):
                write_calibration_file(l2_file(), tmp_path)

        assert "something else happened" in caplog.text

    def test_root_logging_is_left_as_it_was_found(self, tmp_path, caplog):
        """The warning filter must not outlive the write and swallow other logs."""
        filters_before = list(logging.getLogger().filters)

        write_calibration_file(l2_file(), tmp_path)

        assert logging.getLogger().filters == filters_before

        with caplog.at_level(logging.WARNING):
            logging.getLogger().warning("Required schema attribute not present")

        assert "Required schema" in caplog.text

    def test_only_the_requested_variables_are_written(self, tmp_path):
        file = CalibrationFile(
            logical_source="imap_mag_l2-calibration",
            version=1,
            valid_start_date=VALID_START_DATE,
            variables=[Variable("range", list(range(NUM_RANGES)))],
        )

        path = write_calibration_file(file, tmp_path)

        with pycdf.CDF(str(path), readonly=True) as cdf:
            assert list(cdf.keys()) == ["range"]


class TestL2FileContents:
    def test_holds_frame_transforms_but_no_offsets(self, tmp_path):
        path = write_calibration_file(l2_file(), tmp_path)

        with pycdf.CDF(str(path), readonly=True) as cdf:
            assert np.asarray(cdf["URFTOORFO"][...]).shape == (3, 3, NUM_RANGES)
            assert np.asarray(cdf["URFTOORFI"][...]).shape == (3, 3, NUM_RANGES)
            assert "offsets" not in cdf
            assert list(cdf["range"][...]) == list(range(NUM_RANGES))


class TestIalirtFileContents:
    def test_holds_offsets_and_a_diagonal_gradiometer_factor(
        self, tmp_path, frame_transforms, offsets
    ):
        mago, magi = frame_transforms
        file = build_ialirt_file(
            version=1,
            valid_start_date=VALID_START_DATE,
            offsets=offsets,
            frame_transform_mago=mago,
            frame_transform_magi=magi,
            gradiometer_factor=0.35,
        )

        path = write_calibration_file(file, tmp_path)

        with pycdf.CDF(str(path), readonly=True) as cdf:
            np.testing.assert_allclose(np.asarray(cdf["offsets"][...]), offsets)
            np.testing.assert_allclose(
                np.asarray(cdf["gradiometer_factor"][...]), 0.35 * np.eye(3)
            )
            assert [str(value) for value in cdf["sensor"][...]] == ["MAGo", "MAGi"]
            assert [str(value) for value in cdf["axis"][...]] == ["x", "y", "z"]

    def test_valid_start_date_round_trips(self, tmp_path, frame_transforms, offsets):
        mago, magi = frame_transforms
        file = build_ialirt_file(
            version=1,
            valid_start_date=VALID_START_DATE,
            offsets=offsets,
            frame_transform_mago=mago,
            frame_transform_magi=magi,
            gradiometer_factor=0.35,
        )

        path = write_calibration_file(file, tmp_path)

        with pycdf.CDF(str(path), readonly=True) as cdf:
            assert cdf["valid_start_datetime"][...] == VALID_START_DATE


class TestL1dFileContents:
    def test_holds_the_spin_and_quality_parameters(
        self, tmp_path, frame_transforms, offsets
    ):
        mago, magi = frame_transforms
        file = build_l1d_file(
            version=1,
            valid_start_date=VALID_START_DATE,
            offsets=offsets,
            frame_transform_mago=mago,
            frame_transform_magi=magi,
            gradiometer_factor=0.35,
            spin_average_factor=0.75,
            number_of_spins=6,
            quality_flag_threshold=2.5,
        )

        path = write_calibration_file(file, tmp_path)

        with pycdf.CDF(str(path), readonly=True) as cdf:
            assert cdf["spin_average_application_factor"][...] == pytest.approx(0.75)
            assert cdf["number_of_spins"][...] == 6
            assert cdf["quality_flag_threshold"][...] == pytest.approx(2.5)
            assert np.asarray(cdf["offsets"][...]).shape == OFFSETS_SHAPE

"""Unit tests for CalibrationMatrix."""

from datetime import datetime

import numpy as np
import pytest
import xarray as xr

from mag_toolkit.calibration.CalibrationMatrix import CalibrationMatrix


class TestCalibrationMatrixGetZeroRotationDataset:
    def test_returns_xarray_dataset(self):
        ds = CalibrationMatrix.get_zero_rotation_dataset()
        assert isinstance(ds, xr.Dataset)

    def test_contains_expected_data_variables(self):
        ds = CalibrationMatrix.get_zero_rotation_dataset()
        assert "URFTOORFO" in ds.data_vars
        assert "URFTOORFI" in ds.data_vars
        assert "valid_start_datetime" in ds.data_vars

    def test_contains_expected_coordinates(self):
        ds = CalibrationMatrix.get_zero_rotation_dataset()
        assert "sensor" in ds.coords
        assert "range" in ds.coords
        assert "axis" in ds.coords

    def test_sensor_coordinate_has_two_values(self):
        ds = CalibrationMatrix.get_zero_rotation_dataset()
        assert len(ds.coords["sensor"]) == 2

    def test_range_coordinate_has_four_values(self):
        ds = CalibrationMatrix.get_zero_rotation_dataset()
        assert len(ds.coords["range"]) == 4

    def test_urftoorfo_is_a_3x3_identity_like_matrix(self):
        ds = CalibrationMatrix.get_zero_rotation_dataset()
        data = ds["URFTOORFO"].values
        assert data.shape[0] == 3
        assert data.shape[1] == 3

    def test_has_global_attributes(self):
        ds = CalibrationMatrix.get_zero_rotation_dataset()
        assert "Project" in ds.attrs
        assert "Descriptor" in ds.attrs


class TestCalibrationMatrixGetCombinedEpochDataset:
    def _sample_dataset(self) -> xr.Dataset:
        return xr.Dataset(
            {"value": xr.Variable(("dim0",), np.array([1, 2, 3], dtype=np.int32))}
        )

    def test_returns_xarray_dataset(self):
        input_ds = self._sample_dataset()
        result = CalibrationMatrix.get_combined_epoch_dataset_for_imap_processing(
            calibration_dataset=input_ds,
            calibration_dataset_start_date=datetime(2025, 1, 1),
            calibration_dataset_end_date=datetime(2025, 1, 3),
        )
        assert isinstance(result, xr.Dataset)

    def test_contains_epoch_dimension(self):
        input_ds = self._sample_dataset()
        result = CalibrationMatrix.get_combined_epoch_dataset_for_imap_processing(
            calibration_dataset=input_ds,
            calibration_dataset_start_date=datetime(2025, 1, 1),
            calibration_dataset_end_date=datetime(2025, 1, 3),
        )
        assert "epoch" in result.dims

    def test_epoch_spans_start_to_end_date(self):
        input_ds = self._sample_dataset()
        result = CalibrationMatrix.get_combined_epoch_dataset_for_imap_processing(
            calibration_dataset=input_ds,
            calibration_dataset_start_date=datetime(2025, 1, 1),
            calibration_dataset_end_date=datetime(2025, 1, 5),
        )
        assert len(result.coords["epoch"]) == 5

    def test_input_file_version_is_included(self):
        input_ds = self._sample_dataset()
        result = CalibrationMatrix.get_combined_epoch_dataset_for_imap_processing(
            calibration_dataset=input_ds,
            calibration_dataset_start_date=datetime(2025, 1, 1),
            calibration_dataset_end_date=datetime(2025, 1, 3),
            calibration_dataset_version=42,
        )
        assert "input_file_version" in result.data_vars

    def test_get_rotation_dataset_by_cdf_file_raises_for_missing_file(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            CalibrationMatrix.get_rotation_dataset_by_cdf_file(
                tmp_path / "nonexistent.cdf"
            )

"""Tests for CDFLoader load_cdf and write_cdf."""

from unittest.mock import MagicMock, patch

import pytest
import xarray as xr

from mag_toolkit.CDFLoader import load_cdf, write_cdf


class TestCDFLoader:
    def test_load_cdf_raises_when_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_cdf(tmp_path / "nonexistent.cdf")

    def test_load_cdf_calls_xarray_reader_for_existing_file(self, tmp_path):
        cdf_file = tmp_path / "test.cdf"
        cdf_file.touch()

        mock_dataset = MagicMock(spec=xr.Dataset)
        with patch(
            "mag_toolkit.CDFLoader.xarray.cdf_to_xarray", return_value=mock_dataset
        ) as mock_load:
            result = load_cdf(cdf_file)

        mock_load.assert_called_once_with(cdf_file)
        assert result is mock_dataset

    def test_write_cdf_calls_xarray_writer(self, tmp_path):
        output_path = tmp_path / "output.cdf"
        mock_dataset = MagicMock(spec=xr.Dataset)

        with patch("mag_toolkit.CDFLoader.xarray.xarray_to_cdf") as mock_write:
            write_cdf(mock_dataset, output_path)

        mock_write.assert_called_once_with(mock_dataset, output_path)

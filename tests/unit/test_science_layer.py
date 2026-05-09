"""Unit tests for ScienceLayer."""

from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from mag_toolkit.calibration.CalibrationDefinitions import (
    CONSTANTS,
    CalibrationMetadata,
    Mission,
    Sensor,
    Validity,
    ValueType,
)
from mag_toolkit.calibration.ScienceLayer import ScienceLayer

DATASTORE = Path(__file__).parent.parent / "datastore"
SCIENCE_CDF = (
    DATASTORE / "science/mag/l1c/2025/10/imap_mag_l1c_norm-mago_20251017_v001.cdf"
)


def _make_science_layer(with_contents: bool = True):
    layer = ScienceLayer(
        id="test-sci",
        mission=Mission.IMAP,
        validity=Validity(
            start=np.datetime64("2025-01-01"),
            end=np.datetime64("2025-01-02"),
        ),
        sensor=Sensor.MAGO,
        version=1,
        metadata=CalibrationMetadata(
            dependencies=[],
            science=["test.cdf"],
            creation_timestamp=np.datetime64("now"),
        ),
        science_file="test.cdf",
        value_type=ValueType.VECTOR,
    )
    if with_contents:
        layer._contents = pd.DataFrame(
            {
                CONSTANTS.CSV_VARS.EPOCH: pd.to_datetime(["2025-01-01T00:00:00"]),
                CONSTANTS.CSV_VARS.X: [1.0],
                CONSTANTS.CSV_VARS.Y: [2.0],
                CONSTANTS.CSV_VARS.Z: [3.0],
            }
        )
    return layer


class TestScienceLayerFromFile:
    def test_from_cdf_returns_layer_without_contents_by_default(self):
        layer = ScienceLayer.from_file(SCIENCE_CDF, load_contents=False)
        assert layer is not None
        assert layer._contents is None

    def test_from_cdf_returns_layer_with_contents_when_requested(self):
        layer = ScienceLayer.from_file(SCIENCE_CDF, load_contents=True)
        assert layer is not None
        assert layer._contents is not None
        assert len(layer._contents) > 0

    def test_from_cdf_has_correct_sensor(self):
        layer = ScienceLayer.from_file(SCIENCE_CDF)
        assert layer.sensor == Sensor.MAGO

    def test_from_non_cdf_calls_parent_from_file(self):
        with pytest.raises(Exception):
            ScienceLayer.from_file(Path("/nonexistent/path.json"))


class TestScienceLayerCalculateMagnitudes:
    def test_raises_when_contents_none(self):
        layer = _make_science_layer(with_contents=False)
        with pytest.raises(ValueError, match="contents not loaded"):
            layer.calculate_magnitudes()

    def test_adds_magnitude_column_when_contents_loaded(self):
        layer = _make_science_layer(with_contents=True)
        layer.calculate_magnitudes()
        assert CONSTANTS.CSV_VARS.MAGNITUDE in layer._contents.columns


class TestScienceLayerSetDataPath:
    def test_set_data_path_stores_path(self):
        layer = _make_science_layer(with_contents=False)
        test_path = Path("/some/path.cdf")
        layer._set_data_path(test_path)
        assert layer._data_path == test_path


class TestScienceLayerSetContents:
    def test_set_contents_stores_dataframe(self):
        layer = _make_science_layer(with_contents=False)
        df = pd.DataFrame({"a": [1, 2]})
        layer._set_contents(df)
        assert layer._contents is not None
        assert len(layer._contents) == 2


class TestScienceLayerWriteToCsv:
    def test_write_to_csv_writes_contents_to_file(self, tmp_path):
        layer = _make_science_layer(with_contents=True)
        output = tmp_path / "output.csv"
        layer._write_to_csv(output)
        assert output.exists()
        content = output.read_text()
        assert CONSTANTS.CSV_VARS.X in content

    def test_write_to_csv_replaces_nan_with_fill_values(self, tmp_path):
        layer = _make_science_layer(with_contents=False)
        layer._contents = pd.DataFrame(
            {
                CONSTANTS.CSV_VARS.EPOCH: pd.to_datetime(["2025-01-01T00:00:00"]),
                CONSTANTS.CSV_VARS.X: [float("nan")],
                CONSTANTS.CSV_VARS.Y: [2.0],
                CONSTANTS.CSV_VARS.Z: [3.0],
                CONSTANTS.CSV_VARS.MAGNITUDE: [float("nan")],
            }
        )
        output = tmp_path / "output.csv"
        layer._write_to_csv(output)
        assert output.exists()


class TestScienceLayerLoadDataFile:
    def test_load_data_file_loads_cdf_contents(self):
        layer = _make_science_layer(with_contents=False)
        layer._load_data_file(SCIENCE_CDF)
        assert layer._contents is not None

    def test_load_data_file_logs_warning_when_contents_already_set(self):
        layer = _make_science_layer(with_contents=True)
        with patch("mag_toolkit.calibration.ScienceLayer.logger") as mock_logger:
            layer._load_data_file(SCIENCE_CDF)
        mock_logger.warning.assert_called_once()

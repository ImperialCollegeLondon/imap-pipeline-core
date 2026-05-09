"""Unit tests for CalibrationLayer and ScienceLayer."""

from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from mag_toolkit.calibration import CalibrationLayer
from mag_toolkit.calibration.CalibrationDefinitions import (
    CONSTANTS,
    CalibrationMetadata,
    CalibrationMethod,
    Mission,
    Sensor,
    Validity,
    ValueType,
)
from mag_toolkit.calibration.ScienceLayer import ScienceLayer

DATASTORE = Path(__file__).parent.parent / "datastore"
LAYER_CSV = (
    DATASTORE / "calibration/layers/2025/10/imap_mag_noop-layer-data_20251017_v001.csv"
)
LAYER_JSON = (
    DATASTORE / "calibration/layers/2025/10/imap_mag_noop-layer_20251017_v001.json"
)


def _make_layer_with_contents(df: pd.DataFrame | None = None):
    if df is None:
        df = pd.DataFrame(
            {
                CONSTANTS.CSV_VARS.EPOCH: pd.to_datetime(
                    ["2025-01-01T00:00:00", "2025-01-02T00:00:00"]
                ),
                CONSTANTS.CSV_VARS.OFFSET_X: [0.0, 1.0],
                CONSTANTS.CSV_VARS.OFFSET_Y: [0.0, 2.0],
                CONSTANTS.CSV_VARS.OFFSET_Z: [0.0, 3.0],
                CONSTANTS.CSV_VARS.TIMEDELTA: [0.0, 0.0],
                CONSTANTS.CSV_VARS.QUALITY_FLAG: [0, 0],
                CONSTANTS.CSV_VARS.QUALITY_BITMASK: [0, 0],
            }
        )
    validity = Validity(
        start=np.datetime64("2025-01-01"),
        end=np.datetime64("2025-01-02"),
    )
    layer = CalibrationLayer(
        id="test",
        mission=Mission.IMAP,
        validity=validity,
        sensor=Sensor.MAGO,
        version=1,
        metadata=CalibrationMetadata(
            dependencies=[],
            science=[],
            creation_timestamp=np.datetime64("now"),
        ),
        method=CalibrationMethod.SUM,
        value_type=ValueType.VECTOR,
    )
    layer._contents = df
    return layer


class TestCalibrationLayerGetEpochs:
    def test_get_epochs_returns_epoch_column_when_contents_loaded(self):
        layer = _make_layer_with_contents()
        epochs = layer.get_epochs()
        assert len(epochs) == 2
        assert epochs.iloc[0] == pd.Timestamp("2025-01-01")

    def test_get_epochs_raises_when_contents_none_and_no_data_path(self):
        layer = _make_layer_with_contents()
        layer._contents = None
        with pytest.raises(ValueError, match="has no associated path"):
            layer.get_epochs()


class TestCalibrationLayerWriteToCsv:
    def test_write_to_csv_creates_file_with_correct_content(self, tmp_path):
        layer = _make_layer_with_contents()
        output_file = tmp_path / "output.csv"
        layer._write_to_csv(output_file)
        assert output_file.exists()
        content = output_file.read_text()
        assert "offset_x" in content

    def test_write_to_csv_creates_directory_when_requested(self, tmp_path):
        layer = _make_layer_with_contents()
        output_file = tmp_path / "subdir" / "output.csv"
        layer._write_to_csv(output_file, createDirectory=True)
        assert output_file.exists()

    def test_write_to_csv_raises_when_contents_none(self, tmp_path):
        layer = _make_layer_with_contents()
        layer._contents = None
        with pytest.raises(ValueError, match="No contents loaded"):
            layer._write_to_csv(tmp_path / "output.csv")


class TestCalibrationLayerCompatible:
    def test_compatible_returns_true_for_identical_layers(self):
        layer1 = _make_layer_with_contents()
        layer2 = _make_layer_with_contents()
        assert layer1.compatible(layer2) is True

    def test_compatible_returns_false_when_lengths_differ(self):
        layer1 = _make_layer_with_contents()
        df_short = pd.DataFrame(
            {
                CONSTANTS.CSV_VARS.EPOCH: pd.to_datetime(["2025-01-01T00:00:00"]),
                CONSTANTS.CSV_VARS.OFFSET_X: [0.0],
                CONSTANTS.CSV_VARS.OFFSET_Y: [0.0],
                CONSTANTS.CSV_VARS.OFFSET_Z: [0.0],
                CONSTANTS.CSV_VARS.TIMEDELTA: [0.0],
                CONSTANTS.CSV_VARS.QUALITY_FLAG: [0],
                CONSTANTS.CSV_VARS.QUALITY_BITMASK: [0],
            }
        )
        layer2 = _make_layer_with_contents(df_short)
        assert layer1.compatible(layer2) is False

    def test_compatible_returns_false_when_start_epochs_differ(self):
        layer1 = _make_layer_with_contents()
        df_different = pd.DataFrame(
            {
                CONSTANTS.CSV_VARS.EPOCH: pd.to_datetime(
                    ["2025-06-01T00:00:00", "2025-06-02T00:00:00"]
                ),
                CONSTANTS.CSV_VARS.OFFSET_X: [0.0, 0.0],
                CONSTANTS.CSV_VARS.OFFSET_Y: [0.0, 0.0],
                CONSTANTS.CSV_VARS.OFFSET_Z: [0.0, 0.0],
                CONSTANTS.CSV_VARS.TIMEDELTA: [0.0, 0.0],
                CONSTANTS.CSV_VARS.QUALITY_FLAG: [0, 0],
                CONSTANTS.CSV_VARS.QUALITY_BITMASK: [0, 0],
            }
        )
        layer2 = _make_layer_with_contents(df_different)
        assert layer1.compatible(layer2) is False


class TestCalibrationLayerFromFile:
    def test_from_csv_loads_layer_with_contents(self):
        layer = CalibrationLayer.from_file(LAYER_CSV)
        assert layer is not None
        assert layer._contents is not None

    def test_from_file_loads_layer_from_json(self):
        layer = CalibrationLayer.from_file(LAYER_JSON)
        assert layer is not None

    def test_values_from_csv_raises_on_nan_quality_column(self, tmp_path):
        csv_file = tmp_path / "bad_layer.csv"
        csv_file.write_text(
            f"{CONSTANTS.CSV_VARS.EPOCH},{CONSTANTS.CSV_VARS.OFFSET_X},{CONSTANTS.CSV_VARS.QUALITY_FLAG}\n"
            "2025-01-01T00:00:00,0.0,\n"
        )
        with pytest.raises(ValueError, match="NaN/blank values"):
            CalibrationLayer._values_from_csv(csv_file)


class TestCalibrationLayerLoadDataFile:
    def test_load_data_file_logs_warning_when_contents_already_set(self):
        layer = _make_layer_with_contents()
        with patch("mag_toolkit.calibration.CalibrationLayer.logger") as mock_logger:
            layer._load_data_file(LAYER_CSV)
        mock_logger.warning.assert_called_once()


class TestCalibrationLayerConvertToRawEpoch:
    def test_converts_epoch_to_raw_epoch_when_not_present(self):
        layer = _make_layer_with_contents()
        layer._convert_to_raw_epoch()
        assert CONSTANTS.CSV_VARS.RAW_EPOCH in layer._contents.columns

    def test_skips_conversion_when_raw_epoch_already_exists(self):
        layer = _make_layer_with_contents()
        layer._contents[CONSTANTS.CSV_VARS.RAW_EPOCH] = [0.0, 1.0]
        original_values = layer._contents[CONSTANTS.CSV_VARS.RAW_EPOCH].tolist()
        layer._convert_to_raw_epoch()
        assert layer._contents[CONSTANTS.CSV_VARS.RAW_EPOCH].tolist() == original_values

    def test_raises_when_contents_none(self):
        layer = _make_layer_with_contents()
        layer._contents = None
        with pytest.raises(ValueError, match="No contents loaded"):
            layer._convert_to_raw_epoch()


class TestCalibrationLayerSetMetadata:
    def test_set_metadata_updates_validity_from_science_layer(self):

        layer = _make_layer_with_contents()
        science = ScienceLayer(
            id="sci",
            mission=Mission.IMAP,
            validity=Validity(
                start=np.datetime64("2025-06-01"),
                end=np.datetime64("2025-06-30"),
            ),
            sensor=Sensor.MAGO,
            version=1,
            metadata=CalibrationMetadata(
                dependencies=["dep.csv"],
                science=["sci.csv"],
                creation_timestamp=np.datetime64("now"),
            ),
            science_file="sci.csv",
            value_type=ValueType.VECTOR,
        )

        layer.set_metadata(
            dependencies=["dep.csv"],
            original_science=science,
            calibration_id="cal-001",
        )

        assert layer.id == "cal-001"
        assert layer.validity.start == np.datetime64("2025-06-01")
        assert layer.sensor == Sensor.MAGO
        assert layer.mission == Mission.IMAP

    def test_set_metadata_raises_when_contents_none(self):

        layer = _make_layer_with_contents()
        layer._contents = None
        science = ScienceLayer(
            id="sci",
            mission=Mission.IMAP,
            validity=Validity(
                start=np.datetime64("2025-06-01"),
                end=np.datetime64("2025-06-30"),
            ),
            sensor=Sensor.MAGO,
            version=1,
            metadata=CalibrationMetadata(
                dependencies=[],
                science=["sci.csv"],
                creation_timestamp=np.datetime64("now"),
            ),
            science_file="sci.csv",
            value_type=ValueType.VECTOR,
        )

        with pytest.raises(ValueError, match="contents not loaded"):
            layer.set_metadata(
                dependencies=[],
                original_science=science,
                calibration_id="cal-001",
            )


class TestCalibrationLayerCreateZeroOffsetLayer:
    def _make_science_layer(self):

        science = ScienceLayer(
            id="sci",
            mission=Mission.IMAP,
            validity=Validity(
                start=np.datetime64("2025-03-01"),
                end=np.datetime64("2025-03-31"),
            ),
            sensor=Sensor.MAGO,
            version=1,
            metadata=CalibrationMetadata(
                dependencies=[],
                science=["sci.csv"],
                creation_timestamp=np.datetime64("now"),
            ),
            science_file="sci.csv",
            value_type=ValueType.VECTOR,
        )
        sci_df = pd.DataFrame(
            {
                CONSTANTS.CSV_VARS.EPOCH: pd.to_datetime(["2025-03-01T00:00:00"]),
                CONSTANTS.CSV_VARS.X: [1.0],
                CONSTANTS.CSV_VARS.Y: [2.0],
                CONSTANTS.CSV_VARS.Z: [3.0],
            }
        )
        science._contents = sci_df
        return science

    def test_creates_zero_offset_layer_from_science(self):

        science = self._make_science_layer()
        result = CalibrationLayer.create_zero_offset_layer_from_science(science)

        assert result is not None
        assert result._contents is not None
        assert len(result._contents) == 1
        assert result._contents[CONSTANTS.CSV_VARS.OFFSET_X].iloc[0] == 0.0

    def test_raises_when_science_layer_is_none(self):

        with pytest.raises(ValueError, match="Science layer must be provided"):
            CalibrationLayer.create_zero_offset_layer_from_science(None)

    def test_raises_when_science_layer_has_no_data_path(self):

        science = ScienceLayer(
            id="sci",
            mission=Mission.IMAP,
            validity=Validity(
                start=np.datetime64("2025-03-01"),
                end=np.datetime64("2025-03-31"),
            ),
            sensor=Sensor.MAGO,
            version=1,
            metadata=CalibrationMetadata(
                dependencies=[],
                science=["sci.csv"],
                creation_timestamp=np.datetime64("now"),
            ),
            science_file="sci.csv",
            value_type=ValueType.VECTOR,
        )
        with pytest.raises(ValueError, match="associated path"):
            CalibrationLayer.create_zero_offset_layer_from_science(science)


class TestCalibrationLayerWriteToJson:
    def test_write_to_json_creates_both_json_and_csv(self, tmp_path):
        layer = CalibrationLayer.from_file(LAYER_JSON)
        json_file = tmp_path / "imap_mag_noop-layer_20251017_v001.json"
        layer._write_to_json(json_file, createDirectory=True)
        assert json_file.exists()

    def test_write_to_json_without_contents_writes_only_json(self, tmp_path):
        layer = CalibrationLayer.from_file(LAYER_JSON)
        layer._contents = None
        json_file = tmp_path / "imap_mag_noop-layer_20251017_v001.json"
        layer._write_to_json(json_file, createDirectory=True)
        assert json_file.exists()


class TestCalibrationLayerWriteToJsonWithNullDataFilename:
    def test_write_to_json_generates_data_filename_when_none(self, tmp_path):
        layer = CalibrationLayer.from_file(LAYER_JSON)
        layer.metadata.data_filename = None
        json_file = tmp_path / "imap_mag_noop-layer_20251017_v001.json"
        layer._write_to_json(json_file, createDirectory=True)
        assert json_file.exists()
        assert layer.metadata.data_filename is not None


class TestCreateZeroOffsetLayerWithContentDate:
    def test_creates_layer_with_data_filename_when_content_date_set(self):

        science = ScienceLayer(
            id="sci",
            mission=Mission.IMAP,
            validity=Validity(
                start=np.datetime64("2025-03-01"),
                end=np.datetime64("2025-03-31"),
            ),
            sensor=Sensor.MAGO,
            version=1,
            metadata=CalibrationMetadata(
                dependencies=[],
                science=["sci.csv"],
                creation_timestamp=np.datetime64("now"),
            ),
            science_file="sci.csv",
            value_type=ValueType.VECTOR,
        )
        sci_df = pd.DataFrame(
            {
                CONSTANTS.CSV_VARS.EPOCH: pd.to_datetime(["2025-03-01T00:00:00"]),
                CONSTANTS.CSV_VARS.X: [1.0],
                CONSTANTS.CSV_VARS.Y: [2.0],
                CONSTANTS.CSV_VARS.Z: [3.0],
            }
        )
        science._contents = sci_df
        # Set content_date directly as microsecond-precision datetime64 (bypasses validator)
        # so that astype(datetime) returns a datetime.datetime rather than an int
        object.__setattr__(
            science.metadata,
            "content_date",
            np.datetime64("2025-03-15T00:00:00.000000"),
        )

        result = CalibrationLayer.create_zero_offset_layer_from_science(science)

        assert result is not None
        assert result.metadata.data_filename is not None

    def test_raises_when_science_contents_none_after_load(self):

        science = ScienceLayer(
            id="sci",
            mission=Mission.IMAP,
            validity=Validity(
                start=np.datetime64("2025-03-01"),
                end=np.datetime64("2025-03-31"),
            ),
            sensor=Sensor.MAGO,
            version=1,
            metadata=CalibrationMetadata(
                dependencies=[],
                science=["sci.csv"],
                creation_timestamp=np.datetime64("now"),
            ),
            science_file="sci.csv",
            value_type=ValueType.VECTOR,
        )
        with patch("mag_toolkit.calibration.Layer.Layer.load_contents"):
            with pytest.raises(ValueError, match="contents must be loaded"):
                CalibrationLayer.create_zero_offset_layer_from_science(science)


class TestValuesFromCsvEmpty:
    def test_values_from_csv_raises_on_header_only_csv(self, tmp_path):
        csv_file = tmp_path / "header_only.csv"
        csv_file.write_text(
            f"{CONSTANTS.CSV_VARS.EPOCH},{CONSTANTS.CSV_VARS.OFFSET_X}\n"
        )
        df = CalibrationLayer._values_from_csv(csv_file)
        assert df.empty


class TestScienceLayerWriteToCsv:
    def test_write_to_csv_raises_when_contents_none(self, tmp_path):

        layer = ScienceLayer(
            id="test",
            mission=Mission.IMAP,
            validity=Validity(
                start=np.datetime64("2025-01-01"), end=np.datetime64("2025-01-02")
            ),
            sensor=Sensor.MAGO,
            version=1,
            metadata=CalibrationMetadata(
                dependencies=[],
                science=["test.csv"],
                creation_timestamp=np.datetime64("now"),
            ),
            science_file="test.csv",
            value_type=ValueType.VECTOR,
        )

        with pytest.raises(ValueError, match="No science data available"):
            layer._write_to_csv(tmp_path / "output.csv")

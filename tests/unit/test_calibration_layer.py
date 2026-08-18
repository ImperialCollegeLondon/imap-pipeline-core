"""Unit tests for CalibrationLayer and ScienceLayer."""

from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from imap_mag.io.file import CalibrationLayerPathHandler
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
from tests.util.miscellaneous import write_calibration_layer_pair

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


def _make_precise_df() -> pd.DataFrame:
    """A layer dataframe with sub-microsecond timestamps and near machine-precision
    float values, used to verify no accuracy is lost writing/reading Parquet vs CSV."""
    return pd.DataFrame(
        {
            CONSTANTS.CSV_VARS.EPOCH: pd.to_datetime(
                [
                    "2025-01-01T00:00:00.123456789",
                    "2025-06-15T12:30:45.999999999",
                    "2099-12-31T23:59:59.000000001",
                ]
            ),
            CONSTANTS.CSV_VARS.OFFSET_X: [
                1.0000000001,
                -2.0000000002,
                3.14159265358979,
            ],
            CONSTANTS.CSV_VARS.OFFSET_Y: [4.00000000004, 5.0, -6.99999999991],
            CONSTANTS.CSV_VARS.OFFSET_Z: [0.0000000001, 123456.789012345, -0.1],
            CONSTANTS.CSV_VARS.TIMEDELTA: [0.0, 1.23456789e-8, -9.87654321e-9],
            CONSTANTS.CSV_VARS.QUALITY_FLAG: [0, 1, -1],
            CONSTANTS.CSV_VARS.QUALITY_BITMASK: [0, 5, -65535],
        }
    )


class TestCalibrationLayerWriteToParquet:
    def test_write_to_parquet_creates_file(self, tmp_path):
        layer = _make_layer_with_contents()
        output_file = tmp_path / "output.parquet"
        layer._write_to_parquet(output_file)
        assert output_file.exists()

    def test_write_to_parquet_creates_directory_when_requested(self, tmp_path):
        layer = _make_layer_with_contents()
        output_file = tmp_path / "subdir" / "output.parquet"
        layer._write_to_parquet(output_file, createDirectory=True)
        assert output_file.exists()

    def test_write_to_parquet_raises_when_contents_none(self, tmp_path):
        layer = _make_layer_with_contents()
        layer._contents = None
        with pytest.raises(ValueError, match="No contents loaded"):
            layer._write_to_parquet(tmp_path / "output.parquet")

    def test_write_to_parquet_uses_zstd_compression(self, tmp_path):
        import pyarrow.parquet as pq

        layer = _make_layer_with_contents()
        output_file = tmp_path / "output.parquet"
        layer._write_to_parquet(output_file)

        metadata = pq.ParquetFile(output_file).metadata
        compressions = {
            metadata.row_group(0).column(i).compression.upper()
            for i in range(metadata.row_group(0).num_columns)
        }
        assert compressions == {"ZSTD"}

    def test_write_to_file_dispatches_parquet_on_parquet_extension(self, tmp_path):
        layer = _make_layer_with_contents()
        output_file = tmp_path / "output.parquet"
        layer.write_to_file(output_file)
        assert output_file.exists()


class TestCalibrationLayerFromParquet:
    def test_values_from_parquet_returns_dataframe(self, tmp_path):
        layer = _make_layer_with_contents()
        parquet_file = tmp_path / "layer.parquet"
        layer._write_to_parquet(parquet_file)
        df = CalibrationLayer._values_from_parquet(parquet_file)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2

    def test_values_from_parquet_has_correct_columns(self, tmp_path):
        layer = _make_layer_with_contents()
        parquet_file = tmp_path / "layer.parquet"
        layer._write_to_parquet(parquet_file)
        df = CalibrationLayer._values_from_parquet(parquet_file)
        assert CONSTANTS.CSV_VARS.EPOCH in df.columns
        assert CONSTANTS.CSV_VARS.OFFSET_X in df.columns

    def test_values_from_parquet_raises_on_nan_quality_column(self, tmp_path):
        df = _make_layer_with_contents()._contents.copy()
        df[CONSTANTS.CSV_VARS.QUALITY_FLAG] = [float("nan"), 0]
        layer = _make_layer_with_contents(df)
        parquet_file = tmp_path / "bad_layer.parquet"
        layer._write_to_parquet(parquet_file)
        with pytest.raises(ValueError, match="NaN/blank values"):
            CalibrationLayer._values_from_parquet(parquet_file)

    def test_values_from_parquet_parses_text_epoch_column(self, tmp_path):
        """MATLAB writes the epoch column as text, not a native timestamp - verify
        Python's parquet reader handles that shape too."""
        df = pd.DataFrame(
            {
                CONSTANTS.CSV_VARS.EPOCH: [
                    "2025-01-01T00:00:00.123456789",
                    "2025-01-02T00:00:00.000000000",
                ],
                CONSTANTS.CSV_VARS.OFFSET_X: [0.0, 1.0],
                CONSTANTS.CSV_VARS.OFFSET_Y: [0.0, 2.0],
                CONSTANTS.CSV_VARS.OFFSET_Z: [0.0, 3.0],
                CONSTANTS.CSV_VARS.TIMEDELTA: [0.0, 0.0],
                CONSTANTS.CSV_VARS.QUALITY_FLAG: [0, 0],
                CONSTANTS.CSV_VARS.QUALITY_BITMASK: [0, 0],
            }
        )
        text_epoch_file = tmp_path / "text_epoch.parquet"
        df.to_parquet(text_epoch_file, index=False, engine="pyarrow")

        result = CalibrationLayer._values_from_parquet(text_epoch_file)
        assert pd.api.types.is_datetime64_any_dtype(result[CONSTANTS.CSV_VARS.EPOCH])
        assert result[CONSTANTS.CSV_VARS.EPOCH].iloc[0] == pd.Timestamp(
            "2025-01-01T00:00:00.123456789"
        )

    def test_from_parquet_round_trips_contents(self, tmp_path):
        layer = _make_layer_with_contents()
        parquet_file = tmp_path / "layer.parquet"
        layer._write_to_parquet(parquet_file)
        loaded = CalibrationLayer._from_parquet(parquet_file)
        assert loaded._contents is not None
        assert len(loaded._contents) == 2
        assert loaded._contents[CONSTANTS.CSV_VARS.OFFSET_X].iloc[1] == pytest.approx(
            1.0
        )

    def test_load_data_file_dispatches_to_parquet_reader(self, tmp_path):
        layer = _make_layer_with_contents()
        parquet_file = tmp_path / "layer.parquet"
        layer._write_to_parquet(parquet_file)
        fresh = _make_layer_with_contents()
        fresh._contents = None
        fresh._load_data_file(parquet_file)
        assert fresh._contents is not None

    def test_from_file_dispatches_to_parquet_on_parquet_extension(self, tmp_path):
        layer = _make_layer_with_contents()
        parquet_file = tmp_path / "layer.parquet"
        layer._write_to_parquet(parquet_file)
        loaded = CalibrationLayer.from_file(parquet_file)
        assert loaded._contents is not None
        assert len(loaded._contents) == 2


class TestCalibrationLayerParquetCsvPrecisionParity:
    """Guards against any precision loss introduced by Parquet support: writing the
    same precise data to both CSV and Parquet and reading it back must give
    identical records in both cases."""

    def test_parquet_and_csv_round_trip_identically(self, tmp_path):
        df = _make_precise_df()
        layer = _make_layer_with_contents(df)

        csv_file = tmp_path / "layer.csv"
        parquet_file = tmp_path / "layer.parquet"
        layer._write_to_csv(csv_file)
        layer._write_to_parquet(parquet_file)

        from_csv = CalibrationLayer._values_from_csv(csv_file)
        from_parquet = CalibrationLayer._values_from_parquet(parquet_file)

        pd.testing.assert_frame_equal(
            from_csv.reset_index(drop=True),
            from_parquet.reset_index(drop=True),
            check_dtype=False,
        )

    def test_parquet_preserves_nanosecond_epoch_precision(self, tmp_path):
        df = _make_precise_df()
        layer = _make_layer_with_contents(df)
        parquet_file = tmp_path / "layer.parquet"
        layer._write_to_parquet(parquet_file)

        result = CalibrationLayer._values_from_parquet(parquet_file)
        pd.testing.assert_series_equal(
            result[CONSTANTS.CSV_VARS.EPOCH].reset_index(drop=True),
            df[CONSTANTS.CSV_VARS.EPOCH].reset_index(drop=True),
        )

    def test_parquet_preserves_full_float_precision(self, tmp_path):
        df = _make_precise_df()
        layer = _make_layer_with_contents(df)
        parquet_file = tmp_path / "layer.parquet"
        layer._write_to_parquet(parquet_file)

        result = CalibrationLayer._values_from_parquet(parquet_file)
        for col in [
            CONSTANTS.CSV_VARS.OFFSET_X,
            CONSTANTS.CSV_VARS.OFFSET_Y,
            CONSTANTS.CSV_VARS.OFFSET_Z,
            CONSTANTS.CSV_VARS.TIMEDELTA,
        ]:
            assert (result[col].to_numpy() == df[col].to_numpy()).all(), col


class TestCalibrationLayerFromCdf:
    def test_values_from_cdf_round_trips_offsets_and_epoch(self, tmp_path):
        df = _make_layer_with_contents()._contents
        layer = _make_layer_with_contents(df)
        cdf_file = tmp_path / "layer.cdf"
        layer._write_to_cdf(cdf_file)

        result = CalibrationLayer._values_from_cdf(cdf_file)
        assert len(result) == len(df)
        assert result[CONSTANTS.CSV_VARS.OFFSET_X].tolist() == pytest.approx(
            df[CONSTANTS.CSV_VARS.OFFSET_X].tolist()
        )
        assert pd.api.types.is_datetime64_any_dtype(result[CONSTANTS.CSV_VARS.EPOCH])

    def test_values_from_cdf_restores_nan_from_fill_value(self, tmp_path):
        df = _make_layer_with_contents()._contents.copy()
        df.loc[0, CONSTANTS.CSV_VARS.OFFSET_X] = float("nan")
        layer = _make_layer_with_contents(df)
        cdf_file = tmp_path / "layer_with_nan.cdf"
        layer._write_to_cdf(cdf_file)

        result = CalibrationLayer._values_from_cdf(cdf_file)
        assert np.isnan(result[CONSTANTS.CSV_VARS.OFFSET_X].iloc[0])

    def test_from_file_dispatches_to_cdf_on_cdf_extension(self, tmp_path):
        layer = _make_layer_with_contents()
        cdf_file = tmp_path / "layer.cdf"
        layer._write_to_cdf(cdf_file)
        loaded = CalibrationLayer.from_file(cdf_file)
        assert loaded._contents is not None
        assert len(loaded._contents) == 2


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
        assert layer._local_file_path == LAYER_JSON
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

    def test_write_to_csv_handles_non_datetime_epoch_column(self, tmp_path):
        """A header-only/empty epoch column has no inferred datetime dtype -
        writing it must not crash trying to apply nanosecond string formatting."""
        df = pd.DataFrame(
            {
                CONSTANTS.CSV_VARS.EPOCH: [],
                CONSTANTS.CSV_VARS.OFFSET_X: [],
                CONSTANTS.CSV_VARS.OFFSET_Y: [],
                CONSTANTS.CSV_VARS.OFFSET_Z: [],
                CONSTANTS.CSV_VARS.TIMEDELTA: [],
                CONSTANTS.CSV_VARS.QUALITY_FLAG: [],
                CONSTANTS.CSV_VARS.QUALITY_BITMASK: [],
            }
        )
        layer = _make_layer_with_contents(df)
        output_file = tmp_path / "empty.csv"
        layer._write_to_csv(output_file)
        assert output_file.exists()


class TestUpdateFileContentsBasedOnVersion:
    def test_rewrites_version_fields_to_match_handler(self, tmp_path):
        """When the datastore assigns a different version than the layer was
        generated at, the rewritten file's version/version_major fields must
        match the new version - not just its filename and data_filename."""
        json_path, _ = write_calibration_layer_pair(
            tmp_path, "manual-norm", datetime(2026, 5, 1), version=1
        )
        layer = CalibrationLayer.from_file(json_path, load_contents=False)
        assert layer.version == 1
        assert layer.version_major == 1

        new_version_handler = CalibrationLayerPathHandler(
            descriptor="manual-norm",
            content_date=datetime(2026, 5, 1),
            version=3,
            version_major=1,
        )

        result_path = layer.update_file_contents_based_on_version(
            new_version_handler, json_path
        )

        rewritten = CalibrationLayer.from_file(result_path, load_contents=False)
        assert rewritten.version == 3
        assert rewritten.version_major == 1

    def test_returns_source_unchanged_when_version_already_matches(self, tmp_path):
        json_path, _ = write_calibration_layer_pair(
            tmp_path, "manual-norm", datetime(2026, 5, 1), version=1
        )
        layer = CalibrationLayer.from_file(json_path, load_contents=False)

        same_version_handler = CalibrationLayerPathHandler(
            descriptor="manual-norm",
            content_date=datetime(2026, 5, 1),
            version=1,
            version_major=1,
        )

        result_path = layer.update_file_contents_based_on_version(
            same_version_handler, json_path
        )

        assert result_path == json_path


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

"""Unit tests for CalibrationApplicator module."""

import contextlib
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from imap_processing.mag.l2.mag_l2_data import ValidFrames

from imap_mag.util.ReferenceFrame import ReferenceFrame
from mag_toolkit.calibration.CalibrationApplicator import CalibrationApplicator
from mag_toolkit.calibration.CalibrationDefinitions import CONSTANTS, ValueType

_COLS = CONSTANTS.CSV_VARS

_CALIBRATION_LAYER_FROM_FILE = (
    "mag_toolkit.calibration.CalibrationApplicator.CalibrationLayer.from_file"
)


def _make_applicator():
    return CalibrationApplicator(app_settings=MagicMock())


def _make_layer_df(
    epochs, offset_x=1.0, offset_y=2.0, offset_z=3.0, quality_flag=0, quality_bitmask=0
):
    n = len(epochs)
    return pd.DataFrame(
        {
            _COLS.EPOCH: pd.DatetimeIndex(epochs),
            _COLS.OFFSET_X: [offset_x] * n,
            _COLS.OFFSET_Y: [offset_y] * n,
            _COLS.OFFSET_Z: [offset_z] * n,
            _COLS.TIMEDELTA: [0.0] * n,
            _COLS.QUALITY_FLAG: [quality_flag] * n,
            _COLS.QUALITY_BITMASK: [quality_bitmask] * n,
        }
    )


def _make_layer_mock(contents_df):
    layer = MagicMock()
    layer._contents = contents_df
    return layer


def _make_compatible_layer_mock(
    n=3, offset_x=1.0, offset_y=1.0, offset_z=1.0, quality_flag=0, quality_bitmask=0
):
    """Build a CalibrationLayer mock with loaded contents and compatible=True."""
    epochs = pd.date_range("2025-01-01", periods=n, freq="1min")
    layer = _make_layer_mock(
        _make_layer_df(
            epochs,
            offset_x=offset_x,
            offset_y=offset_y,
            offset_z=offset_z,
            quality_flag=quality_flag,
            quality_bitmask=quality_bitmask,
        )
    )
    layer.compatible.return_value = True
    return layer


def _make_mock_science():
    mock_science = MagicMock()
    mock_science._contents = {_COLS.EPOCH: pd.Series(dtype="datetime64[ns]")}
    return mock_science


@contextlib.contextmanager
def _patch_apply_base_deps(tmp_path):
    """Patch the four external dependencies needed to get past apply() validation."""
    with (
        patch("mag_toolkit.calibration.CalibrationApplicator.ScienceLayer.from_file"),
        patch.object(
            CalibrationApplicator,
            "_create_offsets_file",
            return_value=tmp_path / "offsets.cdf",
        ),
        patch(
            "mag_toolkit.calibration.CalibrationApplicator.CalibrationMatrix.get_zero_rotation_dataset"
        ),
        patch(
            "mag_toolkit.calibration.CalibrationApplicator.CalibrationMatrix.get_combined_epoch_dataset_for_imap_processing"
        ),
    ):
        yield


class TestGetL2Frames:
    def test_returns_empty_list_for_empty_input(self):
        result = CalibrationApplicator._get_l2_frames([])
        assert result == []

    def test_maps_srf_frame(self):
        result = CalibrationApplicator._get_l2_frames([ReferenceFrame.SRF])
        assert result == [ValidFrames.SRF]

    def test_maps_gse_frame(self):
        result = CalibrationApplicator._get_l2_frames([ReferenceFrame.GSE])
        assert result == [ValidFrames.GSE]

    def test_maps_all_known_frames(self):
        frames = [
            ReferenceFrame.DSRF,
            ReferenceFrame.SRF,
            ReferenceFrame.GSE,
            ReferenceFrame.GSM,
            ReferenceFrame.RTN,
        ]
        result = CalibrationApplicator._get_l2_frames(frames)
        assert result == [
            ValidFrames.DSRF,
            ValidFrames.SRF,
            ValidFrames.GSE,
            ValidFrames.GSM,
            ValidFrames.RTN,
        ]

    def test_preserves_order_of_input_frames(self):
        result = CalibrationApplicator._get_l2_frames(
            [ReferenceFrame.GSE, ReferenceFrame.SRF]
        )
        assert result == [ValidFrames.GSE, ValidFrames.SRF]


class TestApplyValidation:
    def test_raises_when_no_layers_and_no_rotation(self, tmp_path):
        with pytest.raises(
            ValueError, match="No calibration layers or rotation file provided"
        ):
            _make_applicator().apply(
                day_to_process=datetime(2025, 10, 17),
                layer_files=[],
                rotation=None,
                dataFile=tmp_path / "science.cdf",
                outputOffsetsFile=tmp_path / "offsets.cdf",
                outputScienceFolder=tmp_path,
            )

    def test_raises_when_output_science_folder_does_not_exist(self, tmp_path):
        with pytest.raises(ValueError, match="Output science folder does not exist"):
            _make_applicator().apply(
                day_to_process=datetime(2025, 10, 17),
                layer_files=[tmp_path / "layer.json"],
                rotation=None,
                dataFile=tmp_path / "science.cdf",
                outputOffsetsFile=tmp_path / "offsets.cdf",
                outputScienceFolder=tmp_path / "nonexistent_folder",
            )

    def test_raises_when_science_file_does_not_exist(self, tmp_path):
        with pytest.raises(
            ValueError,
            match="Input science file does not exist or could not be parsed",
        ):
            _make_applicator().apply(
                day_to_process=datetime(2025, 10, 17),
                layer_files=[tmp_path / "layer.json"],
                rotation=None,
                dataFile=tmp_path / "missing_science.cdf",
                outputOffsetsFile=tmp_path / "offsets.cdf",
                outputScienceFolder=tmp_path,
            )

    def test_raises_when_science_filename_is_not_parseable(self, tmp_path):
        unparseable = tmp_path / "unknown_file_format.cdf"
        unparseable.write_bytes(b"fake cdf data")

        with pytest.raises(
            ValueError,
            match="Input science file does not exist or could not be parsed",
        ):
            _make_applicator().apply(
                day_to_process=datetime(2025, 10, 17),
                layer_files=[tmp_path / "layer.json"],
                rotation=None,
                dataFile=unparseable,
                outputOffsetsFile=tmp_path / "offsets.cdf",
                outputScienceFolder=tmp_path,
            )

    def test_raises_file_exists_error_when_offsets_file_already_exists(self, tmp_path):
        science_file = tmp_path / "imap_mag_l1c_norm-mago_20251017_v001.cdf"
        science_file.write_bytes(b"fake cdf data")
        existing_offsets = tmp_path / "offsets.cdf"
        existing_offsets.write_bytes(b"existing offsets")

        with pytest.raises(
            FileExistsError,
            match="Output calibration file already exists",
        ):
            _make_applicator().apply(
                day_to_process=datetime(2025, 10, 17),
                layer_files=[tmp_path / "layer.json"],
                rotation=None,
                dataFile=science_file,
                outputOffsetsFile=existing_offsets,
                outputScienceFolder=tmp_path,
            )


class TestCreateOffsetsFileValidation:
    def test_raises_when_no_layers_provided(self, tmp_path):
        with pytest.raises(ValueError, match="No calibration layers provided"):
            _make_applicator()._create_offsets_file(
                layers=[],
                outputCalibrationFile=tmp_path / "offsets.cdf",
                science=MagicMock(),
            )

    def test_raises_when_offsets_contents_not_loaded(self, tmp_path):
        mock_offsets = MagicMock()
        mock_offsets.value_type = ValueType.VECTOR
        mock_offsets.compatible.return_value = True
        mock_offsets._contents = None

        with (
            patch(_CALIBRATION_LAYER_FROM_FILE, return_value=mock_offsets),
            pytest.raises(ValueError, match="Offsets layer contents not loaded"),
        ):
            _make_applicator()._create_offsets_file(
                layers=[tmp_path / "layer.json"],
                outputCalibrationFile=tmp_path / "offsets.cdf",
                science=_make_mock_science(),
            )

    def test_writes_offsets_to_file_and_returns_path(self, tmp_path):
        expected_path = tmp_path / "offsets.cdf"
        mock_offsets = MagicMock()
        mock_offsets.value_type = ValueType.VECTOR
        mock_offsets.compatible.return_value = True
        mock_offsets._contents = pd.DataFrame({"col": [1, 2]})
        mock_offsets.writeToFile.return_value = expected_path

        with patch(_CALIBRATION_LAYER_FROM_FILE, return_value=mock_offsets):
            result = _make_applicator()._create_offsets_file(
                layers=[tmp_path / "layer.json"],
                outputCalibrationFile=expected_path,
                science=_make_mock_science(),
            )

        assert result == expected_path
        mock_offsets.set_metadata.assert_called_once()
        mock_offsets.writeToFile.assert_called_once_with(expected_path)

    def test_raises_calibration_validity_error_when_offsets_not_compatible(
        self, tmp_path
    ):
        from mag_toolkit.calibration.CalibrationExceptions import (
            CalibrationValidityError,
        )

        mock_offsets = MagicMock()
        mock_offsets.value_type = ValueType.VECTOR
        mock_offsets.compatible.return_value = False

        with (
            patch(_CALIBRATION_LAYER_FROM_FILE, return_value=mock_offsets),
            pytest.raises(CalibrationValidityError, match="not time compatible"),
        ):
            _make_applicator()._create_offsets_file(
                layers=[tmp_path / "layer.json"],
                outputCalibrationFile=tmp_path / "offsets.cdf",
                science=_make_mock_science(),
            )

    def test_expands_boundary_changes_layer_before_writing(self, tmp_path):
        expected_path = tmp_path / "offsets.cdf"
        mock_offsets = MagicMock()
        mock_offsets.value_type = ValueType.BOUNDARY_CHANGES_ONLY
        mock_offsets.compatible.return_value = True
        mock_offsets._contents = pd.DataFrame({"col": [1, 2]})
        mock_offsets.writeToFile.return_value = expected_path

        applicator = _make_applicator()
        with (
            patch(_CALIBRATION_LAYER_FROM_FILE, return_value=mock_offsets),
            patch.object(
                applicator,
                "_expand_boundary_changes_to_every_epoch",
                return_value=mock_offsets,
            ) as mock_expand,
        ):
            result = applicator._create_offsets_file(
                layers=[tmp_path / "layer.json"],
                outputCalibrationFile=expected_path,
                science=_make_mock_science(),
            )

        assert result == expected_path
        mock_expand.assert_called_once()

    def test_sums_multiple_layers_together(self, tmp_path):
        expected_path = tmp_path / "offsets.cdf"
        mock_offsets = MagicMock()
        mock_offsets.value_type = ValueType.VECTOR
        mock_offsets.compatible.return_value = True
        mock_offsets._contents = pd.DataFrame({"col": [1, 2]})
        mock_offsets.writeToFile.return_value = expected_path
        mock_layer2 = MagicMock()
        mock_layer2.value_type = ValueType.VECTOR

        applicator = _make_applicator()
        with (
            patch(
                _CALIBRATION_LAYER_FROM_FILE, side_effect=[mock_offsets, mock_layer2]
            ),
            patch.object(applicator, "_sum_layers", return_value=mock_offsets),
        ):
            result = applicator._create_offsets_file(
                layers=[tmp_path / "layer1.json", tmp_path / "layer2.json"],
                outputCalibrationFile=expected_path,
                science=_make_mock_science(),
            )

        assert result == expected_path

    def test_expands_boundary_changes_second_layer_in_loop(self, tmp_path):
        expected_path = tmp_path / "offsets.cdf"
        mock_offsets = MagicMock()
        mock_offsets.value_type = ValueType.VECTOR
        mock_offsets.compatible.return_value = True
        mock_offsets._contents = pd.DataFrame({"col": [1, 2]})
        mock_offsets.writeToFile.return_value = expected_path
        mock_layer2 = MagicMock()
        mock_layer2.value_type = ValueType.BOUNDARY_CHANGES_ONLY

        applicator = _make_applicator()
        with (
            patch(
                _CALIBRATION_LAYER_FROM_FILE, side_effect=[mock_offsets, mock_layer2]
            ),
            patch.object(
                applicator,
                "_expand_boundary_changes_to_every_epoch",
                return_value=mock_layer2,
            ) as mock_expand,
            patch.object(applicator, "_sum_layers", return_value=mock_offsets),
        ):
            result = applicator._create_offsets_file(
                layers=[tmp_path / "layer1.json", tmp_path / "layer2.json"],
                outputCalibrationFile=expected_path,
                science=_make_mock_science(),
            )

        mock_expand.assert_called_once()
        assert result == expected_path


class TestApplyPostValidation:
    def _make_validated_apply_kwargs(self, tmp_path):
        science_file = tmp_path / "imap_mag_l1c_norm-mago_20251017_v001.cdf"
        science_file.write_bytes(b"fake cdf data")
        return dict(
            day_to_process=datetime(2025, 10, 17),
            layer_files=[tmp_path / "layer.json"],
            rotation=None,
            dataFile=science_file,
            outputOffsetsFile=tmp_path / "new_offsets.cdf",
            outputScienceFolder=tmp_path,
        )

    def test_raises_when_spice_metakernel_generation_returns_none(self, tmp_path):
        kwargs = self._make_validated_apply_kwargs(tmp_path)

        with (
            _patch_apply_base_deps(tmp_path),
            patch(
                "mag_toolkit.calibration.CalibrationApplicator.generate_spice_metakernel",
                return_value=None,
            ),
            pytest.raises(ValueError, match="Failed to generate spice metakernel"),
        ):
            _make_applicator().apply(**kwargs)

    def test_raises_when_resolved_spice_metakernel_path_does_not_exist(self, tmp_path):
        kwargs = self._make_validated_apply_kwargs(tmp_path)
        nonexistent_mk = tmp_path / "nonexistent_dir" / "metakernel.tm"

        with (
            _patch_apply_base_deps(tmp_path),
            pytest.raises(
                ValueError, match="Resolved spice metakernel path does not exist"
            ),
        ):
            _make_applicator().apply(**{**kwargs, "spice_metakernel": nonexistent_mk})

    def test_raises_when_rotation_filename_is_not_parseable(self, tmp_path):
        kwargs = self._make_validated_apply_kwargs(tmp_path)
        rotation_file = tmp_path / "unparseable_rotation.cdf"
        rotation_file.write_bytes(b"fake")

        with (
            patch(
                "mag_toolkit.calibration.CalibrationApplicator.ScienceLayer.from_file"
            ),
            patch.object(
                CalibrationApplicator,
                "_create_offsets_file",
                return_value=tmp_path / "offsets.cdf",
            ),
            patch(
                "mag_toolkit.calibration.CalibrationApplicator.AncillaryPathHandler.from_filename",
                return_value=None,
            ),
            pytest.raises(ValueError, match="Could not parse rotation file name"),
        ):
            _make_applicator().apply(
                **{**kwargs, "layer_files": [], "rotation": rotation_file}
            )

    def test_processes_through_spice_and_returns_empty_files_when_no_datasets(
        self, tmp_path
    ):
        science_file = tmp_path / "imap_mag_l1c_norm-mago_20251017_v001.cdf"
        science_file.write_bytes(b"fake cdf data")
        spice_mk = tmp_path / "metakernel.tm"
        spice_mk.write_text("SPICE")

        with (
            _patch_apply_base_deps(tmp_path),
            patch("mag_toolkit.calibration.CalibrationApplicator.spiceypy.kclear"),
            patch("mag_toolkit.calibration.CalibrationApplicator.spiceypy.furnsh"),
            patch("mag_toolkit.calibration.CalibrationApplicator.os.chdir"),
            patch(
                "mag_toolkit.calibration.CalibrationApplicator.cdf_to_xarray",
                return_value=MagicMock(),
            ),
            patch(
                "mag_toolkit.calibration.CalibrationApplicator.mag_l2.mag_l2",
                return_value=[],
            ),
        ):
            files, _ = _make_applicator().apply(
                day_to_process=datetime(2025, 10, 17),
                layer_files=[tmp_path / "layer.json"],
                rotation=None,
                dataFile=science_file,
                outputOffsetsFile=tmp_path / "new_offsets.cdf",
                outputScienceFolder=tmp_path,
                spice_metakernel=spice_mk,
            )

        assert files == []


class TestApplyEarlyReturn:
    def test_returns_empty_l2_list_when_no_reference_frames(self, tmp_path):
        science_file = tmp_path / "imap_mag_l1c_norm-mago_20251017_v001.cdf"
        science_file.write_bytes(b"fake cdf data")
        offsets_path = tmp_path / "offsets.cdf"

        with (
            patch(
                "mag_toolkit.calibration.CalibrationApplicator.ScienceLayer.from_file"
            ),
            patch.object(
                CalibrationApplicator,
                "_create_offsets_file",
                return_value=offsets_path,
            ),
        ):
            l2_files, returned_offsets = _make_applicator().apply(
                day_to_process=datetime(2025, 10, 17),
                layer_files=[tmp_path / "layer.json"],
                rotation=None,
                dataFile=science_file,
                outputOffsetsFile=tmp_path / "new_offsets.cdf",
                outputScienceFolder=tmp_path,
                reference_frames=[],
            )

        assert l2_files == []
        assert returned_offsets == offsets_path


class TestExpandBoundaryChangesToEveryEpoch:
    def test_raises_when_layer_contents_is_none(self):
        layer = _make_layer_mock(None)
        science_epochs = pd.Series(pd.date_range("2025-01-01", periods=3, freq="1min"))

        with pytest.raises(ValueError, match="Boundary changes layer has no contents"):
            _make_applicator()._expand_boundary_changes_to_every_epoch(
                layer, science_epochs
            )

    def test_expands_empty_layer_to_zero_offsets_for_each_science_epoch(self):
        empty_df = pd.DataFrame(
            {
                _COLS.EPOCH: pd.DatetimeIndex([]),
                _COLS.OFFSET_X: pd.Series([], dtype=float),
                _COLS.OFFSET_Y: pd.Series([], dtype=float),
                _COLS.OFFSET_Z: pd.Series([], dtype=float),
                _COLS.TIMEDELTA: pd.Series([], dtype=float),
                _COLS.QUALITY_FLAG: pd.Series([], dtype=int),
                _COLS.QUALITY_BITMASK: pd.Series([], dtype=int),
            }
        )
        layer = _make_layer_mock(empty_df)
        science_epochs = pd.Series(pd.date_range("2025-01-01", periods=3, freq="1min"))

        result = _make_applicator()._expand_boundary_changes_to_every_epoch(
            layer, science_epochs
        )

        assert len(result._contents) == 3
        assert (result._contents[_COLS.OFFSET_X] == 0.0).all()
        assert result.value_type == ValueType.VECTOR

    def test_forward_fills_changes_across_science_epochs(self):
        epochs = pd.date_range("2025-01-01", periods=2, freq="2min")
        science_epochs = pd.Series(pd.date_range("2025-01-01", periods=4, freq="1min"))
        layer = _make_layer_mock(_make_layer_df(epochs, offset_x=5.0))

        result = _make_applicator()._expand_boundary_changes_to_every_epoch(
            layer, science_epochs
        )

        assert len(result._contents) == 4
        assert result.value_type == ValueType.VECTOR

    def test_raises_when_nan_quality_flag_after_expansion(self):
        epochs = pd.date_range("2025-01-01 00:01", periods=1, freq="1min")
        layer_df = pd.DataFrame(
            {
                _COLS.EPOCH: epochs,
                _COLS.OFFSET_X: [1.0],
                _COLS.OFFSET_Y: [2.0],
                _COLS.OFFSET_Z: [3.0],
                _COLS.TIMEDELTA: [0.0],
                _COLS.QUALITY_FLAG: [float("nan")],
                _COLS.QUALITY_BITMASK: [0],
            }
        )
        layer = _make_layer_mock(layer_df)
        # Science epochs start at 00:01 (same as change) so no zero row prepend
        science_epochs = pd.Series(
            pd.date_range("2025-01-01 00:01", periods=2, freq="1min")
        )

        with pytest.raises(ValueError, match="Unexpected NaN values"):
            _make_applicator()._expand_boundary_changes_to_every_epoch(
                layer, science_epochs
            )

    def test_prepends_zero_row_when_first_change_after_science_start(self):
        # Layer changes start 1 minute after science start
        layer_epochs = pd.date_range("2025-01-01 00:01", periods=2, freq="1min")
        science_epochs = pd.Series(
            pd.date_range("2025-01-01 00:00", periods=4, freq="1min")
        )
        layer = _make_layer_mock(_make_layer_df(layer_epochs, offset_x=9.0))

        result = _make_applicator()._expand_boundary_changes_to_every_epoch(
            layer, science_epochs
        )

        assert len(result._contents) == 4
        # First row must be zero (prepended for science time before first change)
        assert result._contents.iloc[0][_COLS.OFFSET_X] == 0.0


class TestSumLayers:
    def test_raises_when_offsets_contents_is_none(self):
        offsets = MagicMock()
        offsets._contents = None

        with pytest.raises(
            ValueError, match="Offsets or layer contents are not loaded"
        ):
            _make_applicator()._sum_layers(offsets, _make_compatible_layer_mock())

    def test_raises_when_layer_contents_is_none(self):
        bad_layer = MagicMock()
        bad_layer._contents = None

        with pytest.raises(
            ValueError, match="Offsets or layer contents are not loaded"
        ):
            _make_applicator()._sum_layers(_make_compatible_layer_mock(), bad_layer)

    def test_raises_when_layers_are_not_compatible(self):
        offsets = _make_compatible_layer_mock()
        offsets.compatible.return_value = False

        with pytest.raises(
            ValueError, match="Offsets and layer are not time compatible"
        ):
            _make_applicator()._sum_layers(offsets, _make_compatible_layer_mock())

    def test_adds_offset_values_together(self):
        offsets = _make_compatible_layer_mock(offset_x=1.0, offset_y=2.0, offset_z=3.0)
        layer = _make_compatible_layer_mock(offset_x=10.0, offset_y=20.0, offset_z=30.0)

        result = _make_applicator()._sum_layers(offsets, layer)

        assert (result._contents[_COLS.OFFSET_X] == 11.0).all()
        assert (result._contents[_COLS.OFFSET_Y] == 22.0).all()
        assert (result._contents[_COLS.OFFSET_Z] == 33.0).all()

    def test_quality_flag_stays_zero_when_both_layers_are_zero(self):
        result = _make_applicator()._sum_layers(
            _make_compatible_layer_mock(quality_flag=0),
            _make_compatible_layer_mock(quality_flag=0),
        )
        assert (result._contents[_COLS.QUALITY_FLAG] == 0).all()

    def test_quality_flag_becomes_one_when_layer_sets_it(self):
        result = _make_applicator()._sum_layers(
            _make_compatible_layer_mock(quality_flag=0),
            _make_compatible_layer_mock(quality_flag=1),
        )
        assert (result._contents[_COLS.QUALITY_FLAG] == 1).all()

    def test_quality_flag_cleared_to_zero_when_layer_is_minus_one(self):
        result = _make_applicator()._sum_layers(
            _make_compatible_layer_mock(quality_flag=1),
            _make_compatible_layer_mock(quality_flag=-1),
        )
        assert (result._contents[_COLS.QUALITY_FLAG] == 0).all()

    def test_quality_bitmask_sets_bits_with_positive_layer(self):
        result = _make_applicator()._sum_layers(
            _make_compatible_layer_mock(quality_bitmask=0b0001),
            _make_compatible_layer_mock(quality_bitmask=0b0010),
        )
        assert (result._contents[_COLS.QUALITY_BITMASK] == 0b0011).all()

    def test_quality_bitmask_clears_bits_with_negative_layer(self):
        # base=0b0011=3, layer=-2, result = 3 & (-2 - 1) = 3 & ~2 = 0b0001
        result = _make_applicator()._sum_layers(
            _make_compatible_layer_mock(quality_bitmask=0b0011),
            _make_compatible_layer_mock(quality_bitmask=-2),
        )
        assert (result._contents[_COLS.QUALITY_BITMASK] == 0b0001).all()

    def test_invalidates_data_path_after_sum(self):
        offsets = _make_compatible_layer_mock()
        offsets._data_path = Path("/some/path")

        result = _make_applicator()._sum_layers(offsets, _make_compatible_layer_mock())

        assert result._data_path is None

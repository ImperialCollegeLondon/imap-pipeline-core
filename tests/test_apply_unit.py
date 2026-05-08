"""Unit tests for apply CLI command helper functions."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
import typer

from imap_mag.cli.apply import (
    _apply_for_date,
    _prepare_layers_for_application,
    _prepare_rotation_layer_for_application,
    _setup_zero_calibration_layer,
    apply,
    cleanup_workfolder_after_apply,
)
from imap_mag.config import SaveMode
from imap_mag.util import ReferenceFrame, ScienceMode


class TestPrepareLayers:
    def test_raises_when_calibration_handler_cannot_parse_filename(self, tmp_path):
        mock_settings = MagicMock()
        mock_settings.work_folder = tmp_path
        mock_datastore_finder = MagicMock()

        with patch(
            "imap_mag.cli.apply.CalibrationLayerPathHandler.from_filename",
            return_value=None,
        ):
            with pytest.raises(ValueError, match="Could not parse metadata"):
                _prepare_layers_for_application(
                    layers=["unparseable_file.json"],
                    datastore_finder=mock_datastore_finder,
                    appSettings=mock_settings,
                )

    def test_fetches_data_file_when_metadata_has_data_filename(self, tmp_path):
        mock_settings = MagicMock()
        mock_settings.work_folder = tmp_path

        mock_handler = MagicMock()
        mock_datastore_finder = MagicMock()
        mock_datastore_finder.find_by_handler.return_value = (
            tmp_path / "layer_file.json"
        )

        mock_cal_layer = MagicMock()
        mock_cal_layer.metadata.data_filename = "data.csv"

        with (
            patch(
                "imap_mag.cli.apply.CalibrationLayerPathHandler.from_filename",
                return_value=mock_handler,
            ),
            patch(
                "imap_mag.cli.apply.fetch_file_for_work",
                return_value=tmp_path / "work_layer.json",
            ),
            patch(
                "imap_mag.cli.apply.CalibrationLayer.from_file",
                return_value=mock_cal_layer,
            ),
        ):
            result = _prepare_layers_for_application(
                layers=["imap_mag_l2-calibration_20250101_v001.json"],
                datastore_finder=mock_datastore_finder,
                appSettings=mock_settings,
            )

        assert len(result) == 1


class TestPrepareRotationLayer:
    def test_returns_none_when_rotation_is_none(self):
        mock_settings = MagicMock()
        result = _prepare_rotation_layer_for_application(None, mock_settings)
        assert result is None

    def test_raises_when_ancillary_handler_cannot_parse_filename(self, tmp_path):
        mock_settings = MagicMock()
        mock_settings.data_store = tmp_path

        with patch(
            "imap_mag.cli.apply.AncillaryPathHandler.from_filename",
            return_value=None,
        ):
            with pytest.raises(ValueError, match="Could not parse metadata"):
                _prepare_rotation_layer_for_application(
                    "unparseable_rotation.cdf", mock_settings
                )

    def test_returns_work_file_when_rotation_is_valid(self, tmp_path):
        mock_settings = MagicMock()
        mock_settings.work_folder = tmp_path

        mock_handler = MagicMock()
        mock_finder = MagicMock()
        rotation_path = tmp_path / "rotation.cdf"

        with (
            patch(
                "imap_mag.cli.apply.AncillaryPathHandler.from_filename",
                return_value=mock_handler,
            ),
            patch(
                "imap_mag.cli.apply.FileFinder",
                return_value=mock_finder,
            ),
            patch(
                "imap_mag.cli.apply.fetch_file_for_work",
                return_value=rotation_path,
            ),
        ):
            mock_finder.find_by_handler.return_value = tmp_path / "store_rotation.cdf"
            result = _prepare_rotation_layer_for_application(
                "imap_mag_l2-rotation_20250101_v001.cdf", mock_settings
            )

        assert result == rotation_path


def _default_apply_for_date_kwargs():
    return dict(
        layers=["*"],
        date=datetime(2025, 10, 17),
        mode=None,
        input=None,
        offset_file_output_type="cdf",
        l2_output_type="cdf",
        rotation=None,
        save_mode=SaveMode.LocalOnly,
        spice_metakernel=None,
        reference_frames=[ReferenceFrame.SRF],
    )


class TestApplyDateValidation:
    def test_raises_bad_parameter_when_both_date_and_start_date_provided(self):
        with pytest.raises(typer.BadParameter):
            apply(
                layers=["*noop*"],
                date=datetime(2025, 10, 17),
                start_date=datetime(2025, 10, 17),
            )

    def test_raises_bad_parameter_when_neither_date_nor_start_date_provided(self):
        with pytest.raises(typer.BadParameter):
            apply(layers=["*noop*"], date=None, start_date=None)

    def test_loops_once_per_day_in_date_range(self, tmp_path):
        with patch("imap_mag.cli.apply._apply_for_date") as mock_apply_for_date:
            apply(
                layers=["*noop*"],
                start_date=datetime(2025, 10, 17),
                end_date=datetime(2025, 10, 19),
            )
        assert mock_apply_for_date.call_count == 3


class TestApplyForDate:
    def test_raises_when_mode_cannot_be_inferred_from_input(self, tmp_path):
        with (
            patch("imap_mag.cli.apply.AppSettings") as mock_settings_cls,
            patch("imap_mag.cli.apply.initialiseLoggingForCommand"),
        ):
            mock_settings_cls.return_value.setup_work_folder_for_command.return_value = tmp_path
            with pytest.raises(
                ValueError, match="Burst/Normal mode could not be inferred"
            ):
                _apply_for_date(
                    **{**_default_apply_for_date_kwargs(), "input": None, "mode": None}
                )

    def test_infers_burst_mode_from_input_filename(self, tmp_path):
        with (
            patch("imap_mag.cli.apply.AppSettings") as mock_settings_cls,
            patch("imap_mag.cli.apply.initialiseLoggingForCommand"),
            patch("imap_mag.cli.apply.FileFinder", side_effect=RuntimeError("stop")),
        ):
            mock_settings_cls.return_value.setup_work_folder_for_command.return_value = tmp_path
            with pytest.raises(RuntimeError, match="stop"):
                _apply_for_date(
                    **{
                        **_default_apply_for_date_kwargs(),
                        "input": "imap_mag_l1c_norm-burst_20250101_v001.cdf",
                        "mode": None,
                    }
                )

    def test_raises_when_input_filename_cannot_be_parsed(self, tmp_path):
        mock_finder = MagicMock()
        mock_finder.find_layers_by_date_and_patterns.return_value = []

        with (
            patch("imap_mag.cli.apply.AppSettings") as mock_settings_cls,
            patch("imap_mag.cli.apply.initialiseLoggingForCommand"),
            patch("imap_mag.cli.apply.FileFinder", return_value=mock_finder),
            patch(
                "imap_mag.cli.apply.SciencePathHandler.from_filename",
                return_value=None,
            ),
        ):
            mock_settings_cls.return_value.setup_work_folder_for_command.return_value = tmp_path
            with pytest.raises(
                ValueError, match="Could not parse metadata from input file"
            ):
                _apply_for_date(
                    **{
                        **_default_apply_for_date_kwargs(),
                        "input": "unparseable_file.cdf",
                        "mode": ScienceMode.Normal,
                    }
                )


class TestCleanupWorkfolderAfterApply:
    def test_deletes_files_inside_work_folder(self, tmp_path):
        work_folder = tmp_path / "work"
        work_folder.mkdir()

        mock_settings = MagicMock()
        mock_settings.work_folder = work_folder

        science_file = work_folder / "science.cdf"
        science_file.write_text("data")
        layer_file = work_folder / "layer.json"
        layer_file.write_text("data")
        l2_file = work_folder / "l2.cdf"
        l2_file.write_text("data")
        offset_file = work_folder / "offsets.cdf"
        offset_file.write_text("data")

        cleanup_workfolder_after_apply(
            app_settings=mock_settings,
            workScienceFile=science_file,
            workLayers=[layer_file],
            workRotationFile=None,
            L2_files=[l2_file],
            offset_file=offset_file,
        )

        assert not science_file.exists()
        assert not layer_file.exists()
        assert not l2_file.exists()
        assert not offset_file.exists()

    def test_deletes_rotation_file_when_provided(self, tmp_path):
        work_folder = tmp_path / "work"
        work_folder.mkdir()

        mock_settings = MagicMock()
        mock_settings.work_folder = work_folder

        rotation_file = work_folder / "rotation.cdf"
        rotation_file.write_text("data")
        science_file = work_folder / "science.cdf"
        science_file.write_text("data")
        offset_file = work_folder / "offsets.cdf"
        offset_file.write_text("data")

        cleanup_workfolder_after_apply(
            app_settings=mock_settings,
            workScienceFile=science_file,
            workLayers=[],
            workRotationFile=rotation_file,
            L2_files=[],
            offset_file=offset_file,
        )

        assert not rotation_file.exists()

    def test_skips_files_outside_work_folder(self, tmp_path):
        work_folder = tmp_path / "work"
        work_folder.mkdir()
        outside_folder = tmp_path / "outside"
        outside_folder.mkdir()

        mock_settings = MagicMock()
        mock_settings.work_folder = work_folder

        outside_file = outside_folder / "external.cdf"
        outside_file.write_text("data")
        inside_file = work_folder / "inside.cdf"
        inside_file.write_text("data")

        cleanup_workfolder_after_apply(
            app_settings=mock_settings,
            workScienceFile=outside_file,
            workLayers=[],
            workRotationFile=None,
            L2_files=[inside_file],
            offset_file=work_folder / "offsets.cdf",
        )

        assert outside_file.exists()
        assert not inside_file.exists()

    def test_also_deletes_csv_associated_with_json_layer(self, tmp_path):
        work_folder = tmp_path / "work"
        work_folder.mkdir()

        mock_settings = MagicMock()
        mock_settings.work_folder = work_folder

        json_layer = work_folder / "layer.json"
        json_layer.write_text("data")
        csv_layer = work_folder / "layer.csv"
        csv_layer.write_text("data")
        offset_file = work_folder / "offsets.cdf"
        offset_file.write_text("data")
        science_file = work_folder / "science.cdf"
        science_file.write_text("data")

        cleanup_workfolder_after_apply(
            app_settings=mock_settings,
            workScienceFile=science_file,
            workLayers=[json_layer],
            workRotationFile=None,
            L2_files=[],
            offset_file=offset_file,
        )

        assert not json_layer.exists()
        assert not csv_layer.exists()

    def test_does_not_fail_when_file_does_not_exist(self, tmp_path):
        work_folder = tmp_path / "work"
        work_folder.mkdir()

        mock_settings = MagicMock()
        mock_settings.work_folder = work_folder

        nonexistent_file = work_folder / "nonexistent.cdf"

        cleanup_workfolder_after_apply(
            app_settings=mock_settings,
            workScienceFile=nonexistent_file,
            workLayers=[],
            workRotationFile=None,
            L2_files=[],
            offset_file=nonexistent_file,
        )


class TestSetupZeroCalibrationLayer:
    def test_creates_zero_offset_layer_file_in_work_folder(self, tmp_path):
        work_folder = tmp_path / "work"
        work_folder.mkdir()

        mock_science_layer = MagicMock()
        mock_zero_layer = MagicMock()

        with (
            patch(
                "imap_mag.cli.apply.ScienceLayer.from_file",
                return_value=mock_science_layer,
            ),
            patch(
                "imap_mag.cli.apply.CalibrationLayer.create_zero_offset_layer_from_science",
                return_value=mock_zero_layer,
            ),
            patch(
                "imap_mag.cli.apply.CalibrationLayerPathHandler.get_filename",
                return_value="noop_layer.json",
            ),
        ):
            result = _setup_zero_calibration_layer(
                work_folder=work_folder,
                workScienceFile=tmp_path / "science.cdf",
                content_date=datetime(2025, 10, 17),
            )

        mock_zero_layer.writeToFile.assert_called_once()
        assert result == work_folder / "noop_layer.json"

    def test_overwrites_existing_layer_file(self, tmp_path):
        work_folder = tmp_path / "work"
        work_folder.mkdir()

        mock_science_layer = MagicMock()
        mock_zero_layer = MagicMock()

        layer_filename = "noop_layer.json"
        existing_file = work_folder / layer_filename
        existing_file.write_text("old data")

        with (
            patch(
                "imap_mag.cli.apply.ScienceLayer.from_file",
                return_value=mock_science_layer,
            ),
            patch(
                "imap_mag.cli.apply.CalibrationLayer.create_zero_offset_layer_from_science",
                return_value=mock_zero_layer,
            ),
            patch(
                "imap_mag.cli.apply.CalibrationLayerPathHandler.get_filename",
                return_value=layer_filename,
            ),
        ):
            result = _setup_zero_calibration_layer(
                work_folder=work_folder,
                workScienceFile=tmp_path / "science.cdf",
                content_date=datetime(2025, 10, 17),
            )

        assert result == existing_file
        mock_zero_layer.writeToFile.assert_called_once()

"""Unit tests for the plot_ialirt CLI command."""

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

from imap_mag.cli.plot.plot_ialirt import plot_ialirt
from imap_mag.config import SaveMode
from imap_mag.io.file import IALiRTQuicklookPathHandler


def _make_mock_settings(tmp_path: Path, publish_to_data_store: bool = False):
    mock_settings = MagicMock()
    mock_settings.setup_work_folder_for_command.return_value = tmp_path
    mock_settings.plot_ialirt.publish_to_data_store = publish_to_data_store
    mock_settings.data_store = tmp_path
    return mock_settings


class TestPlotIALiRTNoFiles:
    def test_returns_empty_dict_when_no_science_or_hk_files_found(self, tmp_path):
        mock_settings = _make_mock_settings(tmp_path)

        with (
            patch(
                "imap_mag.cli.plot.plot_ialirt.AppSettings", return_value=mock_settings
            ),
            patch("imap_mag.cli.plot.plot_ialirt.initialiseLoggingForCommand"),
            patch(
                "imap_mag.cli.plot.plot_ialirt.fetch_ialirt_files_for_work",
                return_value=[],
            ),
            patch(
                "imap_mag.cli.plot.plot_ialirt.fetch_ialirt_hk_files_for_work",
                return_value=[],
            ),
        ):
            result = plot_ialirt()

        assert result == {}


class TestPlotIALiRTLocalOnly:
    def test_returns_generated_figure_when_not_publishing_to_datastore(self, tmp_path):
        mock_settings = _make_mock_settings(tmp_path, publish_to_data_store=False)

        fake_science_file = tmp_path / "science.csv"
        fake_science_file.write_text("data")
        mock_handler = MagicMock(spec=IALiRTQuicklookPathHandler)
        mock_handler.content_date = None

        with (
            patch(
                "imap_mag.cli.plot.plot_ialirt.AppSettings", return_value=mock_settings
            ),
            patch("imap_mag.cli.plot.plot_ialirt.initialiseLoggingForCommand"),
            patch(
                "imap_mag.cli.plot.plot_ialirt.fetch_ialirt_files_for_work",
                return_value=[fake_science_file],
            ),
            patch(
                "imap_mag.cli.plot.plot_ialirt.fetch_ialirt_hk_files_for_work",
                return_value=[],
            ),
            patch(
                "imap_mag.cli.plot.plot_ialirt.plot_ialirt_files",
                return_value={fake_science_file: mock_handler},
            ),
        ):
            result = plot_ialirt(save_mode=SaveMode.LocalOnly)

        assert result == {fake_science_file: mock_handler}

    def test_logs_info_message_with_file_count(self, tmp_path):
        mock_settings = _make_mock_settings(tmp_path, publish_to_data_store=False)

        fake_hk_file = tmp_path / "hk.csv"
        fake_hk_file.write_text("data")
        mock_handler = MagicMock(spec=IALiRTQuicklookPathHandler)
        mock_handler.content_date = None

        with (
            patch(
                "imap_mag.cli.plot.plot_ialirt.AppSettings", return_value=mock_settings
            ),
            patch("imap_mag.cli.plot.plot_ialirt.initialiseLoggingForCommand"),
            patch(
                "imap_mag.cli.plot.plot_ialirt.fetch_ialirt_files_for_work",
                return_value=[],
            ),
            patch(
                "imap_mag.cli.plot.plot_ialirt.fetch_ialirt_hk_files_for_work",
                return_value=[fake_hk_file],
            ),
            patch(
                "imap_mag.cli.plot.plot_ialirt.plot_ialirt_files",
                return_value={fake_hk_file: mock_handler},
            ),
        ):
            result = plot_ialirt(save_mode=SaveMode.LocalOnly)

        assert fake_hk_file in result


class TestPlotIALiRTPublishToDatastore:
    def test_publishes_generated_figure_to_datastore(self, tmp_path):
        mock_settings = _make_mock_settings(tmp_path, publish_to_data_store=True)

        fake_file = tmp_path / "plot.png"
        fake_file.write_text("data")
        mock_handler = MagicMock(spec=IALiRTQuicklookPathHandler)
        mock_handler.content_date = None

        output_file = tmp_path / "output.png"
        output_handler = MagicMock(spec=IALiRTQuicklookPathHandler)
        output_handler.content_date = None

        mock_datastore = MagicMock()
        mock_datastore.add_file.return_value = (output_file, output_handler)

        with (
            patch(
                "imap_mag.cli.plot.plot_ialirt.AppSettings", return_value=mock_settings
            ),
            patch("imap_mag.cli.plot.plot_ialirt.initialiseLoggingForCommand"),
            patch(
                "imap_mag.cli.plot.plot_ialirt.fetch_ialirt_files_for_work",
                return_value=[fake_file],
            ),
            patch(
                "imap_mag.cli.plot.plot_ialirt.fetch_ialirt_hk_files_for_work",
                return_value=[],
            ),
            patch(
                "imap_mag.cli.plot.plot_ialirt.plot_ialirt_files",
                return_value={fake_file: mock_handler},
            ),
            patch(
                "imap_mag.cli.plot.plot_ialirt.DatastoreFileManager.CreateByMode",
                return_value=mock_datastore,
            ),
        ):
            result = plot_ialirt(save_mode=SaveMode.LocalAndDatabase)

        assert output_file in result
        mock_datastore.add_file.assert_called()

    def test_adds_latest_copy_when_content_date_is_today(self, tmp_path):
        mock_settings = _make_mock_settings(tmp_path, publish_to_data_store=True)
        today = datetime(2025, 10, 21, 0, 0, 0)

        fake_file = tmp_path / "plot.png"
        fake_file.write_text("data")
        mock_handler = MagicMock(spec=IALiRTQuicklookPathHandler)
        mock_handler.content_date = today
        mock_handler.root_folder = "ialirt"
        mock_handler.get_plot_type.return_value = "quicklook"

        output_file = tmp_path / "output.png"
        output_handler = MagicMock(spec=IALiRTQuicklookPathHandler)
        output_handler.content_date = today
        output_handler.root_folder = "ialirt"
        output_handler.get_plot_type.return_value = "quicklook"

        mock_datastore = MagicMock()
        mock_datastore.add_file.return_value = (output_file, output_handler)

        with (
            patch(
                "imap_mag.cli.plot.plot_ialirt.AppSettings", return_value=mock_settings
            ),
            patch("imap_mag.cli.plot.plot_ialirt.initialiseLoggingForCommand"),
            patch(
                "imap_mag.cli.plot.plot_ialirt.fetch_ialirt_files_for_work",
                return_value=[fake_file],
            ),
            patch(
                "imap_mag.cli.plot.plot_ialirt.fetch_ialirt_hk_files_for_work",
                return_value=[],
            ),
            patch(
                "imap_mag.cli.plot.plot_ialirt.plot_ialirt_files",
                return_value={fake_file: mock_handler},
            ),
            patch(
                "imap_mag.cli.plot.plot_ialirt.DatastoreFileManager.CreateByMode",
                return_value=mock_datastore,
            ),
            patch(
                "imap_mag.cli.plot.plot_ialirt.DatetimeProvider.today",
                return_value=today,
            ),
        ):
            result = plot_ialirt(save_mode=SaveMode.LocalAndDatabase)

        assert mock_datastore.add_file.call_count == 2

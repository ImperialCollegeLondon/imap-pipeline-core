"""Tests for fetch NOAA CLI command."""

from pathlib import Path
from unittest.mock import MagicMock, patch

from imap_mag.cli.fetch.noaa import _create_fetch_noaa, _publish_files, fetch_noaa
from imap_mag.config import FetchMode


def test_returns_fetch_noaa_built_from_all_components() -> None:
    # Set up.
    mock_settings = MagicMock()
    mock_work_folder = Path("/tmp/work")
    mock_settings.setup_work_folder_for_command.return_value = mock_work_folder

    with (
        patch("imap_mag.cli.fetch.noaa.NOAARTSWApiClient") as mock_client_cls,
        patch("imap_mag.cli.fetch.noaa.FileFinder") as mock_finder_cls,
        patch("imap_mag.cli.fetch.noaa.FetchNOAA") as mock_fetch_cls,
        patch("imap_mag.cli.fetch.noaa.initialiseLoggingForCommand"),
    ):
        result = _create_fetch_noaa(mock_settings)

    # Verify - FetchNOAA assembled from (api_client, work_folder, file_finder).
    mock_fetch_cls.assert_called_once_with(
        mock_client_cls.return_value,
        mock_work_folder,
        mock_finder_cls.return_value,
    )
    assert result is mock_fetch_cls.return_value


class TestFetchNOAACommand:
    """Tests for the fetch_noaa CLI command."""

    def test_returns_empty_when_no_data_downloaded(
        self, dynamic_work_folder, clean_datastore
    ) -> None:
        # Set up.
        mock_fetch = MagicMock()
        mock_fetch.download_csv.return_value = {}

        with (
            patch("imap_mag.cli.fetch.noaa.NOAARTSWApiClient"),
            patch("imap_mag.cli.fetch.noaa.FetchNOAA", return_value=mock_fetch),
            patch("imap_mag.cli.fetch.noaa.initialiseLoggingForCommand"),
        ):
            result = fetch_noaa(spacecraft="SOLAR1", instrument="mag")

        assert result == {}

    def test_calls_download_csv_with_spacecraft_and_instrument(
        self, dynamic_work_folder, clean_datastore
    ) -> None:
        # Set up.
        mock_fetch = MagicMock()
        mock_fetch.download_csv.return_value = {}

        with (
            patch("imap_mag.cli.fetch.noaa.NOAARTSWApiClient"),
            patch("imap_mag.cli.fetch.noaa.FetchNOAA", return_value=mock_fetch),
            patch("imap_mag.cli.fetch.noaa.initialiseLoggingForCommand"),
        ):
            fetch_noaa(spacecraft="ACE", instrument="plasma")

        mock_fetch.download_csv.assert_called_once_with(
            spacecraft="ACE", instrument="plasma"
        )


class TestPublishFiles:
    """Tests for the _publish_files helper."""

    def test_returns_downloaded_files_unchanged_when_publish_disabled(self) -> None:
        # Set up.
        mock_settings = MagicMock()
        mock_settings.fetch_solar1_ace.publish_to_data_store = False
        downloaded = {Path("/tmp/file.csv"): MagicMock()}

        # Exercise.
        result = _publish_files(mock_settings, downloaded, FetchMode.DownloadOnly)  # type: ignore

        # Verify - files returned as-is, datastore never touched.
        assert result is downloaded

    def test_publishes_each_file_and_returns_datastore_paths(self) -> None:
        # Set up.
        mock_settings = MagicMock()
        mock_settings.fetch_solar1_ace.publish_to_data_store = True

        input_path = Path("/tmp/input.csv")
        input_handler = MagicMock()
        output_path = Path("/datastore/output.csv")
        output_handler = MagicMock()

        mock_manager = MagicMock()
        mock_manager.add_file.return_value = (output_path, output_handler)

        with patch(
            "imap_mag.cli.fetch.noaa.DatastoreFileManager.CreateByMode",
            return_value=mock_manager,
        ) as mock_create:
            result = _publish_files(
                mock_settings,
                {input_path: input_handler},
                FetchMode.DownloadOnly,
            )

        # Verify - CreateByMode called without database; result built from add_file.
        mock_create.assert_called_once_with(mock_settings, use_database=False)
        mock_manager.add_file.assert_called_once_with(input_path, input_handler)
        assert result == {output_path: output_handler}

    def test_uses_database_when_download_and_update_progress_mode(self) -> None:
        # Set up.
        mock_settings = MagicMock()
        mock_settings.fetch_solar1_ace.publish_to_data_store = True

        mock_manager = MagicMock()
        mock_manager.add_file.return_value = (MagicMock(), MagicMock())

        with patch(
            "imap_mag.cli.fetch.noaa.DatastoreFileManager.CreateByMode",
            return_value=mock_manager,
        ) as mock_create:
            _publish_files(
                mock_settings,
                {Path("/tmp/file.csv"): MagicMock()},
                FetchMode.DownloadAndUpdateProgress,
            )

        # Verify - database flag set when progress tracking is requested.
        mock_create.assert_called_once_with(mock_settings, use_database=True)

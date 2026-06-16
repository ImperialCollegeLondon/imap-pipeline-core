"""Tests for DownloadSpinTableFilesStage."""

import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from imap_mag.data_pipelines import FetchByDatesRunParameters
from imap_mag.data_pipelines.DownloadSpinTableFilesStage import (
    DownloadSpinTableFilesStage,
)
from imap_mag.data_pipelines.Record import Record


class TestDownloadSpinTableFilesStageProcess:
    def _make_stage(self, tmp_path, client=None, database=None):
        mock_settings = MagicMock()
        mock_settings.fetch_spice = MagicMock()
        mock_settings.fetch_spice.publish_to_data_store = False
        mock_settings.setup_work_folder_for_command.return_value = tmp_path

        mock_client = client or MagicMock()

        stage = DownloadSpinTableFilesStage(
            client=mock_client,
            settings=mock_settings,
            database=database,
        )
        stage._run_parameters = FetchByDatesRunParameters(
            start_date=datetime(2025, 1, 1), end_date=datetime(2025, 1, 31)
        )
        stage._next_stage = AsyncMock()
        stage._index = 0
        return stage, mock_client

    def test_raises_when_item_has_no_start_date(self, tmp_path):
        stage, _ = self._make_stage(tmp_path)
        item = Record(start_date=None, end_date=None)
        with pytest.raises(ValueError, match="start_date and end_date"):
            asyncio.run(stage.process(item, {}))

    def test_returns_early_when_no_spin_files_found(self, tmp_path):
        stage, mock_client = self._make_stage(tmp_path)
        mock_client.spin_table_query.return_value = []

        item = Record(start_date=datetime(2025, 1, 1), end_date=datetime(2025, 1, 31))
        asyncio.run(stage.process(item, {}))

        stage._next_stage.process.assert_not_called()

    def test_skips_files_without_file_path(self, tmp_path):
        stage, mock_client = self._make_stage(tmp_path)
        mock_client.spin_table_query.return_value = [{"ingestion_date": "2025-01-15"}]

        item = Record(start_date=datetime(2025, 1, 1), end_date=datetime(2025, 1, 31))
        asyncio.run(stage.process(item, {}))

        stage._next_stage.process.assert_not_called()

    def test_skips_files_with_old_ingestion_date(self, tmp_path):
        stage, mock_client = self._make_stage(tmp_path)
        mock_client.spin_table_query.return_value = [
            {
                "file_path": "imap_spin_table_20250101_v001.csv",
                "ingestion_date": "2024-12-31T00:00:00",
            }
        ]

        item = Record(start_date=datetime(2025, 1, 1), end_date=datetime(2025, 1, 31))
        asyncio.run(stage.process(item, {}))

        mock_client.download.assert_not_called()

    def test_skips_empty_downloaded_files(self, tmp_path):
        stage, mock_client = self._make_stage(tmp_path)

        empty_file = tmp_path / "empty.csv"
        empty_file.write_bytes(b"")
        mock_client.spin_table_query.return_value = [
            {
                "file_path": "imap_spin_table_20250115_v001.csv",
                "ingestion_date": "2025-01-16T00:00:00",
            }
        ]
        mock_client.download.return_value = empty_file

        item = Record(start_date=datetime(2025, 1, 1), end_date=datetime(2025, 1, 31))
        asyncio.run(stage.process(item, {}))

        stage._next_stage.process.assert_not_called()

    def test_processes_valid_spin_table_file(self, tmp_path):
        stage, mock_client = self._make_stage(tmp_path)

        csv_file = tmp_path / "imap_spin_table_20250115_v001.csv"
        csv_file.write_bytes(b"data,here")

        mock_client.spin_table_query.return_value = [
            {
                "file_path": "imap_spin_table_20250115_v001.csv",
                "ingestion_date": "2025-01-16T00:00:00",
                "start_date": "20250115",
                "end_date": "20250115",
                "version": "v001",
            }
        ]
        mock_client.download.return_value = csv_file

        mock_handler = MagicMock()
        mock_handler.get_content_date_for_indexing.return_value = datetime(2025, 1, 15)
        mock_handler.add_metadata = MagicMock()

        with patch(
            "imap_mag.data_pipelines.DownloadSpinTableFilesStage.SpinTablePathHandler.from_filename",
            return_value=mock_handler,
        ):
            item = Record(
                start_date=datetime(2025, 1, 1), end_date=datetime(2025, 1, 31)
            )
            asyncio.run(stage.process(item, {}))

        stage._next_stage.process.assert_called_once()

    def test_skips_file_when_handler_cannot_parse(self, tmp_path):
        stage, mock_client = self._make_stage(tmp_path)

        csv_file = tmp_path / "unknown_format_file.csv"
        csv_file.write_bytes(b"data")

        mock_client.spin_table_query.return_value = [
            {
                "file_path": "unknown_format_file.csv",
                "ingestion_date": "2025-01-16T00:00:00",
            }
        ]
        mock_client.download.return_value = csv_file

        with patch(
            "imap_mag.data_pipelines.DownloadSpinTableFilesStage.SpinTablePathHandler.from_filename",
            return_value=None,
        ):
            item = Record(
                start_date=datetime(2025, 1, 1), end_date=datetime(2025, 1, 31)
            )
            asyncio.run(stage.process(item, {}))

        stage._next_stage.process.assert_not_called()

    def test_publishes_to_datastore_when_configured(self, tmp_path):
        mock_settings = MagicMock()
        mock_settings.fetch_spice = MagicMock()
        mock_settings.fetch_spice.publish_to_data_store = True
        mock_settings.setup_work_folder_for_command.return_value = tmp_path

        mock_client = MagicMock()
        csv_file = tmp_path / "imap_spin_table_20250115_v001.csv"
        csv_file.write_bytes(b"data")

        mock_client.spin_table_query.return_value = [
            {
                "file_path": "imap_spin_table_20250115_v001.csv",
                "ingestion_date": "2025-01-16T00:00:00",
                "start_date": "20250115",
                "end_date": "20250115",
                "version": "v001",
            }
        ]
        mock_client.download.return_value = csv_file

        mock_handler = MagicMock()
        mock_handler.get_content_date_for_indexing.return_value = datetime(2025, 1, 15)
        mock_handler.add_metadata = MagicMock()

        mock_manager = MagicMock()
        mock_manager.add_file.return_value = (csv_file, mock_handler)

        stage = DownloadSpinTableFilesStage(
            client=mock_client, settings=mock_settings, database=None
        )
        stage._run_parameters = FetchByDatesRunParameters(
            start_date=datetime(2025, 1, 1), end_date=datetime(2025, 1, 31)
        )
        stage._next_stage = AsyncMock()
        stage._index = 0

        with (
            patch(
                "imap_mag.data_pipelines.DownloadSpinTableFilesStage.SpinTablePathHandler.from_filename",
                return_value=mock_handler,
            ),
            patch(
                "imap_mag.data_pipelines.DownloadSpinTableFilesStage.DatastoreFileManager.CreateByMode",
                return_value=mock_manager,
            ),
        ):
            item = Record(
                start_date=datetime(2025, 1, 1), end_date=datetime(2025, 1, 31)
            )
            asyncio.run(stage.process(item, {}))

        mock_manager.add_file.assert_called_once()

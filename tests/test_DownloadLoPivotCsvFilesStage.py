"""Tests for DownloadLoPivotCsvFilesStage."""

import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from imap_mag.data_pipelines import FetchByDatesRunParameters
from imap_mag.data_pipelines.DownloadLoPivotCsvFilesStage import (
    DownloadLoPivotCsvFilesStage,
)
from imap_mag.data_pipelines.Record import Record


class TestDownloadLoPivotCsvFilesStageProcess:
    def _make_stage(self, tmp_path, client=None):
        mock_settings = MagicMock()
        mock_settings.fetch_webtcad = MagicMock()
        mock_settings.fetch_webtcad.api = MagicMock()
        mock_settings.fetch_webtcad.api.system_id = "IMAP"
        mock_settings.setup_work_folder_for_command.return_value = tmp_path

        mock_client = client or MagicMock()
        stage = DownloadLoPivotCsvFilesStage(client=mock_client, settings=mock_settings)
        stage._run_parameters = FetchByDatesRunParameters(
            start_date=datetime(2025, 1, 1), end_date=datetime(2025, 1, 1)
        )
        stage._next_stage = AsyncMock()
        stage._index = 0
        stage.work_folder = tmp_path
        return stage, mock_client

    def test_raises_when_item_has_no_start_date(self, tmp_path):
        stage, _ = self._make_stage(tmp_path)
        item = Record(start_date=None, end_date=None)
        with pytest.raises(ValueError, match="start_date and end_date"):
            asyncio.run(stage.process(item, {}))

    def test_raises_when_csv_content_empty(self, tmp_path):
        stage, mock_client = self._make_stage(tmp_path)
        mock_client.download_imap_lo_pivot_platform_angle_to_csv_file.return_value = ""

        item = Record(start_date=datetime(2025, 1, 1), end_date=datetime(2025, 1, 1))
        with pytest.raises(RuntimeError, match="empty CSV content"):
            asyncio.run(stage.process(item, {}))

    def test_skips_day_when_csv_has_only_header(self, tmp_path):
        stage, mock_client = self._make_stage(tmp_path)
        mock_client.download_imap_lo_pivot_platform_angle_to_csv_file.return_value = (
            "timestamp,angle"
        )

        item = Record(start_date=datetime(2025, 1, 1), end_date=datetime(2025, 1, 1))
        asyncio.run(stage.process(item, {}))

        stage._next_stage.process.assert_not_called()

    def test_publishes_file_when_data_present(self, tmp_path):
        stage, mock_client = self._make_stage(tmp_path)
        mock_client.download_imap_lo_pivot_platform_angle_to_csv_file.return_value = (
            "timestamp,angle\n2025-01-01T00:00:00,45.0"
        )

        item = Record(start_date=datetime(2025, 1, 1), end_date=datetime(2025, 1, 1))
        asyncio.run(stage.process(item, {}))

        stage._next_stage.process.assert_called_once()

    def test_iterates_over_date_range(self, tmp_path):
        stage, mock_client = self._make_stage(tmp_path)
        mock_client.download_imap_lo_pivot_platform_angle_to_csv_file.return_value = (
            "header_only"
        )

        item = Record(start_date=datetime(2025, 1, 1), end_date=datetime(2025, 1, 3))
        asyncio.run(stage.process(item, {}))

        assert (
            mock_client.download_imap_lo_pivot_platform_angle_to_csv_file.call_count
            == 3
        )

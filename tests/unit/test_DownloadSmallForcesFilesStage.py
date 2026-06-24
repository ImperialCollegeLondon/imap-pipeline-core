"""Tests for DownloadSmallForcesFilesStage."""

import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from imap_mag.data_pipelines import PROGRESS_DATE_CONTEXT_KEY
from imap_mag.data_pipelines.DownloadSmallForcesFilesStage import (
    DownloadSmallForcesFilesStage,
)
from imap_mag.data_pipelines.Record import Record
from imap_mag.data_pipelines.RunParameters import FetchByDatesRunParameters


class TestDownloadSmallForcesFilesStage:
    def _make_stage(
        self,
        tmp_path,
        client=None,
        database=None,
        publish_to_data_store=False,
    ):
        mock_settings = MagicMock()
        mock_settings.fetch_spice = MagicMock()
        mock_settings.fetch_spice.publish_to_data_store = publish_to_data_store
        mock_settings.setup_work_folder_for_command.return_value = tmp_path

        mock_client = client or MagicMock()

        stage = DownloadSmallForcesFilesStage(
            client=mock_client,
            settings=mock_settings,
            database=database,
        )
        stage._run_parameters = FetchByDatesRunParameters(
            start_date=datetime(2026, 1, 1), end_date=datetime(2026, 1, 31)
        )
        stage._next_stage = AsyncMock()
        stage._index = 0
        return stage, mock_client

    def _valid_file_meta(self, ingestion_date="2026-01-16T00:00:00"):
        return {
            "file_path": "imap/spice/activities/imap_2026_036_2026_037_hist_01.sff",
            "ingestion_date": ingestion_date,
            "start_date": "2026-02-05",
            "version": "1",
        }

    # ------------------------------------------------------------------
    # Input validation
    # ------------------------------------------------------------------

    def test_raises_when_item_has_no_start_date(self, tmp_path):
        stage, _ = self._make_stage(tmp_path)
        item = Record(start_date=None, end_date=None)
        with pytest.raises(ValueError, match="start_date and end_date"):
            asyncio.run(stage.process(item, {}))

    def test_raises_when_item_has_no_end_date(self, tmp_path):
        stage, _ = self._make_stage(tmp_path)
        item = Record(start_date=datetime(2026, 1, 1), end_date=None)
        with pytest.raises(ValueError, match="start_date and end_date"):
            asyncio.run(stage.process(item, {}))

    # ------------------------------------------------------------------
    # Empty query results
    # ------------------------------------------------------------------

    def test_returns_early_when_no_small_forces_files(self, tmp_path):
        stage, mock_client = self._make_stage(tmp_path)
        mock_client.small_forces_query.return_value = []

        item = Record(start_date=datetime(2026, 1, 1), end_date=datetime(2026, 1, 31))
        asyncio.run(stage.process(item, {}))

        stage._next_stage.process.assert_not_called()

    # ------------------------------------------------------------------
    # File-level skipping
    # ------------------------------------------------------------------

    def test_skips_files_without_file_path(self, tmp_path):
        stage, mock_client = self._make_stage(tmp_path)
        mock_client.small_forces_query.return_value = [
            {"ingestion_date": "2026-01-15T12:00:00"}
        ]

        item = Record(start_date=datetime(2026, 1, 1), end_date=datetime(2026, 1, 31))
        asyncio.run(stage.process(item, {}))

        mock_client.download.assert_not_called()
        stage._next_stage.process.assert_not_called()

    def test_skips_files_with_ingestion_date_equal_to_start_date(self, tmp_path):
        stage, mock_client = self._make_stage(tmp_path)
        mock_client.small_forces_query.return_value = [
            {
                "file_path": "imap/spice/activities/imap_2026_036_2026_037_hist_01.sff",
                "ingestion_date": "2026-01-01T00:00:00",  # equal to start_date
            }
        ]

        item = Record(start_date=datetime(2026, 1, 1), end_date=datetime(2026, 1, 31))
        asyncio.run(stage.process(item, {}))

        mock_client.download.assert_not_called()

    def test_skips_files_with_ingestion_date_before_start_date(self, tmp_path):
        stage, mock_client = self._make_stage(tmp_path)
        mock_client.small_forces_query.return_value = [
            {
                "file_path": "imap/spice/activities/imap_2026_036_2026_037_hist_01.sff",
                "ingestion_date": "2025-12-31T23:59:59",  # before start_date
            }
        ]

        item = Record(start_date=datetime(2026, 1, 1), end_date=datetime(2026, 1, 31))
        asyncio.run(stage.process(item, {}))

        mock_client.download.assert_not_called()

    def test_skips_empty_downloaded_files(self, tmp_path):
        stage, mock_client = self._make_stage(tmp_path)

        empty_file = tmp_path / "imap_2026_036_2026_037_hist_01.sff"
        empty_file.write_bytes(b"")
        mock_client.small_forces_query.return_value = [self._valid_file_meta()]
        mock_client.download.return_value = empty_file

        item = Record(start_date=datetime(2026, 1, 1), end_date=datetime(2026, 1, 31))
        asyncio.run(stage.process(item, {}))

        stage._next_stage.process.assert_not_called()

    def test_skips_file_when_handler_cannot_parse_filename(self, tmp_path):
        stage, mock_client = self._make_stage(tmp_path)

        unknown_file = tmp_path / "unknown_format.txt"
        unknown_file.write_bytes(b"data")
        mock_client.small_forces_query.return_value = [
            {
                "file_path": "imap/spice/activities/unknown_format.txt",
                "ingestion_date": "2026-01-16T00:00:00",
            }
        ]
        mock_client.download.return_value = unknown_file

        item = Record(start_date=datetime(2026, 1, 1), end_date=datetime(2026, 1, 31))
        asyncio.run(stage.process(item, {}))

        stage._next_stage.process.assert_not_called()

    # ------------------------------------------------------------------
    # Happy path
    # ------------------------------------------------------------------

    def test_processes_valid_file_and_publishes_to_next_stage(self, tmp_path):
        stage, mock_client = self._make_stage(tmp_path)

        sff_file = tmp_path / "imap_2026_036_2026_037_hist_01.sff"
        sff_file.write_bytes(b"small forces data")
        mock_client.small_forces_query.return_value = [self._valid_file_meta()]
        mock_client.download.return_value = sff_file

        item = Record(start_date=datetime(2026, 1, 1), end_date=datetime(2026, 1, 31))
        asyncio.run(stage.process(item, {}))

        stage._next_stage.process.assert_called_once()

    def test_queries_api_with_correct_date_range(self, tmp_path):
        stage, mock_client = self._make_stage(tmp_path)
        mock_client.small_forces_query.return_value = []

        start = datetime(2026, 2, 1)
        end = datetime(2026, 2, 28)
        item = Record(start_date=start, end_date=end)
        asyncio.run(stage.process(item, {}))

        mock_client.small_forces_query.assert_called_once_with(
            start_ingest_date=start.date(),
            end_ingest_date=end.date(),
        )

    # ------------------------------------------------------------------
    # Context / progress tracking
    # ------------------------------------------------------------------

    def test_updates_progress_context_key_with_ingestion_date(self, tmp_path):
        stage, mock_client = self._make_stage(tmp_path)

        sff_file = tmp_path / "imap_2026_036_2026_037_hist_01.sff"
        sff_file.write_bytes(b"data")
        mock_client.small_forces_query.return_value = [
            self._valid_file_meta(ingestion_date="2026-01-16T12:00:00")
        ]
        mock_client.download.return_value = sff_file

        context: dict = {}
        item = Record(start_date=datetime(2026, 1, 1), end_date=datetime(2026, 1, 31))
        asyncio.run(stage.process(item, context))

        assert PROGRESS_DATE_CONTEXT_KEY in context
        assert context[PROGRESS_DATE_CONTEXT_KEY] == datetime(2026, 1, 16, 12, 0, 0)

    def test_does_not_update_progress_when_no_ingestion_date(self, tmp_path):
        stage, mock_client = self._make_stage(tmp_path)

        sff_file = tmp_path / "imap_2026_036_2026_037_hist_01.sff"
        sff_file.write_bytes(b"data")
        mock_client.small_forces_query.return_value = [
            {
                "file_path": "imap/spice/activities/imap_2026_036_2026_037_hist_01.sff",
                # No ingestion_date key
                "start_date": "2026-02-05",
                "version": "1",
            }
        ]
        mock_client.download.return_value = sff_file

        context: dict = {}
        item = Record(start_date=datetime(2026, 1, 1), end_date=datetime(2026, 1, 31))
        asyncio.run(stage.process(item, context))

        assert PROGRESS_DATE_CONTEXT_KEY not in context

    # ------------------------------------------------------------------
    # Content date fallback
    # ------------------------------------------------------------------

    def test_uses_ingestion_date_as_content_date_when_handler_returns_none(
        self, tmp_path
    ):
        stage, mock_client = self._make_stage(tmp_path)

        sff_file = tmp_path / "imap_2026_036_2026_037_hist_01.sff"
        sff_file.write_bytes(b"data")
        mock_client.small_forces_query.return_value = [
            self._valid_file_meta(ingestion_date="2026-01-16T12:00:00")
        ]
        mock_client.download.return_value = sff_file

        mock_handler = MagicMock()
        mock_handler.get_content_date_for_indexing.return_value = None
        mock_handler.add_metadata = MagicMock()

        with patch(
            "imap_mag.data_pipelines.DownloadSmallForcesFilesStage.SmallForcesPathHandler.from_filename",
            return_value=mock_handler,
        ):
            item = Record(
                start_date=datetime(2026, 1, 1), end_date=datetime(2026, 1, 31)
            )
            asyncio.run(stage.process(item, {}))

        # Should still publish, falling back to ingestion_date as content_date
        stage._next_stage.process.assert_called_once()
        published = stage._next_stage.process.call_args[0][0]
        assert published.content_date == datetime(2026, 1, 16, 12, 0, 0)

    # ------------------------------------------------------------------
    # Datastore publishing
    # ------------------------------------------------------------------

    def test_publishes_to_datastore_when_configured(self, tmp_path):
        stage, mock_client = self._make_stage(tmp_path, publish_to_data_store=True)

        sff_file = tmp_path / "imap_2026_036_2026_037_hist_01.sff"
        sff_file.write_bytes(b"data")
        mock_client.small_forces_query.return_value = [self._valid_file_meta()]
        mock_client.download.return_value = sff_file

        mock_handler = MagicMock()
        mock_handler.get_content_date_for_indexing.return_value = datetime(2026, 2, 5)
        mock_handler.add_metadata = MagicMock()

        mock_manager = MagicMock()
        mock_manager.add_file.return_value = (sff_file, mock_handler)

        with (
            patch(
                "imap_mag.data_pipelines.DownloadSmallForcesFilesStage.SmallForcesPathHandler.from_filename",
                return_value=mock_handler,
            ),
            patch(
                "imap_mag.data_pipelines.DownloadSmallForcesFilesStage.DatastoreFileManager.CreateByMode",
                return_value=mock_manager,
            ),
        ):
            item = Record(
                start_date=datetime(2026, 1, 1), end_date=datetime(2026, 1, 31)
            )
            asyncio.run(stage.process(item, {}))

        mock_manager.add_file.assert_called_once()
        stage._next_stage.process.assert_called_once()

    def test_does_not_publish_to_datastore_when_not_configured(self, tmp_path):
        stage, mock_client = self._make_stage(tmp_path, publish_to_data_store=False)

        sff_file = tmp_path / "imap_2026_036_2026_037_hist_01.sff"
        sff_file.write_bytes(b"data")
        mock_client.small_forces_query.return_value = [self._valid_file_meta()]
        mock_client.download.return_value = sff_file

        mock_handler = MagicMock()
        mock_handler.get_content_date_for_indexing.return_value = datetime(2026, 2, 5)
        mock_handler.add_metadata = MagicMock()

        with (
            patch(
                "imap_mag.data_pipelines.DownloadSmallForcesFilesStage.SmallForcesPathHandler.from_filename",
                return_value=mock_handler,
            ),
            patch(
                "imap_mag.data_pipelines.DownloadSmallForcesFilesStage.DatastoreFileManager.CreateByMode",
            ) as mock_create,
        ):
            item = Record(
                start_date=datetime(2026, 1, 1), end_date=datetime(2026, 1, 31)
            )
            asyncio.run(stage.process(item, {}))

        mock_create.assert_not_called()

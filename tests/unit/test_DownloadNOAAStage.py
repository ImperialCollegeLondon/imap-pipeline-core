"""Tests for DownloadNOAAStage."""

import asyncio
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

from imap_mag.data_pipelines import PROGRESS_DATE_CONTEXT_KEY
from imap_mag.data_pipelines.DownloadNOAAStage import DownloadNOAAStage
from imap_mag.data_pipelines.Record import FileRecord, Record
from imap_mag.download.FetchNOAA import FetchNOAA


class TestDownloadNOAAStage:
    def _make_stage(
        self,
        spacecraft: str = "SOLAR1",
        instrument: str = "mag",
        fetcher: FetchNOAA | None = None,
    ) -> tuple[DownloadNOAAStage, MagicMock]:
        mock_fetcher = fetcher or MagicMock(spec=FetchNOAA)
        stage = DownloadNOAAStage(
            spacecraft=spacecraft,
            instrument=instrument,
            fetcher=mock_fetcher,
        )
        stage._next_stage = AsyncMock()
        stage._index = 0
        return stage, mock_fetcher

    def test_does_not_publish_when_no_data_downloaded(self) -> None:
        # Set up.
        stage, mock_fetcher = self._make_stage()
        mock_fetcher.download_csv.return_value = {}

        # Exercise.
        asyncio.run(stage.process(Record("init"), {}))

        # Verify.
        stage._next_stage.process.assert_not_called()

    def test_calls_download_csv_with_spacecraft_and_instrument(self) -> None:
        # Set up.
        stage, mock_fetcher = self._make_stage(spacecraft="ACE", instrument="plasma")
        mock_fetcher.download_csv.return_value = {}

        # Exercise.
        asyncio.run(stage.process(Record("init"), {}))

        # Verify.
        mock_fetcher.download_csv.assert_called_once_with(
            spacecraft="ACE", instrument="plasma"
        )

    def test_publishes_file_record_and_sets_progress_date_for_each_downloaded_file(
        self, tmp_path: Path
    ) -> None:
        # Set up.
        stage, mock_fetcher = self._make_stage()

        content_date = datetime(2026, 7, 21, 9, 0, 0)
        mock_handler = MagicMock()
        mock_handler.content_date = content_date

        csv_file = tmp_path / "SOLAR1_mag_noaa_20260721.csv"
        csv_file.write_bytes(b"time_tag,bx_gsm\n2026-07-21T09:00:00,1.0")

        mock_fetcher.download_csv.return_value = {csv_file: mock_handler}

        context: dict = {}

        # Exercise.
        asyncio.run(stage.process(Record("init"), context))

        # Verify - one FileRecord published; progress date set from path handler.
        stage._next_stage.process.assert_called_once()
        published: FileRecord = stage._next_stage.process.call_args[0][0]
        assert published.file_path == csv_file
        assert published.content_date == content_date
        assert context[PROGRESS_DATE_CONTEXT_KEY] == content_date

    def test_publishes_one_record_per_downloaded_day(self, tmp_path: Path) -> None:
        # Set up - two files from two different days.
        stage, mock_fetcher = self._make_stage()

        date1 = datetime(2026, 7, 21, 9, 0, 0)
        date2 = datetime(2026, 7, 22, 8, 0, 0)

        handler1, handler2 = MagicMock(), MagicMock()
        handler1.content_date = date1
        handler2.content_date = date2

        file1 = tmp_path / "SOLAR1_mag_noaa_20260721.csv"
        file2 = tmp_path / "SOLAR1_mag_noaa_20260722.csv"
        file1.write_bytes(b"data")
        file2.write_bytes(b"data")

        mock_fetcher.download_csv.return_value = {file1: handler1, file2: handler2}

        context: dict = {}

        # Exercise.
        asyncio.run(stage.process(Record("init"), context))

        # Verify - one publish call per file; progress date reflects the last one set.
        assert stage._next_stage.process.call_count == 2
        published_records = [
            call[0][0] for call in stage._next_stage.process.call_args_list
        ]
        assert {r.file_path for r in published_records} == {file1, file2}

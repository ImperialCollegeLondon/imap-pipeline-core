"""Regression tests for the full I-ALiRT download pipeline handling multi-day ranges.

These exercise GetProcessingDatesStage -> DownloadIALiRTStage ->
PublishFileToDatastoreStage -> SaveProcessingDatesStage together, which is how
IALiRTPipeline wires them up. They reproduce a real production bug: when
force_redownload is used with a multi-day date range, the pipeline crashed
partway through (comparing an offset-aware date against the offset-naive
progress_timestamp from the database) after publishing only the first day's
file, silently dropping every subsequent day.
"""

import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import pytest

import imap_mag.data_pipelines as dp
from imap_db.model import WorkflowProgress
from imap_mag.client.IALiRTApiClient import IALiRTApiClient
from imap_mag.data_pipelines.DownloadIALiRTStage import DownloadIALiRTStage
from imap_mag.data_pipelines.GetProcessingDatesStage import GetProcessingDatesStage
from imap_mag.data_pipelines.PublishFileToDatastoreStage import (
    PublishFileToDatastoreStage,
)
from imap_mag.data_pipelines.Record import FileRecord, Record
from imap_mag.data_pipelines.SaveProcessingDatesStage import SaveProcessingDatesStage
from imap_mag.download.FetchIALiRT import FetchIALiRT
from imap_mag.io import FileFinder
from imap_mag.util.DatetimeProvider import DatetimeProvider
from tests.util.miscellaneous import temp_datastore  # noqa: F401

IALIRT_PACKET_DEFINITION = (
    Path(__file__).parent.parent.parent / "src" / "imap_mag" / "packet_def"
)


class _CollectorStage(dp.Stage):
    """Collects all records that make it through the whole pipeline."""

    __test__ = False

    def __init__(self):
        super().__init__()
        self.received: list[Record] = []

    async def process(self, item: Record, context: dict, **kwargs):
        self.received.append(item)
        await self.publish_next(item, context, **kwargs)


def _make_mock_database(progress_timestamp: datetime | None) -> mock.MagicMock:
    """A Database stand-in whose workflow progress is a real (naive) WorkflowProgress."""
    workflow_progress = WorkflowProgress(item_name="SWAPI_MAG_IALIRT")
    if progress_timestamp is not None:
        workflow_progress.progress_timestamp = progress_timestamp

    mock_db = mock.MagicMock()
    mock_db.get_workflow_progress.return_value = workflow_progress
    return mock_db


@pytest.mark.asyncio
async def test_force_redownload_multiday_range_publishes_every_day(
    temp_datastore,  # noqa: F811
):
    """A multi-day force_redownload request must publish a file for every day
    downloaded, not just the first one, and must not crash midway through."""

    mock_ialirt_client = mock.create_autospec(IALiRTApiClient, spec_set=True)
    mock_ialirt_client.get_all_by_dates.side_effect = lambda **_: [
        {"time_utc": "2026-07-24T16:00:00", "data": [1, 2, 3]},
        {"time_utc": "2026-07-25T10:00:00", "data": [4, 5, 6]},
    ]

    work_folder = Path(tempfile.mkdtemp())
    fetch_ialirt = FetchIALiRT(
        data_access=mock_ialirt_client,
        work_folder=work_folder,
        datastore_finder=FileFinder(temp_datastore),
        packet_definition=IALIRT_PACKET_DEFINITION,
    )

    # Simulate a prior successful run that already got as far as 2026-07-26 23:00,
    # matching the "partially up to date" state seen in production - this is the
    # naive datetime read back from the database.
    mock_db = _make_mock_database(progress_timestamp=datetime(2026, 7, 26, 23, 0, 0))

    dp_instance = DatetimeProvider(fixed_now=datetime(2026, 7, 27, 14, 9, 0))

    collector = _CollectorStage()
    pipeline = dp.Pipeline()
    pipeline.initial_context = {"progress_item_name": "SWAPI_MAG_IALIRT"}
    pipeline.build(
        run_parameters=dp.FetchByDatesRunParameters(
            # Timezone-aware, as the run parameters would be when submitted
            # from the UI in a UK summer-time browser session.
            start_date=datetime(
                2026, 7, 24, 15, 7, 0, tzinfo=timezone(timedelta(hours=1))
            ),
            end_date=datetime(
                2026, 7, 27, 0, 0, 0, tzinfo=timezone(timedelta(hours=1))
            ),
            force_redownload=True,
        ),
        stages=[
            GetProcessingDatesStage(database=mock_db, datetime_provider=dp_instance),
            DownloadIALiRTStage(
                instrument="swapi", fetcher=fetch_ialirt, datetime_provider=dp_instance
            ),
            PublishFileToDatastoreStage(enabled=True, database=None),
            SaveProcessingDatesStage(database=mock_db),
            collector,
        ],
    )

    # This used to raise TypeError: can't compare offset-naive and offset-aware
    # datetimes, aborting the loop in DownloadIALiRTStage after the first file.
    await pipeline.run()

    assert len(collector.received) == 2
    file_records = [
        record for record in collector.received if isinstance(record, FileRecord)
    ]
    assert len(file_records) == 2

    published_filenames = sorted(record.file_path.name for record in file_records)
    assert published_filenames == [
        "imap_ialirt_swapi_20260724.csv",
        "imap_ialirt_swapi_20260725.csv",
    ]

    for record in file_records:
        assert record.file_path.exists()

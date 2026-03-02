"""Tests for GetProcessingDatesStage."""

from datetime import datetime

import pytest

import imap_mag.data_pipelines as dp
from imap_mag.data_pipelines.GetProcessingDatesStage import (
    DateResolutionMode,
    GetProcessingDatesStage,
)
from imap_mag.data_pipelines.Record import Record
from tests.util.miscellaneous import (
    BEGINNING_OF_IMAP,
    END_OF_TODAY,
    TODAY,
    YESTERDAY,
)


class _CollectorStage(dp.Stage):
    """Collects all records passed through for assertion in tests."""

    __test__ = False

    def __init__(self):
        super().__init__()
        self.received: list[Record] = []

    async def process(self, item: Record, context: dict, **kwargs):
        self.received.append(item)
        await self.publish_next(item, context, **kwargs)


def _build_pipeline_with_stage(
    stage: GetProcessingDatesStage,
    run_parameters: dp.PipelineRunParameters,
    progress_item_name: str = "test-item",
) -> tuple[dp.Pipeline, _CollectorStage]:
    """Build a minimal pipeline around the given GetProcessingDatesStage."""
    collector = _CollectorStage()
    pipeline = dp.Pipeline()
    pipeline.initial_context = {"progress_item_name": progress_item_name}
    pipeline.build(
        run_parameters=run_parameters,
        stages=[stage, collector],
    )
    return pipeline, collector


# ---- Progress item name validation ----


@pytest.mark.asyncio
async def test_raises_when_progress_item_name_missing_from_context(
    mock_datetime_provider,
):
    """Stage raises ValueError if progress_item_name is not in context."""
    stage = GetProcessingDatesStage(database=None)
    collector = _CollectorStage()
    pipeline = dp.Pipeline()
    # Do NOT set progress_item_name in initial_context
    pipeline.build(
        run_parameters=dp.AutomaticRunParameters(),
        stages=[stage, collector],
    )

    with pytest.raises(
        ValueError, match="progress_item_name must be provided in context"
    ):
        await pipeline.run()


# ---- AutomaticRunParameters (no database) ----


@pytest.mark.asyncio
async def test_automatic_run_publishes_dates_from_beginning_of_imap(
    mock_datetime_provider,
):
    """With AutomaticRunParameters and no database, stage uses IMAP start to end-of-today."""
    stage = GetProcessingDatesStage(database=None)
    pipeline, collector = _build_pipeline_with_stage(
        stage,
        run_parameters=dp.AutomaticRunParameters(),
    )

    await pipeline.run()

    assert len(collector.received) == 1
    record = collector.received[0]
    assert record.start_date == BEGINNING_OF_IMAP
    assert record.end_date == END_OF_TODAY


# ---- FetchByDatesRunParameters with explicit dates ----


@pytest.mark.asyncio
async def test_fetch_by_dates_publishes_explicit_dates(mock_datetime_provider):
    """With FetchByDatesRunParameters, stage uses the provided start and end dates."""
    explicit_start = datetime(2026, 1, 1, 10, 0, 0)
    explicit_end = datetime(2026, 1, 15, 18, 0, 0)

    stage = GetProcessingDatesStage(database=None)
    pipeline, collector = _build_pipeline_with_stage(
        stage,
        run_parameters=dp.FetchByDatesRunParameters(
            start_date=explicit_start,
            end_date=explicit_end,
        ),
    )

    await pipeline.run()

    assert len(collector.received) == 1
    record = collector.received[0]
    assert record.start_date == explicit_start
    assert record.end_date == explicit_end


# ---- FetchByDatesRunParameters with only start_date set ----


@pytest.mark.asyncio
async def test_fetch_by_dates_with_start_date_only_uses_end_of_today(
    mock_datetime_provider,
):
    """When only start_date is set (no end_date), stage defaults end to end-of-today."""
    explicit_start = datetime(2026, 1, 1, 10, 0, 0)

    stage = GetProcessingDatesStage(database=None)
    pipeline, collector = _build_pipeline_with_stage(
        stage,
        run_parameters=dp.FetchByDatesRunParameters(
            start_date=explicit_start,
            end_date=None,
        ),
    )

    await pipeline.run()

    assert len(collector.received) == 1
    record = collector.received[0]
    assert record.start_date == explicit_start
    assert record.end_date == END_OF_TODAY


# ---- force_redownload ----


@pytest.mark.asyncio
async def test_force_redownload_overrides_database_progress(mock_datetime_provider):
    """force_redownload=True forces the requested dates regardless of download state."""
    explicit_start = datetime(2026, 1, 1)
    explicit_end = datetime(2026, 1, 5)

    stage = GetProcessingDatesStage(database=None)
    pipeline, collector = _build_pipeline_with_stage(
        stage,
        run_parameters=dp.FetchByDatesRunParameters(
            start_date=explicit_start,
            end_date=explicit_end,
            force_redownload=True,
        ),
    )

    await pipeline.run()

    assert len(collector.received) == 1
    record = collector.received[0]
    assert record.start_date == explicit_start
    assert record.end_date == explicit_end


# ---- DateResolutionMode.DATE_ONLY ----


@pytest.mark.asyncio
async def test_date_only_mode_strips_time_components(mock_datetime_provider):
    """DATE_ONLY mode truncates start to midnight and extends end to end-of-day."""
    explicit_start = datetime(2026, 1, 10, 14, 30, 0)
    explicit_end = datetime(2026, 1, 20, 9, 15, 0)

    stage = GetProcessingDatesStage(
        database=None,
        date_resolution_mode=DateResolutionMode.DATE_ONLY,
    )
    pipeline, collector = _build_pipeline_with_stage(
        stage,
        run_parameters=dp.FetchByDatesRunParameters(
            start_date=explicit_start,
            end_date=explicit_end,
        ),
    )

    await pipeline.run()

    assert len(collector.received) == 1
    record = collector.received[0]
    assert record.start_date == datetime(2026, 1, 10, 0, 0, 0, 0)
    assert record.end_date == datetime(2026, 1, 20, 23, 59, 59, 999999)


# ---- Nothing to process (dates resolve to None) ----


@pytest.mark.asyncio
async def test_nothing_published_when_download_dates_already_up_to_date(
    mock_datetime_provider,
):
    """Stage publishes nothing when the database says all data is already current."""
    from unittest.mock import MagicMock

    from imap_db.model import WorkflowProgress

    # Mock database that reports progress is past END_OF_TODAY (already fully up-to-date)
    mock_db = MagicMock()
    future_progress = WorkflowProgress(item_name="test-item")
    future_progress.progress_timestamp = END_OF_TODAY  # already up-to-date

    mock_db.get_workflow_progress.return_value = future_progress

    stage = GetProcessingDatesStage(database=mock_db)
    collector = _CollectorStage()
    pipeline = dp.Pipeline()
    pipeline.initial_context = {"progress_item_name": "test-item"}
    pipeline.build(
        run_parameters=dp.FetchByDatesRunParameters(
            start_date=YESTERDAY,
            end_date=TODAY,
        ),
        stages=[stage, collector],
    )

    await pipeline.run()

    # Nothing should have been published since data is already current
    assert len(collector.received) == 0


# ---- Progress mode NEVER_UPDATE_PROGRESS ----


@pytest.mark.asyncio
async def test_never_update_mode_does_not_update_progress_when_nothing_to_do(
    mock_datetime_provider,
):
    """NEVER_UPDATE_PROGRESS mode skips progress update even on empty result."""
    from unittest.mock import MagicMock

    from imap_db.model import WorkflowProgress

    mock_db = MagicMock()
    future_progress = WorkflowProgress(item_name="test-item")
    future_progress.progress_timestamp = END_OF_TODAY  # already up-to-date
    mock_db.get_workflow_progress.return_value = future_progress

    stage = GetProcessingDatesStage(database=mock_db)
    collector = _CollectorStage()
    pipeline = dp.Pipeline()
    pipeline.initial_context = {"progress_item_name": "test-item"}
    pipeline.build(
        run_parameters=dp.FetchByDatesRunParameters(
            start_date=YESTERDAY,
            end_date=TODAY,
            progress_mode=dp.ProgressUpdateMode.NEVER_UPDATE_PROGRESS,
        ),
        stages=[stage, collector],
    )

    await pipeline.run()

    assert len(collector.received) == 0
    # update_last_checked_timestamp should NOT have been called
    future_progress_mock = mock_db.get_workflow_progress.return_value
    assert future_progress_mock.last_checked_date is None

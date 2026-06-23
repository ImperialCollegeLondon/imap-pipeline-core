"""Tests for PublishFileToDatastoreStage."""

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import imap_mag.data_pipelines as dp
from imap_mag.data_pipelines.PublishFileToDatastoreStage import (
    PublishFileToDatastoreStage,
)
from imap_mag.data_pipelines.Record import FileRecord, Record


class _CollectorStage(dp.Stage):
    """Collects all records passed through for assertion."""

    __test__ = False

    def __init__(self):
        super().__init__()
        self.received: list[Record] = []

    async def process(self, item: Record, context: dict, **kwargs):
        self.received.append(item)
        await self.publish_next(item, context, **kwargs)


def _build_pipeline_with_publish_stage(
    publish_stage: PublishFileToDatastoreStage,
) -> tuple[dp.Pipeline, _CollectorStage]:
    """Build a minimal pipeline that runs the publish stage against a source record."""
    collector = _CollectorStage()

    class _SourceStage(dp.SourceStage):
        def __init__(self, item: Record):
            super().__init__()
            self._item = item

        async def start(self, context: dict, **kwargs):
            await self.publish_next(self._item, context, **kwargs)

    pipeline = dp.Pipeline()
    pipeline.build(
        run_parameters=dp.AutomaticRunParameters(),
        stages=[_SourceStage(Record(value="source")), publish_stage, collector],
    )
    return pipeline, collector


# ---- Stage disabled (pass-through) ----


@pytest.mark.asyncio
async def test_disabled_stage_passes_item_through(clean_datastore):
    """When disabled, stage publishes item to next stage unchanged."""
    stage = PublishFileToDatastoreStage(enabled=False, database=None)
    collector = _CollectorStage()

    class _SourceStage(dp.SourceStage):
        async def start(self, context: dict, **kwargs):
            await self.publish_next(Record(value="passthrough"), context, **kwargs)

    pipeline = dp.Pipeline()
    pipeline.build(
        run_parameters=dp.AutomaticRunParameters(),
        stages=[_SourceStage(), stage, collector],
    )
    await pipeline.run()

    assert len(collector.received) == 1
    assert collector.received[0].value == "passthrough"


# ---- Item has no file_path attribute ----


@pytest.mark.asyncio
async def test_raises_when_item_has_no_file_path(clean_datastore):
    """Stage raises ValueError when the record has no file_path attribute."""
    stage = PublishFileToDatastoreStage(enabled=True, database=None)

    class _SourceStage(dp.SourceStage):
        async def start(self, context: dict, **kwargs):
            await self.publish_next(Record(value="no-path"), context, **kwargs)

    pipeline = dp.Pipeline()
    pipeline.build(
        run_parameters=dp.AutomaticRunParameters(),
        stages=[_SourceStage(), stage, _CollectorStage()],
    )

    with pytest.raises(ValueError, match="file_path attribute"):
        await pipeline.run()


# ---- File does not exist ----


@pytest.mark.asyncio
async def test_raises_when_file_does_not_exist(clean_datastore):
    """Stage raises ValueError when file_path points to a non-existent file."""
    stage = PublishFileToDatastoreStage(enabled=True, database=None)
    nonexistent = Path("/tmp/nonexistent_file_xyz_12345.csv")

    class _SourceStage(dp.SourceStage):
        async def start(self, context: dict, **kwargs):
            record = Record(
                value="missing-file",
                file_path=nonexistent,
            )
            await self.publish_next(record, context, **kwargs)

    pipeline = dp.Pipeline()
    pipeline.build(
        run_parameters=dp.AutomaticRunParameters(),
        stages=[_SourceStage(), stage, _CollectorStage()],
    )

    with pytest.raises(ValueError, match="does not exist"):
        await pipeline.run()


# ---- File exists but no content date can be determined ----


@pytest.mark.asyncio
async def test_raises_when_content_date_cannot_be_determined(clean_datastore, tmp_path):
    """Stage raises ValueError when no content date can be determined for the file."""
    # Create a real file with an unknown naming convention
    unknown_file = tmp_path / "unknown_format_file.csv"
    unknown_file.write_text("data")

    mock_handler = MagicMock()
    mock_handler.get_content_date_for_indexing.return_value = None

    stage = PublishFileToDatastoreStage(enabled=True, database=None)

    class _SourceStage(dp.SourceStage):
        async def start(self, context: dict, **kwargs):
            record = Record(value="unknown", file_path=unknown_file)
            await self.publish_next(record, context, **kwargs)

    pipeline = dp.Pipeline()
    pipeline.build(
        run_parameters=dp.AutomaticRunParameters(),
        stages=[_SourceStage(), stage, _CollectorStage()],
    )

    with patch(
        "imap_mag.data_pipelines.PublishFileToDatastoreStage.FilePathHandlerSelector.find_by_path",
        return_value=mock_handler,
    ):
        with pytest.raises(ValueError, match="content date"):
            await pipeline.run()


# ---- Successful publish ----


@pytest.mark.asyncio
async def test_publishes_file_to_datastore_successfully(clean_datastore, tmp_path):
    """Stage successfully copies file to datastore and publishes a FileRecord."""
    # Use a valid HK filename so FilePathHandlerSelector recognises it
    hk_filename = "imap_mag_l1_hsk-pw_20251102_v001.csv"
    source_file = tmp_path / hk_filename
    source_file.write_text("time,value\n2025-11-02T00:00:00,1.0\n")

    stage = PublishFileToDatastoreStage(enabled=True, database=None)

    collector = _CollectorStage()

    class _SourceStage(dp.SourceStage):
        async def start(self, context: dict, **kwargs):
            record = Record(value=hk_filename, file_path=source_file)
            await self.publish_next(record, context, **kwargs)

    pipeline = dp.Pipeline()
    pipeline.build(
        run_parameters=dp.AutomaticRunParameters(),
        stages=[_SourceStage(), stage, collector],
    )
    await pipeline.run()

    assert len(collector.received) == 1
    result = collector.received[0]
    assert isinstance(result, FileRecord)
    assert result.file_path.exists()
    assert result.content_date == datetime(2025, 11, 2, 0, 0, 0)

"""Unit tests for GetDatabaseItemStage."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

from imap_db.model import FileAnalysis
from imap_mag.data_pipelines.GetDatabaseItemStage import GetDatabaseItemStage
from imap_mag.data_pipelines.Record import Record


def _make_stage(database) -> GetDatabaseItemStage[FileAnalysis]:
    stage = GetDatabaseItemStage[FileAnalysis](
        database=database,
        item_type=FileAnalysis,
        key_column=FileAnalysis.file_id,
        get_key=lambda record: record.file_id,
        set_item=lambda record, item: setattr(record, "file_analysis", item),
    )
    stage._next_stage = AsyncMock()
    stage._index = 0
    return stage


def _process(stage, record: Record):
    asyncio.run(stage.process(record, context={}))
    return stage._next_stage.process.call_args[0][0]


def test_attaches_existing_item_to_record():
    existing = MagicMock(spec=FileAnalysis)
    db = MagicMock()
    db.get_item_by_column.return_value = existing

    published = _process(_make_stage(db), Record(file_id=7))

    db.get_item_by_column.assert_called_once_with(FileAnalysis, FileAnalysis.file_id, 7)
    assert published.file_analysis is existing
    assert published.file_id == 7


def test_attaches_none_when_no_existing_item():
    db = MagicMock()
    db.get_item_by_column.return_value = None

    published = _process(_make_stage(db), Record(file_id=7))

    assert published.file_analysis is None


def test_attaches_none_without_database():
    published = _process(_make_stage(None), Record(file_id=7))

    assert published.file_analysis is None


def test_does_not_query_when_key_is_none():
    db = MagicMock()

    published = _process(_make_stage(db), Record(file_id=None, other="x"))

    db.get_item_by_column.assert_not_called()
    assert published.file_analysis is None

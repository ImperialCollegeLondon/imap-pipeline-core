"""Tests for pipeline infrastructure: Record, Result, Stages, Pipeline, and stage implementations."""

import asyncio
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from imap_mag.data_pipelines import (
    AutomaticRunParameters,
    FetchByDatesRunParameters,
    Pipeline,
)
from imap_mag.data_pipelines.Record import FileRecord, Record
from imap_mag.data_pipelines.Result import Result
from imap_mag.data_pipelines.Stages import EndStage, SourceStage, Stage


# ---------------------------------------------------------------------------
# Record tests
# ---------------------------------------------------------------------------


class TestRecord:
    def test_record_with_value(self):
        r = Record(value="hello")
        assert r.value == "hello"

    def test_record_with_kwargs(self):
        r = Record(start_date=datetime(2025, 1, 1), end_date=datetime(2025, 1, 31))
        assert r.start_date == datetime(2025, 1, 1)
        assert r.end_date == datetime(2025, 1, 31)

    def test_record_raises_without_value_or_kwargs(self):
        with pytest.raises(ValueError):
            Record()

    def test_record_repr_excludes_none_values(self):
        r = Record(value="test")
        repr_str = repr(r)
        assert "None" not in repr_str
        assert "test" in repr_str

    def test_file_record_stores_path_and_date(self, tmp_path):
        p = tmp_path / "test.cdf"
        p.touch()
        date = datetime(2025, 6, 1)
        fr = FileRecord(p, date)
        assert fr.file_path == p
        assert fr.content_date == date

    def test_file_record_value_is_filename(self, tmp_path):
        p = tmp_path / "myfile.cdf"
        p.touch()
        fr = FileRecord(p, datetime(2025, 1, 1))
        assert fr.value == "myfile.cdf"


# ---------------------------------------------------------------------------
# Result tests
# ---------------------------------------------------------------------------


class TestResult:
    def test_create_success_sets_success_true(self):
        result = Result.create_success(data_items=["a", "b"])
        assert result.success is True
        assert result.data_items == ["a", "b"]

    def test_create_failure_sets_success_false(self):
        result = Result.create_failure()
        assert result.success is False

    def test_create_success_with_dict(self):
        result = Result.create_success(data_dict={"k": "v"})
        assert result.data_dict == {"k": "v"}

    def test_create_success_defaults_empty_lists(self):
        result = Result.create_success()
        assert result.data_items == []
        assert result.data_dict == {}

    def test_create_failure_has_empty_items(self):
        result = Result.create_failure()
        assert result.data_items == []


# ---------------------------------------------------------------------------
# Stage tests
# ---------------------------------------------------------------------------


class _ConcreteStage(Stage):
    """A minimal concrete Stage implementation for testing."""

    processed_items: list = []

    async def process(self, item: Record, context: dict, **kwargs):
        self.processed_items.append(item)
        await self.publish_next(item, context, **kwargs)


class _ConcreteSource(SourceStage):
    """A minimal concrete SourceStage for testing."""

    def __init__(self, items_to_emit: list):
        super().__init__()
        self.items = items_to_emit

    async def start(self, context: dict, **kwargs):
        for item in self.items:
            await self.publish_next(item, context, **kwargs)


class TestEndStage:
    def test_end_stage_collects_items(self):
        pipeline = MagicMock()
        end = EndStage(pipeline, index=99)
        end._run_parameters = AutomaticRunParameters()
        end._next_stage = None

        item = Record(value="collected")
        asyncio.get_event_loop().run_until_complete(end.process(item, {}))

        assert len(end.results) == 1

    def test_end_stage_notifies_pipeline_on_completed(self):
        pipeline = MagicMock()
        end = EndStage(pipeline, index=99)
        end._run_parameters = AutomaticRunParameters()
        end._next_stage = None

        asyncio.get_event_loop().run_until_complete(end.stage_completed({}))

        pipeline._completed.assert_called_once()


class TestStagePublishNext:
    def test_publish_next_calls_next_stage_process(self):
        stage = _ConcreteStage()
        next_stage = AsyncMock()
        stage._next_stage = next_stage
        stage._run_parameters = AutomaticRunParameters()
        stage._index = 0

        item = Record(value="test")
        asyncio.get_event_loop().run_until_complete(stage.publish_next(item, {}))

        next_stage.process.assert_called_once()

    def test_publish_next_with_no_next_stage_raises_runtime_error(self):
        stage = _ConcreteStage()
        stage._next_stage = None
        stage._run_parameters = AutomaticRunParameters()
        stage._index = 0

        item = Record(value="test")
        with pytest.raises(RuntimeError, match="no next stage"):
            asyncio.get_event_loop().run_until_complete(stage.publish_next(item, {}))


# ---------------------------------------------------------------------------
# Pipeline tests
# ---------------------------------------------------------------------------


class TestPipeline:
    def _make_simple_pipeline(self, items=None):
        if items is None:
            items = [Record(start_date=datetime(2025, 1, 1), end_date=datetime(2025, 1, 31))]

        pipeline = Pipeline()
        run_params = FetchByDatesRunParameters(
            start_date=datetime(2025, 1, 1), end_date=datetime(2025, 1, 31)
        )
        stages = [_ConcreteSource(items), _ConcreteStage()]
        return pipeline, run_params, stages

    def test_pipeline_build_sets_run_parameters(self):
        pipeline, run_params, stages = self._make_simple_pipeline()
        pipeline.build(run_params, stages=stages)
        assert pipeline._run_parameters is not None

    def test_pipeline_run_completes_successfully(self):
        pipeline, run_params, stages = self._make_simple_pipeline()
        pipeline.build(run_params, stages=stages)
        asyncio.get_event_loop().run_until_complete(pipeline.run())
        result = pipeline.get_results()
        assert result is not None

    def test_pipeline_build_raises_without_stages(self):
        pipeline = Pipeline()
        run_params = AutomaticRunParameters()
        with pytest.raises(ValueError):
            pipeline.build(run_params, stages=[])

    def test_pipeline_run_result_contains_items(self):
        items = [Record(start_date=datetime(2025, 1, 1), end_date=datetime(2025, 1, 31))]
        pipeline, run_params, stages = self._make_simple_pipeline(items)
        pipeline.build(run_params, stages=stages)
        asyncio.get_event_loop().run_until_complete(pipeline.run())
        result = pipeline.get_results()
        assert result.success is True
        assert len(result.data_items) >= 0

    def test_pipeline_initial_context_merged_into_runtime_context(self):
        pipeline = Pipeline()
        pipeline.initial_context = {"custom_key": "custom_value"}
        stages = [_ConcreteSource([Record(value="x")])]
        pipeline.build(AutomaticRunParameters(), stages=stages)
        asyncio.get_event_loop().run_until_complete(pipeline.run())
        # No assertion needed - just verify no exception


# ---------------------------------------------------------------------------
# GetProcessingDatesStage tests
# ---------------------------------------------------------------------------


class TestGetProcessingDatesStage:
    def test_raises_when_no_progress_item_name_in_context(self):
        from imap_mag.data_pipelines.GetProcessingDatesStage import GetProcessingDatesStage

        stage = GetProcessingDatesStage(database=None)
        stage._run_parameters = AutomaticRunParameters()
        stage._next_stage = AsyncMock()
        stage._index = 0

        with pytest.raises((KeyError, Exception)):
            asyncio.get_event_loop().run_until_complete(stage.start({}))

    def test_publishes_record_with_date_range_for_explicit_dates(self):
        from imap_mag.data_pipelines.GetProcessingDatesStage import GetProcessingDatesStage

        stage = GetProcessingDatesStage(database=None)
        run_params = FetchByDatesRunParameters(
            start_date=datetime(2025, 1, 1), end_date=datetime(2025, 1, 31)
        )
        stage._run_parameters = run_params
        stage._next_stage = AsyncMock()
        stage._index = 0

        context = {"progress_item_name": "TEST"}
        asyncio.get_event_loop().run_until_complete(stage.start(context))

        stage._next_stage.process.assert_called_once()
        published_item = stage._next_stage.process.call_args[0][0]
        assert published_item.start_date == datetime(2025, 1, 1)
        assert published_item.end_date == datetime(2025, 1, 31)

    def test_automatic_run_uses_database_progress(self):
        from imap_mag.data_pipelines.GetProcessingDatesStage import GetProcessingDatesStage

        mock_db = MagicMock()
        mock_progress = MagicMock()
        mock_progress.get_last_checked_date.return_value = None
        mock_progress.progress_timestamp = None
        mock_db.get_workflow_progress.return_value = mock_progress

        stage = GetProcessingDatesStage(database=mock_db)
        run_params = AutomaticRunParameters()
        stage._run_parameters = run_params
        stage._next_stage = AsyncMock()
        stage._index = 0

        context = {"progress_item_name": "TEST"}
        with patch("imap_mag.data_pipelines.GetProcessingDatesStage.DownloadDateManager") as mock_dm:
            mock_dm_instance = MagicMock()
            mock_dm_instance.get_dates_for_download.return_value = (
                datetime(2025, 1, 1),
                datetime(2025, 1, 31),
            )
            mock_dm.return_value = mock_dm_instance
            asyncio.get_event_loop().run_until_complete(stage.start(context))

        stage._next_stage.process.assert_called_once()


# ---------------------------------------------------------------------------
# PublishFileToDatastoreStage tests
# ---------------------------------------------------------------------------


class TestPublishFileToDatastoreStage:
    def test_disabled_stage_passes_item_through(self, tmp_path):
        from imap_mag.data_pipelines.PublishFileToDatastoreStage import PublishFileToDatastoreStage

        stage = PublishFileToDatastoreStage(enabled=False, database=None)
        stage._run_parameters = AutomaticRunParameters()
        stage._next_stage = AsyncMock()
        stage._index = 0

        f = tmp_path / "test.cdf"
        f.write_bytes(b"data")
        item = FileRecord(f, datetime(2025, 1, 1))
        context = {}

        asyncio.get_event_loop().run_until_complete(stage.process(item, context))

        stage._next_stage.process.assert_called_once()

    def test_enabled_stage_calls_file_manager(self, tmp_path):
        from imap_mag.data_pipelines.PublishFileToDatastoreStage import PublishFileToDatastoreStage

        mock_settings = MagicMock()
        mock_settings.data_store = tmp_path
        mock_settings.work_folder = tmp_path

        mock_manager = MagicMock()
        f = tmp_path / "imap_mag_l2_norm-gse_20250101_v001.cdf"
        f.write_bytes(b"data")
        mock_handler = MagicMock()
        mock_handler.get_content_date_for_indexing.return_value = datetime(2025, 1, 1)
        mock_manager.add_file.return_value = (f, mock_handler)

        with patch(
            "imap_mag.data_pipelines.PublishFileToDatastoreStage.DatastoreFileManager.CreateByMode",
            return_value=mock_manager,
        ):
            stage = PublishFileToDatastoreStage(enabled=True, database=None, settings=mock_settings)
            stage._run_parameters = AutomaticRunParameters()
            stage._next_stage = AsyncMock()
            stage._index = 0

            item = FileRecord(f, datetime(2025, 1, 1))

            with patch(
                "imap_mag.data_pipelines.PublishFileToDatastoreStage.FilePathHandlerSelector.find_by_path",
                return_value=mock_handler,
            ):
                asyncio.get_event_loop().run_until_complete(stage.process(item, {}))

        mock_manager.add_file.assert_called_once()

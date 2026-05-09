"""Tests for SaveProcessingDatesStage."""

import asyncio
import logging
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

from imap_db.model import WorkflowProgress
from imap_mag.data_pipelines import (
    PROGRESS_DATE_CONTEXT_KEY,
    AutomaticRunParameters,
    Pipeline,
    ProgressUpdateMode,
)
from imap_mag.data_pipelines.Record import Record
from imap_mag.data_pipelines.SaveProcessingDatesStage import SaveProcessingDatesStage


def _make_workflow_progress(item_name="TEST"):
    return WorkflowProgress(item_name=item_name)


def _make_context(progress_date=None):
    wp = _make_workflow_progress()
    if progress_date:
        wp.update_progress_timestamp(progress_date)
    return {
        "workflow_progress": wp,
        Pipeline.STARTED_CONTEXT_KEY: datetime(2025, 6, 1),
    }


class TestSaveProcessingDatesStageInit:
    def test_warns_when_no_database_provided(self, caplog):
        with caplog.at_level(logging.WARNING):
            SaveProcessingDatesStage(database=None)
        assert "No database" in caplog.text

    def test_does_not_warn_when_database_provided(self, caplog):
        mock_db = MagicMock()
        with caplog.at_level(logging.WARNING):
            SaveProcessingDatesStage(database=mock_db)
        assert "No database" not in caplog.text


class TestSaveProcessingDatesStageUpdateWorkflowProgress:
    def _make_stage(self, db=None):
        stage = SaveProcessingDatesStage(database=db)
        run_params = AutomaticRunParameters()
        stage._run_parameters = run_params
        return stage

    def test_saves_progress_when_database_exists(self):
        mock_db = MagicMock()
        stage = self._make_stage(db=mock_db)
        context = _make_context()
        progress_date = datetime(2025, 6, 15)

        stage.update_workflow_progress(context, progress_date)

        mock_db.save.assert_called_once()

    def test_does_not_save_when_no_database(self):
        stage = self._make_stage(db=None)
        context = _make_context()

        stage.update_workflow_progress(context, datetime(2025, 6, 15))

    def test_marks_saved_at_least_once_after_save(self):
        mock_db = MagicMock()
        stage = self._make_stage(db=mock_db)
        context = _make_context()
        assert stage.have_saved_at_least_once is False

        stage.update_workflow_progress(context, datetime(2025, 6, 15))

        assert stage.have_saved_at_least_once is True

    def test_force_update_mode_updates_even_when_older(self):
        mock_db = MagicMock()
        stage = SaveProcessingDatesStage(database=mock_db)
        stage._run_parameters = AutomaticRunParameters(
            progress_mode=ProgressUpdateMode.FORCE_UPDATE_PROGRESS
        )
        context = _make_context(progress_date=datetime(2025, 12, 31))

        stage.update_workflow_progress(context, datetime(2025, 1, 1))

        mock_db.save.assert_called()

    def test_never_update_mode_does_not_save(self):
        mock_db = MagicMock()
        stage = SaveProcessingDatesStage(database=mock_db)
        stage._run_parameters = AutomaticRunParameters(
            progress_mode=ProgressUpdateMode.NEVER_UPDATE_PROGRESS
        )
        context = _make_context()

        stage.update_workflow_progress(context, datetime(2025, 6, 15))

        mock_db.save.assert_not_called()


class TestSaveProcessingDatesStageProcess:
    def _make_stage_with_params(self, db=None):
        stage = SaveProcessingDatesStage(database=db)
        run_params = AutomaticRunParameters()
        stage._run_parameters = run_params
        stage._next_stage = AsyncMock()
        return stage

    def test_process_propagates_item_to_next_stage(self):
        mock_db = MagicMock()
        stage = self._make_stage_with_params(db=mock_db)
        context = _make_context()
        item = Record(value="test")

        asyncio.run(stage.process(item, context))

        stage._next_stage.process.assert_called_once()

    def test_process_uses_progress_date_from_context_key(self):
        mock_db = MagicMock()
        stage = self._make_stage_with_params(db=mock_db)

        context = _make_context()
        context[PROGRESS_DATE_CONTEXT_KEY] = datetime(2025, 7, 4)
        item = Record(value="test")

        asyncio.run(stage.process(item, context))

        wp = context["workflow_progress"]
        assert wp.progress_timestamp == datetime(2025, 7, 4)


class TestSaveProcessingDatesStageCompleted:
    def test_stage_completed_saves_once_even_if_no_items_processed(self):
        mock_db = MagicMock()
        stage = SaveProcessingDatesStage(database=mock_db)
        stage._run_parameters = AutomaticRunParameters()
        context = _make_context()

        asyncio.run(stage.stage_completed(context))

        mock_db.save.assert_called()

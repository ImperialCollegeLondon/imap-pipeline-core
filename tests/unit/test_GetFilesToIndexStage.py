"""Unit tests for GetFilesToIndexStage."""

import asyncio
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from imap_db.model import File, FileAnalysis, WorkflowProgress
from imap_mag.data_pipelines import PROGRESS_DATE_CONTEXT_KEY
from imap_mag.data_pipelines.GetFilesToIndexStage import GetFilesToIndexStage
from imap_mag.data_pipelines.RunParameters import (
    AutomaticRunParameters,
    IndexByIdsRunParameters,
    IndexByPathOrModifiedDateRunParameters,
)


class TestGetFilesToIndexStage:
    def _make_stage(
        self,
        run_params=None,
        database=None,
        tmp_path=None,
        paths_to_match=None,
        max_files_per_run=None,
    ) -> GetFilesToIndexStage:
        mock_settings = MagicMock()
        mock_settings.data_store = tmp_path or Path("/nonexistent")
        mock_settings.file_analysis.paths_to_match = paths_to_match or []
        mock_settings.file_analysis.max_files_per_run = max_files_per_run

        stage = GetFilesToIndexStage(database=database, settings=mock_settings)
        stage._run_parameters = run_params or AutomaticRunParameters()
        stage._next_stage = AsyncMock()
        stage._index = 0
        return stage

    def _make_db(
        self,
        files=None,
        workflow_progress=None,
        file_analysis=None,
    ) -> MagicMock:
        db = MagicMock()
        db.get_workflow_progress.return_value = workflow_progress or WorkflowProgress(
            item_name="test"
        )
        db.get_files_by_ids.return_value = files or []
        db.get_active_files_by_path_or_modified_date.return_value = files or []
        db.get_file_analysis_by_file_id.return_value = file_analysis
        return db

    def _make_file(
        self,
        tmp_path: Path,
        rel_path: str,
        file_id: int = 1,
        last_modified_date: datetime | None = None,
        deletion_date: datetime | None = None,
    ) -> MagicMock:
        """Create a real file on disk and return a mock File ORM object."""
        full_path = tmp_path / rel_path
        full_path.parent.mkdir(parents=True, exist_ok=True)
        full_path.write_bytes(b"test")

        f = MagicMock(spec=File)
        f.id = file_id
        f.path = rel_path
        f.last_modified_date = last_modified_date or datetime(2026, 1, 1, tzinfo=UTC)
        f.deletion_date = deletion_date
        return f

    def _context(self, **extra):
        return {"progress_item_name": "test", **extra}

    # ------------------------------------------------------------------
    # Context validation
    # ------------------------------------------------------------------

    def test_raises_when_progress_item_name_missing(self, tmp_path):
        stage = self._make_stage(tmp_path=tmp_path)
        with pytest.raises(ValueError, match="progress_item_name"):
            asyncio.run(stage.start(context={}))

    # ------------------------------------------------------------------
    # No database provided
    # ------------------------------------------------------------------

    def test_no_database_ids_run_publishes_nothing(self):
        stage = self._make_stage(
            run_params=IndexByIdsRunParameters(file_ids=[1, 2]),
            database=None,
        )
        asyncio.run(stage.start(context=self._context()))
        stage._next_stage.process.assert_not_called()

    def test_no_database_paths_run_publishes_nothing(self):
        stage = self._make_stage(
            run_params=IndexByPathOrModifiedDateRunParameters(
                file_paths=["foo/**/*.cdf"]
            ),
            database=None,
        )
        asyncio.run(stage.start(context=self._context()))
        stage._next_stage.process.assert_not_called()

    def test_no_database_date_range_run_publishes_nothing(self):
        stage = self._make_stage(
            run_params=IndexByPathOrModifiedDateRunParameters(
                modified_after=datetime(2026, 1, 1, tzinfo=UTC)
            ),
            database=None,
        )
        asyncio.run(stage.start(context=self._context()))
        stage._next_stage.process.assert_not_called()

    def test_no_database_automatic_run_publishes_nothing(self):
        stage = self._make_stage(
            run_params=AutomaticRunParameters(),
            database=None,
        )
        asyncio.run(stage.start(context=self._context()))
        stage._next_stage.process.assert_not_called()

    # ------------------------------------------------------------------
    # File-on-disk checks
    # ------------------------------------------------------------------

    def test_skips_file_that_does_not_exist_on_disk(self, tmp_path):
        mock_file = MagicMock(spec=File)
        mock_file.id = 1
        mock_file.path = "does/not/exist.cdf"
        mock_file.last_modified_date = datetime(2026, 1, 1, tzinfo=UTC)
        mock_file.deletion_date = None

        db = self._make_db(files=[mock_file])
        stage = self._make_stage(
            run_params=IndexByIdsRunParameters(file_ids=[1]),
            database=db,
            tmp_path=tmp_path,
        )
        asyncio.run(stage.start(context=self._context()))

        stage._next_stage.process.assert_not_called()

    def test_publishes_record_when_file_exists_on_disk(self, tmp_path):
        mock_file = self._make_file(tmp_path, "science/test.cdf")
        db = self._make_db(files=[mock_file])
        stage = self._make_stage(
            run_params=IndexByIdsRunParameters(file_ids=[1]),
            database=db,
            tmp_path=tmp_path,
        )
        asyncio.run(stage.start(context=self._context()))

        stage._next_stage.process.assert_called_once()

    # ------------------------------------------------------------------
    # PROGRESS_DATE_CONTEXT_KEY tracking
    # ------------------------------------------------------------------

    def test_progress_key_is_set_to_latest_modification_date(self, tmp_path):
        early = datetime(2026, 1, 1, tzinfo=UTC)
        late = datetime(2026, 6, 1, tzinfo=UTC)
        f1 = self._make_file(tmp_path, "a/f1.cdf", file_id=1, last_modified_date=early)
        f2 = self._make_file(tmp_path, "b/f2.cdf", file_id=2, last_modified_date=late)

        db = self._make_db(files=[f1, f2])
        stage = self._make_stage(
            run_params=AutomaticRunParameters(),
            database=db,
            tmp_path=tmp_path,
        )
        context = self._context()
        asyncio.run(stage.start(context=context))

        assert context[PROGRESS_DATE_CONTEXT_KEY] == late

    def test_progress_key_uses_latest_even_if_files_out_of_order(self, tmp_path):
        early = datetime(2026, 1, 1, tzinfo=UTC)
        late = datetime(2026, 6, 1, tzinfo=UTC)
        # Give the later file a lower id so it comes first in the list
        f1 = self._make_file(tmp_path, "a/f1.cdf", file_id=1, last_modified_date=late)
        f2 = self._make_file(tmp_path, "b/f2.cdf", file_id=2, last_modified_date=early)

        db = self._make_db(files=[f1, f2])
        stage = self._make_stage(
            run_params=AutomaticRunParameters(),
            database=db,
            tmp_path=tmp_path,
        )
        context = self._context()
        asyncio.run(stage.start(context=context))

        assert context[PROGRESS_DATE_CONTEXT_KEY] == late

    # ------------------------------------------------------------------
    # Existing file analysis attached to record
    # ------------------------------------------------------------------

    def test_existing_file_analysis_is_attached_to_record(self, tmp_path):
        existing_fa = MagicMock(spec=FileAnalysis)
        db = self._make_db(file_analysis=existing_fa)
        mock_file = self._make_file(tmp_path, "a.cdf")
        db.get_files_by_ids.return_value = [mock_file]

        stage = self._make_stage(
            run_params=IndexByIdsRunParameters(file_ids=[1]),
            database=db,
            tmp_path=tmp_path,
        )
        asyncio.run(stage.start(context=self._context()))

        record = stage._next_stage.process.call_args[0][0]
        assert record.file_analysis is existing_fa

    def test_no_existing_file_analysis_attaches_none(self, tmp_path):
        db = self._make_db(file_analysis=None)
        mock_file = self._make_file(tmp_path, "a.cdf")
        db.get_files_by_ids.return_value = [mock_file]

        stage = self._make_stage(
            run_params=IndexByIdsRunParameters(file_ids=[1]),
            database=db,
            tmp_path=tmp_path,
        )
        asyncio.run(stage.start(context=self._context()))

        record = stage._next_stage.process.call_args[0][0]
        assert record.file_analysis is None

    # ------------------------------------------------------------------
    # _apply_paths_filter
    # ------------------------------------------------------------------

    def test_paths_filter_excludes_non_matching_files(self, tmp_path):
        f1 = self._make_file(tmp_path, "science/mag/f1.cdf", file_id=1)
        f2 = self._make_file(tmp_path, "hk/mag/f2.csv", file_id=2)
        db = self._make_db(files=[f1, f2])
        db.get_active_files_by_path_or_modified_date.return_value = [f1, f2]

        stage = self._make_stage(
            run_params=AutomaticRunParameters(),
            database=db,
            tmp_path=tmp_path,
            paths_to_match=["science/**/*.cdf"],
        )
        asyncio.run(stage.start(context=self._context()))

        assert stage._next_stage.process.call_count == 1
        published = stage._next_stage.process.call_args[0][0]
        assert published.file_id == 1

    def test_paths_filter_returns_all_when_no_patterns_configured(self, tmp_path):
        f1 = self._make_file(tmp_path, "a.cdf", file_id=1)
        f2 = self._make_file(tmp_path, "b.csv", file_id=2)
        db = self._make_db(files=[f1, f2])
        db.get_active_files_by_path_or_modified_date.return_value = [f1, f2]

        stage = self._make_stage(
            run_params=AutomaticRunParameters(),
            database=db,
            tmp_path=tmp_path,
            paths_to_match=[],
        )
        asyncio.run(stage.start(context=self._context()))

        assert stage._next_stage.process.call_count == 2

    def test_paths_filter_applied_to_automatic_run(self, tmp_path):
        f1 = self._make_file(tmp_path, "science/mag/a.cdf", file_id=1)
        f2 = self._make_file(tmp_path, "hk/mag/l1/b.csv", file_id=2)
        db = self._make_db()
        db.get_active_files_by_path_or_modified_date.return_value = [f1, f2]

        stage = self._make_stage(
            run_params=AutomaticRunParameters(),
            database=db,
            tmp_path=tmp_path,
            paths_to_match=["hk/**/*.csv"],
        )
        asyncio.run(stage.start(context=self._context()))

        assert stage._next_stage.process.call_count == 1
        published = stage._next_stage.process.call_args[0][0]
        assert published.file_id == 2

    # ------------------------------------------------------------------
    # Date range filtering
    # ------------------------------------------------------------------

    def test_date_range_passes_modified_bounds_to_database(self, tmp_path):
        f1 = self._make_file(tmp_path, "a.cdf", file_id=1)
        db = self._make_db(files=[f1])

        after = datetime(2026, 1, 1, tzinfo=UTC)
        before = datetime(2026, 1, 31, tzinfo=UTC)
        stage = self._make_stage(
            run_params=IndexByPathOrModifiedDateRunParameters(
                modified_after=after, modified_before=before
            ),
            database=db,
            tmp_path=tmp_path,
        )
        asyncio.run(stage.start(context=self._context()))

        db.get_active_files_by_path_or_modified_date.assert_called_once_with(
            [], after, before
        )
        assert self._published_ids(stage) == [1]

    def test_paths_and_dates_combined_passes_date_bounds_to_database(self, tmp_path):
        f1 = self._make_file(tmp_path, "a.cdf", file_id=1)
        db = self._make_db(files=[f1])
        after = datetime(2026, 2, 1, tzinfo=UTC)
        before = datetime(2026, 3, 1, tzinfo=UTC)

        stage = self._make_stage(
            run_params=IndexByPathOrModifiedDateRunParameters(
                file_paths=["*.cdf"],
                modified_after=after,
                modified_before=before,
            ),
            database=db,
            tmp_path=tmp_path,
        )
        asyncio.run(stage.start(context=self._context()))

        db.get_active_files_by_path_or_modified_date.assert_called_once_with(
            ["*.cdf"], after, before
        )
        assert stage._next_stage.process.call_count == 1
        assert stage._next_stage.process.call_args[0][0].file_id == 1

    def test_date_range_without_bounds_passes_no_bounds_to_database(self, tmp_path):
        db = self._make_db()
        db.get_active_files_by_path_or_modified_date.return_value = []

        stage = self._make_stage(
            run_params=IndexByPathOrModifiedDateRunParameters(modified_after=None),
            database=db,
            tmp_path=tmp_path,
        )
        asyncio.run(stage.start(context=self._context()))

        db.get_active_files_by_path_or_modified_date.assert_called_once_with(
            [], None, None
        )

    # ------------------------------------------------------------------
    # Empty IDs / paths lists
    # ------------------------------------------------------------------

    def test_empty_file_ids_list_publishes_nothing(self):
        db = self._make_db()
        stage = self._make_stage(
            run_params=IndexByIdsRunParameters(file_ids=[]),
            database=db,
        )
        asyncio.run(stage.start(context=self._context()))
        stage._next_stage.process.assert_not_called()

    def test_empty_file_paths_list_publishes_nothing(self):
        db = self._make_db()
        stage = self._make_stage(
            run_params=IndexByPathOrModifiedDateRunParameters(file_paths=[]),
            database=db,
        )
        asyncio.run(stage.start(context=self._context()))
        stage._next_stage.process.assert_not_called()

    # ------------------------------------------------------------------
    # Automatic run uses workflow progress timestamp
    # ------------------------------------------------------------------

    def test_automatic_run_passes_workflow_progress_timestamp_to_db(self, tmp_path):
        progress_ts = datetime(2026, 3, 1, tzinfo=UTC)
        wp = WorkflowProgress(item_name="test")
        wp.progress_timestamp = progress_ts
        db = self._make_db(workflow_progress=wp)
        db.get_active_files_by_path_or_modified_date.return_value = []

        stage = self._make_stage(
            run_params=AutomaticRunParameters(),
            database=db,
            tmp_path=tmp_path,
        )
        asyncio.run(stage.start(context=self._context()))

        db.get_active_files_by_path_or_modified_date.assert_called_once_with(
            [], progress_ts, None
        )

    def test_automatic_run_defaults_to_2010_when_no_progress(self, tmp_path):
        wp = WorkflowProgress(item_name="test")
        wp.progress_timestamp = None
        db = self._make_db(workflow_progress=wp)
        db.get_active_files_by_path_or_modified_date.return_value = []

        stage = self._make_stage(
            run_params=AutomaticRunParameters(),
            database=db,
            tmp_path=tmp_path,
        )
        asyncio.run(stage.start(context=self._context()))

        assert db.get_active_files_by_path_or_modified_date.call_args[0][1].year == 2010

    # ------------------------------------------------------------------
    # max_files_per_run
    # ------------------------------------------------------------------

    def _published_ids(self, stage) -> list[int]:
        return [
            call.args[0].file_id for call in stage._next_stage.process.call_args_list
        ]

    def test_automatic_run_limited_to_max_files_per_run(self, tmp_path):
        files = [
            self._make_file(
                tmp_path,
                f"hk/f{i}.csv",
                file_id=i,
                last_modified_date=datetime(2026, 1, i, tzinfo=UTC),
            )
            for i in range(1, 6)
        ]
        db = self._make_db(files=files)
        stage = self._make_stage(database=db, tmp_path=tmp_path, max_files_per_run=2)

        asyncio.run(stage.start(context=self._context()))

        assert self._published_ids(stage) == [1, 2]

    def test_max_files_per_run_keeps_files_sharing_cutoff_modified_date(self, tmp_path):
        """Files with the same modified date as the last file in the batch must not be
        split across runs, or progress would move past them and they'd be skipped."""
        same_date = datetime(2026, 1, 2, tzinfo=UTC)
        files = [
            self._make_file(
                tmp_path,
                "hk/a.csv",
                file_id=1,
                last_modified_date=datetime(2026, 1, 1, tzinfo=UTC),
            ),
            self._make_file(
                tmp_path, "hk/b.csv", file_id=2, last_modified_date=same_date
            ),
            self._make_file(
                tmp_path, "hk/c.csv", file_id=3, last_modified_date=same_date
            ),
            self._make_file(
                tmp_path,
                "hk/d.csv",
                file_id=4,
                last_modified_date=datetime(2026, 1, 3, tzinfo=UTC),
            ),
        ]
        db = self._make_db(files=files)
        stage = self._make_stage(database=db, tmp_path=tmp_path, max_files_per_run=2)

        asyncio.run(stage.start(context=self._context()))

        assert self._published_ids(stage) == [1, 2, 3]

    @pytest.mark.parametrize(
        "run_params",
        [
            IndexByPathOrModifiedDateRunParameters(
                modified_after=datetime(2025, 1, 1, tzinfo=UTC)
            ),
            IndexByPathOrModifiedDateRunParameters(file_paths=["hk/*.csv"]),
            IndexByIdsRunParameters(file_ids=[1, 2, 3]),
        ],
        ids=["date_range", "paths", "ids"],
    )
    def test_max_files_per_run_applied_to_manual_runs(self, tmp_path, run_params):
        files = [
            self._make_file(
                tmp_path,
                f"hk/f{i}.csv",
                file_id=i,
                last_modified_date=datetime(2026, 1, i, tzinfo=UTC),
            )
            for i in range(1, 4)
        ]
        db = self._make_db(files=files)
        stage = self._make_stage(
            run_params=run_params,
            database=db,
            tmp_path=tmp_path,
            max_files_per_run=1,
        )

        asyncio.run(stage.start(context=self._context()))

        assert self._published_ids(stage) == [1]

    @pytest.mark.parametrize(
        "run_params",
        [
            IndexByPathOrModifiedDateRunParameters(file_paths=["*"]),
            IndexByIdsRunParameters(file_ids=[1, 2]),
        ],
        ids=["paths", "ids"],
    )
    def test_paths_to_match_applied_to_manual_runs(self, tmp_path, run_params):
        f1 = self._make_file(tmp_path, "science/mag/f1.cdf", file_id=1)
        f2 = self._make_file(tmp_path, "hk/mag/f2.csv", file_id=2)
        db = self._make_db(files=[f1, f2])
        stage = self._make_stage(
            run_params=run_params,
            database=db,
            tmp_path=tmp_path,
            paths_to_match=["science/**/*.cdf"],
        )

        asyncio.run(stage.start(context=self._context()))

        assert self._published_ids(stage) == [1]

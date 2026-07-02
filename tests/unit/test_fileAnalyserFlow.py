"""Unit tests for _build_run_parameters in fileAnalyserFlow."""

from datetime import UTC, datetime

from imap_mag.data_pipelines import (
    AutomaticRunParameters,
    IndexByDateRangeRunParameters,
    IndexByFileNamesRunParameters,
    IndexByIdsRunParameters,
    ProgressUpdateMode,
)
from prefect_server.fileAnalyserFlow import _build_run_parameters


class TestBuildRunParameters:
    # ------------------------------------------------------------------
    # Return type selection
    # ------------------------------------------------------------------

    def test_returns_automatic_when_nothing_provided(self):
        result = _build_run_parameters(None, None, None, None)
        assert isinstance(result, AutomaticRunParameters)

    def test_returns_ids_params_when_files_provided(self):
        result = _build_run_parameters([1, 2, 3], None, None, None)
        assert isinstance(result, IndexByIdsRunParameters)
        assert result.file_ids == [1, 2, 3]

    def test_returns_paths_params_when_file_paths_provided(self):
        result = _build_run_parameters(
            None, ["science/**/*.cdf", "hk/**/*.csv"], None, None
        )
        assert isinstance(result, IndexByFileNamesRunParameters)
        assert result.file_paths == ["science/**/*.cdf", "hk/**/*.csv"]

    def test_returns_date_range_when_modified_after_provided(self):
        after = datetime(2026, 1, 1, tzinfo=UTC)
        result = _build_run_parameters(None, None, after, None)
        assert isinstance(result, IndexByDateRangeRunParameters)
        assert result.modified_after == after
        assert result.modified_before is None

    def test_returns_date_range_when_modified_before_provided(self):
        before = datetime(2026, 12, 31, tzinfo=UTC)
        result = _build_run_parameters(None, None, None, before)
        assert isinstance(result, IndexByDateRangeRunParameters)
        assert result.modified_after is None
        assert result.modified_before == before

    def test_returns_date_range_with_both_bounds(self):
        after = datetime(2026, 1, 1, tzinfo=UTC)
        before = datetime(2026, 12, 31, tzinfo=UTC)
        result = _build_run_parameters(None, None, after, before)
        assert isinstance(result, IndexByDateRangeRunParameters)
        assert result.modified_after == after
        assert result.modified_before == before

    # ------------------------------------------------------------------
    # Priority ordering: file IDs > file paths > date range > automatic
    # ------------------------------------------------------------------

    def test_ids_take_priority_over_file_paths(self):
        result = _build_run_parameters([1], ["foo/*.cdf"], None, None)
        assert isinstance(result, IndexByIdsRunParameters)

    def test_ids_take_priority_over_date_range(self):
        result = _build_run_parameters(
            [1], None, datetime(2026, 1, 1, tzinfo=UTC), None
        )
        assert isinstance(result, IndexByIdsRunParameters)

    def test_ids_take_priority_over_all(self):
        result = _build_run_parameters(
            [1],
            ["foo/*.cdf"],
            datetime(2026, 1, 1, tzinfo=UTC),
            datetime(2026, 12, 31, tzinfo=UTC),
        )
        assert isinstance(result, IndexByIdsRunParameters)

    def test_file_paths_take_priority_over_date_range(self):
        result = _build_run_parameters(
            None, ["foo/*.cdf"], datetime(2026, 1, 1, tzinfo=UTC), None
        )
        assert isinstance(result, IndexByFileNamesRunParameters)

    # ------------------------------------------------------------------
    # Progress mode
    # ------------------------------------------------------------------

    def test_manual_runs_use_never_update_progress(self):
        ids_result = _build_run_parameters([1], None, None, None)
        paths_result = _build_run_parameters(None, ["foo"], None, None)
        date_result = _build_run_parameters(
            None, None, datetime(2026, 1, 1, tzinfo=UTC), None
        )

        assert ids_result.progress_mode == ProgressUpdateMode.NEVER_UPDATE_PROGRESS
        assert paths_result.progress_mode == ProgressUpdateMode.NEVER_UPDATE_PROGRESS
        assert date_result.progress_mode == ProgressUpdateMode.NEVER_UPDATE_PROGRESS

    def test_automatic_uses_auto_update_progress_mode(self):
        result = _build_run_parameters(None, None, None, None)
        assert result.progress_mode == ProgressUpdateMode.AUTO_UPDATE_PROGRESS_IF_NEWER

    # ------------------------------------------------------------------
    # Edge cases
    # ------------------------------------------------------------------

    def test_empty_files_list_treated_as_falsy_returns_automatic(self):
        """An empty list is falsy in Python, so falls through to automatic."""
        result = _build_run_parameters([], None, None, None)
        assert isinstance(result, AutomaticRunParameters)

    def test_empty_file_paths_list_treated_as_falsy_returns_automatic(self):
        result = _build_run_parameters(None, [], None, None)
        assert isinstance(result, AutomaticRunParameters)

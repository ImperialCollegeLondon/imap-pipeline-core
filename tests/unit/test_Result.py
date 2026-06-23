"""Tests for the Result data class."""

from imap_mag.data_pipelines.Result import Result


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

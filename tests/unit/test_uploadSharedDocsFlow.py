"""Unit tests for uploadSharedDocsFlow module functions."""

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from prefect_server.uploadSharedDocsFlow import (
    _filter_files_by_patterns,
    _get_workflow_progress,
    get_db_batch_size,
    remove_deleted_files,
    upload_new_files,
    upload_shared_docs_flow,
)


class TestFilterFilesByPatterns:
    def _make_mock_file(self, path):
        f = MagicMock()
        f.path = path
        return f

    def test_returns_files_matching_any_pattern(self):
        files = [
            self._make_mock_file("hk/mag/l1/file.csv"),
            self._make_mock_file("science/mag/l1c/file.cdf"),
            self._make_mock_file("other/path.txt"),
        ]

        result = _filter_files_by_patterns(
            files, ["hk/**/*.csv", "science/**/*.cdf"], None
        )

        assert len(result) == 2

    def test_returns_empty_when_no_files_match_patterns(self):
        files = [
            self._make_mock_file("hk/mag/l1/file.csv"),
        ]

        result = _filter_files_by_patterns(files, ["science/**/*.cdf"], None)

        assert result == []

    def test_truncates_results_when_how_many_is_set(self):
        files = [self._make_mock_file(f"file{i}.csv") for i in range(5)]

        result = _filter_files_by_patterns(files, ["*.csv"], how_many=2)

        assert len(result) == 2

    def test_returns_all_matching_when_how_many_is_none(self):
        files = [self._make_mock_file(f"file{i}.csv") for i in range(5)]

        result = _filter_files_by_patterns(files, ["*.csv"], how_many=None)

        assert len(result) == 5


class TestGetDbBatchSize:
    def test_returns_2000_when_how_many_is_none(self):
        assert get_db_batch_size(None) == 2000

    def test_returns_2000_when_how_many_is_zero(self):
        assert get_db_batch_size(0) == 2000

    def test_returns_triple_how_many_when_larger_than_2000(self):
        assert get_db_batch_size(1000) == 3000

    def test_returns_2000_minimum_when_triple_is_less(self):
        assert get_db_batch_size(1) == 2000


class TestGetWorkflowProgress:
    def test_sets_progress_to_imap_epoch_when_none(self):
        mock_db = MagicMock()
        mock_db.get_workflow_progress.return_value.progress_timestamp = None
        started = datetime(2025, 1, 1, tzinfo=UTC)

        workflow_progress, _ = _get_workflow_progress(
            None, mock_db, started, None, "test-key"
        )

        assert workflow_progress.progress_timestamp is not None

    def test_uses_find_files_after_when_provided(self):
        mock_db = MagicMock()
        find_after = datetime(2024, 6, 1, tzinfo=UTC)
        mock_db.get_workflow_progress.return_value.progress_timestamp = datetime(
            2024, 1, 1, tzinfo=UTC
        )
        started = datetime(2025, 1, 1, tzinfo=UTC)

        _, last_modified = _get_workflow_progress(
            None, mock_db, started, find_after, "test-key"
        )

        assert last_modified == find_after

    def test_uses_progress_timestamp_when_find_files_after_is_none(self):
        mock_db = MagicMock()
        progress_ts = datetime(2024, 3, 15, tzinfo=UTC)
        mock_db.get_workflow_progress.return_value.progress_timestamp = progress_ts
        started = datetime(2025, 1, 1, tzinfo=UTC)

        _, last_modified = _get_workflow_progress(None, mock_db, started, None, "key")

        assert last_modified == progress_ts


class TestUploadNewFiles:
    def test_returns_zero_when_no_files_to_upload(self, tmp_path):
        mock_settings = MagicMock()
        mock_settings.data_store = tmp_path
        mock_settings.upload.paths_to_match = ["*.csv"]

        mock_db = MagicMock()
        mock_db.get_workflow_progress.return_value.progress_timestamp = datetime(
            2020, 1, 1, tzinfo=UTC
        )
        mock_db.get_files_since.return_value = []

        result = upload_new_files(
            destination_block_or_blockname="test-block",
            how_many=None,
            app_settings=mock_settings,
            db=mock_db,
            started=datetime(2025, 1, 1, tzinfo=UTC),
            find_files_after=None,
            workflow_progress_key="test-key",
        )

        assert result == 0

    def test_skips_upload_when_file_does_not_exist_on_disk(self, tmp_path):
        mock_settings = MagicMock()
        mock_settings.data_store = tmp_path
        mock_settings.upload.paths_to_match = ["*"]

        mock_db = MagicMock()
        mock_db.get_workflow_progress.return_value.progress_timestamp = datetime(
            2020, 1, 1, tzinfo=UTC
        )

        mock_file = MagicMock()
        mock_file.path = "nonexistent_file.csv"
        mock_file.get_datastore_relative_path.return_value = Path(
            "nonexistent_file.csv"
        )
        mock_file.last_modified_date = datetime(2025, 1, 2, tzinfo=UTC)
        mock_db.get_files_since.return_value = [mock_file]

        with patch(
            "prefect_server.uploadSharedDocsFlow.prefect_managedfiletransfer.upload_file_flow",
        ) as mock_upload:
            upload_new_files(
                destination_block_or_blockname="test-block",
                how_many=None,
                app_settings=mock_settings,
                db=mock_db,
                started=datetime(2025, 1, 1, tzinfo=UTC),
                find_files_after=None,
                workflow_progress_key="test-key",
            )

        mock_upload.assert_not_called()

    def test_uploads_file_and_returns_count(self, tmp_path):
        mock_settings = MagicMock()
        mock_settings.data_store = tmp_path
        mock_settings.upload.paths_to_match = ["*.csv"]
        mock_settings.upload.root_path = "/remote/root"

        test_file = tmp_path / "data.csv"
        test_file.write_text("content")

        mock_db = MagicMock()
        mock_db.get_workflow_progress.return_value.progress_timestamp = datetime(
            2020, 1, 1, tzinfo=UTC
        )

        mock_file = MagicMock()
        mock_file.path = "data.csv"
        mock_file.get_datastore_relative_path.return_value = Path("data.csv")
        mock_file.last_modified_date = datetime(2025, 1, 2, tzinfo=UTC)
        mock_db.get_files_since.return_value = [mock_file]

        with patch(
            "prefect_server.uploadSharedDocsFlow.prefect_managedfiletransfer.upload_file_flow",
        ):
            result = upload_new_files(
                destination_block_or_blockname="test-block",
                how_many=None,
                app_settings=mock_settings,
                db=mock_db,
                started=datetime(2025, 1, 1, tzinfo=UTC),
                find_files_after=None,
                workflow_progress_key="test-key",
            )

        assert result == 1


class TestRemoveDeletedFiles:
    def test_returns_zero_when_no_deleted_files(self, tmp_path):
        mock_settings = MagicMock()
        mock_settings.upload.paths_to_match = ["*.csv"]

        mock_db = MagicMock()
        mock_db.get_workflow_progress.return_value.progress_timestamp = datetime(
            2020, 1, 1, tzinfo=UTC
        )
        mock_db.get_files_deleted_since.return_value = []

        result = remove_deleted_files(
            destination_block_or_blockname="test-block",
            how_many=None,
            app_settings=mock_settings,
            db=mock_db,
            started=datetime(2025, 1, 1, tzinfo=UTC),
            find_files_after=None,
            workflow_progress_key="test-key",
        )

        assert result == 0

    def test_deletes_remote_file_and_returns_count(self, tmp_path):
        mock_settings = MagicMock()
        mock_settings.upload.paths_to_match = ["*.csv"]
        mock_settings.upload.root_path = "/remote/root"

        mock_db = MagicMock()
        mock_db.get_workflow_progress.return_value.progress_timestamp = datetime(
            2020, 1, 1, tzinfo=UTC
        )

        mock_file = MagicMock()
        mock_file.path = "data.csv"
        mock_file.get_datastore_relative_path.return_value = Path("data.csv")
        mock_file.deletion_date = datetime(2025, 2, 1, tzinfo=UTC)
        mock_db.get_files_deleted_since.return_value = [mock_file]

        with patch(
            "prefect_server.uploadSharedDocsFlow.prefect_managedfiletransfer.delete_files_flow",
        ):
            result = remove_deleted_files(
                destination_block_or_blockname="test-block",
                how_many=None,
                app_settings=mock_settings,
                db=mock_db,
                started=datetime(2025, 1, 1, tzinfo=UTC),
                find_files_after=None,
                workflow_progress_key="test-key",
            )

        assert result == 1


class TestUploadSharedDocsFlow:
    @pytest.mark.asyncio
    async def test_returns_no_work_completed_when_no_files(self):
        with (
            patch(
                "prefect_server.uploadSharedDocsFlow.AppSettings",
                return_value=MagicMock(),
            ),
            patch(
                "prefect_server.uploadSharedDocsFlow.Database",
                return_value=MagicMock(),
            ),
            patch(
                "prefect_server.uploadSharedDocsFlow.upload_new_files",
                return_value=0,
            ),
            patch(
                "prefect_server.uploadSharedDocsFlow.remove_deleted_files",
                return_value=0,
            ),
        ):
            result = await upload_shared_docs_flow.fn(
                destination_block_or_blockname="test-block",
            )

        assert result.is_completed()

    @pytest.mark.asyncio
    async def test_returns_completed_with_counts_when_files_uploaded(self):
        with (
            patch(
                "prefect_server.uploadSharedDocsFlow.AppSettings",
                return_value=MagicMock(),
            ),
            patch(
                "prefect_server.uploadSharedDocsFlow.Database",
                return_value=MagicMock(),
            ),
            patch(
                "prefect_server.uploadSharedDocsFlow.upload_new_files",
                return_value=3,
            ),
            patch(
                "prefect_server.uploadSharedDocsFlow.remove_deleted_files",
                return_value=1,
            ),
        ):
            result = await upload_shared_docs_flow.fn(
                destination_block_or_blockname="test-block",
            )

        assert result.is_completed()
        assert "3 files uploaded" in result.message
        assert "1 files deleted" in result.message

    @pytest.mark.asyncio
    async def test_skips_uploads_when_do_uploads_false(self):
        mock_upload = MagicMock(return_value=0)
        mock_delete = MagicMock(return_value=0)

        with (
            patch(
                "prefect_server.uploadSharedDocsFlow.AppSettings",
                return_value=MagicMock(),
            ),
            patch(
                "prefect_server.uploadSharedDocsFlow.Database",
                return_value=MagicMock(),
            ),
            patch(
                "prefect_server.uploadSharedDocsFlow.upload_new_files",
                mock_upload,
            ),
            patch(
                "prefect_server.uploadSharedDocsFlow.remove_deleted_files",
                mock_delete,
            ),
        ):
            await upload_shared_docs_flow.fn(
                destination_block_or_blockname="test-block",
                do_uploads=False,
            )

        mock_upload.assert_not_called()
        mock_delete.assert_called_once()

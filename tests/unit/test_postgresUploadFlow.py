"""Unit tests for postgresUploadFlow module functions."""

import contextlib
import os
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from imap_mag.db.utils import load_database_url
from prefect_server.postgresUploadFlow import (
    _process_files,
    upload_new_files_to_postgres,
)

pytestmark = pytest.mark.usefixtures("in_memory_database_url")


class TestLoadDatabaseUrl:
    def test_reads_the_env_var_first(self):
        with patch.dict(os.environ, {"MY_DB_URL": "postgresql+psycopg://u:p@h/db"}):
            assert load_database_url("MY_DB_URL") == "postgresql+psycopg://u:p@h/db"

    def test_falls_back_to_the_prefect_block(self):
        block = MagicMock()
        block._rendered_url.render_as_string.return_value = "postgresql://u:p@h/db"
        with patch(
            "imap_mag.db.utils.SqlAlchemyConnector.load", return_value=block
        ) as load:
            assert load_database_url("imap-database") == "postgresql://u:p@h/db"
        load.assert_called_once_with("imap-database", _sync=True)

    def test_returns_none_when_there_is_no_block(self):
        assert load_database_url("no-such-block") is None  # tests have no blocks


class TestProcessFiles:
    def _make_mock_settings(self, tmp_path):
        mock_settings = MagicMock()
        mock_settings.data_store = tmp_path
        mock_settings.postgres_upload.enable_history = False
        return mock_settings

    def _make_mock_file(self, path="data.csv"):
        mock_file = MagicMock()
        mock_file.path = path
        mock_file.last_modified_date = datetime(2025, 1, 2, tzinfo=UTC)
        return mock_file

    def test_returns_zero_counts_for_empty_file_list(self, tmp_path):
        mock_settings = self._make_mock_settings(tmp_path)
        mock_crump_config = MagicMock()
        mock_logger = MagicMock()

        uploaded, failed = _process_files(
            [], mock_settings, mock_crump_config, "postgresql://test", None, mock_logger
        )

        assert uploaded == 0
        assert failed == 0

    def test_increments_failed_count_when_file_does_not_exist(self, tmp_path):
        mock_settings = self._make_mock_settings(tmp_path)
        mock_file = self._make_mock_file("nonexistent/data.csv")
        mock_crump_config = MagicMock()
        mock_logger = MagicMock()

        uploaded, failed = _process_files(
            [mock_file],
            mock_settings,
            mock_crump_config,
            "postgresql://test",
            None,
            mock_logger,
        )

        assert uploaded == 0
        assert failed == 1

    def test_increments_failed_count_when_no_crump_job_found(self, tmp_path):
        mock_settings = self._make_mock_settings(tmp_path)
        test_file = tmp_path / "data.csv"
        test_file.write_text("col1,col2\n1,2\n")
        mock_file = self._make_mock_file("data.csv")

        mock_crump_config = MagicMock()
        mock_crump_config.get_job_or_auto_detect.return_value = None
        mock_logger = MagicMock()

        uploaded, failed = _process_files(
            [mock_file],
            mock_settings,
            mock_crump_config,
            "postgresql://test",
            None,
            mock_logger,
        )

        assert uploaded == 0
        assert failed == 1

    def test_syncs_csv_file_and_returns_uploaded_count(self, tmp_path):
        mock_settings = self._make_mock_settings(tmp_path)
        test_file = tmp_path / "data.csv"
        test_file.write_text("col1,col2\n1,2\n")
        mock_file = self._make_mock_file("data.csv")

        mock_job = MagicMock()
        mock_job.filename_to_column = None
        mock_crump_config = MagicMock()
        mock_crump_config.get_job_or_auto_detect.return_value = (mock_job, "test_job")
        mock_logger = MagicMock()

        with patch("prefect_server.postgresUploadFlow.sync_file_to_db", return_value=5):
            uploaded, failed = _process_files(
                [mock_file],
                mock_settings,
                mock_crump_config,
                "postgresql://test",
                None,
                mock_logger,
            )

        assert uploaded == 1
        assert failed == 0

    def test_increments_failed_count_when_sync_raises(self, tmp_path):
        mock_settings = self._make_mock_settings(tmp_path)
        test_file = tmp_path / "data.csv"
        test_file.write_text("col1,col2\n1,2\n")
        mock_file = self._make_mock_file("data.csv")

        mock_job = MagicMock()
        mock_job.filename_to_column = None
        mock_crump_config = MagicMock()
        mock_crump_config.get_job_or_auto_detect.return_value = (mock_job, "test_job")
        mock_logger = MagicMock()

        with patch(
            "prefect_server.postgresUploadFlow.sync_file_to_db",
            side_effect=RuntimeError("db error"),
        ):
            uploaded, failed = _process_files(
                [mock_file],
                mock_settings,
                mock_crump_config,
                "postgresql://test",
                None,
                mock_logger,
            )

        assert uploaded == 0
        assert failed == 1


class TestUploadNewFilesToPostgres:
    def _make_mock_settings(self, tmp_path):
        mock_settings = MagicMock()
        mock_settings.data_store = tmp_path
        mock_settings.postgres_upload.enable_history = False
        return mock_settings

    def _make_mock_db(self, progress_timestamp=datetime(2020, 1, 1, tzinfo=UTC)):
        mock_db = MagicMock()
        mock_db.get_workflow_progress.return_value.progress_timestamp = (
            progress_timestamp
        )
        return mock_db

    def _make_mock_file(self, path="data.csv"):
        mock_file = MagicMock()
        mock_file.path = path
        mock_file.last_modified_date = datetime(2025, 1, 2, tzinfo=UTC)
        return mock_file

    @contextlib.contextmanager
    def _base_patches(self, mock_settings, mock_db):
        with (
            patch(
                "prefect_server.postgresUploadFlow.AppSettings.create",
                return_value=mock_settings,
            ),
            patch(
                "prefect_server.postgresUploadFlow.Database",
                return_value=mock_db,
            ),
        ):
            yield

    @pytest.mark.asyncio
    async def test_returns_completed_with_no_work_when_no_files(self, tmp_path):
        mock_settings = self._make_mock_settings(tmp_path)
        mock_db = self._make_mock_db()
        mock_db.get_files_since.return_value = []

        with (
            self._base_patches(mock_settings, mock_db),
            patch("prefect_server.postgresUploadFlow.CrumpConfig"),
        ):
            result = await upload_new_files_to_postgres.fn(
                paths_to_match=["*.csv"],
            )

        assert result.is_completed()

    @pytest.mark.asyncio
    async def test_defaults_progress_timestamp_to_2010_when_none(self, tmp_path):
        mock_settings = self._make_mock_settings(tmp_path)
        mock_db = self._make_mock_db(progress_timestamp=None)
        mock_db.get_files_since.return_value = []

        with (
            self._base_patches(mock_settings, mock_db),
            patch("prefect_server.postgresUploadFlow.CrumpConfig"),
        ):
            await upload_new_files_to_postgres.fn(
                find_files_after=None,
                paths_to_match=["*.csv"],
            )

        assert (
            mock_db.get_workflow_progress.return_value.progress_timestamp
            == datetime(2010, 1, 1, tzinfo=UTC)
        )

    @pytest.mark.asyncio
    async def test_raises_when_crump_config_not_found(self, tmp_path):
        mock_settings = self._make_mock_settings(tmp_path)
        mock_settings.postgres_upload.crump_config_path.exists.return_value = False
        mock_db = self._make_mock_db()
        mock_db.get_files_since.return_value = []

        with self._base_patches(mock_settings, mock_db):
            with pytest.raises(ValueError, match="Crump configuration file not found"):
                await upload_new_files_to_postgres.fn(
                    paths_to_match=["*.csv"],
                )

    @pytest.mark.asyncio
    async def test_returns_failed_when_file_does_not_exist_on_disk(self, tmp_path):
        mock_settings = self._make_mock_settings(tmp_path)
        mock_db = self._make_mock_db()
        mock_file = self._make_mock_file("nonexistent/data.csv")
        mock_db.get_files_since.return_value = [mock_file]

        with (
            self._base_patches(mock_settings, mock_db),
            patch(
                "prefect_server.postgresUploadFlow.File.filter_to_latest_versions_only",
                return_value=[mock_file],
            ),
            patch("prefect_server.postgresUploadFlow.CrumpConfig"),
        ):
            result = await upload_new_files_to_postgres.fn(
                paths_to_match=["*"],
            )

        assert result.is_failed()

    @pytest.mark.asyncio
    async def test_returns_failed_when_no_crump_job_matches_file(self, tmp_path):
        mock_settings = self._make_mock_settings(tmp_path)
        test_file = tmp_path / "data.csv"
        test_file.write_text("col1,col2\n1,2\n")

        mock_db = self._make_mock_db()
        mock_file = self._make_mock_file("data.csv")
        mock_db.get_files_since.return_value = [mock_file]

        mock_crump_config = MagicMock()
        mock_crump_config.get_job_or_auto_detect.return_value = None

        with (
            self._base_patches(mock_settings, mock_db),
            patch(
                "prefect_server.postgresUploadFlow.File.filter_to_latest_versions_only",
                return_value=[mock_file],
            ),
            patch("prefect_server.postgresUploadFlow.CrumpConfig") as mock_crump_cls,
        ):
            mock_crump_cls.from_yaml.return_value = mock_crump_config
            result = await upload_new_files_to_postgres.fn(
                paths_to_match=["*"],
            )

        assert result.is_failed()

    @pytest.mark.asyncio
    async def test_syncs_csv_file_and_returns_completed(self, tmp_path):
        mock_settings = self._make_mock_settings(tmp_path)
        test_file = tmp_path / "data.csv"
        test_file.write_text("col1,col2\n1,2\n")

        mock_db = self._make_mock_db()
        mock_file = self._make_mock_file("data.csv")
        mock_db.get_files_since.return_value = [mock_file]

        mock_job = MagicMock()
        mock_job.filename_to_column = None

        mock_crump_config = MagicMock()
        mock_crump_config.get_job_or_auto_detect.return_value = (mock_job, "test_job")

        with (
            self._base_patches(mock_settings, mock_db),
            patch(
                "prefect_server.postgresUploadFlow.File.filter_to_latest_versions_only",
                return_value=[mock_file],
            ),
            patch("prefect_server.postgresUploadFlow.CrumpConfig") as mock_crump_cls,
            patch("prefect_server.postgresUploadFlow.sync_file_to_db", return_value=5),
        ):
            mock_crump_cls.from_yaml.return_value = mock_crump_config
            result = await upload_new_files_to_postgres.fn(
                paths_to_match=["*.csv"],
            )

        assert result.is_completed()
        assert "1 file(s) uploaded" in result.message

    @pytest.mark.asyncio
    async def test_extracts_cdf_file_before_syncing_to_database(self, tmp_path):
        mock_settings = self._make_mock_settings(tmp_path)
        test_cdf = tmp_path / "data.cdf"
        test_cdf.write_bytes(b"fake cdf")

        mock_db = self._make_mock_db()
        mock_file = self._make_mock_file("data.cdf")
        mock_db.get_files_since.return_value = [mock_file]

        mock_job = MagicMock()
        mock_job.filename_to_column = None

        mock_crump_config = MagicMock()
        mock_crump_config.get_job_or_auto_detect.return_value = (mock_job, "test_job")

        mock_extraction_result = MagicMock()
        mock_extraction_result.output_file = tmp_path / "extracted.csv"

        with (
            self._base_patches(mock_settings, mock_db),
            patch(
                "prefect_server.postgresUploadFlow.File.filter_to_latest_versions_only",
                return_value=[mock_file],
            ),
            patch("prefect_server.postgresUploadFlow.CrumpConfig") as mock_crump_cls,
            patch(
                "prefect_server.postgresUploadFlow.extract_cdf_to_tabular_file",
                return_value=[mock_extraction_result],
            ) as mock_extract,
            patch("prefect_server.postgresUploadFlow.sync_file_to_db", return_value=3),
        ):
            mock_crump_cls.from_yaml.return_value = mock_crump_config
            result = await upload_new_files_to_postgres.fn(
                paths_to_match=["*.cdf"],
            )

        mock_extract.assert_called_once()
        assert result.is_completed()


class TestPostgresUploadFlowSimpleRun:
    @pytest.mark.asyncio
    async def test_upload_new_files_flow_runs(self):
        mock_db = MagicMock()
        mock_db.get_files_since.return_value = []

        with (
            patch("prefect_server.postgresUploadFlow.Database", return_value=mock_db),
            patch(
                "prefect_server.postgresUploadFlow.try_get_prefect_logger",
                return_value=MagicMock(),
            ),
        ):
            await upload_new_files_to_postgres.fn()

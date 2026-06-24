"""Unit tests for postgresUploadFlow module functions."""

import contextlib
import os
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from prefect_server.postgresUploadFlow import (
    _get_database_connectionstring,
    _process_files,
    upload_new_files_to_postgres,
)


class TestGetDatabaseConnectionstring:
    def _make_app_settings(self, db_url_env_var=None):
        mock_settings = MagicMock()
        mock_settings.postgres_upload.database_url_env_var_or_block_name = (
            db_url_env_var
        )
        return mock_settings

    @pytest.mark.asyncio
    async def test_raises_when_no_connection_info_and_app_settings_has_none(self):
        mock_settings = self._make_app_settings(db_url_env_var=None)

        with pytest.raises(
            RuntimeError, match="Database connection information not provided"
        ):
            await _get_database_connectionstring(mock_settings, None)

    @pytest.mark.asyncio
    async def test_uses_env_var_when_string_and_env_var_set(self):
        mock_settings = self._make_app_settings()

        with patch.dict(os.environ, {"MY_DB_URL": "postgresql://user:pass@host/db"}):
            result = await _get_database_connectionstring(mock_settings, "MY_DB_URL")

        assert result == "postgresql://user:pass@host/db"

    @pytest.mark.asyncio
    async def test_converts_psycopg_driver_spec_in_url(self):
        mock_settings = self._make_app_settings()

        with patch.dict(
            os.environ, {"MY_DB_URL": "postgresql+psycopg://user:pass@host/db"}
        ):
            result = await _get_database_connectionstring(mock_settings, "MY_DB_URL")

        assert result == "postgresql://user:pass@host/db"

    @pytest.mark.asyncio
    async def test_loads_prefect_block_when_env_var_not_set(self):
        mock_settings = self._make_app_settings()
        mock_connector = MagicMock()
        mock_connector._rendered_url.render_as_string.return_value = (
            "postgresql://user:pass@host/db"
        )

        with (
            patch.dict(os.environ, {}, clear=True),
            patch(
                "prefect_server.postgresUploadFlow.SqlAlchemyConnector.aload",
                new_callable=AsyncMock,
                return_value=mock_connector,
            ),
        ):
            result = await _get_database_connectionstring(
                mock_settings, "my-block-name"
            )

        assert "postgresql" in result

    @pytest.mark.asyncio
    async def test_raises_when_block_name_not_found(self):
        mock_settings = self._make_app_settings()

        with (
            patch.dict(os.environ, {}, clear=True),
            patch(
                "prefect_server.postgresUploadFlow.SqlAlchemyConnector.aload",
                side_effect=ValueError("Block not found"),
            ),
        ):
            with pytest.raises(ValueError, match="Invalid database connection input"):
                await _get_database_connectionstring(mock_settings, "nonexistent-block")

    @pytest.mark.asyncio
    async def test_raises_when_none_passed_and_app_settings_has_env_var(self):
        mock_settings = self._make_app_settings(
            db_url_env_var="MY_DB_URL_FROM_SETTINGS"
        )

        with patch.dict(
            os.environ, {"MY_DB_URL_FROM_SETTINGS": "postgresql://host/db"}
        ):
            with pytest.raises(ValueError, match="Invalid database connection input"):
                await _get_database_connectionstring(mock_settings, None)


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
                "prefect_server.postgresUploadFlow.AppSettings",
                return_value=mock_settings,
            ),
            patch(
                "prefect_server.postgresUploadFlow.Database",
                return_value=mock_db,
            ),
            patch(
                "prefect_server.postgresUploadFlow._get_database_connectionstring",
                new_callable=AsyncMock,
                return_value="postgresql://test",
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
                db_env_name_or_block_name_or_block="DB_URL",
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
                db_env_name_or_block_name_or_block="DB_URL",
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
                    db_env_name_or_block_name_or_block="DB_URL",
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
                db_env_name_or_block_name_or_block="DB_URL",
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
                db_env_name_or_block_name_or_block="DB_URL",
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
                db_env_name_or_block_name_or_block="DB_URL",
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
                db_env_name_or_block_name_or_block="DB_URL",
            )

        mock_extract.assert_called_once()
        assert result.is_completed()


class TestPostgresUploadFlowSimpleRun:
    @pytest.mark.asyncio
    async def test_upload_new_files_flow_runs(self):
        mock_db = MagicMock()
        mock_db.get_files_since.return_value = []

        with (
            patch(
                "prefect_server.postgresUploadFlow._get_database_connectionstring",
                new_callable=AsyncMock,
                return_value="postgresql://localhost/test",
            ),
            patch("prefect_server.postgresUploadFlow.Database", return_value=mock_db),
            patch(
                "prefect_server.postgresUploadFlow.try_get_prefect_logger",
                return_value=MagicMock(),
            ),
        ):
            await upload_new_files_to_postgres.fn()

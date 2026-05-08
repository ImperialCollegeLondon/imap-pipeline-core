"""Tests for additional coverage of various modules."""

from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


class TestPartitionedPathHandler:
    def _make_handler(self):
        from imap_mag.io.file.HKBinaryPathHandler import HKBinaryPathHandler

        return HKBinaryPathHandler(
            descriptor="hsk-pw",
            content_date=datetime(2025, 1, 1),
            extension="pkts",
        )

    def test_get_sequence_returns_part(self):
        handler = self._make_handler()
        handler.part = 3
        assert handler.get_sequence() == 3

    def test_set_sequence_updates_part(self):
        handler = self._make_handler()
        handler.set_sequence(5)
        assert handler.part == 5

    def test_increase_sequence_increments_part(self):
        handler = self._make_handler()
        handler.part = 2
        handler.increase_sequence()
        assert handler.part == 3

    def test_get_sequence_variable_name_is_part(self):
        from imap_mag.io.file.PartitionedPathHandler import PartitionedPathHandler

        assert PartitionedPathHandler.get_sequence_variable_name() == "part"


class TestCalibrationJobBase:
    def _make_concrete_job(self, work_folder=None):
        from pathlib import Path

        from mag_toolkit.calibration.CalibrationJobParameters import (
            CalibrationJobParameters,
        )
        from mag_toolkit.calibration.calibrators.CalibrationJob import CalibrationJob

        class ConcreteJob(CalibrationJob):
            def _get_path_handlers(self, params):
                return {}

            def run_calibration(self, cal_handler, config):
                return (Path("/out.csv"), Path("/cal.csv"))

        params = MagicMock(spec=CalibrationJobParameters)
        return ConcreteJob(params, work_folder or Path("/tmp"))

    def test_set_file_sets_required_file(self):
        job = self._make_concrete_job()
        job.required_files["science"] = None
        job.set_file("science", Path("/data/science.cdf"))
        assert job.required_files["science"] == Path("/data/science.cdf")

    def test_set_file_logs_warning_when_file_already_set(self):
        job = self._make_concrete_job()
        job.required_files["science"] = Path("/existing.cdf")
        job.set_file("science", Path("/new.cdf"))
        assert job.required_files["science"] == Path("/existing.cdf")

    def test_check_for_required_files_returns_true_when_all_present(self):
        job = self._make_concrete_job()
        job.required_files["file1"] = Path("/data/file1.cdf")
        assert job._check_for_required_files() is True

    def test_check_for_required_files_returns_false_when_file_missing(self):
        job = self._make_concrete_job()
        job.required_files["file1"] = None
        assert job._check_for_required_files() is False

    def test_check_for_required_data_store_returns_true_when_data_store_set(self):
        job = self._make_concrete_job()
        job.data_store = Path("/datastore")
        assert job._check_for_required_data_store() is True

    def test_check_for_required_data_store_returns_false_when_data_store_none(self):
        job = self._make_concrete_job()
        assert job.data_store is None
        assert job._check_for_required_data_store() is False

    def test_check_environment_returns_false_when_files_missing(self):
        job = self._make_concrete_job()
        job.required_files["file1"] = None
        assert job._check_environment_is_setup() is False

    def test_check_environment_returns_false_when_data_store_missing(self):
        job = self._make_concrete_job()
        assert job._check_environment_is_setup() is False

    def test_check_environment_returns_true_when_everything_set(self):
        job = self._make_concrete_job()
        job.data_store = Path("/datastore")
        assert job._check_environment_is_setup() is True

    def test_setup_datastore_sets_data_store(self):
        job = self._make_concrete_job()
        job.setup_datastore(Path("/datastore"))
        assert job.data_store == Path("/datastore")

    def test_setup_datastore_skips_when_not_needed(self):
        from mag_toolkit.calibration.CalibrationJobParameters import (
            CalibrationJobParameters,
        )
        from mag_toolkit.calibration.calibrators.CalibrationJob import CalibrationJob

        class NoDataStoreJob(CalibrationJob):
            def _get_path_handlers(self, params):
                return {}

            def run_calibration(self, cal_handler, config):
                return (Path("/out.csv"), Path("/cal.csv"))

            def needs_data_store(self):
                return False

        params = MagicMock(spec=CalibrationJobParameters)
        job = NoDataStoreJob(params, Path("/tmp"))
        job.setup_datastore(Path("/datastore"))
        assert job.data_store is None


class TestFetchSpinTables:
    def test_fetch_spin_tables_with_date_range_uses_fetch_by_dates_params(self):
        from imap_mag.cli.fetch.spin_table import fetch_spin_tables

        mock_result = MagicMock()
        mock_result.success = True
        mock_result.data_items = []

        mock_pipeline = MagicMock()
        mock_pipeline.get_results.return_value = mock_result

        with (
            patch("imap_mag.cli.fetch.spin_table.AppSettings") as mock_settings,
            patch("imap_mag.cli.fetch.spin_table.initialiseLoggingForCommand"),
            patch("imap_mag.cli.fetch.spin_table.SDCDataAccess"),
            patch("imap_mag.cli.fetch.spin_table.SpinTablePipeline", return_value=mock_pipeline),
            patch("imap_mag.cli.fetch.spin_table.asyncio.run"),
        ):
            mock_settings.return_value.setup_work_folder_for_command.return_value = Path("/tmp")
            fetch_spin_tables(start_date=datetime(2025, 1, 1), end_date=datetime(2025, 1, 31))

        mock_pipeline.build.assert_called_once()

    def test_fetch_spin_tables_without_dates_uses_automatic_params(self):
        from imap_mag.cli.fetch.spin_table import fetch_spin_tables
        from imap_mag.data_pipelines import AutomaticRunParameters

        mock_result = MagicMock()
        mock_result.success = True
        mock_result.data_items = []

        mock_pipeline = MagicMock()
        mock_pipeline.get_results.return_value = mock_result

        with (
            patch("imap_mag.cli.fetch.spin_table.AppSettings") as mock_settings,
            patch("imap_mag.cli.fetch.spin_table.initialiseLoggingForCommand"),
            patch("imap_mag.cli.fetch.spin_table.SDCDataAccess"),
            patch("imap_mag.cli.fetch.spin_table.SpinTablePipeline", return_value=mock_pipeline),
            patch("imap_mag.cli.fetch.spin_table.asyncio.run"),
        ):
            mock_settings.return_value.setup_work_folder_for_command.return_value = Path("/tmp")
            fetch_spin_tables()

        call_args = mock_pipeline.build.call_args[0][0]
        assert isinstance(call_args, AutomaticRunParameters)

    def test_fetch_spin_tables_raises_on_pipeline_failure(self):
        from imap_mag.cli.fetch.spin_table import fetch_spin_tables

        mock_result = MagicMock()
        mock_result.success = False

        mock_pipeline = MagicMock()
        mock_pipeline.get_results.return_value = mock_result

        with (
            patch("imap_mag.cli.fetch.spin_table.AppSettings") as mock_settings,
            patch("imap_mag.cli.fetch.spin_table.initialiseLoggingForCommand"),
            patch("imap_mag.cli.fetch.spin_table.SDCDataAccess"),
            patch("imap_mag.cli.fetch.spin_table.SpinTablePipeline", return_value=mock_pipeline),
            patch("imap_mag.cli.fetch.spin_table.asyncio.run"),
        ):
            mock_settings.return_value.setup_work_folder_for_command.return_value = Path("/tmp")
            with pytest.raises(RuntimeError, match="Pipeline failed"):
                fetch_spin_tables()

    def test_fetch_spin_tables_creates_database_when_use_database_true(self):
        from imap_mag.cli.fetch.spin_table import fetch_spin_tables

        mock_result = MagicMock()
        mock_result.success = True
        mock_result.data_items = []

        mock_pipeline = MagicMock()
        mock_pipeline.get_results.return_value = mock_result

        with (
            patch("imap_mag.cli.fetch.spin_table.AppSettings") as mock_settings,
            patch("imap_mag.cli.fetch.spin_table.initialiseLoggingForCommand"),
            patch("imap_mag.cli.fetch.spin_table.SDCDataAccess"),
            patch("imap_mag.cli.fetch.spin_table.SpinTablePipeline", return_value=mock_pipeline),
            patch("imap_mag.cli.fetch.spin_table.asyncio.run"),
            patch("imap_mag.cli.fetch.spin_table.Database") as mock_db,
        ):
            mock_settings.return_value.setup_work_folder_for_command.return_value = Path("/tmp")
            fetch_spin_tables(use_database=True)

        mock_db.assert_called_once()


class TestCleanupFlowHelpers:
    def _make_file(self, version=1, content_date=None, minutes_old=60):
        f = MagicMock()
        f.version = version
        f.content_date = content_date or datetime(2025, 1, 1)
        f.descriptor = "test-descriptor"
        f.last_modified_date = datetime.now(tz=UTC) - timedelta(minutes=minutes_old)
        f.deletion_date = None
        return f

    def test_get_files_to_cleanup_with_keep_latest_only_filters_to_non_latest(self):
        from imap_mag.config.DatastoreCleanupConfig import CleanupTask
        from prefect_server.datastoreCleanupFlow import _get_files_to_cleanup

        old_date = datetime.now(tz=UTC) - timedelta(days=60)
        file_v1 = MagicMock()
        file_v1.version = 1
        file_v1.content_date = datetime(2025, 1, 1)
        file_v1.descriptor = "test"
        file_v1.last_modified_date = old_date
        file_v1.deletion_date = None

        file_v2 = MagicMock()
        file_v2.version = 2
        file_v2.content_date = datetime(2025, 1, 1)
        file_v2.descriptor = "test"
        file_v2.last_modified_date = old_date
        file_v2.deletion_date = None

        from imap_db.model import File

        with patch.object(File, "filter_to_latest_versions_only", return_value=[file_v2]):
            task = CleanupTask(
                name="test",
                paths_to_match=["*.cdf"],
                files_older_than="1d",
                keep_latest_version_only=True,
            )
            with patch("imap_mag.config.DatastoreCleanupConfig.DatetimeProvider.now",
                       return_value=datetime.now()):
                result = _get_files_to_cleanup([file_v1, file_v2], task)

        assert file_v1 in result
        assert file_v2 not in result

    def test_cleanup_flow_dry_run_does_not_delete_files(self):
        from imap_mag.config.DatastoreCleanupConfig import CleanupMode, CleanupTask
        from prefect_server.datastoreCleanupFlow import cleanup_datastore_flow

        old_time = datetime.now(tz=UTC) - timedelta(days=60)

        mock_file = MagicMock()
        mock_file.path = "/data/test.cdf"
        mock_file.last_modified_date = old_time
        mock_file.descriptor = "test"
        mock_file.content_date = datetime(2025, 1, 1)
        mock_file.version = 1
        mock_file.deletion_date = None

        task = CleanupTask(
            name="test_task",
            paths_to_match=["*.cdf"],
            files_older_than="1d",
            keep_latest_version_only=False,
            cleanup_mode=CleanupMode.DELETE,
        )

        mock_config = MagicMock()
        mock_config.tasks = [task]
        mock_config.dry_run = True

        mock_db = MagicMock()
        mock_db.get_active_files_matching_patterns.return_value = [mock_file]

        mock_settings = MagicMock()
        mock_settings.datastore_cleanup = mock_config

        mock_manager = MagicMock()

        with (
            patch("prefect_server.datastoreCleanupFlow.AppSettings", return_value=mock_settings),
            patch("prefect_server.datastoreCleanupFlow.Database", return_value=mock_db),
            patch("prefect_server.datastoreCleanupFlow.DBIndexedDatastoreFileManager", return_value=mock_manager),
            patch("prefect_server.datastoreCleanupFlow._get_files_to_cleanup", return_value=[mock_file]),
        ):
            import asyncio
            asyncio.get_event_loop().run_until_complete(
                cleanup_datastore_flow.fn(dry_run=True)
            )

        mock_manager.delete_file.assert_not_called()

    def test_cleanup_flow_delete_mode_deletes_files(self):
        from imap_mag.config.DatastoreCleanupConfig import CleanupMode, CleanupTask
        from prefect_server.datastoreCleanupFlow import cleanup_datastore_flow

        mock_file = MagicMock()
        mock_file.path = "/data/test.cdf"

        task = CleanupTask(
            name="test_task",
            paths_to_match=["*.cdf"],
            files_older_than="1d",
            keep_latest_version_only=False,
            cleanup_mode=CleanupMode.DELETE,
        )

        mock_config = MagicMock()
        mock_config.tasks = [task]
        mock_config.dry_run = False

        mock_db = MagicMock()
        mock_db.get_active_files_matching_patterns.return_value = [mock_file]

        mock_settings = MagicMock()
        mock_settings.datastore_cleanup = mock_config

        mock_manager = MagicMock()

        with (
            patch("prefect_server.datastoreCleanupFlow.AppSettings", return_value=mock_settings),
            patch("prefect_server.datastoreCleanupFlow.Database", return_value=mock_db),
            patch("prefect_server.datastoreCleanupFlow.DBIndexedDatastoreFileManager", return_value=mock_manager),
            patch("prefect_server.datastoreCleanupFlow._get_files_to_cleanup", return_value=[mock_file]),
        ):
            import asyncio
            asyncio.get_event_loop().run_until_complete(
                cleanup_datastore_flow.fn(dry_run=False)
            )

        mock_manager.delete_file.assert_called_once_with(mock_file)

    def test_cleanup_flow_stops_at_max_file_operations(self):
        from imap_mag.config.DatastoreCleanupConfig import CleanupMode, CleanupTask
        from prefect_server.datastoreCleanupFlow import cleanup_datastore_flow

        files = [MagicMock(path=f"/data/file{i}.cdf") for i in range(10)]

        task = CleanupTask(
            name="test_task",
            paths_to_match=["*.cdf"],
            files_older_than="1d",
            keep_latest_version_only=False,
            cleanup_mode=CleanupMode.DELETE,
        )

        mock_config = MagicMock()
        mock_config.tasks = [task]
        mock_config.dry_run = False

        mock_db = MagicMock()
        mock_db.get_active_files_matching_patterns.return_value = files

        mock_settings = MagicMock()
        mock_settings.datastore_cleanup = mock_config

        mock_manager = MagicMock()

        with (
            patch("prefect_server.datastoreCleanupFlow.AppSettings", return_value=mock_settings),
            patch("prefect_server.datastoreCleanupFlow.Database", return_value=mock_db),
            patch("prefect_server.datastoreCleanupFlow.DBIndexedDatastoreFileManager", return_value=mock_manager),
            patch("prefect_server.datastoreCleanupFlow._get_files_to_cleanup", return_value=files),
        ):
            import asyncio
            asyncio.get_event_loop().run_until_complete(
                cleanup_datastore_flow.fn(dry_run=False, max_file_operations=3)
            )

        assert mock_manager.delete_file.call_count == 3

    def test_cleanup_flow_skips_when_no_files_match(self):
        from imap_mag.config.DatastoreCleanupConfig import CleanupTask
        from prefect_server.datastoreCleanupFlow import cleanup_datastore_flow

        task = CleanupTask(
            name="test_task",
            paths_to_match=["*.cdf"],
            files_older_than="1d",
        )

        mock_config = MagicMock()
        mock_config.tasks = [task]
        mock_config.dry_run = False

        mock_db = MagicMock()
        mock_db.get_active_files_matching_patterns.return_value = []

        mock_settings = MagicMock()
        mock_settings.datastore_cleanup = mock_config

        mock_manager = MagicMock()

        with (
            patch("prefect_server.datastoreCleanupFlow.AppSettings", return_value=mock_settings),
            patch("prefect_server.datastoreCleanupFlow.Database", return_value=mock_db),
            patch("prefect_server.datastoreCleanupFlow.DBIndexedDatastoreFileManager", return_value=mock_manager),
        ):
            import asyncio
            result = asyncio.get_event_loop().run_until_complete(
                cleanup_datastore_flow.fn()
            )

        assert "No files to clean up" in result.message


class TestImapMagVersion:
    def test_get_version_returns_unknown_when_not_installed(self):
        from importlib.metadata import PackageNotFoundError

        from imap_mag import get_version

        with patch("imap_mag.version", side_effect=PackageNotFoundError):
            result = get_version()

        assert result == "unknown"

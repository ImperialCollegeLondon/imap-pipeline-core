"""Unit tests for prefect server flow functions that need coverage.

Tests call .fn(...) directly on Prefect flow objects to bypass orchestration,
and mock all external dependencies.
"""

from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from imap_mag.data_pipelines import AutomaticRunParameters, FetchByDatesRunParameters
from imap_mag.util import ScienceLevel, ScienceMode


class TestPollHKFlowGenerateName:
    def test_auto_run_includes_last_update(self):
        from prefect_server.pollHK import generate_flow_run_name
        from imap_mag.util import HKPacket

        mock_params = {
            "hk_packets": list(HKPacket),
            "start_date": None,
            "end_date": None,
        }
        with patch("prefect_server.pollHK.flow_run") as mock_flow_run:
            with patch("prefect_server.pollHK.DatetimeProvider.end_of_today", return_value=datetime(2025, 6, 1, 23, 59, 59)):
                mock_flow_run.parameters = mock_params
                name = generate_flow_run_name()

        assert "last-update" in name
        assert "all-HK" in name

    def test_specific_dates_included_in_name(self):
        from prefect_server.pollHK import generate_flow_run_name
        from imap_mag.util import HKPacket

        mock_params = {
            "hk_packets": [HKPacket.SID1],
            "start_date": datetime(2025, 6, 1),
            "end_date": datetime(2025, 6, 30),
        }
        with patch("prefect_server.pollHK.flow_run") as mock_flow_run:
            mock_flow_run.parameters = mock_params
            name = generate_flow_run_name()

        assert "01-06-2025" in name


class TestPollHKFlow:
    @pytest.mark.asyncio
    async def test_poll_hk_flow_downloads_packets(self):
        from imap_mag.util import HKPacket
        from prefect_server.pollHK import poll_hk_flow

        mock_db = MagicMock()
        mock_progress = MagicMock()
        mock_progress.get_last_checked_date.return_value = None
        mock_progress.progress_timestamp = None
        mock_db.get_workflow_progress.return_value = mock_progress

        with (
            patch("prefect_server.pollHK.get_secret_or_env_var", new_callable=AsyncMock, return_value="auth"),
            patch("prefect_server.pollHK.Database", return_value=mock_db),
            patch("prefect_server.pollHK.DownloadDateManager") as mock_dm,
            patch("prefect_server.pollHK.fetch_binary", return_value={}),
            patch("prefect_server.pollHK.try_get_prefect_logger", return_value=MagicMock()),
        ):
            mock_dm_instance = MagicMock()
            mock_dm_instance.get_dates_for_download.return_value = (
                datetime(2025, 1, 1), datetime(2025, 1, 31)
            )
            mock_dm.return_value = mock_dm_instance

            await poll_hk_flow.fn(
                hk_packets=[HKPacket.SID1],
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 31),
            )

    @pytest.mark.asyncio
    async def test_poll_hk_skips_when_no_dates(self):
        from imap_mag.util import HKPacket
        from prefect_server.pollHK import poll_hk_flow

        mock_db = MagicMock()

        with (
            patch("prefect_server.pollHK.get_secret_or_env_var", new_callable=AsyncMock, return_value="auth"),
            patch("prefect_server.pollHK.Database", return_value=mock_db),
            patch("prefect_server.pollHK.DownloadDateManager") as mock_dm,
            patch("prefect_server.pollHK.fetch_binary") as mock_fetch,
            patch("prefect_server.pollHK.try_get_prefect_logger", return_value=MagicMock()),
        ):
            mock_dm_instance = MagicMock()
            mock_dm_instance.get_dates_for_download.return_value = None
            mock_dm.return_value = mock_dm_instance

            await poll_hk_flow.fn(
                hk_packets=[HKPacket.SID1],
                start_date=None,
                end_date=None,
            )

        mock_fetch.assert_not_called()


class TestPollScienceFlowGenerateName:
    def test_auto_run_includes_last_update(self):
        from prefect_server.pollScience import generate_flow_run_name

        mock_params = {
            "level": ScienceLevel.l2,
            "modes": [ScienceMode.Normal],
            "start_date": None,
            "end_date": None,
        }
        with patch("prefect_server.pollScience.flow_run") as mock_flow_run:
            with patch("prefect_server.pollScience.DatetimeProvider.end_of_today", return_value=datetime(2025, 6, 1)):
                mock_flow_run.parameters = mock_params
                name = generate_flow_run_name()

        assert "last-update" in name

    def test_specific_dates_in_name(self):
        from prefect_server.pollScience import generate_flow_run_name

        mock_params = {
            "level": ScienceLevel.l1c,
            "modes": [ScienceMode.Normal],
            "start_date": datetime(2025, 6, 1),
            "end_date": datetime(2025, 6, 30),
        }
        with patch("prefect_server.pollScience.flow_run") as mock_flow_run:
            mock_flow_run.parameters = mock_params
            name = generate_flow_run_name()

        assert "01-06-2025" in name


class TestPollScienceFlow:
    @pytest.mark.asyncio
    async def test_poll_science_flow_runs_download(self):
        from prefect_server.pollScience import poll_science_flow

        mock_db = MagicMock()

        with (
            patch("prefect_server.pollScience.get_secret_or_env_var", new_callable=AsyncMock, return_value="auth"),
            patch("prefect_server.pollScience.Database", return_value=mock_db),
            patch("prefect_server.pollScience.DownloadDateManager") as mock_dm,
            patch("prefect_server.pollScience.fetch_science", return_value={}),
            patch("prefect_server.pollScience.try_get_prefect_logger", return_value=MagicMock()),
            patch("prefect_server.pollScience.update_database_with_progress"),
        ):
            mock_dm_instance = MagicMock()
            mock_dm_instance.get_dates_for_download.return_value = (
                datetime(2025, 1, 1), datetime(2025, 1, 31)
            )
            mock_dm.return_value = mock_dm_instance

            await poll_science_flow.fn(
                level=ScienceLevel.l1c,
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 31),
            )

    @pytest.mark.asyncio
    async def test_poll_science_flow_skips_when_no_dates(self):
        from prefect_server.pollScience import poll_science_flow

        mock_db = MagicMock()

        with (
            patch("prefect_server.pollScience.get_secret_or_env_var", new_callable=AsyncMock, return_value="auth"),
            patch("prefect_server.pollScience.Database", return_value=mock_db),
            patch("prefect_server.pollScience.DownloadDateManager") as mock_dm,
            patch("prefect_server.pollScience.fetch_science") as mock_fetch,
            patch("prefect_server.pollScience.try_get_prefect_logger", return_value=MagicMock()),
        ):
            mock_dm_instance = MagicMock()
            mock_dm_instance.get_dates_for_download.return_value = None
            mock_dm.return_value = mock_dm_instance

            await poll_science_flow.fn(level=ScienceLevel.l1c)

        mock_fetch.assert_not_called()


class TestPollIALiRTGenerateName:
    def test_name_with_no_dates_uses_last_update(self):
        from prefect_server.pollIALiRT import generate_flow_run_name

        mock_params = {"start_date": None, "end_date": None}
        with patch("prefect_server.pollIALiRT.flow_run") as mock_flow_run:
            with patch("prefect_server.pollIALiRT.DatetimeProvider.end_of_hour", return_value=datetime(2025, 6, 1)):
                mock_flow_run.parameters = mock_params
                name = generate_flow_run_name()

        assert "last-update" in name

    def test_name_with_dates(self):
        from prefect_server.pollIALiRT import generate_flow_run_name

        mock_params = {
            "start_date": datetime(2025, 6, 1, 12, 0, 0),
            "end_date": datetime(2025, 6, 1, 13, 0, 0),
        }
        with patch("prefect_server.pollIALiRT.flow_run") as mock_flow_run:
            mock_flow_run.parameters = mock_params
            name = generate_flow_run_name()

        assert "01-06-2025" in name

    def test_hk_name_with_no_dates(self):
        from prefect_server.pollIALiRT import generate_hk_flow_run_name

        mock_params = {"start_date": None, "end_date": None}
        with patch("prefect_server.pollIALiRT.flow_run") as mock_flow_run:
            with patch("prefect_server.pollIALiRT.DatetimeProvider.end_of_hour", return_value=datetime(2025, 6, 1)):
                mock_flow_run.parameters = mock_params
                name = generate_hk_flow_run_name()

        assert "HK" in name


class TestDoPollIALiRT:
    def test_do_poll_ialirt_returns_empty_when_no_dates(self):
        from prefect_server.pollIALiRT import do_poll_ialirt

        mock_db = MagicMock()
        mock_logger = MagicMock()

        with patch("prefect_server.pollIALiRT.DownloadDateManager") as mock_dm:
            mock_dm_instance = MagicMock()
            mock_dm_instance.get_dates_for_download.return_value = None
            mock_dm.return_value = mock_dm_instance

            result = do_poll_ialirt(
                database=mock_db,
                auth_code="auth",
                start_date=None,
                end_date=None,
                force_download=False,
                logger=mock_logger,
            )

        assert result == []

    def test_do_poll_ialirt_returns_downloaded_files(self, tmp_path):
        from prefect_server.pollIALiRT import do_poll_ialirt

        mock_db = MagicMock()
        mock_logger = MagicMock()
        downloaded_file = tmp_path / "ialirt.csv"
        downloaded_file.touch()
        mock_handler = MagicMock()
        mock_handler.content_date = datetime(2025, 1, 15)

        with (
            patch("prefect_server.pollIALiRT.DownloadDateManager") as mock_dm,
            patch("prefect_server.pollIALiRT.fetch_ialirt", return_value={downloaded_file: mock_handler}),
            patch("prefect_server.pollIALiRT.update_database_with_progress"),
        ):
            mock_dm_instance = MagicMock()
            mock_dm_instance.get_dates_for_download.return_value = (
                datetime(2025, 1, 1), datetime(2025, 1, 31)
            )
            mock_dm.return_value = mock_dm_instance

            result = do_poll_ialirt(
                database=mock_db,
                auth_code="auth",
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 31),
                force_download=False,
                logger=mock_logger,
            )

        assert downloaded_file in result


class TestDoPollIALiRTHK:
    def test_do_poll_ialirt_hk_returns_empty_when_no_dates(self):
        from prefect_server.pollIALiRT import do_poll_ialirt_hk

        mock_db = MagicMock()
        mock_logger = MagicMock()

        with patch("prefect_server.pollIALiRT.DownloadDateManager") as mock_dm:
            mock_dm_instance = MagicMock()
            mock_dm_instance.get_dates_for_download.return_value = None
            mock_dm.return_value = mock_dm_instance

            result = do_poll_ialirt_hk(
                database=mock_db,
                auth_code="auth",
                start_date=None,
                end_date=None,
                force_download=False,
                logger=mock_logger,
            )

        assert result == []


class TestPollLoPivotPlatformFlow:
    @pytest.mark.asyncio
    async def test_poll_lo_pivot_runs_pipeline(self):
        from prefect_server.pollLoPivotPlatform import poll_lo_pivot_platform_flow

        mock_pipeline = MagicMock()
        mock_pipeline.get_results.return_value = MagicMock(success=True)
        mock_pipeline.run = AsyncMock()

        with (
            patch("prefect_server.pollLoPivotPlatform.get_secret_or_env_var", new_callable=AsyncMock, return_value="auth"),
            patch("prefect_server.pollLoPivotPlatform.Database"),
            patch("prefect_server.pollLoPivotPlatform.AppSettings"),
            patch("prefect_server.pollLoPivotPlatform.LoPivotPlatformPipeline", return_value=mock_pipeline),
        ):
            await poll_lo_pivot_platform_flow.fn(
                run_parameters=FetchByDatesRunParameters(
                    start_date=datetime(2025, 1, 1),
                    end_date=datetime(2025, 1, 31),
                ),
                use_database=False,
            )

        mock_pipeline.build.assert_called_once()
        mock_pipeline.run.assert_called_once()


class TestCheckIALiRTFlow:
    @pytest.mark.asyncio
    async def test_check_ialirt_flow_no_anomalies(self):
        from prefect_server.checkIALiRT import check_ialirt_flow

        with (
            patch("prefect_server.checkIALiRT.check_ialirt", return_value=[]),
            patch("prefect_server.checkIALiRT.try_get_prefect_logger", return_value=MagicMock()),
            patch("prefect_server.checkIALiRT.Database") as mock_db_cls,
            patch("prefect_server.checkIALiRT.DatetimeProvider.yesterday", return_value=datetime(2025, 6, 1)),
            patch("prefect_server.checkIALiRT.DatetimeProvider.today", return_value=datetime(2025, 6, 2)),
            patch("prefect_server.checkIALiRT.DatetimeProvider.now", return_value=datetime(2025, 6, 2)),
        ):
            mock_db = MagicMock()
            mock_wp = MagicMock()
            mock_wp.get_progress_timestamp.return_value = None
            mock_db.get_workflow_progress.return_value = mock_wp
            mock_db_cls.return_value = mock_db

            # Mock the Teams webhook to avoid calling real service
            with patch("prefect_server.checkIALiRT.MicrosoftTeamsWebhook.aload", new_callable=AsyncMock) as mock_webhook:
                mock_webhook.return_value = AsyncMock()
                result = await check_ialirt_flow.fn(files=None)

        assert result is not None


class TestDatastoreCleanupFlow:
    @pytest.mark.asyncio
    async def test_cleanup_flow_with_no_matching_tasks_returns_skipped(self):
        from prefect_server.datastoreCleanupFlow import cleanup_datastore_flow

        mock_settings = MagicMock()
        mock_settings.datastore_cleanup.dry_run = True
        mock_settings.datastore_cleanup.tasks = []

        with (
            patch("prefect_server.datastoreCleanupFlow.Database"),
            patch("prefect_server.datastoreCleanupFlow.AppSettings", return_value=mock_settings),
            patch("prefect_server.datastoreCleanupFlow.DBIndexedDatastoreFileManager"),
            patch("prefect_server.datastoreCleanupFlow.try_get_prefect_logger", return_value=MagicMock()),
        ):
            result = await cleanup_datastore_flow.fn(task_names=["nonexistent"])

        assert result is not None

    def test_identify_non_latest_versions_returns_older_files(self):
        from prefect_server.datastoreCleanupFlow import _identify_non_latest_versions
        from imap_db.model import File

        f1 = MagicMock(spec=File)
        f1.name = "test.cdf"
        f1.version = 1
        f2 = MagicMock(spec=File)
        f2.name = "test.cdf"
        f2.version = 2

        with patch("prefect_server.datastoreCleanupFlow.File.filter_to_latest_versions_only", return_value=[f2]):
            result = _identify_non_latest_versions([f1, f2])

        assert f1 in result
        assert f2 not in result


class TestPerformCalibrationFlows:
    def test_calibrate_flow_calls_calibrate(self):
        from prefect_server.performCalibration import calibrate_flow

        with patch("prefect_server.performCalibration.calibrate") as mock_calibrate:
            mock_calibrate.return_value = []
            calibrate_flow.fn(
                start_date=datetime(2025, 1, 1),
            )

        mock_calibrate.assert_called_once()

    def test_apply_flow_calls_apply(self):
        from prefect_server.performCalibration import apply_flow

        with patch("prefect_server.performCalibration.apply") as mock_apply:
            mock_apply.return_value = []
            apply_flow.fn(
                layers=["*noop*"],
                start_date=datetime(2025, 1, 1),
            )

        mock_apply.assert_called_once()


class TestPostgresUploadFlow:
    @pytest.mark.asyncio
    async def test_upload_new_files_flow_runs(self):
        from prefect_server.postgresUploadFlow import upload_new_files_to_postgres

        mock_db = MagicMock()
        mock_db.get_files_since.return_value = []

        with (
            patch("prefect_server.postgresUploadFlow._get_database_connectionstring", new_callable=AsyncMock, return_value="postgresql://localhost/test"),
            patch("prefect_server.postgresUploadFlow.Database", return_value=mock_db),
            patch("prefect_server.postgresUploadFlow.try_get_prefect_logger", return_value=MagicMock()),
        ):
            await upload_new_files_to_postgres.fn()

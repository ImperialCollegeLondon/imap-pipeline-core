"""Tests for performCalibration flows, datastoreCleanupFlow, postgresUploadFlow,
prefectUtils secrets, pollIALiRT helpers, and check_ialirt CLI."""

import asyncio
import os
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from imap_mag.util import ScienceMode


# ---------------------------------------------------------------------------
# performCalibration additional flow tests
# ---------------------------------------------------------------------------


class TestPerformCalibrationFlowNames:
    def test_generate_calibrate_and_apply_name_includes_date_and_sensor(self):
        from prefect_server.performCalibration import (
            generate_calibrate_and_apply_flow_run_name,
        )

        mock_params = {
            "start_date": datetime(2025, 1, 15),
            "end_date": None,
            "method": MagicMock(value="kepko"),
            "mode": MagicMock(value="norm"),
            "sensor": MagicMock(value="mago"),
        }

        with patch("prefect_server.performCalibration.flow_run") as mock_flow_run:
            mock_flow_run.parameters = mock_params
            result = generate_calibrate_and_apply_flow_run_name()

        assert "15-01-2025" in result
        assert "kepko" in result

    def test_generate_calibration_flow_name_with_date_range(self):
        from prefect_server.performCalibration import (
            generate_calibration_flow_run_name,
        )
        from prefect_server.constants import PREFECT_CONSTANTS

        mock_params = {
            "start_date": datetime(2025, 1, 1),
            "end_date": datetime(2025, 1, 31),
            "method": MagicMock(value="kepko"),
            "mode": MagicMock(value="norm"),
            "sensor": MagicMock(value="mago"),
        }

        with patch("prefect_server.performCalibration.flow_run") as mock_flow_run:
            mock_flow_run.flow_name = PREFECT_CONSTANTS.FLOW_NAMES.CALIBRATE
            mock_flow_run.parameters = mock_params
            result = generate_calibration_flow_run_name()

        assert "01-01-2025" in result
        assert "31-01-2025" in result

    def test_generate_apply_calibration_name_truncates_many_layers(self):
        from prefect_server.performCalibration import (
            generate_apply_calibration_flow_run_name,
        )

        mock_params = {
            "start_date": datetime(2025, 1, 15),
            "end_date": None,
            "layers": ["layer1", "layer2", "layer3", "layer4", "layer5"],
        }

        with patch("prefect_server.performCalibration.flow_run") as mock_flow_run:
            mock_flow_run.parameters = mock_params
            result = generate_apply_calibration_flow_run_name()

        assert "+2" in result  # truncation indicator

    def test_gradiometry_flow_calls_gradiometry(self):
        from prefect_server.performCalibration import gradiometry_flow

        with patch("prefect_server.performCalibration.gradiometry") as mock_gradiometry:
            gradiometry_flow.fn(
                start_date=datetime(2025, 1, 1),
                mode=ScienceMode.Normal,
            )

        mock_gradiometry.assert_called_once()

    def test_calibrate_and_apply_flow_calls_both(self):
        from prefect_server.performCalibration import calibrate_and_apply_flow

        mock_layer = MagicMock()
        mock_layer.metadata.science = ["test_science.cdf"]

        with (
            patch(
                "prefect_server.performCalibration.calibrate",
                return_value=[Path("layer.json")],
            ) as mock_calibrate,
            patch(
                "prefect_server.performCalibration.CalibrationLayer.from_file",
                return_value=mock_layer,
            ),
            patch("prefect_server.performCalibration.apply") as mock_apply,
        ):
            calibrate_and_apply_flow.fn(
                start_date=datetime(2025, 1, 1),
            )

        mock_calibrate.assert_called_once()
        mock_apply.assert_called_once()


# ---------------------------------------------------------------------------
# datastoreCleanupFlow tests
# ---------------------------------------------------------------------------


class TestDatastoreCleanupFlow:
    def test_identify_non_latest_versions_returns_older_files(self):
        from imap_db.model import File
        from prefect_server.datastoreCleanupFlow import _identify_non_latest_versions

        f1 = MagicMock(spec=File)
        f1.name = "test_v001.cdf"
        f1.version = "v001"
        f1.deletion_date = None

        f2 = MagicMock(spec=File)
        f2.name = "test_v002.cdf"
        f2.version = "v002"
        f2.deletion_date = None

        with patch.object(File, "filter_to_latest_versions_only", return_value=[f2]):
            non_latest = _identify_non_latest_versions([f1, f2])

        assert f1 in non_latest
        assert f2 not in non_latest

    def test_get_files_to_cleanup_returns_empty_when_no_files(self):
        from prefect_server.datastoreCleanupFlow import _get_files_to_cleanup

        mock_task = MagicMock()
        result = _get_files_to_cleanup([], mock_task)
        assert result == []

    def test_get_files_to_cleanup_filters_by_age(self):
        from datetime import timedelta

        from prefect_server.datastoreCleanupFlow import _get_files_to_cleanup

        mock_task = MagicMock()
        mock_task.keep_latest_version_only = False
        mock_task.get_file_age_cutoff.return_value = datetime(2025, 6, 1, tzinfo=UTC)

        old_file = MagicMock()
        old_file.last_modified_date = datetime(2025, 1, 1, tzinfo=UTC)

        new_file = MagicMock()
        new_file.last_modified_date = datetime(2025, 12, 1, tzinfo=UTC)

        result = _get_files_to_cleanup([old_file, new_file], mock_task)

        assert old_file in result
        assert new_file not in result

    def test_cleanup_flow_returns_skipped_when_no_tasks_match(self):
        from prefect_server.datastoreCleanupFlow import cleanup_datastore_flow

        mock_settings = MagicMock()
        mock_settings.datastore_cleanup.tasks = []
        mock_settings.datastore_cleanup.dry_run = False

        mock_db = MagicMock()

        with (
            patch(
                "prefect_server.datastoreCleanupFlow.AppSettings",
                return_value=mock_settings,
            ),
            patch(
                "prefect_server.datastoreCleanupFlow.Database",
                return_value=mock_db,
            ),
            patch(
                "prefect_server.datastoreCleanupFlow.DBIndexedDatastoreFileManager",
            ),
        ):
            result = asyncio.get_event_loop().run_until_complete(
                cleanup_datastore_flow.fn(task_names=["nonexistent_task"])
            )

        assert result is not None


# ---------------------------------------------------------------------------
# postgresUploadFlow tests
# ---------------------------------------------------------------------------


class TestPostgresUploadFlow:
    def test_get_database_connectionstring_from_env_var(self):
        from prefect_server.postgresUploadFlow import _get_database_connectionstring

        mock_settings = MagicMock()
        mock_settings.postgres_upload.database_url_env_var_or_block_name = "MY_DB_URL"

        with patch.dict(os.environ, {"MY_DB_URL": "postgresql://localhost/test"}):
            result = asyncio.get_event_loop().run_until_complete(
                _get_database_connectionstring(mock_settings, "MY_DB_URL")
            )

        assert result == "postgresql://localhost/test"

    def test_get_database_connectionstring_raises_without_connection(self):
        from prefect_server.postgresUploadFlow import _get_database_connectionstring

        mock_settings = MagicMock()
        mock_settings.postgres_upload.database_url_env_var_or_block_name = None

        with pytest.raises((ValueError, RuntimeError)):
            asyncio.get_event_loop().run_until_complete(
                _get_database_connectionstring(mock_settings, None)
            )

    def test_get_database_connectionstring_converts_psycopg_url(self):
        from prefect_server.postgresUploadFlow import _get_database_connectionstring

        mock_settings = MagicMock()

        with patch.dict(
            os.environ, {"DB_URL": "postgresql+psycopg://user:pass@host/db"}
        ):
            result = asyncio.get_event_loop().run_until_complete(
                _get_database_connectionstring(mock_settings, "DB_URL")
            )

        assert "postgresql://" in result
        assert "+psycopg" not in result

    def test_upload_flow_with_no_files_completes(self):
        from prefect_server.postgresUploadFlow import upload_new_files_to_postgres

        mock_db = MagicMock()
        mock_db.get_files_since.return_value = []
        mock_db.get_workflow_progress.return_value = MagicMock(
            progress_timestamp=datetime(2025, 1, 1, tzinfo=UTC)
        )

        mock_settings = MagicMock()
        mock_settings.postgres_upload.paths_to_match = ["**/*.cdf"]
        mock_settings.postgres_upload.crump_config_path = MagicMock()
        mock_settings.postgres_upload.crump_config_path.exists.return_value = True

        mock_crump_config = MagicMock()

        with (
            patch(
                "prefect_server.postgresUploadFlow._get_database_connectionstring",
                new_callable=AsyncMock,
                return_value="postgresql://localhost/test",
            ),
            patch(
                "prefect_server.postgresUploadFlow.Database", return_value=mock_db
            ),
            patch(
                "prefect_server.postgresUploadFlow.AppSettings",
                return_value=mock_settings,
            ),
            patch("prefect_server.postgresUploadFlow.try_get_prefect_logger", return_value=MagicMock()),
            patch("prefect_server.postgresUploadFlow.CrumpConfig.from_yaml", return_value=mock_crump_config),
        ):
            asyncio.get_event_loop().run_until_complete(
                upload_new_files_to_postgres.fn(
                    db_env_name_or_block_name_or_block="postgresql://localhost/test"
                )
            )

        mock_db.get_files_since.assert_called_once()


# ---------------------------------------------------------------------------
# prefectUtils advanced tests
# ---------------------------------------------------------------------------


class TestGetSecretOrEnvVar:
    def test_returns_env_var_when_not_in_prefect_context(self):
        from prefect_server.prefectUtils import get_secret_or_env_var

        with patch.dict(os.environ, {"MY_VAR": "my_value"}):
            with patch("prefect.context.FlowRunContext.get", return_value=None), patch(
                "prefect.context.TaskRunContext.get", return_value=None
            ):
                result = asyncio.get_event_loop().run_until_complete(
                    get_secret_or_env_var("secret_name", "MY_VAR")
                )

        assert result == "my_value"

    def test_raises_when_neither_secret_nor_env_var_available(self):
        from prefect_server.prefectUtils import get_secret_or_env_var

        with patch("prefect.context.FlowRunContext.get", return_value=None), patch(
            "prefect.context.TaskRunContext.get", return_value=None
        ):
            env_backup = os.environ.pop("MISSING_VAR", None)
            try:
                with pytest.raises(ValueError, match="both undefined"):
                    asyncio.get_event_loop().run_until_complete(
                        get_secret_or_env_var("secret_name", "MISSING_VAR")
                    )
            finally:
                if env_backup is not None:
                    os.environ["MISSING_VAR"] = env_backup


class TestGetSecretBlock:
    def test_raises_when_secret_block_is_empty(self):
        from prefect_server.prefectUtils import get_secret_block

        mock_secret = MagicMock()
        mock_secret.get.return_value = None

        with patch(
            "prefect_server.prefectUtils.Secret.aload",
            new_callable=AsyncMock,
            return_value=mock_secret,
        ):
            with pytest.raises(ValueError, match="empty"):
                asyncio.get_event_loop().run_until_complete(
                    get_secret_block("empty_secret")
                )

    def test_returns_value_when_secret_exists(self):
        from prefect_server.prefectUtils import get_secret_block

        mock_secret = MagicMock()
        mock_secret.get.return_value = "my_secret_value"

        with patch(
            "prefect_server.prefectUtils.Secret.aload",
            new_callable=AsyncMock,
            return_value=mock_secret,
        ):
            result = asyncio.get_event_loop().run_until_complete(
                get_secret_block("my_secret")
            )

        assert result == "my_secret_value"

    def test_raises_when_secret_block_not_found(self):
        from prefect_server.prefectUtils import get_secret_block

        with patch(
            "prefect_server.prefectUtils.Secret.aload",
            new_callable=AsyncMock,
            side_effect=ValueError("Block not found"),
        ):
            with pytest.raises(ValueError):
                asyncio.get_event_loop().run_until_complete(
                    get_secret_block("nonexistent_secret")
                )


# ---------------------------------------------------------------------------
# pollIALiRT _do_poll helper tests
# ---------------------------------------------------------------------------


class TestDoPollIALiRT:
    def test_do_poll_ialirt_returns_empty_when_no_dates(self):
        from prefect_server.pollIALiRT import do_poll_ialirt

        mock_db = MagicMock()
        mock_logger = MagicMock()

        with patch(
            "prefect_server.pollIALiRT.DownloadDateManager"
        ) as mock_dm_class:
            mock_dm = MagicMock()
            mock_dm.get_dates_for_download.return_value = None
            mock_dm_class.return_value = mock_dm

            result = do_poll_ialirt(
                database=mock_db,
                auth_code="test_auth",
                start_date=None,
                end_date=None,
                force_download=False,
                logger=mock_logger,
            )

        assert result == []

    def test_do_poll_ialirt_returns_file_paths_on_download(self, tmp_path):
        from prefect_server.pollIALiRT import do_poll_ialirt

        mock_db = MagicMock()
        mock_logger = MagicMock()

        test_file = tmp_path / "test_ialirt.cdf"
        test_file.write_bytes(b"cdf")

        mock_handler = MagicMock()
        mock_handler.content_date = datetime(2025, 1, 15)

        with (
            patch("prefect_server.pollIALiRT.DownloadDateManager") as mock_dm_class,
            patch(
                "prefect_server.pollIALiRT.fetch_ialirt",
                return_value={test_file: mock_handler},
            ),
            patch("prefect_server.pollIALiRT.Environment"),
            patch("prefect_server.pollIALiRT.update_database_with_progress"),
        ):
            mock_dm = MagicMock()
            mock_dm.get_dates_for_download.return_value = (
                datetime(2025, 1, 1),
                datetime(2025, 1, 31),
            )
            mock_dm_class.return_value = mock_dm

            result = do_poll_ialirt(
                database=mock_db,
                auth_code="test_auth",
                start_date=None,
                end_date=None,
                force_download=False,
                logger=mock_logger,
            )

        assert len(result) == 1
        assert test_file in result


# ---------------------------------------------------------------------------
# check_ialirt CLI tests
# ---------------------------------------------------------------------------


class TestCheckIALiRTCLI:
    def test_returns_empty_list_when_no_work_files(self, tmp_path):
        from imap_mag.cli.check.check_ialirt import check_ialirt

        mock_settings = MagicMock()
        mock_settings.check_ialirt = MagicMock()
        mock_settings.packet_definition = MagicMock()
        mock_settings.data_store = tmp_path
        mock_settings.setup_work_folder_for_command.return_value = tmp_path

        with (
            patch("imap_mag.cli.check.check_ialirt.AppSettings", return_value=mock_settings),
            patch("imap_mag.cli.check.check_ialirt.initialiseLoggingForCommand"),
            patch(
                "imap_mag.cli.check.check_ialirt.fetch_ialirt_hk_files_for_work",
                return_value=[],
            ),
        ):
            result = check_ialirt(start_date=datetime(2025, 1, 1))

        assert result == []

    def test_returns_anomalies_when_found(self, tmp_path):
        from imap_mag.cli.check.check_ialirt import check_ialirt
        from imap_mag.check import IALiRTAnomaly

        mock_settings = MagicMock()
        mock_settings.check_ialirt = MagicMock()
        mock_settings.packet_definition = MagicMock()
        mock_settings.data_store = tmp_path
        mock_settings.setup_work_folder_for_command.return_value = tmp_path

        fake_file = tmp_path / "test.cdf"
        fake_file.write_bytes(b"cdf")

        mock_anomaly = MagicMock(spec=IALiRTAnomaly)
        mock_anomaly.log = MagicMock()

        with (
            patch("imap_mag.cli.check.check_ialirt.AppSettings", return_value=mock_settings),
            patch("imap_mag.cli.check.check_ialirt.initialiseLoggingForCommand"),
            patch(
                "imap_mag.cli.check.check_ialirt.fetch_ialirt_hk_files_for_work",
                return_value=[fake_file],
            ),
            patch(
                "imap_mag.cli.check.check_ialirt.check_ialirt_files",
                return_value=[mock_anomaly],
            ),
        ):
            result = check_ialirt(
                start_date=datetime(2025, 1, 1),
                error_on_anomaly=False,
            )

        assert len(result) == 1

    def test_raises_anomaly_error_when_configured(self, tmp_path):
        from imap_mag.cli.check.check_ialirt import IALiRTAnomalyError, check_ialirt
        from imap_mag.check import IALiRTAnomaly

        mock_settings = MagicMock()
        mock_settings.check_ialirt = MagicMock()
        mock_settings.packet_definition = MagicMock()
        mock_settings.data_store = tmp_path
        mock_settings.setup_work_folder_for_command.return_value = tmp_path

        fake_file = tmp_path / "test.cdf"
        fake_file.write_bytes(b"cdf")

        mock_anomaly = MagicMock(spec=IALiRTAnomaly)
        mock_anomaly.log = MagicMock()

        with (
            patch("imap_mag.cli.check.check_ialirt.AppSettings", return_value=mock_settings),
            patch("imap_mag.cli.check.check_ialirt.initialiseLoggingForCommand"),
            patch(
                "imap_mag.cli.check.check_ialirt.fetch_ialirt_hk_files_for_work",
                return_value=[fake_file],
            ),
            patch(
                "imap_mag.cli.check.check_ialirt.check_ialirt_files",
                return_value=[mock_anomaly],
            ),
        ):
            with pytest.raises(IALiRTAnomalyError):
                check_ialirt(
                    start_date=datetime(2025, 1, 1),
                    error_on_anomaly=True,
                )

    def test_no_error_when_no_anomalies(self, tmp_path):
        from imap_mag.cli.check.check_ialirt import check_ialirt

        mock_settings = MagicMock()
        mock_settings.check_ialirt = MagicMock()
        mock_settings.packet_definition = MagicMock()
        mock_settings.data_store = tmp_path
        mock_settings.setup_work_folder_for_command.return_value = tmp_path

        fake_file = tmp_path / "test.cdf"
        fake_file.write_bytes(b"cdf")

        with (
            patch("imap_mag.cli.check.check_ialirt.AppSettings", return_value=mock_settings),
            patch("imap_mag.cli.check.check_ialirt.initialiseLoggingForCommand"),
            patch(
                "imap_mag.cli.check.check_ialirt.fetch_ialirt_hk_files_for_work",
                return_value=[fake_file],
            ),
            patch(
                "imap_mag.cli.check.check_ialirt.check_ialirt_files",
                return_value=[],
            ),
        ):
            result = check_ialirt(
                start_date=datetime(2025, 1, 1),
                error_on_anomaly=True,
            )

        assert result == []

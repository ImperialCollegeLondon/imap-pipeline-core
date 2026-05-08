import re
import sys
from datetime import datetime, timedelta
from pathlib import Path
from unittest import mock
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from prefect.exceptions import FailedRun

from imap_mag.check import IALiRTAnomaly, SeverityLevel
from imap_mag.cli.check.check_ialirt import check_ialirt as check_ialirt_cli
from imap_mag.util import CONSTANTS, DatetimeProvider
from prefect_server.checkIALiRT import check_ialirt_flow, send_monthly_test_message
from tests.util.database import test_database  # noqa: F401
from tests.util.miscellaneous import (
    NOW,
    TEST_DATA,
    TODAY,
    YESTERDAY,
    mock_datetime_provider,  # noqa: F401
    temp_datastore,  # noqa: F401
)
from tests.util.prefect_test_utils import (  # noqa: F401
    mock_teams_webhook_block,
    prefect_test_fixture,
)


@pytest.mark.asyncio
async def test_check_ialirt_no_issues(
    temp_datastore,  # noqa: F811
    test_database,  # noqa: F811
    prefect_test_fixture,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
    capture_cli_logs,
):
    # Exercise.
    await check_ialirt_flow(files=[TEST_DATA / "ialirt_hk_data_without_anomalies.csv"])

    # Verify.
    assert "No anomalies detected in I-ALiRT data." in capture_cli_logs.text

    ialirt_check_workflow = test_database.get_workflow_progress(
        CONSTANTS.DATABASE.IALIRT_VALIDATION_ID
    )

    assert ialirt_check_workflow.get_progress_timestamp() is None
    assert ialirt_check_workflow.get_last_checked_date() == NOW


@pytest.mark.skipif(sys.version_info < (3, 13), reason="Requires python3.13 or higher")
@pytest.mark.asyncio
async def test_check_ialirt_with_issues(
    mock_teams_webhook_block: mock.Mock,  # noqa: F811
    temp_datastore,  # noqa: F811
    prefect_test_fixture,  # noqa: F811
    capture_cli_logs,
):
    CONSTANTS.IALIRT_PACKET_DEFINITION_FILE = "ialirt_4.05_unittest.yaml"
    # Exercise.
    with pytest.raises(
        FailedRun, match=re.escape("Anomalies detected in I-ALiRT data.")
    ):
        await check_ialirt_flow(files=[TEST_DATA / "ialirt_hk_data_with_anomalies.csv"])

    # Verify.
    assert "Detected 7 anomalies in I-ALiRT data:" in capture_cli_logs.text

    mock_teams_webhook_block.notify.assert_called()
    assert mock_teams_webhook_block.notify.call_count == 7


@pytest.mark.asyncio
async def test_check_ialirt_no_files_default_dates(
    temp_datastore,  # noqa: F811
    test_database,  # noqa: F811
    prefect_test_fixture,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
    capture_cli_logs,
):
    # Exercise.
    await check_ialirt_flow()

    # Verify.
    assert (
        f"Loading I-ALiRT HK data from {YESTERDAY} to {TODAY}." in capture_cli_logs.text
    )
    assert "No anomalies detected in I-ALiRT data." in capture_cli_logs.text

    ialirt_check_workflow = test_database.get_workflow_progress(
        CONSTANTS.DATABASE.IALIRT_VALIDATION_ID
    )

    assert ialirt_check_workflow.get_progress_timestamp() is None
    assert ialirt_check_workflow.get_last_checked_date() == NOW


# Force the next tests to be at first Monday of the month
NOW_FIRST_MONDAY_OF_MONTH = NOW.replace(
    year=2025,
    month=11,
    day=3,
)


@pytest.fixture(autouse=False)
def mock_datetime_provider_first_monday_of_month(monkeypatch):
    monkeypatch.setattr(DatetimeProvider, "now", lambda: NOW_FIRST_MONDAY_OF_MONTH)


@pytest.mark.skipif(sys.version_info < (3, 13), reason="Requires python3.13 or higher")
@pytest.mark.asyncio
async def test_check_ialirt_first_monday_of_month_first_time(
    temp_datastore,  # noqa: F811
    test_database,  # noqa: F811
    prefect_test_fixture,  # noqa: F811
    mock_datetime_provider_first_monday_of_month,
    mock_teams_webhook_block,  # noqa: F811
    capture_cli_logs,
) -> None:
    # Exercise.
    await check_ialirt_flow(files=[TEST_DATA / "ialirt_hk_data_without_anomalies.csv"])

    # Verify.
    assert "No anomalies detected in I-ALiRT data." in capture_cli_logs.text

    ialirt_check_workflow = test_database.get_workflow_progress(
        CONSTANTS.DATABASE.IALIRT_VALIDATION_ID
    )

    assert ialirt_check_workflow.get_progress_timestamp() == NOW_FIRST_MONDAY_OF_MONTH
    assert ialirt_check_workflow.get_last_checked_date() == NOW_FIRST_MONDAY_OF_MONTH

    mock_teams_webhook_block.notify.assert_called_once()


@pytest.mark.skipif(sys.version_info < (3, 13), reason="Requires python3.13 or higher")
@pytest.mark.asyncio
async def test_check_ialirt_first_monday_of_month_not_first_time(
    temp_datastore,  # noqa: F811
    test_database,  # noqa: F811
    prefect_test_fixture,  # noqa: F811
    mock_datetime_provider_first_monday_of_month,
    mock_teams_webhook_block,  # noqa: F811
    capture_cli_logs,
) -> None:
    # Set up.
    ialirt_check_workflow = test_database.get_workflow_progress(
        CONSTANTS.DATABASE.IALIRT_VALIDATION_ID
    )
    previous_progress_timestamp = NOW_FIRST_MONDAY_OF_MONTH - timedelta(seconds=10)

    ialirt_check_workflow.update_progress_timestamp(previous_progress_timestamp)
    test_database.save(ialirt_check_workflow)

    # Exercise.
    await check_ialirt_flow(files=[TEST_DATA / "ialirt_hk_data_without_anomalies.csv"])

    # Verify.
    assert "No anomalies detected in I-ALiRT data." in capture_cli_logs.text

    ialirt_check_workflow = test_database.get_workflow_progress(
        CONSTANTS.DATABASE.IALIRT_VALIDATION_ID
    )

    assert ialirt_check_workflow.get_progress_timestamp() == previous_progress_timestamp
    assert ialirt_check_workflow.get_last_checked_date() == NOW_FIRST_MONDAY_OF_MONTH

    mock_teams_webhook_block.notify.assert_not_called()


class TestSendMonthlyTestMessage:
    """Unit tests for send_monthly_test_message without Docker."""

    def _make_mock_logger(self):
        return MagicMock()

    @pytest.mark.asyncio
    async def test_sends_notification_on_first_monday_of_month_with_no_previous_progress(
        self,
    ):
        logger = self._make_mock_logger()
        first_monday = datetime(2025, 6, 2, 10, 0, 0)  # first Monday of June 2025

        mock_progress = MagicMock()
        mock_progress.get_progress_timestamp.return_value = None

        mock_db = MagicMock()
        mock_db.get_workflow_progress.return_value = mock_progress

        mock_webhook = AsyncMock()
        mock_webhook.notify = AsyncMock()

        with (
            patch("prefect_server.checkIALiRT.Database", return_value=mock_db),
            patch(
                "prefect_server.checkIALiRT.DatetimeProvider.now",
                return_value=first_monday,
            ),
            patch(
                "prefect_server.checkIALiRT.MicrosoftTeamsWebhook.aload",
                return_value=mock_webhook,
            ),
        ):
            await send_monthly_test_message(logger, "test-webhook")

        mock_webhook.notify.assert_called_once()

    @pytest.mark.asyncio
    async def test_skips_notification_when_not_first_monday(self):
        logger = self._make_mock_logger()
        regular_tuesday = datetime(2025, 6, 3, 10, 0, 0)  # a Tuesday

        mock_progress = MagicMock()
        mock_progress.get_progress_timestamp.return_value = None

        mock_db = MagicMock()
        mock_db.get_workflow_progress.return_value = mock_progress

        with (
            patch("prefect_server.checkIALiRT.Database", return_value=mock_db),
            patch(
                "prefect_server.checkIALiRT.DatetimeProvider.now",
                return_value=regular_tuesday,
            ),
        ):
            await send_monthly_test_message(logger, "test-webhook")

        logger.debug.assert_called_once()

    @pytest.mark.asyncio
    async def test_skips_notification_when_already_sent_today(self):
        logger = self._make_mock_logger()
        first_monday = datetime(2025, 6, 2, 10, 0, 0)

        mock_progress = MagicMock()
        mock_progress.get_progress_timestamp.return_value = (
            first_monday  # already sent today
        )

        mock_db = MagicMock()
        mock_db.get_workflow_progress.return_value = mock_progress

        with (
            patch("prefect_server.checkIALiRT.Database", return_value=mock_db),
            patch(
                "prefect_server.checkIALiRT.DatetimeProvider.now",
                return_value=first_monday,
            ),
        ):
            await send_monthly_test_message(logger, "test-webhook")

        logger.debug.assert_called_once()

    @pytest.mark.asyncio
    async def test_updates_last_checked_timestamp(self):
        logger = self._make_mock_logger()
        regular_tuesday = datetime(2025, 6, 3, 10, 0, 0)

        mock_progress = MagicMock()
        mock_progress.get_progress_timestamp.return_value = None

        mock_db = MagicMock()
        mock_db.get_workflow_progress.return_value = mock_progress

        with (
            patch("prefect_server.checkIALiRT.Database", return_value=mock_db),
            patch(
                "prefect_server.checkIALiRT.DatetimeProvider.now",
                return_value=regular_tuesday,
            ),
        ):
            await send_monthly_test_message(logger, "test-webhook")

        mock_progress.update_last_checked_timestamp.assert_called_once_with(
            regular_tuesday
        )
        mock_db.save.assert_called_once_with(mock_progress)


class TestCheckIALiRTFlowUnit:
    """Unit tests for check_ialirt_flow without Docker."""

    @pytest.mark.asyncio
    async def test_flow_returns_failed_state_when_anomalies_found(self):
        mock_anomaly = MagicMock(spec=IALiRTAnomaly)
        mock_anomaly.severity = SeverityLevel.Danger
        mock_anomaly.get_anomaly_description.return_value = "Test anomaly description"

        mock_webhook = AsyncMock()
        mock_webhook.notify = AsyncMock()

        with (
            patch(
                "prefect_server.checkIALiRT.check_ialirt", return_value=[mock_anomaly]
            ),
            patch(
                "prefect_server.checkIALiRT.MicrosoftTeamsWebhook.aload",
                return_value=mock_webhook,
            ),
            patch("prefect_server.checkIALiRT.flow_run") as mock_flow_run,
        ):
            mock_flow_run.id = "test-run-id"
            result = await check_ialirt_flow.fn(
                files=[Path("/some/file.csv")],
                imap_notification_webhook_name="test-webhook",
            )

        assert result.is_failed()
        mock_webhook.notify.assert_called_once()

    @pytest.mark.asyncio
    async def test_flow_sends_warning_type_for_non_danger_anomaly(self):
        mock_anomaly = MagicMock(spec=IALiRTAnomaly)
        mock_anomaly.severity = SeverityLevel.Warning
        mock_anomaly.get_anomaly_description.return_value = "Warning anomaly"

        mock_webhook = AsyncMock()
        mock_webhook.notify = AsyncMock()

        with (
            patch(
                "prefect_server.checkIALiRT.check_ialirt", return_value=[mock_anomaly]
            ),
            patch(
                "prefect_server.checkIALiRT.MicrosoftTeamsWebhook.aload",
                return_value=mock_webhook,
            ),
            patch("prefect_server.checkIALiRT.flow_run") as mock_flow_run,
        ):
            mock_flow_run.id = "test-run-id"
            await check_ialirt_flow.fn(
                files=[Path("/some/file.csv")],
                imap_notification_webhook_name="test-webhook",
            )

        assert mock_webhook.notify_type == "warning"


class TestCheckIALiRTCLIUnit:
    def test_returns_empty_list_when_no_work_files(self, tmp_path):
        mock_settings = MagicMock()
        mock_settings.check_ialirt = MagicMock()
        mock_settings.packet_definition = MagicMock()
        mock_settings.data_store = tmp_path
        mock_settings.setup_work_folder_for_command.return_value = tmp_path

        with (
            patch(
                "imap_mag.cli.check.check_ialirt.AppSettings",
                return_value=mock_settings,
            ),
            patch("imap_mag.cli.check.check_ialirt.initialiseLoggingForCommand"),
            patch(
                "imap_mag.cli.check.check_ialirt.fetch_ialirt_hk_files_for_work",
                return_value=[],
            ),
        ):
            result = check_ialirt_cli(start_date=datetime(2025, 1, 1))

        assert result == []

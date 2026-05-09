"""Unit tests for checkIALiRT helpers and flow logic."""

from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from imap_mag.check import IALiRTAnomaly, SeverityLevel
from imap_mag.cli.check.check_ialirt import check_ialirt as check_ialirt_cli
from prefect_server.checkIALiRT import check_ialirt_flow, send_monthly_test_message

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

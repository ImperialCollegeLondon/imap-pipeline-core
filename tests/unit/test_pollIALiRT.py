"""Unit tests for pollIALiRT helpers, flow logic, and name generation."""

from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytz

from prefect_server.pollIALiRT import (
    poll_ialirt_flow,
    run_ialirt_polling_pipeline_task,
)


class TestIALiRTPollingTask:
    """Unit tests for run_ialirt_polling_pipeline_task."""

    @pytest.fixture
    def mock_pipeline_deps(self):
        with (
            patch("prefect_server.pollIALiRT.IALiRTPipeline") as mock_pipeline_class,
            patch("prefect_server.pollIALiRT.emit_event") as mock_emit_event,
            patch("prefect_server.pollIALiRT.flow_run") as mock_flow_run,
            patch("prefect_server.pollIALiRT.try_get_prefect_logger"),
        ):
            mock_pipeline = mock_pipeline_class.return_value
            mock_pipeline.run = AsyncMock()

            # default successful result
            mock_result = MagicMock()
            mock_result.success = True
            mock_pipeline.get_results.return_value = mock_result

            mock_flow_run.id = "test-flow-id"
            mock_flow_run.name = "test-flow-name"

            yield {
                "pipeline_class": mock_pipeline_class,
                "pipeline": mock_pipeline,
                "emit": mock_emit_event,
                "result": mock_result,
            }

    @pytest.mark.asyncio
    async def test_successful_run_emits_standard_event(self, mock_pipeline_deps):
        """Test a successful run for a standard instrument."""
        instrument = "mag"

        result = await run_ialirt_polling_pipeline_task.fn(
            instrument=instrument,
            task_start_date=datetime(2025, 1, 1),
            task_end_date=datetime(2025, 1, 2),
            database=MagicMock(),
            settings=MagicMock(),
        )

        assert result.success is True
        mock_pipeline_deps["emit"].assert_called_once()

        # Check that the event was emitted
        call_kwargs = mock_pipeline_deps["emit"].call_args.kwargs
        assert "imap.ialirt.updated" in str(call_kwargs["event"])
        assert call_kwargs["payload"]["instrument"] == "mag"

    @pytest.mark.asyncio
    async def test_successful_run_emits_hk_event(self, mock_pipeline_deps):
        """Test a successful run for an HK instrument."""
        instrument = "mag_hk"

        await run_ialirt_polling_pipeline_task.fn(
            instrument=instrument,
            task_start_date=datetime(2025, 1, 1),
            task_end_date=datetime(2025, 1, 2),
            database=MagicMock(),
            settings=MagicMock(),
        )

        call_kwargs = mock_pipeline_deps["emit"].call_args.kwargs
        assert "imap.ialirt_hk.updated" in str(call_kwargs["event"])

    @pytest.mark.asyncio
    async def test_raises_runtime_error_on_pipeline_failure(self, mock_pipeline_deps):
        """Test that the task raises a RuntimeError if the pipeline fails."""
        mock_pipeline_deps["result"].success = False

        with pytest.raises(RuntimeError, match="I-ALiRT Pipeline failed for mag"):
            await run_ialirt_polling_pipeline_task.fn(
                instrument="mag",
                task_start_date=datetime(2025, 1, 1),
                task_end_date=datetime(2025, 1, 2),
                database=MagicMock(),
                settings=MagicMock(),
            )


class TestPollIALiRTFlow:
    """Unit tests for poll_ialirt_flow."""

    @pytest.fixture
    def mock_flow(self):
        with (
            patch("prefect_server.pollIALiRT.try_get_prefect_logger"),
            patch("prefect_server.pollIALiRT.Database"),
            patch("prefect_server.pollIALiRT.AppSettings"),
            patch(
                "prefect_server.pollIALiRT.get_secret_or_env_var",
                new_callable=AsyncMock,
            ) as mock_secret,
            patch(
                "prefect_server.pollIALiRT.run_ialirt_polling_pipeline_task",
                new_callable=AsyncMock,
            ) as mock_task,
            patch(
                "prefect_server.pollIALiRT.asyncio.sleep", new_callable=AsyncMock
            ) as mock_sleep,
        ):
            mock_secret.return_value = "fake-auth-code"

            with (
                patch("prefect_server.pollIALiRT.VALID_IALIRT_INSTRUMENTS", ["mag"]),
                patch("prefect_server.pollIALiRT.VALID_IALIRT_HK_INSTRUMENTS", ["hit"]),
            ):
                yield {
                    "task": mock_task,
                    "sleep": mock_sleep,
                }

    @pytest.mark.asyncio
    async def test_invalid_date_range_aborts_flow(self, mock_flow):
        """Flow should immediately return Failed if start_date > end_date."""
        mock_run_params = MagicMock()
        mock_run_params.start_date = datetime(2025, 1, 2)
        mock_run_params.end_date = datetime(2025, 1, 1)

        result = await poll_ialirt_flow.fn(
            run_parameters=mock_run_params, datetime_provider=MagicMock()
        )

        assert result.is_failed()
        assert "start_date is after end_date" in result.message  # pyright: ignore[reportOperatorIssue]

    @pytest.mark.asyncio
    async def test_single_batch_when_wait_for_new_data_is_false(self, mock_flow):
        """Flow should run exactly one batch and break if wait_for_new_data_to_arrive is False."""
        mock_run_params = MagicMock()
        mock_run_params.start_date = datetime(2025, 1, 1, 12, 0)
        mock_run_params.end_date = datetime(2025, 1, 1, 13, 0)

        mock_dp = MagicMock()
        mock_dp.now.return_value = datetime(2025, 1, 1, 12, 5)

        #  return a dummy successful coroutine
        mock_flow["task"].return_value = AsyncMock(return_value="Success")()

        result = await poll_ialirt_flow.fn(
            run_parameters=mock_run_params,
            datetime_provider=mock_dp,
            wait_for_new_data_to_arrive=False,
            plot_last_3_days=False,
        )

        assert mock_flow["task"].call_count == 2  # Called for 2 instruments
        assert result.is_completed()

    @pytest.mark.asyncio
    async def test_skips_batch_if_future_window(self, mock_flow):
        """Flow should sleep and skip task creation if the current window is in the future."""
        mock_run_params = MagicMock()
        mock_run_params.start_date = datetime(2025, 1, 1, 12, 10)
        mock_run_params.end_date = datetime(2025, 1, 1, 13, 0)

        # mock datetime_provider
        mock_dp = MagicMock()
        mock_dp.now.side_effect = [
            datetime(2025, 1, 1, 12, 0),  # before start_date, should skip
            datetime(2025, 1, 1, 13, 5),  # after end_date, should break
        ]

        await poll_ialirt_flow.fn(
            run_parameters=mock_run_params,
            datetime_provider=mock_dp,
            timeout_seconds=300,
            wait_for_new_data_to_arrive=False,
            plot_last_3_days=False,
        )

        # check it skipped the first
        mock_flow["sleep"].assert_called_once_with(300)

        # assert it ran the task for 2 instruments for the second datetime
        assert mock_flow["task"].call_count == 2

    @pytest.mark.asyncio
    async def test_fails_when_all_batch_tasks_raise_exceptions(self, mock_flow):
        """Flow should return Failed if every instrument pipeline throws an exception."""
        mock_run_params = MagicMock()
        mock_run_params.start_date = datetime(2025, 1, 1, 12, 0)
        mock_run_params.end_date = datetime(2025, 1, 1, 13, 0)

        mock_dp = MagicMock()
        mock_dp.now.return_value = datetime(2025, 1, 1, 12, 5)

        async def mock_failed_task(*args, **kwargs):
            raise RuntimeError("API Offline")

        mock_flow["task"].side_effect = mock_failed_task

        result = await poll_ialirt_flow.fn(
            run_parameters=mock_run_params,
            datetime_provider=mock_dp,
            wait_for_new_data_to_arrive=False,
            plot_last_3_days=False,
        )

        assert result.is_failed()
        assert "All instrument pipelines failed" in result.message  # type: ignore

    @pytest.mark.asyncio
    async def test_teams_webhook_fires_at_6am_uk_time(self, mock_flow):
        """Ensure Quicklook triggers and Webhook fires specifically at 6 AM UK time."""
        # 06:00 UK time
        utc_6am = datetime(2025, 1, 1, 6, 0, tzinfo=pytz.UTC)

        mock_run_params = MagicMock()
        mock_run_params.start_date = utc_6am - timedelta(hours=1)
        mock_run_params.end_date = utc_6am

        mock_dp = MagicMock()
        mock_dp.now.return_value = utc_6am + timedelta(minutes=5)
        mock_dp.today.return_value = datetime(2025, 1, 1)

        mock_flow["task"].return_value = AsyncMock(return_value="Success")()

        with (
            patch(
                "prefect_server.pollIALiRT.quicklook_ialirt_flow",
                new_callable=AsyncMock,
            ) as mock_quicklook,
            patch(
                "prefect_server.pollIALiRT.MicrosoftTeamsWebhook"
            ) as mock_webhook_class,
            patch("prefect_server.pollIALiRT.UTC", pytz.UTC),
        ):
            mock_webhook_block = MagicMock()
            mock_webhook_block.notify = AsyncMock()

            mock_webhook_class.aload = AsyncMock(return_value=mock_webhook_block)

            await poll_ialirt_flow.fn(
                run_parameters=mock_run_params,
                datetime_provider=mock_dp,
                wait_for_new_data_to_arrive=True,
                plot_last_3_days=True,
                imap_notification_webhook_name="test-webhook",
            )

            mock_quicklook.assert_called_once()

            mock_webhook_class.aload.assert_called_once_with("test-webhook")
            mock_webhook_block.notify.assert_called_once()

            call_kwargs = mock_webhook_block.notify.call_args.kwargs
            assert call_kwargs["subject"] == "I-ALiRT Latest Quicklook"

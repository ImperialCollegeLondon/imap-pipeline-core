"""Unit tests for pollIALiRT helpers, flow logic, and name generation."""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from prefect_server.pollIALiRT import (
    AutomaticRunParameters,
    FetchByDatesRunParameters,
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
            database=MagicMock(),
            settings=MagicMock(),
            run_parameters=AutomaticRunParameters(),
        )

        assert result.success is True
        mock_pipeline_deps["pipeline"].build.assert_called_once()
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
            database=MagicMock(),
            settings=MagicMock(),
            run_parameters=AutomaticRunParameters(),
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
                database=MagicMock(),
                settings=MagicMock(),
                run_parameters=AutomaticRunParameters(),
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
            mock_task.return_value = "Success"

            with (
                patch("prefect_server.pollIALiRT.VALID_IALIRT_INSTRUMENTS", ["mag"]),
                patch("prefect_server.pollIALiRT.VALID_IALIRT_HK_INSTRUMENTS", ["hit"]),
            ):
                yield {
                    "task": mock_task,
                    "sleep": mock_sleep,
                }

    @pytest.mark.asyncio
    async def test_raises_value_error_when_waiting_with_non_automatic_run_parameters(
        self, mock_flow
    ):
        """Flow should raise ValueError if waiting for new data with non-Automatic run parameters."""
        bounded_params = FetchByDatesRunParameters(
            start_date=datetime(2025, 1, 1), end_date=datetime(2025, 1, 2)
        )

        with pytest.raises(
            ValueError, match="run_parameters must be of type Automatic Run"
        ):
            await poll_ialirt_flow.fn(
                run_parameters=bounded_params,
                wait_for_new_data_to_arrive_up_to_an_hour=True,
            )

    @pytest.mark.asyncio
    async def test_single_batch_when_wait_for_new_data_is_false(self, mock_flow):
        """Flow should run exactly one batch and break if wait_for_new_data_to_arrive_up_to_an_hour is False."""
        mock_dp = MagicMock()
        mock_dp.end_of_hour.return_value = datetime(2025, 1, 1, 13, 0)

        result = await poll_ialirt_flow.fn(
            run_parameters=AutomaticRunParameters(),
            datetime_provider=mock_dp,
            wait_for_new_data_to_arrive_up_to_an_hour=False,
            plot_last_3_days=False,
        )

        assert mock_flow["task"].call_count == 2  # Called for 2 instruments
        mock_flow["sleep"].assert_not_called()
        assert result.is_completed()

    @pytest.mark.asyncio
    async def test_multiple_iterations_sleep_between_batches_when_waiting(
        self, mock_flow
    ):
        """Flow should sleep between iterations and stop once nearing the end of the hour."""
        mock_dp = MagicMock()
        mock_dp.end_of_hour.return_value = datetime(2025, 1, 1, 13, 0)
        mock_dp.now.side_effect = [
            datetime(2025, 1, 1, 12, 0),  # plenty of time left, sleep and loop again
            datetime(2025, 1, 1, 12, 53),  # close to end of hour, stop
        ]

        result = await poll_ialirt_flow.fn(
            run_parameters=AutomaticRunParameters(),
            datetime_provider=mock_dp,
            polling_interval_seconds=300,
            wait_for_new_data_to_arrive_up_to_an_hour=True,
            plot_last_3_days=False,
        )

        mock_flow["sleep"].assert_called_once_with(300)
        assert mock_flow["task"].call_count == 4  # 2 instruments x 2 iterations
        assert result.is_completed()

    @pytest.mark.asyncio
    async def test_fails_when_all_batch_tasks_raise_exceptions(self, mock_flow):
        """Flow should return Failed if every instrument pipeline throws an exception."""
        mock_dp = MagicMock()
        mock_dp.end_of_hour.return_value = datetime(2025, 1, 1, 13, 0)

        async def mock_failed_task(*args, **kwargs):
            raise RuntimeError("API Offline")

        mock_flow["task"].side_effect = mock_failed_task

        result = await poll_ialirt_flow.fn(
            run_parameters=AutomaticRunParameters(),
            datetime_provider=mock_dp,
            wait_for_new_data_to_arrive_up_to_an_hour=False,
            plot_last_3_days=False,
        )

        assert result.is_failed()
        assert "All I-ALiRT downloads failed" in result.message  # type: ignore

    @pytest.mark.asyncio
    async def test_teams_webhook_fires_at_6am_uk_time(self, mock_flow):
        """Ensure Quicklook triggers and Webhook fires specifically at 6 AM UK time."""
        mock_dp = MagicMock()
        # end_of_hour - polling_interval_seconds (300s default) lands on 06:00 UTC == 06:00 UK (winter)
        mock_dp.end_of_hour.return_value = datetime(2025, 1, 1, 6, 5)
        mock_dp.now.return_value = datetime(2025, 1, 1, 6, 0)
        mock_dp.today.return_value = datetime(2025, 1, 1)

        with (
            patch(
                "prefect_server.pollIALiRT.quicklook_ialirt_flow",
                new_callable=AsyncMock,
            ) as mock_quicklook,
            patch(
                "prefect_server.pollIALiRT.MicrosoftTeamsWebhook"
            ) as mock_webhook_class,
        ):
            mock_webhook_block = MagicMock()
            mock_webhook_block.notify = AsyncMock()

            mock_webhook_class.aload = AsyncMock(return_value=mock_webhook_block)

            await poll_ialirt_flow.fn(
                run_parameters=AutomaticRunParameters(),
                datetime_provider=mock_dp,
                wait_for_new_data_to_arrive_up_to_an_hour=True,
                plot_last_3_days=True,
                imap_notification_webhook_name="test-webhook",
            )

            mock_quicklook.assert_called_once()

            mock_webhook_class.aload.assert_called_once_with("test-webhook")
            mock_webhook_block.notify.assert_called_once()

            call_kwargs = mock_webhook_block.notify.call_args.kwargs
            assert call_kwargs["subject"] == "I-ALiRT Latest Quicklook"

    @pytest.mark.asyncio
    async def test_teams_webhook_does_not_fire_outside_6am_uk_time(self, mock_flow):
        """Quicklook still runs but the Teams webhook should not fire outside 6 AM UK time."""
        mock_dp = MagicMock()
        mock_dp.end_of_hour.return_value = datetime(2025, 1, 1, 12, 5)
        mock_dp.now.return_value = datetime(2025, 1, 1, 12, 0)
        mock_dp.today.return_value = datetime(2025, 1, 1)

        with (
            patch(
                "prefect_server.pollIALiRT.quicklook_ialirt_flow",
                new_callable=AsyncMock,
            ) as mock_quicklook,
            patch(
                "prefect_server.pollIALiRT.MicrosoftTeamsWebhook"
            ) as mock_webhook_class,
        ):
            mock_webhook_class.aload = AsyncMock()

            await poll_ialirt_flow.fn(
                run_parameters=AutomaticRunParameters(),
                datetime_provider=mock_dp,
                wait_for_new_data_to_arrive_up_to_an_hour=True,
                plot_last_3_days=True,
            )

            mock_quicklook.assert_called_once()
            mock_webhook_class.aload.assert_not_called()

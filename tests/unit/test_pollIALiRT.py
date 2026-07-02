"""Unit tests for pollIALiRT helpers, flow logic, and name generation."""

from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from imap_mag.util.DatetimeProvider import DatetimeProvider
from prefect_server.pollIALiRT import (
    generate_flow_run_name,
    poll_ialirt_flow,
    run_ialirt_polling_pipeline_task,
)


class TestRunIalirtPollingPipelineTask:
    """
    Unit tests for run_ialirt_polling_pipeline_task.
    """

    @pytest.mark.asyncio
    @patch("prefect_server.pollIALiRT.IALiRTPipeline")
    @patch("prefect_server.pollIALiRT.emit_event")
    @patch("prefect_server.pollIALiRT.flow_run")
    async def test_task_runs_pipeline_successfully_and_emits_event(
        self, mock_flow_run, mock_emit_event, mock_pipeline_class
    ):
        mock_flow_run.id = "test-flow-id"
        mock_flow_run.name = "test-flow-name"

        mock_pipeline_inst = MagicMock()
        mock_pipeline_class.return_value = mock_pipeline_inst
        mock_result = MagicMock()
        mock_result.success = True
        mock_pipeline_inst.get_results.return_value = mock_result
        mock_pipeline_inst.run = AsyncMock()

        await run_ialirt_polling_pipeline_task.fn(
            instrument="mag",
            run_parameters=MagicMock(),
            database=MagicMock(),
            settings=MagicMock(),
        )

        mock_emit_event.assert_called_once()
        called_kwargs = mock_emit_event.call_args.kwargs
        assert called_kwargs["event"] == "imap.ialirt.updated"

    @pytest.mark.asyncio
    @patch("prefect_server.pollIALiRT.IALiRTPipeline")
    @patch("prefect_server.pollIALiRT.emit_event")
    @patch("prefect_server.pollIALiRT.flow_run")
    async def test_task_emits_hk_event_for_housekeeping_instruments(
        self, mock_flow_run, mock_emit_event, mock_pipeline_class
    ):
        mock_pipeline_inst = MagicMock()
        mock_pipeline_class.return_value = mock_pipeline_inst
        mock_result = MagicMock()
        mock_result.success = True
        mock_pipeline_inst.get_results.return_value = mock_result
        mock_pipeline_inst.run = AsyncMock()

        await run_ialirt_polling_pipeline_task.fn(
            instrument="mag_hk",
            run_parameters=MagicMock(),
            database=MagicMock(),
            settings=MagicMock(),
        )

        mock_emit_event.assert_called_once()
        called_kwargs = mock_emit_event.call_args.kwargs
        assert called_kwargs["event"] == "imap.ialirt_hk.updated"

    @pytest.mark.asyncio
    @patch("prefect_server.pollIALiRT.try_get_prefect_logger")
    @patch("prefect_server.pollIALiRT.IALiRTPipeline")
    @patch("prefect_server.pollIALiRT.emit_event", return_value=None)
    @patch("prefect_server.pollIALiRT.flow_run")
    async def test_logs_error_when_event_emission_fails(
        self, mock_flow_run, mock_emit_event, mock_pipeline_class, mock_logger_func
    ):
        mock_logger = MagicMock()
        mock_logger_func.return_value = mock_logger

        mock_pipeline_inst = MagicMock()
        mock_pipeline_class.return_value = mock_pipeline_inst
        mock_result = MagicMock()
        mock_result.success = True
        mock_pipeline_inst.get_results.return_value = mock_result
        mock_pipeline_inst.run = AsyncMock()

        await run_ialirt_polling_pipeline_task.fn(
            instrument="mag",
            run_parameters=MagicMock(),
            database=MagicMock(),
            settings=MagicMock(),
        )

        mock_logger.error.assert_called_once()
        assert "Failed to emit" in mock_logger.error.call_args[0][0]


class TestPollIALiRTFlowUnit:
    """Unit tests for poll_ialirt_flow without Docker."""

    @pytest.mark.asyncio
    @patch("prefect_server.pollIALiRT.try_get_prefect_logger")
    @patch("prefect_server.pollIALiRT.Database")
    @patch("prefect_server.pollIALiRT.AppSettings")
    @patch("prefect_server.pollIALiRT.get_secret_or_env_var", new_callable=AsyncMock)
    @patch(
        "prefect_server.pollIALiRT.run_ialirt_polling_pipeline_task",
        new_callable=AsyncMock,
    )
    @patch("prefect_server.pollIALiRT.quicklook_ialirt_flow", new_callable=AsyncMock)
    async def test_flow_generates_quicklook_when_plot_last_3_days_true(
        self,
        mock_quicklook,
        mock_task,
        mock_secret,
        mock_settings,
        mock_db,
        mock_logger,
    ):
        base_time = datetime(2025, 1, 1, 12, 0, 0)

        def dynamic_now():
            nonlocal base_time
            base_time += timedelta(minutes=5)
            return base_time

        mock_dp = MagicMock()
        mock_dp.now.side_effect = dynamic_now
        mock_dp.today.return_value = datetime(2025, 1, 1).date()

        mock_secret.return_value = "auth-code"
        mock_task.return_value = MagicMock()

        await poll_ialirt_flow.fn(
            wait_for_new_data_to_arrive=True,
            plot_last_3_days=True,
            datetime_provider=mock_dp,
            imap_notification_webhook_name="test-webhook",
        )

        mock_quicklook.assert_awaited_once()


class TestPollIALiRTHKFlowUnit:
    """Unit tests handling loop exiting and polling conditions."""

    @pytest.mark.asyncio
    @patch("prefect_server.pollIALiRT.try_get_prefect_logger")
    @patch("prefect_server.pollIALiRT.Database")
    @patch("prefect_server.pollIALiRT.AppSettings")
    @patch("prefect_server.pollIALiRT.get_secret_or_env_var", new_callable=AsyncMock)
    @patch(
        "prefect_server.pollIALiRT.run_ialirt_polling_pipeline_task",
        new_callable=AsyncMock,
    )
    @patch("prefect_server.pollIALiRT.asyncio.sleep", new_callable=AsyncMock)
    async def test_flow_polls_once_when_wait_for_new_data_is_false(
        self, mock_sleep, mock_task, mock_secret, mock_settings, mock_db, mock_logger
    ):
        mock_dp = MagicMock()
        mock_dp.now.return_value = datetime(2025, 1, 1, 12, 0, 0)
        mock_secret.return_value = "auth-code"
        mock_task.return_value = MagicMock()

        await poll_ialirt_flow.fn(
            wait_for_new_data_to_arrive=False,
            plot_last_3_days=False,
            datetime_provider=mock_dp,
        )

        # Assert it polls immediately but does not sleep/loop
        assert mock_task.call_count > 0
        mock_sleep.assert_not_awaited()

    @pytest.mark.asyncio
    @patch("prefect_server.pollIALiRT.try_get_prefect_logger")
    @patch("prefect_server.pollIALiRT.Database")
    @patch("prefect_server.pollIALiRT.AppSettings")
    @patch("prefect_server.pollIALiRT.get_secret_or_env_var", new_callable=AsyncMock)
    @patch(
        "prefect_server.pollIALiRT.run_ialirt_polling_pipeline_task",
        new_callable=AsyncMock,
    )
    @patch("prefect_server.pollIALiRT.asyncio.sleep", new_callable=AsyncMock)
    async def test_flow_polls_once_when_wait_for_new_data_is_true_and_exits(
        self, mock_sleep, mock_task, mock_secret, mock_settings, mock_db, mock_logger
    ):
        now_calls = [
            datetime(2025, 1, 1, 12, 0, 0),
            datetime(2025, 1, 1, 12, 56, 0),
        ]

        def fake_now():
            return now_calls.pop(0) if now_calls else datetime(2025, 1, 1, 13, 0, 0)

        mock_dp = DatetimeProvider()
        mock_secret.return_value = "auth-code"

        mock_run_params = MagicMock()
        mock_run_params.start_date = datetime(2025, 1, 1, 11, 0, 0)
        mock_run_params.end_date = datetime(2025, 1, 1, 12, 55, 0)

        async def dummy_pipeline_task(*args, **kwargs):
            return MagicMock(success=True)

        mock_task.side_effect = dummy_pipeline_task

        with patch.object(mock_dp, "now", side_effect=fake_now):
            await poll_ialirt_flow.fn(
                run_parameters=mock_run_params,
                wait_for_new_data_to_arrive=True,
                plot_last_3_days=False,
                datetime_provider=mock_dp,
            )

        assert mock_task.call_count > 0


class TestPollIALiRTGenerateName:
    """Unit tests for flow name generation."""

    def test_name_with_no_dates_uses_last_update(self):
        mock_params = {"start_date": None, "end_date": None}
        mock_dp = MagicMock()
        mock_dp.end_of_hour.return_value = datetime(2025, 6, 1, 13, 0, 0)

        with patch("prefect_server.pollIALiRT.flow_run") as mock_flow_run:
            mock_flow_run.parameters = mock_params
            name = generate_flow_run_name(datetime_provider=mock_dp)

        assert "last-update" in name

    def test_name_with_dates(self):
        mock_params = {
            "start_date": datetime(2025, 6, 1, 12, 0, 0),
            "end_date": datetime(2025, 6, 1, 13, 0, 0),
        }
        with patch("prefect_server.pollIALiRT.flow_run") as mock_flow_run:
            mock_flow_run.parameters = mock_params
            name = generate_flow_run_name()

        assert "01-06-2025" in name

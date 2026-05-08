"""Tests for prefect server components: constants, publishFlow, quicklookIALiRT, pollSpinTable."""

import os
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from prefect_server.constants import PREFECT_CONSTANTS


class TestPrefectConstants:
    def test_flow_names_are_defined(self):
        assert PREFECT_CONSTANTS.FLOW_NAMES.POLL_IALIRT == "poll-ialirt"
        assert PREFECT_CONSTANTS.FLOW_NAMES.POLL_HK == "poll-hk"
        assert PREFECT_CONSTANTS.FLOW_NAMES.POLL_SCIENCE == "poll-science"

    def test_queue_names_are_defined(self):
        assert PREFECT_CONSTANTS.QUEUES.HIGH_PRIORITY is not None
        assert PREFECT_CONSTANTS.QUEUES.DEFAULT is not None
        assert PREFECT_CONSTANTS.QUEUES.LOW is not None

    def test_block_names_are_defined(self):
        assert PREFECT_CONSTANTS.IMAP_DATABASE_BLOCK_NAME is not None
        assert PREFECT_CONSTANTS.IMAP_WEBHOOK_BLOCK_NAME is not None

    def test_env_var_names_are_defined(self):
        assert PREFECT_CONSTANTS.ENV_VAR_NAMES.SQLALCHEMY_URL == "SQLALCHEMY_URL"
        assert PREFECT_CONSTANTS.ENV_VAR_NAMES.POLL_IALIRT_CRON == "IMAP_CRON_POLL_IALIRT"


class TestPublishFlow:
    @pytest.mark.asyncio
    async def test_publish_flow_calls_publish_with_auth_code(self):
        from prefect_server.publishFlow import publish_flow

        mock_publish = MagicMock()

        with (
            patch("prefect_server.publishFlow.get_secret_or_env_var", new_callable=AsyncMock, return_value="test_auth_code"),
            patch("prefect_server.publishFlow.publish", mock_publish),
            patch("prefect_server.publishFlow.try_get_prefect_logger", return_value=MagicMock()),
            patch("prefect_server.publishFlow.Environment") as mock_env_ctx,
        ):
            mock_env_ctx.return_value.__enter__ = MagicMock(return_value=None)
            mock_env_ctx.return_value.__exit__ = MagicMock(return_value=False)

            await publish_flow.fn(files=[Path("test.cdf")])

        mock_env_ctx.assert_called_once()


class TestQuicklookIALiRTFlow:
    def test_generate_flow_run_name_uses_dates(self):
        from unittest.mock import patch

        from prefect_server.quicklookIALiRT import generate_flow_run_name

        mock_params = {
            "start_date": datetime(2025, 6, 1),
            "end_date": datetime(2025, 6, 2),
        }
        with patch("prefect_server.quicklookIALiRT.flow_run") as mock_flow_run:
            mock_flow_run.parameters = mock_params
            name = generate_flow_run_name()

        assert "2025" in name

    @pytest.mark.asyncio
    async def test_quicklook_ialirt_flow_calls_plot_ialirt(self):
        from prefect_server.quicklookIALiRT import quicklook_ialirt_flow

        mock_plot = MagicMock()

        with patch("prefect_server.quicklookIALiRT.plot_ialirt", mock_plot):
            await quicklook_ialirt_flow.fn(
                start_date=datetime(2025, 6, 1),
                end_date=datetime(2025, 6, 2),
            )

        mock_plot.assert_called_once()


class TestPollSpinTableFlow:
    def test_generate_flow_run_name_with_auto_run(self):
        from imap_mag.data_pipelines import AutomaticRunParameters
        from prefect_server.pollSpinTable import generate_flow_run_name

        mock_params = {"run_parameters": AutomaticRunParameters()}

        with patch("prefect_server.pollSpinTable.flow_run") as mock_flow_run:
            mock_flow_run.parameters = mock_params
            name = generate_flow_run_name()

        assert "last-update" in name

    def test_generate_flow_run_name_with_date_params(self):
        from imap_mag.data_pipelines import FetchByDatesRunParameters
        from prefect_server.pollSpinTable import generate_flow_run_name

        run_params = FetchByDatesRunParameters(
            start_date=datetime(2025, 6, 1),
            end_date=datetime(2025, 6, 30),
        )
        mock_params = {"run_parameters": run_params}

        with patch("prefect_server.pollSpinTable.flow_run") as mock_flow_run:
            mock_flow_run.parameters = mock_params
            name = generate_flow_run_name()

        assert "01-06-2025" in name

    @pytest.mark.asyncio
    async def test_poll_spin_table_flow_runs_pipeline(self):
        from imap_mag.data_pipelines import FetchByDatesRunParameters
        from prefect_server.pollSpinTable import poll_spin_table_flow

        mock_pipeline = MagicMock()
        mock_pipeline.get_results.return_value = MagicMock(success=True)
        mock_pipeline.run = AsyncMock()

        run_params = FetchByDatesRunParameters(
            start_date=datetime(2025, 1, 1),
            end_date=datetime(2025, 1, 31),
        )

        with (
            patch("prefect_server.pollSpinTable.get_secret_or_env_var", new_callable=AsyncMock, return_value="test_auth"),
            patch("prefect_server.pollSpinTable.Database"),
            patch("prefect_server.pollSpinTable.AppSettings"),
            patch("prefect_server.pollSpinTable.SDCDataAccess"),
            patch("prefect_server.pollSpinTable.SpinTablePipeline", return_value=mock_pipeline),
        ):
            await poll_spin_table_flow.fn(
                run_parameters=run_params,
                use_database=False,
            )

        mock_pipeline.build.assert_called_once()
        mock_pipeline.run.assert_called_once()

    @pytest.mark.asyncio
    async def test_poll_spin_table_flow_raises_on_pipeline_failure(self):
        from imap_mag.data_pipelines import AutomaticRunParameters
        from prefect_server.pollSpinTable import poll_spin_table_flow

        mock_pipeline = MagicMock()
        mock_pipeline.get_results.return_value = MagicMock(success=False)
        mock_pipeline.run = AsyncMock()

        with (
            patch("prefect_server.pollSpinTable.get_secret_or_env_var", new_callable=AsyncMock, return_value="test_auth"),
            patch("prefect_server.pollSpinTable.Database"),
            patch("prefect_server.pollSpinTable.AppSettings"),
            patch("prefect_server.pollSpinTable.SDCDataAccess"),
            patch("prefect_server.pollSpinTable.SpinTablePipeline", return_value=mock_pipeline),
        ):
            with pytest.raises(RuntimeError, match="Pipeline failed"):
                await poll_spin_table_flow.fn(
                    run_parameters=AutomaticRunParameters(),
                    use_database=False,
                )

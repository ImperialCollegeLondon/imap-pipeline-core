"""Unit tests for pollSmallForces flow name generation and flow logic."""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from imap_mag.data_pipelines import AutomaticRunParameters, FetchByDatesRunParameters
from prefect_server.pollSmallForces import (
    generate_flow_run_name,
    poll_small_forces_flow,
)


class TestGenerateFlowRunName:
    def test_auto_run_uses_last_update_and_end_of_today(self):
        mock_params = {"run_parameters": AutomaticRunParameters()}

        with (
            patch("prefect_server.pollSmallForces.flow_run") as mock_flow_run,
            patch("prefect_server.pollSmallForces.DatetimeProvider") as mock_dp_class,
        ):
            mock_flow_run.parameters = mock_params
            mock_dp_class.return_value.end_of_today.return_value = datetime(2026, 1, 1)

            name = generate_flow_run_name()

        assert name == "Download-SmallForces-from-last-update-to-01-01-2026"

    def test_specific_dates_included_in_name(self):
        run_params = FetchByDatesRunParameters(
            start_date=datetime(2026, 4, 1),
            end_date=datetime(2026, 4, 5),
        )
        mock_params = {"run_parameters": run_params}

        with patch("prefect_server.pollSmallForces.flow_run") as mock_flow_run:
            mock_flow_run.parameters = mock_params

            name = generate_flow_run_name()

        assert name == "Download-SmallForces-from-01-04-2026-to-05-04-2026"


class TestPollSmallForcesFlow:
    @pytest.fixture
    def mock_flow_deps(self):
        with (
            patch(
                "prefect_server.pollSmallForces.SmallForcesPipeline"
            ) as mock_pipeline_class,
            patch("prefect_server.pollSmallForces.Database") as mock_database_class,
            patch("prefect_server.pollSmallForces.AppSettings") as mock_settings_class,
            patch("prefect_server.pollSmallForces.SDCDataAccess") as mock_client_class,
            patch(
                "prefect_server.pollSmallForces.get_secret_or_env_var",
                new_callable=AsyncMock,
            ) as mock_get_secret,
        ):
            mock_pipeline = mock_pipeline_class.return_value
            mock_pipeline.run = AsyncMock()

            mock_result = MagicMock()
            mock_result.success = True
            mock_pipeline.get_results.return_value = mock_result

            mock_get_secret.return_value = "fake-auth-code"

            yield {
                "pipeline_class": mock_pipeline_class,
                "pipeline": mock_pipeline,
                "database_class": mock_database_class,
                "settings_class": mock_settings_class,
                "client_class": mock_client_class,
                "result": mock_result,
            }

    @pytest.mark.asyncio
    async def test_creates_database_when_use_database_true(self, mock_flow_deps):
        await poll_small_forces_flow.fn(
            run_parameters=AutomaticRunParameters(),
            use_database=True,
        )

        mock_flow_deps["database_class"].assert_called_once()
        _, kwargs = mock_flow_deps["pipeline_class"].call_args
        assert kwargs["database"] is mock_flow_deps["database_class"].return_value

    @pytest.mark.asyncio
    async def test_no_database_when_use_database_false(self, mock_flow_deps):
        await poll_small_forces_flow.fn(
            run_parameters=AutomaticRunParameters(),
            use_database=False,
        )

        mock_flow_deps["database_class"].assert_not_called()
        _, kwargs = mock_flow_deps["pipeline_class"].call_args
        assert kwargs["database"] is None

    @pytest.mark.asyncio
    async def test_builds_client_with_auth_code_from_secret(self, mock_flow_deps):
        await poll_small_forces_flow.fn(
            run_parameters=AutomaticRunParameters(),
            use_database=True,
        )

        mock_settings = mock_flow_deps["settings_class"].return_value
        _, kwargs = mock_flow_deps["client_class"].call_args
        assert kwargs["auth_code"].get_secret_value() == "fake-auth-code"
        assert kwargs["sdc_url"] == mock_settings.fetch_spice.api.url_base

    @pytest.mark.asyncio
    async def test_builds_and_runs_pipeline_with_given_parameters(self, mock_flow_deps):
        run_parameters = AutomaticRunParameters()

        await poll_small_forces_flow.fn(
            run_parameters=run_parameters,
            use_database=True,
        )

        mock_flow_deps["pipeline"].build.assert_called_once_with(run_parameters)
        mock_flow_deps["pipeline"].run.assert_awaited_once()
        mock_flow_deps["pipeline"].get_results.assert_called_once()

    @pytest.mark.asyncio
    async def test_raises_runtime_error_on_pipeline_failure(self, mock_flow_deps):
        mock_flow_deps["result"].success = False

        with pytest.raises(RuntimeError, match="Pipeline failed"):
            await poll_small_forces_flow.fn(
                run_parameters=AutomaticRunParameters(),
                use_database=True,
            )

    @pytest.mark.asyncio
    async def test_does_not_raise_on_pipeline_success(self, mock_flow_deps):
        mock_flow_deps["result"].success = True

        await poll_small_forces_flow.fn(
            run_parameters=AutomaticRunParameters(),
            use_database=True,
        )

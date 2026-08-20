"""Unit tests for webTCADFlowHelpers: flow name generation and pipeline invocation."""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import SecretStr

from imap_mag.data_pipelines import AutomaticRunParameters, FetchByDatesRunParameters
from prefect_server.webTCADFlowHelpers import make_flow_run_name, run_webtcad_pipeline


class TestMakeFlowRunName:
    def test_auto_run_uses_last_update_and_end_of_today(self):
        mock_params = {"run_parameters": AutomaticRunParameters()}

        with (
            patch("prefect_server.webTCADFlowHelpers.flow_run") as mock_flow_run,
            patch(
                "prefect_server.webTCADFlowHelpers.DatetimeProvider"
            ) as mock_dp_class,
        ):
            mock_flow_run.parameters = mock_params
            mock_dp_class.return_value.end_of_today.return_value = datetime(2026, 1, 1)

            name = make_flow_run_name("HI45-ESA-STEP")()

        assert name == "Download-HI45-ESA-STEP-from-last-update-to-01-01-2026"

    def test_specific_dates_included_in_name(self):
        run_params = FetchByDatesRunParameters(
            start_date=datetime(2025, 6, 1),
            end_date=datetime(2025, 6, 30),
        )
        mock_params = {"run_parameters": run_params}

        with patch("prefect_server.webTCADFlowHelpers.flow_run") as mock_flow_run:
            mock_flow_run.parameters = mock_params

            name = make_flow_run_name("LO-PivotAngle")()

        assert name == "Download-LO-PivotAngle-from-01-06-2025-to-30-06-2025"

    def test_different_labels_produce_independent_generators(self):
        mock_params = {"run_parameters": AutomaticRunParameters()}

        with (
            patch("prefect_server.webTCADFlowHelpers.flow_run") as mock_flow_run,
            patch(
                "prefect_server.webTCADFlowHelpers.DatetimeProvider"
            ) as mock_dp_class,
        ):
            mock_flow_run.parameters = mock_params
            mock_dp_class.return_value.end_of_today.return_value = datetime(2026, 1, 1)

            first_name = make_flow_run_name("HI45-ESA-STEP")()
            second_name = make_flow_run_name("HI90-ESA-STEP")()

        assert "HI45-ESA-STEP" in first_name
        assert "HI90-ESA-STEP" in second_name


class TestRunWebtcadPipeline:
    @pytest.fixture
    def mock_pipeline_deps(self):
        with (
            patch(
                "prefect_server.webTCADFlowHelpers.WebTCADTelemetryItemPipeline"
            ) as mock_pipeline_class,
            patch("prefect_server.webTCADFlowHelpers.Database") as mock_database_class,
            patch(
                "prefect_server.webTCADFlowHelpers.AppSettings"
            ) as mock_settings_class,
            patch(
                "prefect_server.webTCADFlowHelpers.get_secret_or_env_var",
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
                "get_secret": mock_get_secret,
                "result": mock_result,
            }

    @pytest.mark.asyncio
    async def test_creates_database_when_use_database_true(self, mock_pipeline_deps):
        await run_webtcad_pipeline(
            item=MagicMock(),
            run_parameters=AutomaticRunParameters(),
            use_database=True,
        )

        mock_pipeline_deps["database_class"].assert_called_once()
        _, kwargs = mock_pipeline_deps["pipeline_class"].call_args
        assert kwargs["database"] is mock_pipeline_deps["database_class"].return_value

    @pytest.mark.asyncio
    async def test_no_database_when_use_database_false(self, mock_pipeline_deps):
        await run_webtcad_pipeline(
            item=MagicMock(),
            run_parameters=AutomaticRunParameters(),
            use_database=False,
        )

        mock_pipeline_deps["database_class"].assert_not_called()
        _, kwargs = mock_pipeline_deps["pipeline_class"].call_args
        assert kwargs["database"] is None

    @pytest.mark.asyncio
    async def test_sets_auth_code_on_settings_from_secret(self, mock_pipeline_deps):
        mock_settings = mock_pipeline_deps["settings_class"].return_value

        await run_webtcad_pipeline(
            item=MagicMock(),
            run_parameters=AutomaticRunParameters(),
            use_database=True,
        )

        assert mock_settings.fetch_webtcad.api.auth_code == SecretStr("fake-auth-code")

    @pytest.mark.asyncio
    async def test_builds_and_runs_pipeline_with_given_item_and_parameters(
        self, mock_pipeline_deps
    ):
        item = MagicMock()
        run_parameters = AutomaticRunParameters()

        await run_webtcad_pipeline(
            item=item,
            run_parameters=run_parameters,
            use_database=True,
        )

        _, kwargs = mock_pipeline_deps["pipeline_class"].call_args
        assert kwargs["item"] is item

        mock_pipeline_deps["pipeline"].build.assert_called_once_with(run_parameters)
        mock_pipeline_deps["pipeline"].run.assert_awaited_once()
        mock_pipeline_deps["pipeline"].get_results.assert_called_once()

    @pytest.mark.asyncio
    async def test_passes_through_custom_datetime_provider(self, mock_pipeline_deps):
        mock_dp = MagicMock()

        await run_webtcad_pipeline(
            item=MagicMock(),
            run_parameters=AutomaticRunParameters(),
            use_database=True,
            datetime_provider=mock_dp,
        )

        _, kwargs = mock_pipeline_deps["pipeline_class"].call_args
        assert kwargs["datetime_provider"] is mock_dp

    @pytest.mark.asyncio
    async def test_raises_runtime_error_on_pipeline_failure(self, mock_pipeline_deps):
        mock_pipeline_deps["result"].success = False

        with pytest.raises(RuntimeError, match="Pipeline failed"):
            await run_webtcad_pipeline(
                item=MagicMock(),
                run_parameters=AutomaticRunParameters(),
                use_database=True,
            )

    @pytest.mark.asyncio
    async def test_does_not_raise_on_pipeline_success(self, mock_pipeline_deps):
        mock_pipeline_deps["result"].success = True

        await run_webtcad_pipeline(
            item=MagicMock(),
            run_parameters=AutomaticRunParameters(),
            use_database=True,
        )

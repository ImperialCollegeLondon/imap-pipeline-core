"""Unit tests for pollNOAA flow name generation and flow logic."""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from imap_mag.data_pipelines import AutomaticRunParameters
from prefect_server.pollNOAA import generate_flow_run_name, poll_noaa_flow

_PATCHES = (
    "prefect_server.pollNOAA.NOAAPipeline",
    "prefect_server.pollNOAA.Database",
    "prefect_server.pollNOAA.AppSettings",
)


class TestGenerateFlowRunName:
    def test_name_includes_spacecraft_instrument_and_timestamp(self) -> None:
        mock_params = {"spacecraft": "SOLAR1", "instrument": "mag"}

        with (
            patch("prefect_server.pollNOAA.flow_run") as mock_flow_run,
            patch("prefect_server.pollNOAA.DatetimeProvider") as mock_dp_class,
        ):
            mock_flow_run.parameters = mock_params
            mock_dp_class.return_value.now.return_value = datetime(
                2026, 7, 21, 8, 11, 9
            )

            name = generate_flow_run_name()

        assert name == "Download-NOAA-SOLAR1-mag-at-21-07-2026-08-11-09"


class TestPollNOAAFlow:
    @pytest.fixture
    def mock_flow_deps(self):
        with (
            patch(_PATCHES[0]) as mock_pipeline_class,
            patch(_PATCHES[1]) as mock_database_class,
            patch(_PATCHES[2]) as mock_settings_class,
        ):
            mock_pipeline = mock_pipeline_class.return_value
            mock_pipeline.run = AsyncMock()
            mock_pipeline.get_results.return_value = MagicMock(success=True)

            yield {
                "pipeline_class": mock_pipeline_class,
                "pipeline": mock_pipeline,
                "database_class": mock_database_class,
                "settings_class": mock_settings_class,
            }

    @pytest.mark.asyncio
    async def test_creates_database_when_use_database_true(
        self, mock_flow_deps
    ) -> None:
        await poll_noaa_flow.fn(
            spacecraft="SOLAR1",
            instrument="mag",
            use_database=True,
        )

        mock_flow_deps["database_class"].assert_called_once()
        _, kwargs = mock_flow_deps["pipeline_class"].call_args
        assert kwargs["database"] is mock_flow_deps["database_class"].return_value

    @pytest.mark.asyncio
    async def test_no_database_when_use_database_false(self, mock_flow_deps) -> None:
        await poll_noaa_flow.fn(
            spacecraft="SOLAR1",
            instrument="mag",
            use_database=False,
        )

        mock_flow_deps["database_class"].assert_not_called()
        _, kwargs = mock_flow_deps["pipeline_class"].call_args
        assert kwargs["database"] is None

    @pytest.mark.asyncio
    async def test_creates_pipeline_with_spacecraft_instrument_and_settings(
        self, mock_flow_deps
    ) -> None:
        await poll_noaa_flow.fn(
            spacecraft="ACE",
            instrument="wind",
        )

        mock_flow_deps["pipeline_class"].assert_called_once_with(
            spacecraft="ACE",
            instrument="wind",
            database=mock_flow_deps["database_class"].return_value,
            settings=mock_flow_deps["settings_class"].return_value,
        )

    @pytest.mark.asyncio
    async def test_builds_and_runs_pipeline_with_given_parameters(
        self, mock_flow_deps
    ) -> None:
        run_parameters = AutomaticRunParameters()

        await poll_noaa_flow.fn(
            spacecraft="SOLAR1",
            instrument="mag",
            run_parameters=run_parameters,
        )

        mock_flow_deps["pipeline"].build.assert_called_once_with(run_parameters)
        mock_flow_deps["pipeline"].run.assert_awaited_once()
        mock_flow_deps["pipeline"].get_results.assert_called_once()

    @pytest.mark.asyncio
    async def test_raises_runtime_error_on_pipeline_failure(
        self, mock_flow_deps
    ) -> None:
        mock_flow_deps["pipeline"].get_results.return_value = MagicMock(success=False)

        with pytest.raises(RuntimeError, match="Pipeline failed"):
            await poll_noaa_flow.fn(
                spacecraft="SOLAR1",
                instrument="mag",
            )

    @pytest.mark.asyncio
    async def test_does_not_raise_on_pipeline_success(self, mock_flow_deps) -> None:
        await poll_noaa_flow.fn(
            spacecraft="SOLAR1",
            instrument="mag",
        )

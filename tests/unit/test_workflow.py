"""Unit tests for workflow module functions."""

import contextlib
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from prefect_server.workflow import adeploy_flows, get_matlab_license_server

_ALL_FLOW_NAMES = [
    "PollIALiRTFlow",
    "PollHKFlow",
    "PollSpiceFlow",
    "PollSpinTableFlow",
    "PollScienceFlow",
    "PollSmallForcesFlow",
    "poll_lo_pivot_platform_flow",
    "poll_hi45_esa_step_flow",
    "poll_hi90_esa_step_flow",
    "publish_flow",
    "check_ialirt_flow",
    "quicklook_ialirt_flow",
    "upload_shared_docs_flow",
    "upload_new_files_to_postgres",
    "cleanup_datastore_flow",
    "index_datastore_flow",
    "calibrate_flow",
    "gradiometry_flow",
    "apply_flow",
    "calibrate_and_apply_flow",
]


@contextlib.contextmanager
def _patch_adeploy_deps(gather_side_effect, cron_side_effect=None):
    """Patch all external dependencies for adeploy_flows tests."""
    with contextlib.ExitStack() as stack:
        stack.enter_context(
            patch(
                "prefect_server.workflow.ServerConfig.initialise",
                new_callable=AsyncMock,
            )
        )
        stack.enter_context(
            patch(
                "prefect_server.workflow.get_matlab_license_server",
                new_callable=AsyncMock,
                return_value="test-license",
            )
        )
        stack.enter_context(
            patch(
                "prefect_server.workflow.get_cron_from_env",
                side_effect=cron_side_effect if cron_side_effect else lambda _: None,
            )
        )
        mock_gather = stack.enter_context(
            patch(
                "asyncio.gather",
                new_callable=AsyncMock,
                side_effect=gather_side_effect,
            )
        )
        for name in _ALL_FLOW_NAMES:
            stack.enter_context(patch(f"prefect_server.workflow.{name}"))
        yield mock_gather


class TestGetMatlabLicenseServer:
    @pytest.mark.asyncio
    async def test_returns_variable_value_when_set(self):
        with patch(
            "prefect_server.workflow.Variable.aget",
            new_callable=AsyncMock,
            return_value="matlab-license-server:27000",
        ):
            result = await get_matlab_license_server()
        assert result == "matlab-license-server:27000"

    @pytest.mark.asyncio
    async def test_returns_none_when_variable_not_set(self):
        with patch(
            "prefect_server.workflow.Variable.aget",
            new_callable=AsyncMock,
            return_value=None,
        ):
            result = await get_matlab_license_server()
        assert result is None


class TestAdDeployFlows:
    @pytest.mark.asyncio
    async def test_adeploy_flows_local_debug_mode(self):
        matlab_deployables = [MagicMock() for _ in range(4)]
        regular_deployables = [MagicMock() for _ in range(13)]

        with (
            _patch_adeploy_deps(
                gather_side_effect=[matlab_deployables, regular_deployables]
            ),
            patch("prefect_server.workflow.aserve", new_callable=AsyncMock),
        ):
            await adeploy_flows(local_debug=True)

    @pytest.mark.asyncio
    async def test_adeploy_flows_docker_mode(self):
        matlab_deployables = [MagicMock() for _ in range(4)]
        regular_deployables = [MagicMock() for _ in range(13)]

        with (
            _patch_adeploy_deps(
                gather_side_effect=[matlab_deployables, regular_deployables]
            ),
            patch(
                "prefect_server.workflow.deploy",
                new_callable=AsyncMock,
                side_effect=[
                    [MagicMock() for _ in range(13)],
                    [MagicMock() for _ in range(4)],
                ],
            ),
        ):
            await adeploy_flows(local_debug=False)

    @pytest.mark.asyncio
    async def test_adeploy_flows_with_science_cron_schedules(self):
        matlab_deployables = [MagicMock() for _ in range(4)]
        regular_deployables = [MagicMock() for _ in range(13)]

        def cron_side_effect(env_var_name):
            if "L1C_NORM" in env_var_name or "L1B_BURST" in env_var_name:
                return "*/5 * * * *"
            return None

        with (
            _patch_adeploy_deps(
                gather_side_effect=[matlab_deployables, regular_deployables],
                cron_side_effect=cron_side_effect,
            ),
            patch("prefect_server.workflow.aserve", new_callable=AsyncMock),
        ):
            await adeploy_flows(local_debug=True)

"""Unit tests for generate_leinweber_offsets_flow."""

from datetime import date, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from prefect_server.constants import PREFECT_CONSTANTS
from prefect_server.generateLeinweberOffsetsFlow import (
    generate_leinweber_offsets_flow,
    generate_leinweber_offsets_flow_run_name,
)


class TestGenerateLeinweberOffsetsFlowRunName:
    def test_with_date_range(self):
        mock_params = {
            "start_date": date(2025, 1, 1),
            "end_date": date(2025, 1, 31),
            "duration_hours": 24,
        }
        with patch(
            "prefect_server.generateLeinweberOffsetsFlow.flow_run"
        ) as mock_flow_run:
            mock_flow_run.parameters = mock_params
            result = generate_leinweber_offsets_flow_run_name()

        assert "01-01-2025" in result
        assert "31-01-2025" in result
        assert "24hr" in result

    def test_without_end_date(self):
        mock_params = {
            "start_date": date(2025, 1, 15),
            "end_date": None,
            "duration_hours": 24,
        }
        with patch(
            "prefect_server.generateLeinweberOffsetsFlow.flow_run"
        ) as mock_flow_run:
            mock_flow_run.parameters = mock_params
            result = generate_leinweber_offsets_flow_run_name()

        assert result == "Leinweber-offsets-15-01-2025-24hr"


class TestGenerateLeinweberOffsetsFlow:
    def test_raises_when_end_date_before_start_date(self):
        with pytest.raises(ValueError, match="cannot be before start date"):
            generate_leinweber_offsets_flow.fn(
                start_date=date(2025, 1, 5),
                end_date=date(2025, 1, 1),
            )

    def test_requires_matlab_repo(self, tmp_path):
        # _resolve_matlab_repo_path (shared with calibrate_flow) raises TypeError
        # for an unresolvable matlab_repo rather than returning None.
        mock_settings = MagicMock()
        mock_settings.work_folder = tmp_path
        with patch(
            "prefect_server.generateLeinweberOffsetsFlow.AppSettings",
            return_value=mock_settings,
        ):
            with pytest.raises(TypeError, match="Unsupported matlab_repo type"):
                generate_leinweber_offsets_flow.fn(
                    start_date=date(2026, 1, 30),
                    matlab_repo=None,
                )

    def test_calls_generate_leinweber_offsets_once_per_day(self, tmp_path):
        repo = tmp_path / "repo"
        repo.mkdir()
        mock_settings = MagicMock()
        mock_settings.work_folder = tmp_path

        with (
            patch(
                "prefect_server.generateLeinweberOffsetsFlow.AppSettings",
                return_value=mock_settings,
            ),
            patch(
                "prefect_server.generateLeinweberOffsetsFlow.generate_leinweber_offsets",
                return_value=[Path("offsets.csv")],
            ) as mock_generate,
        ):
            result = generate_leinweber_offsets_flow.fn(
                start_date=date(2026, 1, 30),
                end_date=date(2026, 2, 1),
                matlab_repo=str(repo),
            )

        assert mock_generate.call_count == 3
        assert result == [Path("offsets.csv")] * 3
        first_call_date = mock_generate.call_args_list[0].kwargs["date"]
        assert first_call_date == datetime(2026, 1, 30)

    def test_raises_runtime_error_when_nothing_produced(self, tmp_path):
        repo = tmp_path / "repo"
        repo.mkdir()
        mock_settings = MagicMock()
        mock_settings.work_folder = tmp_path

        with (
            patch(
                "prefect_server.generateLeinweberOffsetsFlow.AppSettings",
                return_value=mock_settings,
            ),
            patch(
                "prefect_server.generateLeinweberOffsetsFlow.generate_leinweber_offsets",
                return_value=[],
            ),
        ):
            with pytest.raises(RuntimeError, match="No Leinweber offsets"):
                generate_leinweber_offsets_flow.fn(
                    start_date=date(2026, 1, 30),
                    matlab_repo=str(repo),
                )


class TestGenerateLeinweberOffsetsFlowSplitByDay:
    def _make_flow_run(self, name):
        fake = MagicMock()
        fake.name = name
        return fake

    def test_splits_range_into_per_day_deployment_runs(self):
        with (
            patch(
                "prefect_server.calibrationFlowCommon.run_deployment",
                side_effect=lambda **kwargs: self._make_flow_run(
                    str(kwargs["parameters"]["start_date"])
                ),
            ) as mock_run_deployment,
            patch(
                "prefect_server.generateLeinweberOffsetsFlow.generate_leinweber_offsets"
            ) as mock_generate,
        ):
            result = generate_leinweber_offsets_flow.fn(
                start_date=date(2025, 1, 1),
                end_date=date(2025, 1, 3),
                matlab_repo="some-repo",
                split_by_day=True,
            )

        assert mock_generate.call_count == 0
        assert mock_run_deployment.call_count == 3
        for call in mock_run_deployment.call_args_list:
            assert call.kwargs["name"] == (
                f"{PREFECT_CONSTANTS.FLOW_NAMES.GENERATE_LEINWEBER_OFFSETS}/"
                f"{PREFECT_CONSTANTS.DEPLOYMENT_NAMES.GENERATE_LEINWEBER_OFFSETS}"
            )
            assert call.kwargs["parameters"]["split_by_day"] is False
        assert len(result) == 3

    def test_single_day_does_not_split(self, tmp_path):
        repo = tmp_path / "repo"
        repo.mkdir()
        mock_settings = MagicMock()
        mock_settings.work_folder = tmp_path

        with (
            patch(
                "prefect_server.calibrationFlowCommon.run_deployment"
            ) as mock_run_deployment,
            patch(
                "prefect_server.generateLeinweberOffsetsFlow.AppSettings",
                return_value=mock_settings,
            ),
            patch(
                "prefect_server.generateLeinweberOffsetsFlow.generate_leinweber_offsets",
                return_value=[Path("offsets.csv")],
            ) as mock_generate,
        ):
            generate_leinweber_offsets_flow.fn(
                start_date=date(2026, 1, 30),
                matlab_repo=str(repo),
                split_by_day=True,
            )

        mock_run_deployment.assert_not_called()
        mock_generate.assert_called_once()

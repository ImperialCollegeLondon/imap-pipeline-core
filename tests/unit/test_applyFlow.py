"""Unit tests for apply_flow."""

from datetime import datetime
from unittest.mock import MagicMock, patch

from prefect_server.applyFlow import (
    apply_flow,
    generate_apply_calibration_flow_run_name,
)
from prefect_server.constants import PREFECT_CONSTANTS


class TestGenerateApplyCalibrationFlowRunName:
    def test_truncates_many_layers(self):
        mock_params = {
            "start_date": datetime(2025, 1, 15),
            "end_date": None,
            "layers": ["layer1", "layer2", "layer3", "layer4", "layer5"],
        }

        with patch("prefect_server.applyFlow.flow_run") as mock_flow_run:
            mock_flow_run.parameters = mock_params
            result = generate_apply_calibration_flow_run_name()

        assert "+2" in result

    def test_does_not_truncate_few_layers(self):
        mock_params = {
            "start_date": datetime(2025, 1, 15),
            "end_date": None,
            "layers": ["layer1", "layer2"],
        }

        with patch("prefect_server.applyFlow.flow_run") as mock_flow_run:
            mock_flow_run.parameters = mock_params
            result = generate_apply_calibration_flow_run_name()

        assert "layer1,layer2" in result
        assert "+" not in result

    def test_with_date_range(self):
        mock_params = {
            "start_date": datetime(2025, 1, 1),
            "end_date": datetime(2025, 1, 5),
            "layers": ["layer1"],
        }

        with patch("prefect_server.applyFlow.flow_run") as mock_flow_run:
            mock_flow_run.parameters = mock_params
            result = generate_apply_calibration_flow_run_name()

        assert "01-01-2025-to-05-01-2025" in result


class TestApplyFlow:
    def test_calls_apply(self):
        with patch("prefect_server.applyFlow.apply") as mock_apply:
            mock_apply.return_value = []
            apply_flow.fn(
                layers=["*noop*"],
                start_date=datetime(2025, 1, 1),
            )

        mock_apply.assert_called_once()


class TestApplyFlowSplitByDay:
    def _make_flow_run(self, name):
        fake = MagicMock()
        fake.name = name
        return fake

    def test_splits_range(self):
        with (
            patch(
                "prefect_server.calibrationFlowCommon.run_deployment",
                return_value=self._make_flow_run("child"),
            ) as mock_run_deployment,
            patch("prefect_server.applyFlow.apply") as mock_apply,
        ):
            result = apply_flow.fn(
                layers=["*noop*"],
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 3),
                split_by_day=True,
            )

        assert mock_run_deployment.call_count == 3
        assert len(result) == 3
        mock_apply.assert_not_called()
        first_params = mock_run_deployment.call_args_list[0].kwargs["parameters"]
        assert first_params["layers"] == ["*noop*"]
        assert first_params["split_by_day"] is False
        assert (
            mock_run_deployment.call_args.kwargs["name"]
            == f"{PREFECT_CONSTANTS.FLOW_NAMES.APPLY_CALIBRATION}/{PREFECT_CONSTANTS.DEPLOYMENT_NAMES.APPLY_CALIBRATION}"
        )

    def test_default_does_not_split(self):
        with (
            patch(
                "prefect_server.calibrationFlowCommon.run_deployment"
            ) as mock_run_deployment,
            patch("prefect_server.applyFlow.apply") as mock_apply,
        ):
            apply_flow.fn(
                layers=["*noop*"],
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 3),
            )

        mock_run_deployment.assert_not_called()
        mock_apply.assert_called_once()

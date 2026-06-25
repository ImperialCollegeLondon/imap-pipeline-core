"""Unit tests for performCalibration flow helper functions."""

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

from imap_mag.util import ScienceMode
from prefect_server.constants import PREFECT_CONSTANTS
from prefect_server.performCalibration import (
    calibrate_and_apply_flow,
    calibrate_flow,
    generate_apply_calibration_flow_run_name,
    generate_calibrate_and_apply_flow_run_name,
    generate_calibration_flow_run_name,
    gradiometry_flow,
)


class TestPerformCalibrationFlowNames:
    def test_generate_calibrate_and_apply_name_includes_date_and_sensor(self):
        mock_params = {
            "start_date": datetime(2025, 1, 15),
            "end_date": None,
            "method": MagicMock(value="kepko"),
            "mode": MagicMock(value="norm"),
            "sensor": MagicMock(value="mago"),
        }

        with patch("prefect_server.performCalibration.flow_run") as mock_flow_run:
            mock_flow_run.parameters = mock_params
            result = generate_calibrate_and_apply_flow_run_name()

        assert "15-01-2025" in result
        assert "kepko" in result

    def test_generate_calibration_flow_name_with_date_range(self):
        mock_params = {
            "start_date": datetime(2025, 1, 1),
            "end_date": datetime(2025, 1, 31),
            "method": MagicMock(value="kepko"),
            "mode": MagicMock(value="norm"),
            "sensor": MagicMock(value="mago"),
        }

        with patch("prefect_server.performCalibration.flow_run") as mock_flow_run:
            mock_flow_run.flow_name = PREFECT_CONSTANTS.FLOW_NAMES.CALIBRATE
            mock_flow_run.parameters = mock_params
            result = generate_calibration_flow_run_name()

        assert "01-01-2025" in result
        assert "31-01-2025" in result

    def test_generate_apply_calibration_name_truncates_many_layers(self):
        mock_params = {
            "start_date": datetime(2025, 1, 15),
            "end_date": None,
            "layers": ["layer1", "layer2", "layer3", "layer4", "layer5"],
        }

        with patch("prefect_server.performCalibration.flow_run") as mock_flow_run:
            mock_flow_run.parameters = mock_params
            result = generate_apply_calibration_flow_run_name()

        assert "+2" in result

    def test_gradiometry_flow_calls_gradiometry(self):
        with patch("prefect_server.performCalibration.gradiometry") as mock_gradiometry:
            gradiometry_flow.fn(
                start_date=datetime(2025, 1, 1),
                mode=ScienceMode.Normal,
            )

        mock_gradiometry.assert_called_once()

    def test_calibrate_and_apply_flow_calls_both(self):
        mock_layer = MagicMock()
        mock_layer.metadata.science = ["test_science.cdf"]

        with (
            patch(
                "prefect_server.performCalibration.calibrate",
                return_value=[Path("layer.json")],
            ) as mock_calibrate,
            patch(
                "prefect_server.performCalibration.CalibrationLayer.from_file",
                return_value=mock_layer,
            ),
            patch("prefect_server.performCalibration.apply") as mock_apply,
        ):
            calibrate_and_apply_flow.fn(
                start_date=datetime(2025, 1, 1),
            )

        mock_calibrate.assert_called_once()
        mock_apply.assert_called_once()


class TestPerformCalibrationFlows:
    def test_calibrate_flow_calls_calibrate(self):
        with patch("prefect_server.performCalibration.calibrate") as mock_calibrate:
            mock_calibrate.return_value = []
            calibrate_flow.fn(
                start_date=datetime(2025, 1, 1),
            )

        mock_calibrate.assert_called_once()

    def test_apply_flow_calls_apply(self):
        from prefect_server.performCalibration import apply_flow

        with patch("prefect_server.performCalibration.apply") as mock_apply:
            mock_apply.return_value = []
            apply_flow.fn(
                layers=["*noop*"],
                start_date=datetime(2025, 1, 1),
            )

        mock_apply.assert_called_once()

"""Unit tests for calibrate_and_apply_flow."""

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from prefect.filesystems import LocalFileSystem
from prefect_github import GitHubRepository

from mag_toolkit.calibration import DatastoreAccessMode, LayerDataFormat
from mag_toolkit.calibration.CalibrationConfig import GradiometryConfig
from prefect_server.calibrateAndApplyFlow import (
    calibrate_and_apply_flow,
    generate_calibrate_and_apply_flow_run_name,
)
from prefect_server.calibrationFlowCommon import PrefectScriptedL2CalibrationConfig
from prefect_server.constants import PREFECT_CONSTANTS


class TestGenerateCalibrateAndApplyFlowRunName:
    def test_includes_date_and_sensor(self):
        mock_configuration = MagicMock()
        mock_configuration.get_method.return_value = MagicMock(value="kepko")
        mock_params = {
            "start_date": datetime(2025, 1, 15),
            "end_date": None,
            "configuration": mock_configuration,
            "mode": MagicMock(value="norm"),
            "sensor": MagicMock(value="mago"),
        }

        with patch("prefect_server.calibrateAndApplyFlow.flow_run") as mock_flow_run:
            mock_flow_run.parameters = mock_params
            result = generate_calibrate_and_apply_flow_run_name()

        assert "15-01-2025" in result
        assert "kepko" in result

    def test_with_date_range(self):
        mock_configuration = MagicMock()
        mock_configuration.get_method.return_value = MagicMock(value="kepko")
        mock_params = {
            "start_date": datetime(2025, 1, 1),
            "end_date": datetime(2025, 1, 5),
            "configuration": mock_configuration,
            "mode": MagicMock(value="norm"),
            "sensor": MagicMock(value="mago"),
        }

        with patch("prefect_server.calibrateAndApplyFlow.flow_run") as mock_flow_run:
            mock_flow_run.parameters = mock_params
            result = generate_calibrate_and_apply_flow_run_name()

        assert "01-01-2025-to-05-01-2025" in result


class TestCalibrateAndApplyFlow:
    def test_calls_both(self):
        mock_layer = MagicMock()
        mock_layer.metadata.science = ["test_science.cdf"]

        with (
            patch(
                "prefect_server.calibrationFlowCommon.calibrate",
                return_value=[
                    Path("imap_mag_gradiometry-norm-layer_20260101_v002.0002.json")
                ],
            ) as mock_calibrate,
            patch(
                "prefect_server.calibrateAndApplyFlow.CalibrationLayer.from_file",
                return_value=mock_layer,
            ),
            patch("prefect_server.calibrateAndApplyFlow.apply") as mock_apply,
        ):
            calibrate_and_apply_flow.fn(
                start_date=datetime(2025, 1, 1),
                configuration=GradiometryConfig(),
            )

        mock_calibrate.assert_called_once()
        mock_apply.assert_called_once()

    def test_passes_scripted_l2_options_through(self, tmp_path):
        """calibrate_and_apply_flow must accept and forward every option that
        calibrate_flow supports for the scripted-l2 method."""
        repo = tmp_path / "repo"
        repo.mkdir()
        block = LocalFileSystem(basepath=str(repo))
        mock_settings = MagicMock()
        mock_settings.work_folder = tmp_path
        config = PrefectScriptedL2CalibrationConfig(
            calibration_matrix_version=8,
            input_json_file="input.json",
            matlab_repo=block,
            datastore_access_mode=DatastoreAccessMode.LOCAL_WORK_FOLDER_COPY,
        )

        mock_layer = MagicMock()
        mock_layer.metadata.science = ["test_science.cdf"]

        with (
            patch(
                "prefect_server.calibrationFlowCommon.AppSettings",
                return_value=mock_settings,
            ),
            patch(
                "prefect_server.calibrationFlowCommon.calibrate",
                return_value=[
                    Path("imap_mag_manual-norm-layer_20260130_v002.0002.json")
                ],
            ) as mock_calibrate,
            patch(
                "prefect_server.calibrateAndApplyFlow.CalibrationLayer.from_file",
                return_value=mock_layer,
            ),
            patch("prefect_server.calibrateAndApplyFlow.apply") as mock_apply,
        ):
            calibrate_and_apply_flow.fn(
                start_date=datetime(2026, 1, 30),
                configuration=config,
                metakernel=Path("mk.txt"),
            )

        mock_calibrate.assert_called_once()
        kwargs = mock_calibrate.call_args.kwargs
        assert kwargs["metakernel"] == Path("mk.txt")
        mock_apply.assert_called_once()

    def test_scripted_requires_matlab_repo(self, tmp_path):
        mock_settings = MagicMock()
        mock_settings.work_folder = tmp_path
        with patch(
            "prefect_server.calibrationFlowCommon.AppSettings",
            return_value=mock_settings,
        ):
            with pytest.raises(
                ValueError,
                match="Could not load a MATLAB repository block named 'not found' or resolve it as a local path",
            ):
                calibrate_and_apply_flow.fn(
                    start_date=datetime(2026, 1, 30),
                    configuration=PrefectScriptedL2CalibrationConfig(
                        calibration_matrix_version=8,
                        input_json_file="input.json",
                        matlab_repo="not found",
                        datastore_access_mode=DatastoreAccessMode.LOCAL_WORK_FOLDER_COPY,
                    ),
                )

    def test_forwards_layer_data_format(self):
        mock_layer = MagicMock()
        mock_layer.metadata.science = ["test_science.cdf"]

        with (
            patch(
                "prefect_server.calibrationFlowCommon.calibrate",
                return_value=[Path("imap_mag_manual-norm-layer_20260130_v001.json")],
            ) as mock_calibrate,
            patch(
                "prefect_server.calibrateAndApplyFlow.CalibrationLayer.from_file",
                return_value=mock_layer,
            ),
            patch("prefect_server.calibrateAndApplyFlow.apply"),
        ):
            calibrate_and_apply_flow.fn(
                start_date=datetime(2026, 1, 30),
                configuration=GradiometryConfig(
                    kappa=0.1, sc_interference_threshold=0.2
                ),
                layer_data_format=LayerDataFormat.CSV,
            )

        assert mock_calibrate.call_args.kwargs["layer_data_format"] == (
            LayerDataFormat.CSV
        )


class TestCalibrateAndApplyFlowSplitByDay:
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
            patch("prefect_server.calibrationFlowCommon.calibrate") as mock_calibrate,
            patch("prefect_server.calibrateAndApplyFlow.apply") as mock_apply,
        ):
            result = calibrate_and_apply_flow.fn(
                configuration=GradiometryConfig(),
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 2),
                split_by_day=True,
            )

        assert mock_run_deployment.call_count == 2
        assert len(result) == 2
        mock_calibrate.assert_not_called()
        mock_apply.assert_not_called()
        assert (
            mock_run_deployment.call_args.kwargs["name"]
            == f"{PREFECT_CONSTANTS.FLOW_NAMES.CALIBRATE_AND_APPLY}/{PREFECT_CONSTANTS.DEPLOYMENT_NAMES.CALIBRATE_AND_APPLY}"
        )

    def test_split_by_day_forwards_rotation_and_frames(self):
        """split_by_day must forward rotation_calibration_file_name and reference_frames to each child run."""
        from imap_mag.util import ReferenceFrame

        with (
            patch(
                "prefect_server.calibrationFlowCommon.run_deployment",
                return_value=self._make_flow_run("child"),
            ) as mock_run_deployment,
            patch("prefect_server.calibrationFlowCommon.calibrate"),
            patch("prefect_server.calibrateAndApplyFlow.apply"),
        ):
            calibrate_and_apply_flow.fn(
                configuration=GradiometryConfig(),
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 2),
                split_by_day=True,
                rotation_calibration_file_name="imap_mag_l2-calibration_20260101_v003.cdf",
                reference_frames=[ReferenceFrame.SRF, ReferenceFrame.GSE],
            )

        assert mock_run_deployment.call_count == 2
        for call in mock_run_deployment.call_args_list:
            params = call.kwargs["parameters"]
            assert (
                params["rotation_calibration_file_name"]
                == "imap_mag_l2-calibration_20260101_v003.cdf"
            )
            assert params["reference_frames"] == [
                ReferenceFrame.SRF,
                ReferenceFrame.GSE,
            ]

    def test_passes_ref_for_github_block(self):
        block = GitHubRepository(
            repository_url="git@github.com:example-org/example-repo.git"
        )
        block._block_document_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        config = PrefectScriptedL2CalibrationConfig(
            calibration_matrix_version=9,
            input_json_file="input.json",
            matlab_repo=block,
        )

        with (
            patch(
                "prefect_server.calibrationFlowCommon.run_deployment",
                return_value=self._make_flow_run("child"),
            ) as mock_run_deployment,
            patch("prefect_server.calibrationFlowCommon.calibrate"),
            patch("prefect_server.calibrateAndApplyFlow.apply"),
        ):
            calibrate_and_apply_flow.fn(
                configuration=config,
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 2),
                split_by_day=True,
            )

        assert mock_run_deployment.call_count == 2
        for call in mock_run_deployment.call_args_list:
            configuration_param = call.kwargs["parameters"]["configuration"]
            assert isinstance(configuration_param, dict)
            assert configuration_param["matlab_repo"] == {
                "$ref": {"block_document_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
            }

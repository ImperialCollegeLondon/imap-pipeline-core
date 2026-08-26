"""Unit tests for calibrate_flow."""

from datetime import date, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from prefect.filesystems import LocalFileSystem
from prefect_github import GitHubRepository

from mag_toolkit.calibration import DatastoreAccessMode, LayerDataFormat
from mag_toolkit.calibration.CalibrationConfig import GradiometryConfig
from prefect_server.calibrateFlow import (
    calibrate_flow,
    generate_calibration_flow_run_name,
)
from prefect_server.calibrationFlowCommon import PrefectScriptedL2CalibrationConfig
from prefect_server.constants import PREFECT_CONSTANTS


class TestGenerateCalibrationFlowRunName:
    def test_with_date_range(self):
        mock_configuration = MagicMock()
        mock_configuration.get_method.return_value = MagicMock(value="kepko")
        mock_params = {
            "start_date": datetime(2025, 1, 1),
            "end_date": datetime(2025, 1, 31),
            "configuration": mock_configuration,
            "mode": MagicMock(value="norm"),
            "sensor": MagicMock(value="mago"),
        }

        with patch("prefect_server.calibrateFlow.flow_run") as mock_flow_run:
            mock_flow_run.flow_name = PREFECT_CONSTANTS.FLOW_NAMES.CALIBRATE
            mock_flow_run.parameters = mock_params
            result = generate_calibration_flow_run_name()

        assert "01-01-2025" in result
        assert "31-01-2025" in result

    def test_without_end_date(self):
        mock_configuration = MagicMock()
        mock_configuration.get_method.return_value = MagicMock(value="kepko")
        mock_params = {
            "start_date": datetime(2025, 1, 15),
            "end_date": None,
            "configuration": mock_configuration,
            "mode": MagicMock(value="norm"),
            "sensor": MagicMock(value="mago"),
        }

        with patch("prefect_server.calibrateFlow.flow_run") as mock_flow_run:
            mock_flow_run.parameters = mock_params
            result = generate_calibration_flow_run_name()

        assert result == "Calibrating-15-01-2025-for-mago-norm-with-kepko"


class TestCalibrateFlow:
    def test_calls_calibrate(self):
        with patch("prefect_server.calibrationFlowCommon.calibrate") as mock_calibrate:
            mock_calibrate.return_value = [Path("some_layer.json")]
            calibrate_flow.fn(
                start_date=datetime(2025, 1, 1),
                configuration=GradiometryConfig(
                    kappa=0.1, sc_interference_threshold=0.2
                ),
            )

        mock_calibrate.assert_called_once()

    def test_forwards_layer_data_format(self):
        with patch("prefect_server.calibrationFlowCommon.calibrate") as mock_calibrate:
            mock_calibrate.return_value = [Path("some_layer.json")]
            calibrate_flow.fn(
                start_date=datetime(2025, 1, 1),
                configuration=GradiometryConfig(
                    kappa=0.1, sc_interference_threshold=0.2
                ),
                layer_data_format=LayerDataFormat.CSV,
            )

        assert mock_calibrate.call_args.kwargs["layer_data_format"] == (
            LayerDataFormat.CSV
        )

    def test_defaults_layer_data_format_to_parquet(self):
        with patch("prefect_server.calibrationFlowCommon.calibrate") as mock_calibrate:
            mock_calibrate.return_value = [Path("some_layer.json")]
            calibrate_flow.fn(
                start_date=datetime(2025, 1, 1),
                configuration=GradiometryConfig(
                    kappa=0.1, sc_interference_threshold=0.2
                ),
            )

        assert mock_calibrate.call_args.kwargs["layer_data_format"] == (
            LayerDataFormat.PARQUET
        )


class TestCalibrateFlowDateHandling:
    def test_ensures_plain_date_objects_passed_as_dates(self):
        with patch(
            "prefect_server.calibrationFlowCommon.calibrate",
            return_value=[Path("layer.json")],
        ) as mock_calibrate:
            calibrate_flow.fn(
                start_date=date(2025, 1, 1),
                end_date=date(2025, 1, 2),
                configuration=GradiometryConfig(),
            )

        kwargs = mock_calibrate.call_args.kwargs
        assert kwargs["start_date"] == datetime(2025, 1, 1)
        assert kwargs["end_date"] == datetime(2025, 1, 2)

    def test_raises_when_end_date_before_start_date(self):
        with pytest.raises(ValueError, match="cannot be before start date"):
            calibrate_flow.fn(
                start_date=datetime(2025, 1, 5),
                end_date=datetime(2025, 1, 1),
                configuration=GradiometryConfig(),
            )

    def test_raises_runtime_error_when_no_layers_generated(self):
        with patch(
            "prefect_server.calibrationFlowCommon.calibrate",
            return_value=[],
        ):
            with pytest.raises(RuntimeError, match="No calibration layers"):
                calibrate_flow.fn(
                    start_date=datetime(2025, 1, 1),
                    configuration=GradiometryConfig(),
                )

    def test_returns_all_paths_when_multiple_layers_generated(self):
        paths = [Path("layer1.json"), Path("layer2.json")]
        with patch(
            "prefect_server.calibrationFlowCommon.calibrate",
            return_value=paths,
        ):
            result = calibrate_flow.fn(
                start_date=datetime(2025, 1, 1),
                configuration=GradiometryConfig(),
            )

        assert result == paths


class TestCalibrateFlowScripted:
    def test_scripted_requires_matlab_repo(self, tmp_path):
        mock_settings = MagicMock()
        mock_settings.work_folder = tmp_path
        with patch(
            "prefect_server.calibrationFlowCommon.AppSettings",
            return_value=mock_settings,
        ):
            with pytest.raises(TypeError, match="Unsupported matlab_repo type"):
                calibrate_flow.fn(
                    start_date=datetime(2026, 1, 30),
                    configuration=PrefectScriptedL2CalibrationConfig(
                        calibration_matrix_version=8,
                        input_json_file="input.json",
                        matlab_repo=None,
                        datastore_access_mode=DatastoreAccessMode.LOCAL_WORK_FOLDER_COPY,
                    ),
                )

    def test_scripted_resolves_repo_and_passes_to_calibrate(self, tmp_path):
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

        with (
            patch(
                "prefect_server.calibrationFlowCommon.AppSettings",
                return_value=mock_settings,
            ),
            patch(
                "prefect_server.calibrationFlowCommon.calibrate",
                return_value=[Path("layer.json")],
            ) as mock_calibrate,
        ):
            calibrate_flow.fn(
                start_date=datetime(2026, 1, 30),
                configuration=config,
                metakernel=Path("mk.txt"),
            )

        mock_calibrate.assert_called_once()
        kwargs = mock_calibrate.call_args.kwargs
        assert kwargs["metakernel"] == Path("mk.txt")


class TestCalibrateFlowSplitByDay:
    """split_by_day fans a date range out into one deployment run per day."""

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
            patch("prefect_server.calibrationFlowCommon.calibrate") as mock_calibrate,
        ):
            result = calibrate_flow.fn(
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 3),
                configuration=GradiometryConfig(),
                split_by_day=True,
            )

        # one deployment run per day, and the local calibration never runs
        assert mock_run_deployment.call_count == 3
        assert len(result) == 3
        mock_calibrate.assert_not_called()

        # each child run targets a single day with split_by_day disabled
        for call, day in zip(
            mock_run_deployment.call_args_list,
            [datetime(2025, 1, 1), datetime(2025, 1, 2), datetime(2025, 1, 3)],
        ):
            params = call.kwargs["parameters"]
            assert params["start_date"] == day
            assert params["end_date"] == day
            assert params["split_by_day"] is False
            assert call.kwargs["timeout"] == 0
            assert (
                call.kwargs["name"]
                == f"{PREFECT_CONSTANTS.FLOW_NAMES.CALIBRATE}/{PREFECT_CONSTANTS.DEPLOYMENT_NAMES.CALIBRATE}"
            )

    def test_single_day_runs_inline_even_when_split_requested(self):
        with (
            patch(
                "prefect_server.calibrationFlowCommon.run_deployment"
            ) as mock_run_deployment,
            patch(
                "prefect_server.calibrationFlowCommon.calibrate",
                return_value=[Path("layer.json")],
            ) as mock_calibrate,
        ):
            calibrate_flow.fn(
                start_date=datetime(2025, 1, 1),
                configuration=GradiometryConfig(),
                split_by_day=True,
            )

        mock_run_deployment.assert_not_called()
        mock_calibrate.assert_called_once()

    def test_range_without_split_runs_inline(self):
        with (
            patch(
                "prefect_server.calibrationFlowCommon.run_deployment"
            ) as mock_run_deployment,
            patch(
                "prefect_server.calibrationFlowCommon.calibrate",
                return_value=[Path("layer.json")],
            ) as mock_calibrate,
        ):
            calibrate_flow.fn(
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 3),
                configuration=GradiometryConfig(),
                split_by_day=False,
            )

        mock_run_deployment.assert_not_called()
        mock_calibrate.assert_called_once()

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
        ):
            calibrate_flow.fn(
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 2),
                configuration=config,
                split_by_day=True,
            )

        assert mock_run_deployment.call_count == 2
        for call in mock_run_deployment.call_args_list:
            configuration_param = call.kwargs["parameters"]["configuration"]
            assert isinstance(configuration_param, dict), (
                "configuration must be a dict (not a Pydantic model) so Prefect "
                "can resolve the $ref block reference in the child run"
            )
            assert configuration_param["matlab_repo"] == {
                "$ref": {"block_document_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
            }

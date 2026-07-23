"""Unit tests for performCalibration flow helper functions."""

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from prefect.filesystems import LocalFileSystem
from prefect_github import GitHubRepository

from mag_toolkit.calibration import DatastoreAccessMode
from mag_toolkit.calibration.CalibrationConfig import GradiometryConfig
from prefect_server.constants import PREFECT_CONSTANTS
from prefect_server.performCalibration import (
    PrefectScriptedL2CalibrationConfig,
    _days_in_range,
    _github_repo_name,
    _resolve_matlab_repo_path,
    apply_flow,
    calibrate_and_apply_flow,
    calibrate_flow,
    generate_apply_calibration_flow_run_name,
    generate_calibrate_and_apply_flow_run_name,
    generate_calibration_flow_run_name,
)


class TestPerformCalibrationFlowNames:
    def test_generate_calibrate_and_apply_name_includes_date_and_sensor(self):
        mock_configuration = MagicMock()
        mock_configuration.get_method.return_value = MagicMock(value="kepko")
        mock_params = {
            "start_date": datetime(2025, 1, 15),
            "end_date": None,
            "configuration": mock_configuration,
            "mode": MagicMock(value="norm"),
            "sensor": MagicMock(value="mago"),
        }

        with patch("prefect_server.performCalibration.flow_run") as mock_flow_run:
            mock_flow_run.parameters = mock_params
            result = generate_calibrate_and_apply_flow_run_name()

        assert "15-01-2025" in result
        assert "kepko" in result

    def test_generate_calibration_flow_name_with_date_range(self):
        mock_configuration = MagicMock()
        mock_configuration.get_method.return_value = MagicMock(value="kepko")
        mock_params = {
            "start_date": datetime(2025, 1, 1),
            "end_date": datetime(2025, 1, 31),
            "configuration": mock_configuration,
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
                configuration=GradiometryConfig(),
            )

        mock_calibrate.assert_called_once()
        mock_apply.assert_called_once()

    def test_calibrate_and_apply_flow_passes_scripted_l2_options_through(
        self, tmp_path
    ):
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
                "prefect_server.performCalibration.AppSettings",
                return_value=mock_settings,
            ),
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
                start_date=datetime(2026, 1, 30),
                configuration=config,
                metakernel=Path("mk.txt"),
            )

        mock_calibrate.assert_called_once()
        kwargs = mock_calibrate.call_args.kwargs
        assert kwargs["metakernel"] == Path("mk.txt")
        mock_apply.assert_called_once()

    def test_calibrate_and_apply_flow_scripted_requires_matlab_repo(self, tmp_path):
        mock_settings = MagicMock()
        mock_settings.work_folder = tmp_path
        with patch(
            "prefect_server.performCalibration.AppSettings",
            return_value=mock_settings,
        ):
            with pytest.raises(ValueError, match="matlab_repo is required"):
                calibrate_and_apply_flow.fn(
                    start_date=datetime(2026, 1, 30),
                    configuration=PrefectScriptedL2CalibrationConfig(
                        calibration_matrix_version=8,
                        input_json_file="input.json",
                        matlab_repo=None,
                        datastore_access_mode=DatastoreAccessMode.LOCAL_WORK_FOLDER_COPY,
                    ),
                )
            with pytest.raises(ValueError, match="matlab_repo is required"):
                calibrate_and_apply_flow.fn(
                    start_date=datetime(2026, 1, 30),
                    configuration=PrefectScriptedL2CalibrationConfig(
                        calibration_matrix_version=8,
                        input_json_file="input.json",
                        matlab_repo="",
                        datastore_access_mode=DatastoreAccessMode.LOCAL_WORK_FOLDER_COPY,
                    ),
                )


class TestPerformCalibrationFlows:
    def test_calibrate_flow_calls_calibrate(self):
        with patch("prefect_server.performCalibration.calibrate") as mock_calibrate:
            mock_calibrate.return_value = []
            calibrate_flow.fn(
                start_date=datetime(2025, 1, 1),
                configuration=GradiometryConfig(
                    kappa=0.1, sc_interference_threshold=0.2
                ),
            )

        mock_calibrate.assert_called_once()

    def test_apply_flow_calls_apply(self):
        with patch("prefect_server.performCalibration.apply") as mock_apply:
            mock_apply.return_value = []
            apply_flow.fn(
                layers=["*noop*"],
                start_date=datetime(2025, 1, 1),
            )

        mock_apply.assert_called_once()


class TestDaysInRange:
    def test_single_day_when_end_date_none(self):
        assert _days_in_range(datetime(2025, 1, 5), None) == [datetime(2025, 1, 5)]

    def test_single_day_when_end_equals_start(self):
        assert _days_in_range(datetime(2025, 1, 5), datetime(2025, 1, 5)) == [
            datetime(2025, 1, 5)
        ]

    def test_inclusive_range(self):
        days = _days_in_range(datetime(2025, 1, 1), datetime(2025, 1, 3))
        assert days == [
            datetime(2025, 1, 1),
            datetime(2025, 1, 2),
            datetime(2025, 1, 3),
        ]

    def test_preserves_time_of_day(self):
        days = _days_in_range(datetime(2025, 1, 1, 6, 30), datetime(2025, 1, 2, 6, 30))
        assert days == [
            datetime(2025, 1, 1, 6, 30),
            datetime(2025, 1, 2, 6, 30),
        ]


class TestSplitByDay:
    """split_by_day fans a date range out into one deployment run per day."""

    def _make_flow_run(self, name):
        fake = MagicMock()
        fake.name = name
        return fake

    def test_calibrate_flow_splits_range_into_per_day_deployment_runs(self):
        with (
            patch(
                "prefect_server.performCalibration.run_deployment",
                side_effect=lambda **kwargs: self._make_flow_run(
                    str(kwargs["parameters"]["start_date"])
                ),
            ) as mock_run_deployment,
            patch("prefect_server.performCalibration.calibrate") as mock_calibrate,
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

    def test_calibrate_flow_single_day_runs_inline_even_when_split_requested(self):
        with (
            patch(
                "prefect_server.performCalibration.run_deployment"
            ) as mock_run_deployment,
            patch(
                "prefect_server.performCalibration.calibrate",
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

    def test_calibrate_flow_range_without_split_runs_inline(self):
        with (
            patch(
                "prefect_server.performCalibration.run_deployment"
            ) as mock_run_deployment,
            patch(
                "prefect_server.performCalibration.calibrate",
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

    def test_calibrate_and_apply_flow_splits_range(self):
        with (
            patch(
                "prefect_server.performCalibration.run_deployment",
                return_value=self._make_flow_run("child"),
            ) as mock_run_deployment,
            patch("prefect_server.performCalibration.calibrate") as mock_calibrate,
            patch("prefect_server.performCalibration.apply") as mock_apply,
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

    def test_apply_flow_splits_range(self):
        with (
            patch(
                "prefect_server.performCalibration.run_deployment",
                return_value=self._make_flow_run("child"),
            ) as mock_run_deployment,
            patch("prefect_server.performCalibration.apply") as mock_apply,
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

    def test_apply_flow_default_does_not_split(self):
        with (
            patch(
                "prefect_server.performCalibration.run_deployment"
            ) as mock_run_deployment,
            patch("prefect_server.performCalibration.apply") as mock_apply,
        ):
            apply_flow.fn(
                layers=["*noop*"],
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 3),
            )

        mock_run_deployment.assert_not_called()
        mock_apply.assert_called_once()


class TestGithubRepoName:
    def test_ssh_url(self):
        assert (
            _github_repo_name("git@github.com:example-org/example-repo.git")
            == "example-repo"
        )

    def test_https_url(self):
        assert _github_repo_name("https://github.com/Org/Repo.git") == "Repo"

    def test_url_without_git_suffix(self):
        assert _github_repo_name("https://github.com/Org/Repo") == "Repo"

    def test_trailing_slash(self):
        assert _github_repo_name("https://github.com/Org/Repo/") == "Repo"


class TestResolveMatlabRepoPath:
    def test_none_returns_none(self, tmp_path):
        assert _resolve_matlab_repo_path(None, tmp_path) is None

    def test_local_filesystem_block(self, tmp_path):
        repo = tmp_path / "repo"
        repo.mkdir()
        block = LocalFileSystem(basepath=str(repo))
        assert _resolve_matlab_repo_path(block, tmp_path) == repo

    def test_local_filesystem_missing_raises(self, tmp_path):
        block = LocalFileSystem(basepath=str(tmp_path / "does-not-exist"))
        with pytest.raises(FileNotFoundError):
            _resolve_matlab_repo_path(block, tmp_path)

    def test_github_block_clones_into_work_folder(self, tmp_path):
        work = tmp_path / "work"
        work.mkdir()
        block = GitHubRepository(repository_url="git@github.com:Org/MyRepo.git")

        def fake_get_directory(local_path=None, from_path=None):
            Path(local_path).mkdir(parents=True, exist_ok=True)

        with patch.object(
            block, "get_directory", side_effect=fake_get_directory
        ) as mock_gd:
            result = _resolve_matlab_repo_path(block, work)

        assert result == work / "MyRepo"
        mock_gd.assert_called_once()

    def test_github_block_failed_clone_raises(self, tmp_path):
        block = GitHubRepository(repository_url="git@github.com:Org/MyRepo.git")

        with patch.object(block, "get_directory"):  # does not create the dir
            with pytest.raises(FileNotFoundError):
                _resolve_matlab_repo_path(block, tmp_path)

    def test_block_name_loads_block(self, tmp_path):
        repo = tmp_path / "repo"
        repo.mkdir()
        block = LocalFileSystem(basepath=str(repo))
        with patch(
            "prefect_server.performCalibration._load_matlab_repo_block",
            return_value=block,
        ):
            assert _resolve_matlab_repo_path("my-block", tmp_path) == repo

    def test_unknown_block_name_raises(self, tmp_path):
        with patch(
            "prefect_server.performCalibration._load_matlab_repo_block",
            return_value=None,
        ):
            with pytest.raises(ValueError, match="Could not load"):
                _resolve_matlab_repo_path("missing-block", tmp_path)


class TestCalibrateFlowScripted:
    def test_scripted_requires_matlab_repo(self, tmp_path):
        mock_settings = MagicMock()
        mock_settings.work_folder = tmp_path
        with patch(
            "prefect_server.performCalibration.AppSettings",
            return_value=mock_settings,
        ):
            with pytest.raises(ValueError, match="matlab_repo is required"):
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
                "prefect_server.performCalibration.AppSettings",
                return_value=mock_settings,
            ),
            patch(
                "prefect_server.performCalibration.calibrate",
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

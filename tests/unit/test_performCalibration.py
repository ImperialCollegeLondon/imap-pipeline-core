"""Unit tests for performCalibration flow helper functions."""

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from prefect.filesystems import LocalFileSystem
from prefect_github import GitHubRepository

from imap_mag.config import ScriptedL2CalibrationConfig
from imap_mag.util import ScienceMode
from mag_toolkit.calibration import CalibrationMethod, DatastoreAccessMode
from prefect_server.constants import PREFECT_CONSTANTS
from prefect_server.performCalibration import (
    _github_repo_name,
    _resolve_matlab_repo_path,
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
                    method=CalibrationMethod.SCRIPTED_L2_CALIBRATION,
                    matlab_repo=None,
                )

    def test_scripted_resolves_repo_and_passes_to_calibrate(self, tmp_path):
        repo = tmp_path / "repo"
        repo.mkdir()
        block = LocalFileSystem(basepath=str(repo))
        mock_settings = MagicMock()
        mock_settings.work_folder = tmp_path
        config = ScriptedL2CalibrationConfig(
            calibration_matrix_version=8, input_json_file="input.json"
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
                method=CalibrationMethod.SCRIPTED_L2_CALIBRATION,
                configuration=config,
                metakernel=Path("mk.txt"),
                matlab_repo=block,
                datastore_access_mode=DatastoreAccessMode.LOCAL_WORK_FOLDER_COPY,
            )

        mock_calibrate.assert_called_once()
        kwargs = mock_calibrate.call_args.kwargs
        assert kwargs["matlab_repo_path"] == repo
        assert kwargs["metakernel"] == Path("mk.txt")
        assert (
            kwargs["datastore_access_mode"]
            == DatastoreAccessMode.LOCAL_WORK_FOLDER_COPY
        )

    def test_non_scripted_method_ignores_matlab_repo(self):
        """Existing methods must not attempt any matlab_repo resolution."""
        with patch(
            "prefect_server.performCalibration.calibrate", return_value=[]
        ) as mock_calibrate:
            calibrate_flow.fn(start_date=datetime(2025, 1, 1))

        mock_calibrate.assert_called_once()
        assert mock_calibrate.call_args.kwargs["matlab_repo_path"] is None

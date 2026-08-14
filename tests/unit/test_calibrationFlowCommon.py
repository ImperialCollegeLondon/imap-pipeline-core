"""Unit tests for calibrationFlowCommon shared helper functions."""

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from prefect.filesystems import LocalFileSystem
from prefect_github import GitHubRepository

from mag_toolkit.calibration.CalibrationConfig import GradiometryConfig
from prefect_server.calibrationFlowCommon import (
    PrefectScriptedL2CalibrationConfig,
    _configuration_for_deployment,
    _days_in_range,
    _github_repo_name,
    _load_matlab_repo_block,
    _resolve_matlab_repo_path,
)


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


class TestConfigurationForDeployment:
    """_configuration_for_deployment replaces block objects with $ref references."""

    def test_non_scripted_config_returned_unchanged(self):
        config = GradiometryConfig()
        assert _configuration_for_deployment(config) is config

    def test_scripted_config_without_block_returned_unchanged(self):
        config = PrefectScriptedL2CalibrationConfig(
            calibration_matrix_version=8,
            input_json_file="input.json",
            matlab_repo="my-block-name",
        )
        result = _configuration_for_deployment(config)
        assert result is config

    def test_scripted_config_with_block_but_no_doc_id_returned_unchanged(self):
        block = GitHubRepository(
            repository_url="git@github.com:example-org/example-repo.git"
        )
        config = PrefectScriptedL2CalibrationConfig(
            calibration_matrix_version=8,
            input_json_file="input.json",
            matlab_repo=block,
        )
        result = _configuration_for_deployment(config)
        assert result is config

    def test_github_block_with_doc_id_replaced_by_ref(self):
        block = GitHubRepository(
            repository_url="git@github.com:example-org/example-repo.git"
        )
        block._block_document_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        config = PrefectScriptedL2CalibrationConfig(
            calibration_matrix_version=8,
            input_json_file="input.json",
            matlab_repo=block,
        )

        result = _configuration_for_deployment(config)

        assert isinstance(result, dict)
        assert result["matlab_repo"] == {
            "$ref": {"block_document_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}
        }

    def test_local_filesystem_block_with_doc_id_replaced_by_ref(self, tmp_path):
        repo = tmp_path / "repo"
        repo.mkdir()
        block = LocalFileSystem(basepath=str(repo))
        block._block_document_id = "11111111-2222-3333-4444-555555555555"
        config = PrefectScriptedL2CalibrationConfig(
            calibration_matrix_version=8,
            input_json_file="input.json",
            matlab_repo=block,
        )

        result = _configuration_for_deployment(config)

        assert isinstance(result, dict)
        assert result["matlab_repo"] == {
            "$ref": {"block_document_id": "11111111-2222-3333-4444-555555555555"}
        }

    def test_ref_dict_contains_other_config_fields(self):
        block = GitHubRepository(
            repository_url="git@github.com:example-org/example-repo.git"
        )
        block._block_document_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        config = PrefectScriptedL2CalibrationConfig(
            calibration_matrix_version=7,
            input_json_file="my-input.json",
            matlab_repo=block,
        )

        result = _configuration_for_deployment(config)

        assert isinstance(result, dict)
        assert result["calibration_matrix_version"] == 7
        assert result["input_json_file"] == "my-input.json"


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
            "prefect_server.calibrationFlowCommon._load_matlab_repo_block",
            return_value=block,
        ):
            assert _resolve_matlab_repo_path("my-block", tmp_path) == repo

    def test_unknown_block_name_raises(self, tmp_path):
        with patch(
            "prefect_server.calibrationFlowCommon._load_matlab_repo_block",
            return_value=None,
        ):
            with pytest.raises(ValueError, match="Could not load"):
                _resolve_matlab_repo_path("missing-block", tmp_path)

    def test_string_falls_back_to_local_path_when_no_block_found(self, tmp_path):
        repo = tmp_path / "local-repo"
        repo.mkdir()
        with patch(
            "prefect_server.calibrationFlowCommon._load_matlab_repo_block",
            return_value=None,
        ):
            assert _resolve_matlab_repo_path(str(repo), tmp_path) == repo

    def test_github_block_clears_existing_target_before_pull(self, tmp_path):
        work = tmp_path / "work"
        work.mkdir()
        block = GitHubRepository(repository_url="git@github.com:Org/MyRepo.git")

        stale_target = work / "MyRepo"
        stale_target.mkdir()
        (stale_target / "stale.txt").write_text("old")

        def fake_get_directory(local_path=None, from_path=None):
            Path(local_path).mkdir(parents=True, exist_ok=True)

        with patch.object(block, "get_directory", side_effect=fake_get_directory):
            result = _resolve_matlab_repo_path(block, work)

        assert result == stale_target
        assert not (stale_target / "stale.txt").exists()

    def test_unsupported_type_raises(self, tmp_path):
        with pytest.raises(TypeError, match="Unsupported matlab_repo type"):
            _resolve_matlab_repo_path(123, tmp_path)


class TestLoadMatlabRepoBlock:
    def test_returns_block_from_first_matching_type(self):
        fake_block = MagicMock(spec=GitHubRepository)
        with patch.object(GitHubRepository, "load", return_value=fake_block):
            result = _load_matlab_repo_block("my-block")

        assert result is fake_block

    def test_falls_through_to_next_type_on_failure(self):
        fake_block = MagicMock(spec=LocalFileSystem)
        with (
            patch.object(
                GitHubRepository, "load", side_effect=ValueError("no such block")
            ),
            patch.object(LocalFileSystem, "load", return_value=fake_block),
        ):
            result = _load_matlab_repo_block("my-block")

        assert result is fake_block

    def test_returns_none_when_no_type_matches(self):
        with (
            patch.object(GitHubRepository, "load", side_effect=ValueError("nope")),
            patch.object(LocalFileSystem, "load", side_effect=ValueError("nope")),
        ):
            result = _load_matlab_repo_block("missing-block")

        assert result is None

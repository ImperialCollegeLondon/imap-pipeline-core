"""Unit tests for MatlabWrapper module functions."""

import os
from unittest.mock import MagicMock, patch

import pytest

import mag_toolkit.calibration.MatlabWrapper as matlab_wrapper
from mag_toolkit.calibration.MatlabWrapper import (
    call_matlab,
    get_matlab_command,
)


@pytest.fixture(autouse=True)
def reset_matlab_path_initialized():
    """Reset the process-level path-setup guard so each test is deterministic."""
    matlab_wrapper._matlab_path_initialized = False
    yield
    matlab_wrapper._matlab_path_initialized = False


def _make_mock_process(returncode=0, output_lines=None):
    """Create a mock subprocess.Popen result."""
    if output_lines is None:
        output_lines = []
    mock_process = MagicMock()
    mock_process.returncode = returncode
    # readline returns lines then "" to signal end
    mock_process.stdout.readline.side_effect = [*output_lines, ""]
    mock_process.wait.return_value = None
    return mock_process


class TestGetMatlabCommand:
    def test_returns_matlab_when_not_in_ci(self):
        with patch.dict(os.environ, {"CI": "false"}, clear=False):
            result = get_matlab_command()
        assert result == "matlab"

    def test_returns_matlab_when_ci_true_but_no_license_token(self):
        with patch.dict(os.environ, {"CI": "true"}, clear=False):
            if "MLM_LICENSE_TOKEN" in os.environ:
                del os.environ["MLM_LICENSE_TOKEN"]
            result = get_matlab_command()
        assert result == "matlab"

    def test_returns_matlab_batch_when_ci_with_token_and_command_exists(self):
        with (
            patch.dict(
                os.environ,
                {"CI": "true", "MLM_LICENSE_TOKEN": "test-token"},
                clear=False,
            ),
            patch(
                "mag_toolkit.calibration.MatlabWrapper.which",
                return_value="/usr/bin/matlab-batch",
            ),
        ):
            result = get_matlab_command()
        assert result == "matlab-batch"

    def test_returns_matlab_when_ci_with_token_but_command_not_found(self):
        with (
            patch.dict(
                os.environ,
                {"CI": "true", "MLM_LICENSE_TOKEN": "test-token"},
                clear=False,
            ),
            patch("mag_toolkit.calibration.MatlabWrapper.which", return_value=None),
        ):
            result = get_matlab_command()
        assert result == "matlab"


class TestSetupMatlabPathPrefix:
    def test_prefix_includes_both_paths_and_savepath(self):
        prefix = matlab_wrapper._build_path_setup_prefix()
        assert "/app/matlab" in prefix
        assert "src/matlab" in prefix
        assert "addpath" in prefix
        assert "savepath" in prefix


class TestCallMatlab:
    def test_folds_path_setup_into_command_on_first_call(self):
        """A single Popen call is made with addpath/savepath prepended to the command."""
        mock_process = _make_mock_process(returncode=0)

        with (
            patch(
                "mag_toolkit.calibration.MatlabWrapper.subprocess.Popen",
                return_value=mock_process,
            ) as mock_popen,
            patch(
                "mag_toolkit.calibration.MatlabWrapper.get_matlab_command",
                return_value="matlab",
            ),
        ):
            call_matlab("disp('hello')", first_call=True)

        mock_popen.assert_called_once()
        batch_arg = mock_popen.call_args[0][0][-1]
        assert "addpath" in batch_arg
        assert "savepath" in batch_arg
        assert "disp('hello')" in batch_arg

    def test_skips_path_setup_on_subsequent_first_calls_in_same_process(self):
        """Path setup prefix is only included in the very first call, not subsequent ones."""
        mock_process = _make_mock_process(returncode=0)
        mock_process.stdout.readline.side_effect = None
        mock_process.stdout.readline.return_value = ""

        with (
            patch(
                "mag_toolkit.calibration.MatlabWrapper.subprocess.Popen",
                return_value=mock_process,
            ) as mock_popen,
            patch(
                "mag_toolkit.calibration.MatlabWrapper.get_matlab_command",
                return_value="matlab",
            ),
        ):
            call_matlab("disp('one')", first_call=True)
            call_matlab("disp('two')", first_call=True)

        assert mock_popen.call_count == 2
        first_batch = mock_popen.call_args_list[0][0][0][-1]
        second_batch = mock_popen.call_args_list[1][0][0][-1]
        assert "addpath" in first_batch
        assert "disp('one')" in first_batch
        assert "addpath" not in second_batch
        assert "disp('two')" in second_batch

    def test_skips_path_setup_when_not_first_call(self):
        mock_process = _make_mock_process(returncode=0)

        with (
            patch(
                "mag_toolkit.calibration.MatlabWrapper.subprocess.Popen",
                return_value=mock_process,
            ) as mock_popen,
            patch(
                "mag_toolkit.calibration.MatlabWrapper.get_matlab_command",
                return_value="matlab",
            ),
        ):
            call_matlab("disp('hello')", first_call=False)

        mock_popen.assert_called_once()
        batch_arg = mock_popen.call_args[0][0][-1]
        assert "addpath" not in batch_arg
        assert "disp('hello')" in batch_arg

    def test_raises_runtime_error_when_matlab_command_fails(self):
        mock_process = _make_mock_process(returncode=1)

        with (
            patch(
                "mag_toolkit.calibration.MatlabWrapper.subprocess.Popen",
                return_value=mock_process,
            ),
            patch(
                "mag_toolkit.calibration.MatlabWrapper.get_matlab_command",
                return_value="matlab",
            ),
        ):
            with pytest.raises(RuntimeError, match="MATLAB command failed"):
                call_matlab("disp('hello')")


class TestCallMatlabExternalRepo:
    def test_passes_cwd_and_unsets_display(self, tmp_path):
        mock_process = _make_mock_process(returncode=0)

        with (
            patch(
                "mag_toolkit.calibration.MatlabWrapper.subprocess.Popen",
                return_value=mock_process,
            ) as mock_popen,
            patch(
                "mag_toolkit.calibration.MatlabWrapper.get_matlab_command",
                return_value="matlab",
            ),
            patch.dict(os.environ, {"DISPLAY": ":0"}, clear=False),
        ):
            call_matlab(
                "run()",
                cwd=tmp_path,
                unset_display=True,
                include_project_paths=False,
            )

        kwargs = mock_popen.call_args.kwargs
        assert kwargs["cwd"] == str(tmp_path)
        assert "DISPLAY" not in kwargs["env"]

    def test_include_project_paths_false_skips_prefix_on_first_call(self):
        mock_process = _make_mock_process(returncode=0)

        with (
            patch(
                "mag_toolkit.calibration.MatlabWrapper.subprocess.Popen",
                return_value=mock_process,
            ) as mock_popen,
            patch(
                "mag_toolkit.calibration.MatlabWrapper.get_matlab_command",
                return_value="matlab",
            ),
        ):
            call_matlab("run()", first_call=True, include_project_paths=False)

        batch_arg = mock_popen.call_args[0][0][-1]
        assert "addpath" not in batch_arg
        assert "run()" in batch_arg

    def test_default_call_has_no_cwd_and_keeps_display(self):
        mock_process = _make_mock_process(returncode=0)

        with (
            patch(
                "mag_toolkit.calibration.MatlabWrapper.subprocess.Popen",
                return_value=mock_process,
            ) as mock_popen,
            patch(
                "mag_toolkit.calibration.MatlabWrapper.get_matlab_command",
                return_value="matlab",
            ),
            patch.dict(os.environ, {"DISPLAY": ":99"}, clear=False),
        ):
            call_matlab("disp('hi')")

        kwargs = mock_popen.call_args.kwargs
        assert kwargs["cwd"] is None
        assert kwargs["env"].get("DISPLAY") == ":99"

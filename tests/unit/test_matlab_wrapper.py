"""Unit tests for MatlabWrapper module functions."""

import os
from unittest.mock import MagicMock, patch

import pytest

from mag_toolkit.calibration.MatlabWrapper import (
    call_matlab,
    get_matlab_command,
    setup_matlab_path,
)


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


class TestSetupMatlabPath:
    def test_runs_matlab_setup_command_and_returns_on_success(self):
        mock_process = _make_mock_process(
            returncode=0, output_lines=["Setting up paths"]
        )

        with patch(
            "mag_toolkit.calibration.MatlabWrapper.subprocess.Popen",
            return_value=mock_process,
        ):
            setup_matlab_path(["/app/matlab"], "matlab")

        mock_process.wait.assert_called_once_with(timeout=60)

    def test_raises_runtime_error_when_matlab_returns_nonzero(self):
        mock_process = _make_mock_process(returncode=1)

        with patch(
            "mag_toolkit.calibration.MatlabWrapper.subprocess.Popen",
            return_value=mock_process,
        ):
            with pytest.raises(RuntimeError, match="MATLAB setup command failed"):
                setup_matlab_path(["/app/matlab"], "matlab")

    def test_accepts_string_path_instead_of_list(self):
        mock_process = _make_mock_process(returncode=0)

        with patch(
            "mag_toolkit.calibration.MatlabWrapper.subprocess.Popen",
            return_value=mock_process,
        ) as mock_popen:
            setup_matlab_path("/app/matlab", "matlab")

        mock_popen.assert_called_once()
        cmd = mock_popen.call_args[0][0]
        assert "/app/matlab" in " ".join(cmd)


class TestCallMatlab:
    def test_calls_setup_and_runs_command_on_first_call(self):
        mock_process = _make_mock_process(returncode=0)

        with (
            patch(
                "mag_toolkit.calibration.MatlabWrapper.subprocess.Popen",
                return_value=mock_process,
            ),
            patch(
                "mag_toolkit.calibration.MatlabWrapper.setup_matlab_path"
            ) as mock_setup,
            patch(
                "mag_toolkit.calibration.MatlabWrapper.get_matlab_command",
                return_value="matlab",
            ),
        ):
            call_matlab("disp('hello')", first_call=True)

        mock_setup.assert_called_once()

    def test_skips_setup_when_not_first_call(self):
        mock_process = _make_mock_process(returncode=0)

        with (
            patch(
                "mag_toolkit.calibration.MatlabWrapper.subprocess.Popen",
                return_value=mock_process,
            ),
            patch(
                "mag_toolkit.calibration.MatlabWrapper.setup_matlab_path"
            ) as mock_setup,
            patch(
                "mag_toolkit.calibration.MatlabWrapper.get_matlab_command",
                return_value="matlab",
            ),
        ):
            call_matlab("disp('hello')", first_call=False)

        mock_setup.assert_not_called()

    def test_raises_runtime_error_when_matlab_command_fails(self):
        mock_process = _make_mock_process(returncode=1)

        with (
            patch(
                "mag_toolkit.calibration.MatlabWrapper.subprocess.Popen",
                return_value=mock_process,
            ),
            patch("mag_toolkit.calibration.MatlabWrapper.setup_matlab_path"),
            patch(
                "mag_toolkit.calibration.MatlabWrapper.get_matlab_command",
                return_value="matlab",
            ),
        ):
            with pytest.raises(RuntimeError, match="MATLAB command failed"):
                call_matlab("disp('hello')")

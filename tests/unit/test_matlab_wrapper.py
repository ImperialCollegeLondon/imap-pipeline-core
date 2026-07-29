"""Unit tests for MatlabWrapper module functions."""

import os
import signal
import subprocess
from unittest.mock import MagicMock, call, patch

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
            call_matlab("disp('hello')")

        mock_popen.assert_called_once()
        batch_arg = mock_popen.call_args[0][0][-1]
        assert "addpath" in batch_arg
        assert "savepath" in batch_arg
        assert "disp('hello')" in batch_arg

    def test_skips_path_setup_on_subsequent_calls_in_same_process(self):
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
            call_matlab("disp('one')")
            call_matlab("disp('two')")

        assert mock_popen.call_count == 2
        first_batch = mock_popen.call_args_list[0][0][0][-1]
        second_batch = mock_popen.call_args_list[1][0][0][-1]
        assert "addpath" in first_batch
        assert "disp('one')" in first_batch
        assert "addpath" not in second_batch
        assert "disp('two')" in second_batch

    def test_skips_path_setup_when_project_paths_excluded(self):
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
            call_matlab("disp('hello')", include_project_paths=False)

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
    def test_passes_cwd_to_popen(self, tmp_path):
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
            call_matlab("run()", cwd=tmp_path, include_project_paths=False)

        kwargs = mock_popen.call_args.kwargs
        assert kwargs["cwd"] == str(tmp_path)

    def test_always_unsets_display(self):
        """DISPLAY is removed from the MATLAB environment on every call."""
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
            call_matlab("run()")

        kwargs = mock_popen.call_args.kwargs
        assert "DISPLAY" not in kwargs["env"]

    def test_default_call_has_no_cwd(self):
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
            call_matlab("disp('hi')")

        assert mock_popen.call_args.kwargs["cwd"] is None


class TestCallMatlabLineLogging:
    def test_logs_each_line_at_its_prefixed_level(self):
        mock_process = _make_mock_process(
            returncode=0,
            output_lines=[
                "INFO: info message",
                "WARN: warn message",
                "ERROR: error message",
                "DEBUG: debug message",
                "CRITICAL: critical message",
                "plain message",
            ],
        )

        with (
            patch(
                "mag_toolkit.calibration.MatlabWrapper.subprocess.Popen",
                return_value=mock_process,
            ),
            patch(
                "mag_toolkit.calibration.MatlabWrapper.get_matlab_command",
                return_value="matlab",
            ),
            patch.object(matlab_wrapper.logger, "info") as mock_info,
            patch.object(matlab_wrapper.logger, "warning") as mock_warning,
            patch.object(matlab_wrapper.logger, "error") as mock_error,
            patch.object(matlab_wrapper.logger, "debug") as mock_debug,
            patch.object(matlab_wrapper.logger, "critical") as mock_critical,
        ):
            call_matlab("disp('hi')", include_project_paths=False)

        mock_info.assert_any_call("info message")
        mock_warning.assert_any_call("warn message")
        mock_error.assert_any_call("error message")
        mock_debug.assert_any_call("debug message")
        mock_critical.assert_any_call("critical message")
        mock_info.assert_any_call("plain message")

    def test_blank_lines_are_not_logged(self):
        mock_process = _make_mock_process(
            returncode=0, output_lines=["  ", "INFO: real message"]
        )

        with (
            patch(
                "mag_toolkit.calibration.MatlabWrapper.subprocess.Popen",
                return_value=mock_process,
            ),
            patch(
                "mag_toolkit.calibration.MatlabWrapper.get_matlab_command",
                return_value="matlab",
            ),
            patch.object(matlab_wrapper.logger, "info") as mock_info,
        ):
            call_matlab("disp('hi')", include_project_paths=False)

        assert call("") not in mock_info.call_args_list
        mock_info.assert_any_call("real message")


class TestCallMatlabExceptionHandling:
    def test_timeout_terminates_process_group_and_reraises(self):
        mock_process = _make_mock_process(returncode=0)
        mock_process.wait.side_effect = subprocess.TimeoutExpired(
            cmd="matlab", timeout=5
        )

        with (
            patch(
                "mag_toolkit.calibration.MatlabWrapper.subprocess.Popen",
                return_value=mock_process,
            ),
            patch(
                "mag_toolkit.calibration.MatlabWrapper.get_matlab_command",
                return_value="matlab",
            ),
            patch(
                "mag_toolkit.calibration.MatlabWrapper._terminate_process_group"
            ) as mock_terminate,
        ):
            with pytest.raises(subprocess.TimeoutExpired):
                call_matlab("disp('hi')", timeout=5)

        mock_terminate.assert_called_once_with(mock_process)

    def test_keyboard_interrupt_terminates_process_group_and_reraises(self):
        mock_process = _make_mock_process(returncode=0)
        mock_process.wait.side_effect = KeyboardInterrupt()

        with (
            patch(
                "mag_toolkit.calibration.MatlabWrapper.subprocess.Popen",
                return_value=mock_process,
            ),
            patch(
                "mag_toolkit.calibration.MatlabWrapper.get_matlab_command",
                return_value="matlab",
            ),
            patch(
                "mag_toolkit.calibration.MatlabWrapper._terminate_process_group"
            ) as mock_terminate,
        ):
            with pytest.raises(KeyboardInterrupt):
                call_matlab("disp('hi')")

        mock_terminate.assert_called_once_with(mock_process)

    def test_unexpected_error_terminates_process_group_and_reraises(self):
        mock_process = _make_mock_process(returncode=0)
        mock_process.wait.side_effect = ValueError("boom")

        with (
            patch(
                "mag_toolkit.calibration.MatlabWrapper.subprocess.Popen",
                return_value=mock_process,
            ),
            patch(
                "mag_toolkit.calibration.MatlabWrapper.get_matlab_command",
                return_value="matlab",
            ),
            patch(
                "mag_toolkit.calibration.MatlabWrapper._terminate_process_group"
            ) as mock_terminate,
        ):
            with pytest.raises(ValueError, match="boom"):
                call_matlab("disp('hi')")

        mock_terminate.assert_called_once_with(mock_process)

    def test_terminates_process_group_when_returncode_missing_after_wait(self):
        """If the process never sets a returncode, the finally block still terminates it."""
        mock_process = _make_mock_process(returncode=None)

        with (
            patch(
                "mag_toolkit.calibration.MatlabWrapper.subprocess.Popen",
                return_value=mock_process,
            ),
            patch(
                "mag_toolkit.calibration.MatlabWrapper.get_matlab_command",
                return_value="matlab",
            ),
            patch(
                "mag_toolkit.calibration.MatlabWrapper._terminate_process_group"
            ) as mock_terminate,
        ):
            with pytest.raises(RuntimeError, match="MATLAB command failed"):
                call_matlab("disp('hi')")

        mock_terminate.assert_called_once_with(mock_process)


class TestSetParentDeathSignalLinux:
    def test_noop_on_non_posix(self):
        with (
            patch("mag_toolkit.calibration.MatlabWrapper.os.name", "nt"),
            patch(
                "mag_toolkit.calibration.MatlabWrapper.ctypes.util.find_library"
            ) as mock_find_library,
        ):
            matlab_wrapper._set_parent_death_signal_linux()

        mock_find_library.assert_not_called()

    def test_noop_when_libc_not_found(self):
        with (
            patch("mag_toolkit.calibration.MatlabWrapper.os.name", "posix"),
            patch(
                "mag_toolkit.calibration.MatlabWrapper.ctypes.util.find_library",
                return_value=None,
            ),
            patch("mag_toolkit.calibration.MatlabWrapper.ctypes.CDLL") as mock_cdll,
        ):
            matlab_wrapper._set_parent_death_signal_linux()

        mock_cdll.assert_not_called()

    def test_sets_pdeathsig_successfully(self):
        mock_libc = MagicMock()
        mock_libc.prctl.return_value = 0

        with (
            patch("mag_toolkit.calibration.MatlabWrapper.os.name", "posix"),
            patch(
                "mag_toolkit.calibration.MatlabWrapper.ctypes.util.find_library",
                return_value="libc.so.6",
            ),
            patch(
                "mag_toolkit.calibration.MatlabWrapper.ctypes.CDLL",
                return_value=mock_libc,
            ),
        ):
            matlab_wrapper._set_parent_death_signal_linux()

        mock_libc.prctl.assert_called_once_with(1, signal.SIGKILL)

    def test_raises_oserror_when_prctl_fails(self):
        mock_libc = MagicMock()
        mock_libc.prctl.return_value = -1

        with (
            patch("mag_toolkit.calibration.MatlabWrapper.os.name", "posix"),
            patch(
                "mag_toolkit.calibration.MatlabWrapper.ctypes.util.find_library",
                return_value="libc.so.6",
            ),
            patch(
                "mag_toolkit.calibration.MatlabWrapper.ctypes.CDLL",
                return_value=mock_libc,
            ),
            patch(
                "mag_toolkit.calibration.MatlabWrapper.ctypes.get_errno",
                return_value=5,
            ),
        ):
            with pytest.raises(OSError):
                matlab_wrapper._set_parent_death_signal_linux()


class TestTerminateProcessGroup:
    def test_returns_immediately_if_process_already_exited(self):
        process = MagicMock()
        process.poll.return_value = 0

        with patch("mag_toolkit.calibration.MatlabWrapper.os.killpg") as mock_killpg:
            matlab_wrapper._terminate_process_group(process)

        mock_killpg.assert_not_called()
        process.wait.assert_not_called()

    def test_sigterm_succeeds_without_escalation(self):
        process = MagicMock()
        process.poll.return_value = None
        process.pid = 1234
        process.wait.return_value = None

        with patch("mag_toolkit.calibration.MatlabWrapper.os.killpg") as mock_killpg:
            matlab_wrapper._terminate_process_group(process, timeout=5.0)

        mock_killpg.assert_called_once_with(1234, signal.SIGTERM)
        process.wait.assert_called_once_with(timeout=5.0)

    def test_process_lookup_error_on_sigterm_returns_without_escalating(self):
        process = MagicMock()
        process.poll.return_value = None
        process.pid = 1234

        with patch(
            "mag_toolkit.calibration.MatlabWrapper.os.killpg",
            side_effect=ProcessLookupError,
        ) as mock_killpg:
            matlab_wrapper._terminate_process_group(process)

        mock_killpg.assert_called_once()
        process.wait.assert_not_called()

    def test_escalates_to_sigkill_after_sigterm_timeout(self):
        process = MagicMock()
        process.poll.return_value = None
        process.pid = 1234
        process.wait.side_effect = [
            subprocess.TimeoutExpired(cmd="matlab", timeout=5.0),
            None,
        ]

        with patch("mag_toolkit.calibration.MatlabWrapper.os.killpg") as mock_killpg:
            matlab_wrapper._terminate_process_group(process, timeout=5.0)

        assert mock_killpg.call_args_list == [
            call(1234, signal.SIGTERM),
            call(1234, signal.SIGKILL),
        ]
        assert process.wait.call_count == 2

    def test_process_lookup_error_on_sigkill_returns(self):
        process = MagicMock()
        process.poll.return_value = None
        process.pid = 1234
        process.wait.side_effect = subprocess.TimeoutExpired(cmd="matlab", timeout=5.0)

        with patch(
            "mag_toolkit.calibration.MatlabWrapper.os.killpg",
            side_effect=[None, ProcessLookupError()],
        ) as mock_killpg:
            matlab_wrapper._terminate_process_group(process, timeout=5.0)

        assert mock_killpg.call_count == 2
        assert process.wait.call_count == 1

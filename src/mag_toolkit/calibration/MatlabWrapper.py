import ctypes
import ctypes.util
import logging
import os
import signal
import subprocess
from pathlib import Path
from shutil import which

logger = logging.getLogger(__name__)

_MATLAB_DEFAULT_PATH = "/app/matlab"
_MATLAB_LOCAL_PATH = "src/matlab"

# Tracks whether the MATLAB path has already been set up in this process.
# ``savepath`` persists the path to disk, so setup only needs to run once.
_matlab_path_initialized = False


def _set_parent_death_signal_linux() -> None:
    """Configure the child process to receive a signal when parent dies.

    Linux-only: sets ``PR_SET_PDEATHSIG`` so the MATLAB child receives
    ``SIGKILL`` if this Python process exits unexpectedly (including SIGKILL).
    """
    if os.name != "posix":
        return

    libc_name = ctypes.util.find_library("c")
    if libc_name is None:
        return

    libc = ctypes.CDLL(libc_name, use_errno=True)
    pr_set_pdeathsig = 1
    if libc.prctl(pr_set_pdeathsig, signal.SIGKILL) != 0:
        err = ctypes.get_errno()
        raise OSError(err, "prctl(PR_SET_PDEATHSIG) failed")


def _terminate_process_group(process: subprocess.Popen, timeout: float = 10.0) -> None:
    """Terminate the MATLAB process group, escalating to SIGKILL if required."""
    if process.poll() is not None:
        return

    try:
        os.killpg(process.pid, signal.SIGTERM)
        process.wait(timeout=timeout)
        return
    except ProcessLookupError:
        return
    except subprocess.TimeoutExpired:
        logger.warning("MATLAB did not terminate after SIGTERM; sending SIGKILL")

    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        return

    process.wait()


def _build_path_setup_prefix() -> str:
    """Return MATLAB commands that add project paths and persist them."""
    return (
        f'addpath(genpath("{_MATLAB_DEFAULT_PATH}")); '
        f'addpath(genpath("{_MATLAB_LOCAL_PATH}")); '
        f"savepath; "
    )


def get_matlab_command():
    if (
        os.getenv("CI") == "true"
        and os.getenv("MLM_LICENSE_TOKEN")
        and (which("matlab-batch") is not None)
    ):
        return "matlab-batch"
    else:
        return "matlab"


def _get_display_wrapper() -> list[str]:
    """Return an xvfb-run prefix if available, otherwise an empty list.

    MATLAB's graphics subsystem needs some X11 infrastructure even in batch
    mode when generating figures for report files.  ``xvfb-run`` spins up an
    ephemeral virtual framebuffer for each invocation so MATLAB can render
    off-screen without a physical display.  When ``xvfb-run`` is absent (e.g.
    on a developer machine) the prefix is omitted and the caller falls back to
    the previous behaviour of running MATLAB directly.
    """
    if os.name == "posix" and which("xvfb-run") is not None:
        return ["xvfb-run", "--auto-servernum"]
    return []


def call_matlab(
    command,
    timeout=60 * 5,
    cwd: Path | str | None = None,
    include_project_paths: bool = True,
) -> list[str]:
    """Run a MATLAB batch command, folding project path setup into the first call.

    Returns the ans= output lines from MATLAB as a list of strings.
    If function output is a single string, the list will contain one string with quotes stripped.
    If function output is a 1d cell array, the list will contain one string with the array as a whole string.

    When ``include_project_paths`` is True and the imap-mag project paths have not
    yet been set up in this process, the ``addpath``/``savepath`` preamble is
    prepended to ``command`` so that both path setup and the actual work happen in
    a single MATLAB cold-start instead of two. Subsequent calls skip the preamble
    (it is persisted via ``savepath``), and self-contained external MATLAB projects
    pass ``include_project_paths=False`` to opt out entirely.

    ``DISPLAY`` is always removed from the environment passed to the subprocess.
    When ``xvfb-run`` is available it wraps the MATLAB command and sets its own
    isolated ``DISPLAY`` for the child process, allowing MATLAB to render figures
    off-screen without a physical display.

    Args:
        command: The MATLAB command to run inside ``matlab -batch``.
        timeout: Timeout in seconds for the MATLAB process.
        cwd: Working directory to run MATLAB from. When calibrating with an
            externally-acquired MATLAB project the working directory must be the
            root of that project so its own ``addpath(pwd)`` logic resolves.
        include_project_paths: If True, prepend the imap-mag project MATLAB path
            preamble on the first call. Set False when invoking a self-contained
            external MATLAB project that sets up its own paths.
    """
    global _matlab_path_initialized

    MATLAB_COMMAND = get_matlab_command()

    if include_project_paths and not _matlab_path_initialized:
        batch_command = _build_path_setup_prefix() + command
        _matlab_path_initialized = True
        logger.info(
            f"Prepending MATLAB path setup for {_MATLAB_LOCAL_PATH} and {_MATLAB_DEFAULT_PATH}"
        )
    else:
        batch_command = command

    display_wrapper = _get_display_wrapper()
    cmd = [
        *display_wrapper,
        MATLAB_COMMAND,
        "-nodesktop",
        "-nojvm",
        "-batch",
        batch_command,
    ]

    # Remove DISPLAY so the host display never leaks into the subprocess.
    # xvfb-run (when present) sets its own isolated DISPLAY for MATLAB.
    env = os.environ.copy()
    env.pop("DISPLAY", None)

    logger.info(
        f"Calling MATLAB with command (cwd={cwd or os.getcwd()}): \n  {' '.join(cmd)}"
    )
    p = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        cwd=str(cwd) if cwd is not None else None,
        env=env,
        start_new_session=True,
        preexec_fn=_set_parent_death_signal_linux if os.name == "posix" else None,
    )

    answer_lines: list[str] = []
    started_answering = False
    try:
        while (line := p.stdout.readline()) != "":  # type: ignore
            line = line.rstrip()
            log_method = logger.info
            if line.startswith("INFO: "):
                line = line[len("INFO: ") :]
            elif line.startswith("WARN: "):
                line = line[len("WARN: ") :]
                log_method = logger.warning
            elif line.startswith("ERROR: "):
                line = line[len("ERROR: ") :]
                log_method = logger.error
            elif line.startswith("DEBUG: "):
                line = line[len("DEBUG: ") :]
                log_method = logger.debug
            elif line.startswith("CRITICAL: "):
                line = line[len("CRITICAL: ") :]
                log_method = logger.critical

            # Capture the MATLAB output after the first "ans =" line, which is the start of the function's return value.
            # only log these lines to debug since they are logged anyway after the process finishes.
            if started_answering and line:
                answer_lines.append(line)
                log_method = logger.debug

            if line.startswith("ans ="):
                started_answering = True  # next and later lines are collected
                log_method = logger.debug

            if line:
                log_method(line)

        p.wait(timeout=timeout)
    except (subprocess.TimeoutExpired, KeyboardInterrupt):
        logger.warning("Stopping MATLAB process group after interruption/timeout")
        _terminate_process_group(p)
        raise
    except Exception:
        logger.exception(
            "Unexpected error while waiting for MATLAB; terminating process"
        )
        _terminate_process_group(p)
        raise
    finally:
        if p.returncode is None:
            logger.warning("MATLAB process did not exit cleanly; terminating process")
            _terminate_process_group(p)

    if p.returncode != 0:
        logger.error(f"MATLAB command failed with return code {p.returncode}")
        raise RuntimeError(f"MATLAB command failed with return code {p.returncode}")

    answer_lines = [
        line.strip().strip('"') for line in answer_lines if line.strip()
    ]  # remove empty lines and whitespace

    if len(answer_lines) > 0:
        logger.info(
            "Result from MATLAB command: ("
            + str(len(answer_lines))
            + " line(s))\n"
            + "\n".join(answer_lines)
        )

    logger.info(f"MATLAB process finished with return code {p.returncode}")

    return answer_lines

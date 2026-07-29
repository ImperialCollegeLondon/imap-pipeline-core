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


def call_matlab(
    command,
    timeout=60 * 5,
    cwd: Path | str | None = None,
    include_project_paths: bool = True,
):
    """Run a MATLAB batch command, folding project path setup into the first call.

    When ``include_project_paths`` is True and the imap-mag project paths have not
    yet been set up in this process, the ``addpath``/``savepath`` preamble is
    prepended to ``command`` so that both path setup and the actual work happen in
    a single MATLAB cold-start instead of two. Subsequent calls skip the preamble
    (it is persisted via ``savepath``), and self-contained external MATLAB projects
    pass ``include_project_paths=False`` to opt out entirely.

    ``DISPLAY`` is always removed from the environment so MATLAB never tries to
    open plot windows.

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

    cmd = [MATLAB_COMMAND, "-nodesktop", "-nojvm", "-batch", batch_command]

    # Always unset DISPLAY so MATLAB does not attempt to open plot windows.
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

    try:
        while (line := p.stdout.readline()) != "":  # type: ignore
            line = line.rstrip()
            if line.startswith("INFO: "):
                line = line[len("INFO: ") :]
            if line:
                logger.info(line)

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

    logger.info(f"MATLAB process finished with return code {p.returncode}")

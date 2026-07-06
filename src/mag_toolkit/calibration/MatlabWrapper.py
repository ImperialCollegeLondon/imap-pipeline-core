import logging
import os
import subprocess
from pathlib import Path
from shutil import which

logger = logging.getLogger(__name__)

_MATLAB_DEFAULT_PATH = "/app/matlab"
_MATLAB_LOCAL_PATH = "src/matlab"

# Tracks whether the MATLAB path has already been set up in this process.
# ``savepath`` persists the path to disk, so setup only needs to run once.
_matlab_path_initialized = False


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
    first_call=True,
    timeout=60 * 5,
    cwd: Path | str | None = None,
    unset_display: bool = False,
    include_project_paths: bool = True,
):
    """Run a MATLAB batch command, folding path setup into the first invocation.

    On the first call in a process (``first_call=True``, ``include_project_paths``
    and path not yet initialised) the ``addpath``/``savepath`` preamble is
    prepended to ``command`` so that both path setup and the actual work happen
    in a single MATLAB cold-start instead of two.

    Args:
        command: The MATLAB command to run inside ``matlab -batch``.
        first_call: Whether this is the first MATLAB call in the process (controls
            whether the project path preamble is folded in).
        timeout: Timeout in seconds for the MATLAB process.
        cwd: Working directory to run MATLAB from. When calibrating with an
            externally-acquired MATLAB project the working directory must be the
            root of that project so its own ``addpath(pwd)`` logic resolves.
        unset_display: If True, remove the ``DISPLAY`` env var for the MATLAB
            process so it does not attempt to open plot windows.
        include_project_paths: If True, allow prepending the imap-mag project
            MATLAB path preamble. Set False when invoking a self-contained
            external MATLAB project that sets up its own paths.
    """
    global _matlab_path_initialized

    MATLAB_COMMAND = get_matlab_command()

    if include_project_paths and first_call and not _matlab_path_initialized:
        batch_command = _build_path_setup_prefix() + command
        _matlab_path_initialized = True
        logger.info(
            f"Prepending MATLAB path setup for {_MATLAB_LOCAL_PATH} and {_MATLAB_DEFAULT_PATH}"
        )
    else:
        batch_command = command

    cmd = [MATLAB_COMMAND, "-nodesktop", "-batch", batch_command]

    env = os.environ.copy()
    if unset_display:
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
    )

    while (line := p.stdout.readline()) != "":  # type: ignore
        line = line.rstrip()
        logger.info(line)

    p.wait(timeout=timeout)

    logger.info(f"MATLAB process finished with return code {p.returncode}")

    if p.returncode != 0:
        logger.error(f"MATLAB command failed with return code {p.returncode}")
        raise RuntimeError(f"MATLAB command failed with return code {p.returncode}")

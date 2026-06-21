import logging
import os
import subprocess
from shutil import which

logger = logging.getLogger(__name__)

# Tracks whether the MATLAB path has already been set up in this process.
# ``setup_matlab_path`` calls ``savepath``, which persists the path, so it only
# needs to run once per process rather than before every single MATLAB command.
_matlab_path_initialized = False


def setup_matlab_path(paths: list[str], matlab_command):
    add_path_commands = ""
    for path in paths if isinstance(paths, list) else [paths]:
        add_path_commands += f'addpath(genpath("{path}")); '
    cmd = [matlab_command, "-nodesktop", "-batch", f"'{add_path_commands} savepath;'"]

    logger.info(f"MATLAB setup command: \n  {' '.join(cmd)}")

    p = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )

    while (line := p.stdout.readline()) != "":  # type: ignore
        line = line.rstrip()
        logger.info(line)

    p.wait(timeout=60)
    if p.returncode != 0:
        logger.error(f"MATLAB setup command failed with return code {p.returncode}")
        raise RuntimeError(
            f"MATLAB setup command failed with return code {p.returncode}"
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


def call_matlab(command, first_call=True, timeout=60 * 5):
    global _matlab_path_initialized

    MATLAB_COMMAND = get_matlab_command()
    if first_call and not _matlab_path_initialized:
        default_matlab_path = "/app/matlab"
        local_matlab_path = "src/matlab"
        setup_matlab_path([default_matlab_path, local_matlab_path], MATLAB_COMMAND)
        _matlab_path_initialized = True
        logger.info(
            f"Added {local_matlab_path} and {default_matlab_path} files to path"
        )

    cmd = [MATLAB_COMMAND, "-nodesktop", "-batch"]
    cmd.append(command)

    logger.info(f"Calling MATLAB with command: \n  {' '.join(cmd)}")
    p = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )

    while (line := p.stdout.readline()) != "":  # type: ignore
        line = line.rstrip()
        logger.info(line)

    p.wait(timeout=timeout)

    logger.info(f"MATLAB process finished with return code {p.returncode}")

    if p.returncode != 0:
        logger.error(f"MATLAB command failed with return code {p.returncode}")
        raise RuntimeError(f"MATLAB command failed with return code {p.returncode}")

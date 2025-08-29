import logging
import subprocess

logger = logging.getLogger(__name__)


def setup_matlab_path(path, matlab_command):
    cmd = [matlab_command, "-batch", f'addpath(genpath("{path}")); savepath']
    p = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )

    while (line := p.stdout.readline()) != "":  # type: ignore
        line = line.rstrip()
        logger.info(line)


def get_matlab_command():
    return "matlab"


def call_matlab(command, first_call=True, timeout=60 * 5):
    MATLAB_COMMAND = get_matlab_command()
    if first_call:
        default_matlab_path = "/app/matlab"
        local_matlab_path = "src/matlab"
        setup_matlab_path(default_matlab_path, MATLAB_COMMAND)
        setup_matlab_path(local_matlab_path, MATLAB_COMMAND)
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
        raise RuntimeError("MATLAB command failed")

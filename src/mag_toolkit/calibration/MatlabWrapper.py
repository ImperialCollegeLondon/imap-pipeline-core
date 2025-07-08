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


def call_matlab(command, first_call=True):
    MATLAB_COMMAND = "matlab"
    if first_call:
        default_matlab_path = "/home/matlab/Documents/MATLAB"
        local_matlab_path = "src/matlab"
        setup_matlab_path(default_matlab_path, MATLAB_COMMAND)
        setup_matlab_path(local_matlab_path, MATLAB_COMMAND)
        logger.info(
            f"Added {local_matlab_path} and {default_matlab_path} files to path"
        )

    cmd = [MATLAB_COMMAND, "-nojvm", "-nodesktop", "-batch"]
    cmd.append(command)

    logger.debug(f"Calling MATLAB with command: {cmd}")
    p = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )

    while (line := p.stdout.readline()) != "":  # type: ignore
        line = line.rstrip()
        logger.info(line)

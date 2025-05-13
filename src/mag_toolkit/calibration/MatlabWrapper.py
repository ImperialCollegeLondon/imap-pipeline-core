import logging
import subprocess

logger = logging.getLogger(__name__)


def setup_matlab_path(path, matlab_command):
    subprocess.run(
        [
            matlab_command,
            "-batch",
            f'addpath(genpath("{path}")); savepath',
        ]
    )


def call_matlab(command, first_call=True):
    MATLAB_COMMAND = "matlab"
    if first_call:
        default_matlab_path = "/home/matlab/Documents/MATLAB"
        setup_matlab_path(default_matlab_path, MATLAB_COMMAND)
        logger.info("Added necessary files to path")

    cmd = [MATLAB_COMMAND, "-nojvm", "-nodesktop", "-batch"]
    cmd.append(command)

    logger.debug(f"Calling MATLAB with command: {cmd}")
    p = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )

    while (line := p.stdout.readline()) != "":  # type: ignore
        line = line.rstrip()
        logger.info(line)

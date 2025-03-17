import logging
import subprocess

class_logger = logging.getLogger(__name__)


def call_matlab(command, first_call=True, flow_logger=None):
    if flow_logger:
        logger = flow_logger
    else:
        logger = class_logger

    if first_call:
        subprocess.run(
            [
                "matlab",
                "-batch",
                'addpath(genpath("/home/matlab/Documents/MATLAB")); savepath',
            ]
        )

        logger.info("Added necessary files to path")

    logger.info("Running MATLAB...")
    cmd = ["matlab", "-nojvm", "-nodesktop", "-batch"]
    cmd.extend(command)
    p = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )

    while (line := p.stdout.readline()) != "":  # type: ignore
        line = line.rstrip()
        logger.info(line)

    logger.info("Finished")

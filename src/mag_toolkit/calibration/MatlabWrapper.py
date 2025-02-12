import logging
import subprocess
from datetime import datetime

import numpy as np
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class BasicCalibration(BaseModel):
    timestamps: list[datetime]
    x_offsets: list[float]
    y_offsets: list[float]
    z_offsets: list[float]


def call_matlab(first_call=True):
    logger.info("Testing logging!")
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
    cmd = ["matlab", "-batch", "helloworld"]
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True)

    while (line := p.stdout.readline()) != "":
        line = line.rstrip()
        logger.info(line)

    logger.info("Finished")


def simulateSpinAxisCalibration(xarray) -> BasicCalibration:
    # TODO: Transfer to MATLAB to get spin axis offsets
    timestamps = [datetime(2022, 3, 3)]
    offsets = [3.256]

    return BasicCalibration(
        timestamps=timestamps,
        x_offsets=np.zeros(len(offsets)),
        y_offsets=np.zeros(len(offsets)),
        z_offsets=offsets,
    )


def simulateSpinPlaneCalibration(xarray):
    # TODO: Transfer to MATLAB to get spin plane offsets
    timestamps = [datetime(2022, 3, 3)]
    offsets_x = [3.256]
    offsets_y = [2.76]

    return BasicCalibration(
        timestamps=timestamps,
        x_offsets=offsets_x,
        y_offsets=offsets_y,
        z_offsets=np.zeros(len(offsets_x)),
    )

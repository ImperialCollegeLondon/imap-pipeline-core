import logging
import os
from pathlib import Path
from typing import Annotated

import typer

from imap_mag import appConfig, appUtils
from imap_mag.api.apiUtils import commandInit, prepareWorkFile
from mag_toolkit import CDFLoader
from mag_toolkit.calibration.calibrationFormatProcessor import (
    CalibrationFormatProcessor,
)
from mag_toolkit.calibration.Calibrator import (
    Calibrator,
    CalibratorType,
    SpinAxisCalibrator,
    SpinPlaneCalibrator,
)


# E.g., imap-mag calibrate --config calibration_config.yaml --method SpinAxisCalibrator imap_mag_l1b_norm-mago_20250502_v000.cdf
def calibrate(
    config: Annotated[Path, typer.Option()] = Path("calibration_config.yaml"),
    method: Annotated[CalibratorType, typer.Option()] = "SpinAxisCalibrator",
    input: str = typer.Argument(
        help="The file name or pattern to match for the input file"
    ),
):
    # TODO: Define specific calibration configuration
    # Using AppConfig for now to piggyback off of configuration
    # verification and work area setup
    configFile: appConfig.CommandConfigBase = commandInit(config)

    workFile = prepareWorkFile(input, configFile)

    if workFile is None:
        logging.critical(
            "Unable to find a file to process in %s", configFile.source.folder
        )
        raise typer.Abort()

    calibrator: Calibrator

    match method:
        case CalibratorType.SPINAXIS:
            calibrator = SpinAxisCalibrator()
        case CalibratorType.SPINPLANE:
            calibrator = SpinPlaneCalibrator()

    inputData = CDFLoader.load_cdf(workFile)
    calibration = calibrator.generateCalibration(inputData)

    tempOutputFile = os.path.join(configFile.work_folder, "calibration.json")

    result = CalibrationFormatProcessor.writeToFile(calibration, tempOutputFile)

    appUtils.copyFileToDestination(result, configFile.destination)

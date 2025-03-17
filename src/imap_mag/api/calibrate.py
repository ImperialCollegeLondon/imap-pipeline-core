import logging
from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer

from imap_mag import appConfig, appUtils
from imap_mag.api.apiUtils import commandInit, prepareWorkFile
from mag_toolkit import CDFLoader
from mag_toolkit.calibration.Calibrator import (
    CalibrationMethod,
    Calibrator,
    SpinAxisCalibrator,
    SpinPlaneCalibrator,
)

app = typer.Typer()


@app.command()
def generate(
    method: CalibrationMethod,
    from_date: Annotated[datetime, typer.Option("--from")],
    to_date: Annotated[datetime, typer.Option("--to")],
):
    pass


# TODO: ?
def interpolate():
    pass


def publish():
    pass


# E.g., imap-mag calibrate --config calibration_config.yaml --method SpinAxisCalibrator imap_mag_l1b_norm-mago_20250502_v000.cdf
def calibrate(
    config: Annotated[Path, typer.Option()] = Path("calibration_config.yaml"),
    method: Annotated[CalibrationMethod, typer.Option()] = CalibrationMethod.KEPKO,
    input: str = typer.Argument(
        help="The file name or pattern to match for the input file"
    ),
):
    # TODO: Define specific calibration configuration
    # Using AppConfig for now to piggyback off of configuration
    # verification and work area setup
    configFile: appConfig.AppConfig = commandInit(config)

    workFile = prepareWorkFile(input, configFile)

    if workFile is None:
        logging.critical(
            "Unable to find a file to process in %s", configFile.source.folder
        )
        raise typer.Abort()

    calibrator: Calibrator

    match method:
        case CalibrationMethod.KEPKO:
            calibrator = SpinAxisCalibrator()
        case CalibrationMethod.LEINWEBER:
            calibrator = SpinPlaneCalibrator()
        case _:
            raise Exception("Undefined calibrator")

    inputData = CDFLoader.load_cdf(workFile)
    result: Path = calibrator.runCalibration(inputData)

    appUtils.copyFileToDestination(result, configFile.destination)

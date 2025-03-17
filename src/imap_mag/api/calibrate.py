import logging
from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer

from imap_mag import appConfig
from imap_mag.api.apiUtils import commandInit, prepareWorkFile
from mag_toolkit.calibration.Calibrator import (
    CalibrationMethod,
    EmptyCalibrator,
    IMAPLoCalibrator,
    SpinAxisCalibrator,
    SpinPlaneCalibrator,
)

app = typer.Typer()

logger = logging.getLogger(__name__)


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
    from_date: Annotated[datetime, typer.Option("--from")],
    to_date: Annotated[datetime, typer.Option("--to")],
    method: Annotated[CalibrationMethod, typer.Option()] = CalibrationMethod.KEPKO,
    input: str = typer.Argument(
        help="The file name or pattern to match for the input file"
    ),
):
    # TODO: Define specific calibration configuration
    # Using AppConfig for now to piggyback off of configuration
    # verification and work area setup
    configFile: appConfig.AppConfig = commandInit(None)

    workFile = prepareWorkFile(input, configFile)

    if workFile is None:
        logging.critical(
            "Unable to find a file to process in %s", configFile.source.folder
        )
        raise typer.Abort()

    match method:
        case CalibrationMethod.LEINWEBER:
            calibrator = SpinAxisCalibrator()
        case CalibrationMethod.KEPKO:
            calibrator = SpinPlaneCalibrator()
        case CalibrationMethod.IMAPLO_PIVOT:
            calibrator = IMAPLoCalibrator()
        case CalibrationMethod.NOOP:
            calibrator = EmptyCalibrator()
        case _:
            raise Exception("Undefined calibrator")

    calfile = Path(
        f"data_store/imap/cal_layers/{from_date.year}/{from_date.month}/{from_date.day}_{calibrator.name}_v000.json"
    )

    result: Path = calibrator.runCalibration(
        from_date,
        workFile,
        calfile,
        "data_store",
        None,
    )

    logger.info(f"Calibration file written to {result}")

    # appUtils.copyFileToDestination(result, configFile.destination)

import logging
from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer

from imap_mag import appConfig, appUtils
from imap_mag.api.apiUtils import commandInit, prepareWorkFile
from mag_toolkit.calibration.calibrationFormat import ScienceLayerZero
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
    config: Annotated[Path, typer.Option()] = Path("calibration_config.yaml"),
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

    scienceLayer = ScienceLayerZero.from_file(workFile)
    scienceLayerPath = configFile.work_folder / f"{scienceLayer.id}.json"
    scienceLayer.writeToFile(scienceLayerPath)

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

    temp_cal_file_name = configFile.work_folder / configFile.destination.filename

    cal_folder = (
        configFile.destination.folder / str(from_date.year) / str(from_date.month)
    )

    TIMEFORMAT = "%d-%m-%Y"

    cal_file_destination = appConfig.Destination(
        folder=cal_folder,
        filename=f"{from_date.strftime(TIMEFORMAT)}-{to_date.strftime(TIMEFORMAT)}_{calibrator.name.value}_v000.json",
    )

    result: Path = calibrator.runCalibration(
        from_date,
        scienceLayerPath,
        temp_cal_file_name,
        configFile.work_folder,
        None,
    )

    appUtils.copyFileToDestination(result, cal_file_destination)
    logger.info(f"Calibration file written to {cal_file_destination}")

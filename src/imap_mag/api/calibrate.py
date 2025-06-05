import logging
from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer

from imap_mag import appConfig, appUtils
from imap_mag.api import apply
from imap_mag.api.apiUtils import commandInit, prepareWorkFile
from mag_toolkit.calibration import CalibrationMethod, EmptyCalibrator, ScienceLayer

app = typer.Typer()

logger = logging.getLogger(__name__)

app.command()(apply.apply)


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
    """
    Generate calibration parameters for a given input file.
    imap-mag calibrate --from [date] --to [date] --method [method] [input]

    e.g. imap-mag calibrate --from 2025-10-17 --to 2025-10-17 --method noop imap_mag_l1b_norm-mago_20251017_v002.cdf

    """
    # TODO: Define specific calibration configuration
    # Using AppConfig for now to piggyback off of configuration
    # verification and work area setup
    configFile: appConfig.CommandConfigBase = commandInit(config)

    full_input_path = (
        Path(configFile.source.folder)
        / "l1b"
        / str(from_date.year)
        / f"{from_date.month:02d}"
        / input
    )

    workFile = prepareWorkFile(full_input_path, configFile.work_folder)

    if workFile is None:
        logging.critical(
            "Unable to find a file to process in %s", configFile.source.folder
        )
        raise typer.Abort()

    scienceLayer = ScienceLayer.from_file(workFile)
    scienceLayerPath = configFile.work_folder / f"{scienceLayer.id}.json"
    scienceLayer.writeToFile(scienceLayerPath)

    match method:
        case CalibrationMethod.NOOP:
            calibrator = EmptyCalibrator()
        case _:
            raise ValueError("Calibration method is not implemented")

    temp_cal_file_name = configFile.work_folder / configFile.destination.filename

    cal_folder = (
        configFile.destination.folder / str(from_date.year) / f"{from_date.month:02d}"
    )

    # TODO: Standardised constant?
    TIMEFORMAT = "%Y%m%d"

    cal_file_destination = appConfig.Destination(
        folder=cal_folder,
        filename=f"{from_date.strftime(TIMEFORMAT)}_{to_date.strftime(TIMEFORMAT)}_{scienceLayer.sensor.value}-{calibrator.name.value}-offsets_v000.json",
    )

    result: Path = calibrator.runCalibration(
        from_date, scienceLayerPath, temp_cal_file_name, configFile.work_folder, None
    )

    appUtils.copyFileToDestination(result, cal_file_destination)
    logger.info(f"Calibration file written to {cal_file_destination}")

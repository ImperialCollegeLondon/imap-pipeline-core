import logging
from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer

from imap_mag.cli import apply
from imap_mag.cli.cliUtils import fetch_file_for_work, initialiseLoggingForCommand
from imap_mag.config import AppSettings
from imap_mag.io import (
    CalibrationLayerPathHandler,
    InputManager,
    OutputManager,
)
from imap_mag.util import ScienceMode
from mag_toolkit.calibration import (
    CalibrationMethod,
    EmptyCalibrator,
    GradiometerCalibrator,
    Sensor,
)

app = typer.Typer()

logger = logging.getLogger(__name__)

app.command()(apply.apply)


# TODO: ?
def interpolate():
    pass


def publish():
    pass


# E.g., imap-mag calibrate --method SpinAxisCalibrator imap_mag_l1b_norm-mago_20250502_v000.cdf
def calibrate(
    date: Annotated[datetime, typer.Option("--date", help="Date to calibrate")],
    method: Annotated[
        CalibrationMethod, typer.Option(help="Calibration method")
    ] = CalibrationMethod.KEPKO,
    mode: Annotated[
        ScienceMode, typer.Option(help="Science mode")
    ] = ScienceMode.Normal,
    sensor: Annotated[
        Sensor, typer.Option(help="Sensor to calibrate, e.g., mago")
    ] = Sensor.MAGO,
):
    """
    Generate calibration parameters for a given input file.
    imap-mag calibrate --from [date] --to [date] --method [method] [input]

    e.g. imap-mag calibrate --date 2025-10-17 --mode norm --sensor mago --method noop imap_mag_l1b_norm-mago_20251017_v002.cdf

    """
    app_settings = AppSettings()  # type: ignore
    work_folder = app_settings.setup_work_folder_for_command(app_settings.fetch_science)
    initialiseLoggingForCommand(
        work_folder
    )  # DO NOT log anything before this point (it won't be captured in the log file)

    input_manager = InputManager(app_settings.data_store)

    match method:
        case CalibrationMethod.NOOP:
            calibrator = EmptyCalibrator()
        case CalibrationMethod.GRADIOMETRY:
            calibrator = GradiometerCalibrator()
        case _:
            raise ValueError("Calibration method is not implemented")

    (science_path_handlers, other_path_handlers) = (
        calibrator.get_handlers_of_files_needed_for_calibration(date, mode, sensor)
    )

    # TODO: Handle other_path_handlers if needed
    for path_handler in science_path_handlers:
        input_file = input_manager.get_versioned_file(path_handler)
        if not input_file:
            logger.critical(
                "Unable to find a science file to process matching %s",
                path_handler.get_filename(),
                " required for calibration",
            )
            raise FileNotFoundError(
                f"Unable to find a file to process matching {path_handler.get_filename()}"
            )
        workFile = fetch_file_for_work(
            input_file, app_settings.work_folder, throw_if_not_found=True
        )
        print(workFile)

    if calibrator.needs_data_store():
        calibrator.setup_datastore(app_settings.data_store)

    calibrationLayerHandler = CalibrationLayerPathHandler(
        calibration_descriptor=method.value, content_date=date
    )
    result: Path = calibrator.runCalibration(
        date, Path(), Path(calibrationLayerHandler.get_filename()), ""
    )

    outputManager = OutputManager(app_settings.data_store)
    (output_calibration_path, _) = outputManager.add_file(
        result, path_handler=calibrationLayerHandler
    )  # type: ignore

    return output_calibration_path

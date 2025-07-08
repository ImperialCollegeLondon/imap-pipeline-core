import logging
from datetime import datetime
from pathlib import Path
from typing import Annotated, TypeVar

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
    Calibrator,
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


C = TypeVar("C", bound=Calibrator)


def generic_calibration_setup(app_settings: AppSettings, calibrator: C) -> C:
    """
    Generic calibration setup function.
    This is a placeholder for any common setup logic needed for calibration commands.
    """

    input_manager = InputManager(app_settings.data_store)

    path_handlers = calibrator.get_handlers_of_files_needed_for_calibration()

    for key in path_handlers:
        path_handler = path_handlers[key]
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
        work_file = fetch_file_for_work(
            input_file, app_settings.work_folder, throw_if_not_found=True
        )
        calibrator.set_file(key, work_file)

    if calibrator.needs_data_store():
        calibrator.setup_datastore(app_settings.data_store)

    return calibrator


def gradiometry(
    date: Annotated[datetime, typer.Option("--date", help="Date to calibrate")],
    mode: Annotated[
        ScienceMode, typer.Option(help="Science mode")
    ] = ScienceMode.Normal,
    kappa: Annotated[float, typer.Option(help="Kappa value for gradiometry")] = 0.0,
    sc_interference_threshold: Annotated[
        float, typer.Option(help="SC interference threshold")
    ] = 10.0,
):
    """
    Run gradiometry calibration.
    """

    app_settings = AppSettings()  # type: ignore
    work_folder = app_settings.setup_work_folder_for_command(app_settings.fetch_science)
    initialiseLoggingForCommand(work_folder)
    method = CalibrationMethod.GRADIOMETER
    calibrator = GradiometerCalibrator(date, mode, Sensor.MAGO)

    calibrator = generic_calibration_setup(app_settings, calibrator)

    calibrator.kappa = kappa
    calibrator.sc_interference_threshold = sc_interference_threshold

    calibrationLayerHandler = CalibrationLayerPathHandler(
        calibration_descriptor=method.value, content_date=date
    )
    result: Path = calibrator.run_calibration(
        Path(app_settings.work_folder) / calibrationLayerHandler.get_filename()
    )

    outputManager = OutputManager(app_settings.data_store)
    (output_calibration_path, _) = outputManager.add_file(
        result, path_handler=calibrationLayerHandler
    )  # type: ignore

    return output_calibration_path


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

    match method:
        case CalibrationMethod.NOOP:
            calibrator = EmptyCalibrator(date, mode, sensor)
        case CalibrationMethod.GRADIOMETER:
            calibrator = GradiometerCalibrator(date, mode, sensor)
        case _:
            raise ValueError("Calibration method is not implemented")

    calibrator = generic_calibration_setup(app_settings, calibrator)

    calibrationLayerHandler = CalibrationLayerPathHandler(
        calibration_descriptor=method.value, content_date=date
    )
    result: Path = calibrator.run_calibration(
        Path(app_settings.work_folder) / calibrationLayerHandler.get_filename()
    )

    outputManager = OutputManager(app_settings.data_store)
    (output_calibration_path, _) = outputManager.add_file(
        result, path_handler=calibrationLayerHandler
    )  # type: ignore

    return output_calibration_path

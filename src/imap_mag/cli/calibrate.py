import logging
from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer

from imap_mag.cli import apply
from imap_mag.cli.cliUtils import initialiseLoggingForCommand
from imap_mag.config import AppSettings
from imap_mag.config.CalibrationConfig import CalibrationConfig, GradiometryConfig
from imap_mag.io import DatastoreFileFinder, OutputManager
from imap_mag.io.file import CalibrationLayerPathHandler
from imap_mag.util import ScienceMode
from mag_toolkit.calibration import (
    CalibrationJobParameters,
    CalibrationMethod,
    EmptyCalibrationJob,
    GradiometerCalibrationJob,
    Sensor,
)

app = typer.Typer()

logger = logging.getLogger(__name__)

app.command()(apply.apply)


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

    datastore_finder = DatastoreFileFinder(app_settings.data_store)

    method = CalibrationMethod.GRADIOMETER
    calibration_job_parameters = CalibrationJobParameters(
        date=date, mode=mode, sensor=Sensor.MAGO
    )
    calibration_configuration = CalibrationConfig(
        gradiometer=GradiometryConfig(
            kappa=kappa, sc_interference_threshold=sc_interference_threshold
        )
    )
    calibrator = GradiometerCalibrationJob(calibration_job_parameters)
    calibrator.setup_calibration_files(datastore_finder, work_folder)
    calibrator.setup_datastore(app_settings.data_store)

    calibration_layer_handler = CalibrationLayerPathHandler(
        calibration_descriptor=method.short_name, content_date=date
    )
    result: Path = calibrator.run_calibration(
        work_folder / Path(calibration_layer_handler.get_filename()),
        calibration_configuration,
    )

    outputManager = OutputManager(app_settings.data_store)
    (output_calibration_path, _) = outputManager.add_file(
        result, path_handler=calibration_layer_handler
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
    configuration: Annotated[
        str | None,
        typer.Option(
            help="Configuration for the calibration - should be a YAML file or a JSON string",
        ),
    ] = None,
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

    if configuration is None:
        calibration_configuration = CalibrationConfig()
    elif Path(configuration).is_file():
        logger.info(f"Loading calibration configuration from {configuration}")
        calibration_configuration = CalibrationConfig.from_file(Path(configuration))
    else:
        calibration_configuration = CalibrationConfig.model_validate_json(configuration)

    calibration_job_parameters = CalibrationJobParameters(
        date=date, mode=mode, sensor=sensor
    )

    match method:
        case CalibrationMethod.NOOP:
            calibrator = EmptyCalibrationJob(calibration_job_parameters)
        case CalibrationMethod.GRADIOMETER:
            calibrator = GradiometerCalibrationJob(calibration_job_parameters)
        case _:
            raise ValueError("Calibration method is not implemented")

    calibrator.setup_calibration_files(
        DatastoreFileFinder(app_settings.data_store), work_folder
    )
    calibrator.setup_datastore(app_settings.data_store)

    calibration_layer_handler = CalibrationLayerPathHandler(
        calibration_descriptor=method.value, content_date=date
    )

    result: Path = calibrator.run_calibration(
        work_folder / Path(calibration_layer_handler.get_filename()),
        calibration_configuration,
    )

    outputManager = OutputManager(app_settings.data_store)
    (output_calibration_path, _) = outputManager.add_file(
        result, path_handler=calibration_layer_handler
    )  # type: ignore

    return output_calibration_path

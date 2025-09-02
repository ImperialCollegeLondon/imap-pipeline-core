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
) -> Path:
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

    calibrator = GradiometerCalibrationJob(calibration_job_parameters, work_folder)
    calibrator.setup_calibration_files(datastore_finder)
    calibrator.setup_datastore(app_settings.data_store)

    calibration_handler = CalibrationLayerPathHandler(
        descriptor=method.short_name, content_date=date
    )
    # TODO: REFACTOR - We are trying to get the path of the next available version of a path but here we are creating 2 path handler objects and passing one into the other. Seems convoluted. Why not just pick the right version when we create the handler to start with in a class constructor?
    calibration_handler = calibrator.get_next_viable_version_layer(
        datastore_finder, calibration_handler
    )

    # TODO: REFACTOR - we are passing 2 things here because of the separate CSV data files needing a path. We should pass one thing and refactor the complexity of having to pass a handler for the data file. Perhaps a layer object should just manage the CSV data file.
    metadata_path, data_path = calibrator.run_calibration(
        calibration_handler, calibration_configuration
    )

    outputManager = OutputManager(app_settings.data_store)

    # TODO: REFACTOR - this is convoluted to add the 2 files. Something like outputManager.add_files(layer.get_output_files()) would be better
    (output_calibration_path, _) = outputManager.add_file(
        metadata_path, path_handler=calibration_handler
    )
    outputManager.add_file(
        data_path, path_handler=calibration_handler.get_equivalent_data_handler()
    )

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
) -> Path:
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
            calibrator = EmptyCalibrationJob(calibration_job_parameters, work_folder)
        case CalibrationMethod.GRADIOMETER:
            calibrator = GradiometerCalibrationJob(
                calibration_job_parameters, work_folder
            )
        case _:
            raise ValueError("Calibration method is not implemented")

    datastore_finder = DatastoreFileFinder(app_settings.data_store)

    calibrator.setup_calibration_files(datastore_finder)
    calibrator.setup_datastore(app_settings.data_store)

    calibration_handler = CalibrationLayerPathHandler(
        descriptor=method.short_name, content_date=date
    )
    calibration_handler = calibrator.get_next_viable_version_layer(
        datastore_finder, calibration_handler
    )

    metadata_path, data_path = calibrator.run_calibration(
        calibration_handler, calibration_configuration
    )

    outputManager = OutputManager(app_settings.data_store)

    (output_calibration_path, _) = outputManager.add_file(
        metadata_path, path_handler=calibration_handler
    )
    outputManager.add_file(
        data_path, path_handler=calibration_handler.get_equivalent_data_handler()
    )

    return output_calibration_path

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Annotated

import typer

from imap_mag.cli import apply
from imap_mag.cli.cliUtils import initialiseLoggingForCommand
from imap_mag.config import AppSettings, CalibrationConfig, GradiometryConfig, SaveMode
from imap_mag.io import DatastoreFileFinder, DatastoreFileManager
from imap_mag.io.file import CalibrationLayerPathHandler
from imap_mag.util import ScienceMode
from mag_toolkit.calibration import (
    CalibrationJobParameters,
    CalibrationMethod,
    EmptyCalibrationJob,
    GradiometerCalibrationJob,
    Sensor,
    SetQualityAndNaNCalibrationJob,
)

app = typer.Typer()

logger = logging.getLogger(__name__)

app.command()(apply.apply)


def gradiometry(
    start_date: Annotated[datetime, typer.Option("--date", help="Date to calibrate")],
    mode: Annotated[
        ScienceMode, typer.Option(help="Science mode")
    ] = ScienceMode.Normal,
    kappa: Annotated[float, typer.Option(help="Kappa value for gradiometry")] = 0.0,
    sc_interference_threshold: Annotated[
        float, typer.Option(help="SC interference threshold")
    ] = 10.0,
    save_mode: Annotated[
        SaveMode,
        typer.Option(help="Whether to save locally only or to also save to database"),
    ] = SaveMode.LocalOnly,
) -> Path:
    """
    Run gradiometry calibration.
    """
    configuration = CalibrationConfig(
        gradiometer=GradiometryConfig(
            kappa=kappa, sc_interference_threshold=sc_interference_threshold
        )
    )

    return _calibrate_for_date(
        start_date=start_date,
        method=CalibrationMethod.GRADIOMETER,
        mode=mode,
        sensor=Sensor.MAGO,
        configuration=configuration.model_dump_json(),
        save_mode=save_mode,
    )


def calibrate(
    start_date: Annotated[
        datetime | None,
        typer.Option(
            "--date",
            help="Date to calibrate (single date mode)",
        ),
    ] = None,
    end_date: Annotated[
        datetime | None,
        typer.Option(
            "--end-date",
            help="End date for calibrating a date range (inclusive)",
        ),
    ] = None,
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
    save_mode: Annotated[
        SaveMode,
        typer.Option(help="Whether to save locally only or to also save to database"),
    ] = SaveMode.LocalOnly,
) -> list[Path]:
    """
    Generate calibration parameters for a given input file.

    Supports single date (--date) or date ranges (--start-date/--end-date).

    e.g. imap-mag calibrate --date 2025-10-17 --mode norm --sensor mago --method noop
    e.g. imap-mag calibrate --start-date 2025-10-17 --end-date 2025-10-20 --method noop
    """
    if start_date is None:
        raise typer.BadParameter("A date must be provided via --date or --start-date.")

    effective_end = end_date or start_date
    current = start_date
    results: list[Path] = []
    while current <= effective_end:
        result = _calibrate_for_date(
            start_date=current,
            method=method,
            mode=mode,
            sensor=sensor,
            configuration=configuration,
            save_mode=save_mode,
        )
        results.append(result)
        current += timedelta(days=1)
    return results


def _calibrate_for_date(
    start_date: datetime,
    method: CalibrationMethod,
    mode: ScienceMode,
    sensor: Sensor,
    configuration: str | None,
    save_mode: SaveMode,
) -> Path:
    """Run calibration for a single date."""
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
        date=start_date, mode=mode, sensor=sensor
    )

    match method:
        case CalibrationMethod.NOOP:
            calibrator = EmptyCalibrationJob(calibration_job_parameters, work_folder)
        case CalibrationMethod.GRADIOMETER:
            calibrator = GradiometerCalibrationJob(
                calibration_job_parameters, work_folder
            )
        case CalibrationMethod.SET_QUALITY_AND_NAN:
            calibrator = SetQualityAndNaNCalibrationJob(
                calibration_job_parameters, work_folder
            )
        case _:
            raise ValueError("Calibration method is not implemented")

    datastore_finder = DatastoreFileFinder(app_settings.data_store)

    calibrator.setup_calibration_files(datastore_finder)
    calibrator.setup_datastore(app_settings.data_store)

    calibration_handler = CalibrationLayerPathHandler(
        descriptor=f"{method.short_name}-{mode.value}", content_date=start_date
    )
    calibration_handler = calibrator.get_next_viable_version_layer(
        datastore_finder, calibration_handler
    )

    metadata_path, data_path = calibrator.run_calibration(
        calibration_handler, calibration_configuration
    )

    outputManager = DatastoreFileManager.CreateByMode(
        app_settings, use_database=save_mode == SaveMode.LocalAndDatabase
    )

    (output_calibration_path, _) = outputManager.add_file(
        metadata_path, path_handler=calibration_handler
    )
    outputManager.add_file(
        data_path, path_handler=calibration_handler.get_equivalent_data_handler()
    )

    return output_calibration_path

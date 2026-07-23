import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Annotated

import typer

from imap_mag.cli import apply
from imap_mag.cli.cliUtils import initialiseLoggingForCommand
from imap_mag.config import (
    AppSettings,
    CalibrationConfig,
    GradiometryConfig,
    SaveMode,
)
from imap_mag.db.Database import Database
from imap_mag.io import DatastoreFileManager, FileFinder
from imap_mag.io.file import CalibrationLayerPathHandler
from imap_mag.util import ScienceMode
from mag_toolkit.calibration import (
    CalibrationJob,
    CalibrationJobParameters,
    CalibrationMethod,
    GradiometerCalibrationJob,
    ScriptedL2CalibrationJob,
    Sensor,
    SetQualityAndNaNCalibrationJob,
)
from mag_toolkit.calibration.CalibrationLayer import CalibrationLayer

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
    configuration = GradiometryConfig(
        kappa=kappa, sc_interference_threshold=sc_interference_threshold
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
    ] = CalibrationMethod.SET_QUALITY_AND_NAN,
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
    metakernel: Annotated[
        Path | None,
        typer.Option(
            help="Filename of the SPICE metakernel to use (scripted-l2 method). "
            "Must exist in the spice/mk folder of the datastore. If omitted for "
            "scripted-l2, one is generated.",
        ),
    ] = None,
) -> list[Path]:
    """
    Generate calibration parameters for a given input file.

    Supports single date (--date) or date ranges (--start-date/--end-date).

    e.g. imap-mag calibrate --date 2025-10-17 --mode norm --sensor mago --method gradiometer
    e.g. imap-mag calibrate --start-date 2025-10-17 --end-date 2025-10-20 --method gradiometer
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
            metakernel=metakernel,
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
    metakernel: Path | None = None,
) -> Path:
    """Run calibration for a single date."""
    if method == CalibrationMethod.NOOP:
        # NOOP is retained only as the internal descriptor for the zero-offset layer
        # that ``apply`` auto-creates; it is not a runnable calibration job.
        raise ValueError(
            "The 'noop' calibration method is not runnable. It exists only for the "
            "internal zero-offset layer. Choose a real calibration method."
        )
    app_settings = AppSettings()  # type: ignore
    # Use the dedicated calibrate command config so each run gets its own uniquely
    # named work folder (based on the date + mode being calibrated).
    work_folder = app_settings.setup_work_folder_for_command(
        app_settings.calibrate,
        name_context={
            "date": start_date.strftime("%Y%m%d"),
            "mode": mode.value,
            "sensor": sensor.value,
        },
    )
    initialiseLoggingForCommand(
        work_folder
    )  # DO NOT log anything before this point (it won't be captured in the log file)
    datastore_finder = FileFinder(
        app_settings.data_store,
        work_folder,
        database=Database() if Database.get_environment_url() else None,
    )

    # The scripted-l2 method uses an extended configuration with extra required
    # fields, so parse against the correct model for the chosen method.
    config_cls = CalibrationConfig.get_class(method)
    if configuration is None or len(configuration.strip()) == 0:
        raise ValueError(
            f"Calibration method {method.short_name} requires a configuration to be provided"
        )
    elif Path(configuration).is_file():
        logger.info(f"Loading calibration configuration from {configuration}")
        calibration_configuration = config_cls.from_file(Path(configuration))
    else:
        calibration_configuration = config_cls.model_validate_json(configuration)

    calibration_job_parameters = CalibrationJobParameters(
        date=start_date, mode=mode, sensor=sensor
    )
    calibrator: CalibrationJob
    match method:
        case CalibrationMethod.GRADIOMETER:
            calibrator = GradiometerCalibrationJob(
                calibration_job_parameters, work_folder
            )
        case CalibrationMethod.SET_QUALITY_AND_NAN:
            calibrator = SetQualityAndNaNCalibrationJob(
                calibration_job_parameters, work_folder, datastore_finder
            )
        case CalibrationMethod.SCRIPTED_L2_CALIBRATION:
            calibrator = ScriptedL2CalibrationJob(
                calibration_job_parameters,
                app_settings,
                matlab_repo_path=calibration_configuration.matlab_repo,
                metakernel=metakernel,
            )
        case _:
            raise ValueError("Calibration method is not implemented")

    calibrator.setup_calibration_files(datastore_finder)
    calibrator.setup_datastore(app_settings.data_store)
    outputManager = DatastoreFileManager.CreateByMode(
        app_settings, use_database=save_mode == SaveMode.LocalAndDatabase
    )
    calibration_handler = CalibrationLayerPathHandler(
        descriptor=f"{method.short_name}-{mode.value}",
        content_date=start_date,
        version_major=app_settings.version_major,
    )

    metadata_path, data_path = calibrator.run_calibration(
        calibration_handler, calibration_configuration
    )

    # verify that the generated work-folder pair is internally consistent
    layer = CalibrationLayer.from_file(metadata_path, load_contents=False)
    if not layer.metadata.data_filename or not data_path.exists():
        raise FileNotFoundError(
            f"Calibration layer file at {metadata_path!s} has data file {layer.metadata.data_filename!s} that was not found"
        )
    if data_path.name != layer.metadata.data_filename.name:
        raise ValueError(
            f"Calibration layer metadata file {metadata_path!s} specifies data file {layer.metadata.data_filename!s} but actual data file is {data_path!s}."
        )

    # Enforce pipeline metadata, ensures hash is correct as MATLAB cal may not do it
    layer.save_calibration_layer(
        metadata_path, createDirectory=False, save_contents=False
    )

    (output_calibration_path, _) = outputManager.add_file(
        metadata_path, path_handler=calibration_handler
    )

    outputManager.add_file(
        data_path, path_handler=calibration_handler.get_equivalent_data_handler()
    )

    return output_calibration_path

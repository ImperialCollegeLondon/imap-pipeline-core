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
    ScriptedL2CalibrationConfig,
)
from imap_mag.db.Database import Database
from imap_mag.io import DatastoreFileManager, FileFinder, IDatastoreFileManager
from imap_mag.io.file import CalibrationLayerPathHandler
from imap_mag.io.file.IFilePathHandler import IFilePathHandler
from imap_mag.io.file.VersionedPathHandler import VersionedPathHandler
from imap_mag.io.FilePathHandlerSelector import (
    FilePathHandlerSelector,
)
from imap_mag.util import ScienceMode
from mag_toolkit.calibration import (
    CalibrationJobParameters,
    CalibrationMethod,
    Sensor,
)
from mag_toolkit.calibration.CalibrationLayer import CalibrationLayer
from mag_toolkit.calibration.calibrators import (
    CalibrationJob,
    GradiometerCalibrationJob,
    ScriptedL2CalibrationJob,
    SetQualityAndNaNCalibrationJob,
)

app = typer.Typer()

logger = logging.getLogger(__name__)

app.command()(apply.apply)


def _validate_version_number_override(
    override: tuple[int, int] | None,
) -> tuple[int, int] | None:
    """Validate a (major, minor) version override tuple.

    Each element must be a non-negative whole integer with major <= 999 and
    minor <= 9999, mirroring the range checks MATLAB applies to output_version.

    Args:
        override: A (major, minor) pair, or None to use default versioning.

    Returns:
        The validated pair, or None.

    Raises:
        ValueError: If any element is a bool, non-integer, negative, or out of range.
    """
    if override is None:
        return None

    major, minor = override
    for val, name, max_val in [
        (major, "major", 999),
        (minor, "minor", 9999),
    ]:
        if isinstance(val, bool):
            raise ValueError(f"Version {name} must be an integer, not bool.")
        if not isinstance(val, int):
            raise ValueError(
                f"Version {name} must be an integer, got {type(val).__name__}."
            )
        if val < 0:
            raise ValueError(f"Version {name} must be non-negative, got {val}.")
        if val > max_val:
            raise ValueError(f"Version {name} must be at most {max_val}, got {val}.")

    logger.warning(
        f"Version number override active: forcing output to v{major:03d}.{minor:04d}. "
        "Existing layers at this version may be overwritten."
    )

    return (major, minor)


def _save_calibration_outputs(
    returned: list[Path],
    outputManager: IDatastoreFileManager,
    version_number_override: tuple[int, int] | None = None,
) -> list[Path]:

    if not returned:
        raise ValueError("Calibration produced no files to save.")

    # Tuple: (is_layer_metadata, original_output_path, handler) for all returned files.
    file_handlers: list[tuple[bool, Path, IFilePathHandler]] = []
    saved_file_paths: list[Path] = []

    # Resolve a path handler for the returned files. The selector raises
    # NoProviderFoundError for any file it cannot recognise, so an unhandled
    # file type surfaces immediately rather than being silently dropped.
    for path in returned:
        handler: IFilePathHandler = FilePathHandlerSelector.find_by_path(path)

        if isinstance(handler, CalibrationLayerPathHandler):
            if not handler.is_metadata_file():
                # Companion data file for a calibration layer; skip it for now and
                # handle it when the JSON layer is processed.
                continue

            if version_number_override is not None:
                handler.versioning_mode = VersionedPathHandler.VersionMode.USER_OVERRIDE
                if (handler.version_major, handler.version) != version_number_override:
                    raise ValueError(
                        f"Version number override {version_number_override} does not match the version {handler.version_major, handler.version} for discovered output file {path}."
                    )

            file_handlers.append((True, path, handler))
        else:
            file_handlers.append((False, path, handler))

    # Process JSON calibration-layer files first so co-versioning is preserved:
    # adding the JSON bumps its handler's version, and the CSV is then saved with
    # the bumped handler — both land on the same version number.

    for is_layer_metadata, path, handler in sorted(
        file_handlers, key=lambda x: x[0], reverse=True
    ):
        companion_path: Path | None = None
        if is_layer_metadata:
            layer = CalibrationLayer.from_file(path, load_contents=False)
            if not layer.metadata.data_filename:
                raise FileNotFoundError(
                    f"Calibration layer file at {path!s} has no data_filename in its metadata."
                )

            companion_path = next(
                (p for p in returned if p.name == layer.metadata.data_filename.name),
                None,
            )
            if companion_path is None or not companion_path.exists():
                raise FileNotFoundError(
                    f"Calibration layer file at {path!s} has data file "
                    f"{layer.metadata.data_filename!s} that was not found among the "
                    "calibration outputs."
                )
            if companion_path.name != layer.metadata.data_filename.name:
                raise ValueError(
                    f"Calibration layer metadata file {path!s} specifies data file "
                    f"{layer.metadata.data_filename!s} but matched data file is "
                    f"{companion_path!s}."
                )

            # Enforce pipeline metadata (ensures hash is correct).
            layer.save_calibration_layer(
                path, createDirectory=False, save_contents=False
            )

        destination, _, _ = outputManager.add_file(path, path_handler=handler)
        saved_file_paths.append(destination)

        if companion_path:
            # Add the companion data file to the output manager with its equivalent data handler, ensuring both files are saved together and co-versioned.
            # we deliberately did not save the data file with its own handler, as that would cause it to be versioned independently of the JSON layer.
            outputManager.add_file(
                companion_path,
                path_handler=handler.get_equivalent_data_handler(),  # type: ignore
            )

    return saved_file_paths


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
) -> list[Path]:
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
    cleanup_temp_files_after_run: Annotated[
        bool,
        typer.Option(
            help="Whether to clean up temporary files after the calibration run. "
            "If False, temporary files are retained in the work folder for inspection.",
        ),
    ] = True,
    version_number_override: Annotated[
        tuple[int, int] | None,
        typer.Option(
            "--version-number-override",
            help="Force a specific (major, minor) version number for output layers "
            "instead of auto-incrementing. Existing layers at that version are "
            "overwritten. Provide as two integers, e.g. --version-number-override 1 5.",
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
            cleanup_temp_files_after_run=cleanup_temp_files_after_run,
            version_number_override=version_number_override,
        )
        if result:
            results.extend(result)
        else:
            logger.warning(
                f"No calibration outputs produced for {current.strftime('%Y-%m-%d')}."
            )
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
    cleanup_temp_files_after_run: bool = True,
    version_number_override: tuple[int, int] | None = None,
) -> list[Path]:
    """Run calibration for a single date."""
    if method == CalibrationMethod.NOOP:
        # NOOP is retained only as the internal descriptor for the zero-offset layer
        # that ``apply`` auto-creates; it is not a runnable calibration job.
        raise ValueError(
            "The 'noop' calibration method is not runnable. It exists only for the "
            "internal zero-offset layer. Choose a real calibration method."
        )

    version_number_override = _validate_version_number_override(version_number_override)

    app_settings = AppSettings()
    # Use the dedicated calibrate command config so each run gets its own uniquely
    # named work folder (based on the date + mode being calibrated).
    work_folder = app_settings.setup_work_folder_for_command(
        app_settings.calibrate,
        name_context={
            "date": start_date.strftime("%Y%m%d"),
            "mode": mode.short_name,
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
        date=start_date,
        mode=mode,
        sensor=sensor,
        cleanup_temp_files_after_run=cleanup_temp_files_after_run,
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
            assert isinstance(calibration_configuration, ScriptedL2CalibrationConfig)
            matlab_repo_path = Path(calibration_configuration.matlab_repo)
            if not matlab_repo_path.exists():
                raise ValueError(f"MATLAB repo path {matlab_repo_path} does not exist.")

            calibrator = ScriptedL2CalibrationJob(
                calibration_job_parameters,
                app_settings,
                matlab_repo_path=matlab_repo_path,
                metakernel=metakernel,
            )
        case _:
            raise ValueError("Calibration method is not implemented")

    calibrator.setup(app_settings.data_store, datastore_finder)

    if app_settings.calibrate.output_folder_override:
        logger.info(
            f"Overriding output folder for calibration layers to {app_settings.calibrate.output_folder_override}"
        )
        settings_for_output = app_settings.model_copy()
        settings_for_output.data_store = Path(
            app_settings.calibrate.output_folder_override
        )
    else:
        settings_for_output = app_settings

    outputManager = DatastoreFileManager.CreateByMode(
        settings_for_output, use_database=save_mode == SaveMode.LocalAndDatabase
    )

    calibration_handler = CalibrationLayerPathHandler.from_method(
        method=method,
        content_date=start_date,
        mode=mode,
        version_number_override=version_number_override,
        settings=app_settings,
    )

    try:
        returned = list(
            calibrator.run_calibration(calibration_handler, calibration_configuration)
        )
    finally:
        calibrator.cleanup()

    ancillaries = list(calibrator.get_ancillary_files())

    returning = (
        _save_calibration_outputs(
            returned=returned,
            outputManager=outputManager,
            version_number_override=version_number_override,
        )
        + ancillaries
    )

    return returning

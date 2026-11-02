import json
import logging
import shutil
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Annotated

import typer

from imap_mag.cli.cliUtils import initialiseLoggingForCommand
from imap_mag.config import AppSettings, SaveMode
from imap_mag.io import DatastoreFileManager
from imap_mag.io.file.CalculatedOffsetsPathHandler import (
    LEINWEBER,
    CalculatedOffsetsPathHandler,
)
from mag_toolkit.calibration import Sensor
from mag_toolkit.calibration.MatlabWrapper import call_matlab

logger = logging.getLogger(__name__)


class ConfigureMidpoint(StrEnum):
    """How generate_raw_leinweber_offset derives its analysis midpoint from --date.

    Mirrors the ``Options.configure_midpoint`` name-value option of the MATLAB
    tool exactly (values are passed straight through).
    """

    USE_DATE_AS_IS = "USE_DATE_AS_IS"
    USE_MID_DAY = "USE_MID_DAY"
    USE_12_HRS_AFTER_PIVOT_PLATFORM_MOVEMENT = (
        "USE_12_HRS_AFTER_PIVOT_PLATFORM_MOVEMENT"
    )


# Relative path (within the MATLAB repo) of the tool we invoke. Used to fail fast if
# the acquired repo does not actually contain the expected entry point.
MATLAB_SCRIPT_RELATIVE_PATH = (
    Path("+calibration") / "+tools" / "generate_raw_leinweber_offset.m"
)

# Name of the dynamically-generated MATLAB file-path config written to the work
# folder before each run and cleaned up afterwards.
USER_CONFIG_FILENAME = "imap_mag_generate_leinweber_offsets_file_path_config.json"

# Subfolder of the work folder where MATLAB writes its output files.
OUTPUT_SUBFOLDER_NAME = "outputs"


def generate_leinweber_offsets(
    date: Annotated[
        datetime,
        typer.Option(
            "--date",
            help="Calendar day to generate offsets for. The MATLAB tool derives "
            "its analysis midpoint from this day per --configure-midpoint (by "
            "default, 12 hours after the IMAP-Lo pivot-platform movement time of "
            "day), then runs over a window of --duration-hours centred on that.",
        ),
    ],
    matlab_repo: Annotated[
        Path | None,
        typer.Option(
            help="Path to the IMAP_MAG_Calibration MATLAB repository containing "
            "generate_raw_leinweber_offset."
        ),
    ] = None,
    duration_hours: Annotated[
        int | None,
        typer.Option(
            help="Width (in hours) of the window centred on the derived midpoint. "
            "Defaults to the configured value (normally 24)."
        ),
    ] = None,
    configure_midpoint: Annotated[
        ConfigureMidpoint | None,
        typer.Option(
            help="How to derive the analysis midpoint from --date. Left unset, "
            "MATLAB applies its own default "
            "(USE_12_HRS_AFTER_PIVOT_PLATFORM_MOVEMENT)."
        ),
    ] = None,
    sensors: Annotated[
        list[Sensor],
        typer.Option(help="Sensors to generate offsets for."),
    ] = [Sensor.MAGO, Sensor.MAGI],
    calibration_matrix_version: Annotated[
        int | None,
        typer.Option(
            help="Version of the calibration matrices to load. Defaults to the "
            "configured value."
        ),
    ] = None,
    calibration_input_json_file: Annotated[
        str | None,
        typer.Option(
            help="Calibration input JSON (path relative to the MATLAB repo), for "
            "the IMAP-Lo pivot-platform deltas. Defaults to the configured value."
        ),
    ] = None,
    verify: Annotated[
        bool,
        typer.Option(
            help="Re-run Leinweber on the corrected data and record the residual "
            "(doubles runtime)."
        ),
    ] = True,
    save_mode: Annotated[
        SaveMode,
        typer.Option(help="Whether to save locally only or to also save to database"),
    ] = SaveMode.LocalAndDatabase,
    cleanup_temp_files_after_run: Annotated[
        bool,
        typer.Option(
            help="Whether to clean up temporary files after the run. If False, "
            "temporary files are retained in the work folder for inspection."
        ),
    ] = True,
    allow_pivot_platform_extrapolation: Annotated[
        bool,
        typer.Option(
            help="If False (default), a sensor/day is skipped rather than "
            "corrected with extrapolated IMAP-Lo pivot-platform angles when the "
            "available pivot-platform data doesn't cover the window."
        ),
    ] = False,
) -> list[Path]:
    """
    Generate daily Leinweber (solar-wind) offsets via the MATLAB
    generate_raw_leinweber_offset tool, and publish them to the datastore.

    e.g. imap-mag calibration generate-leinweber-offsets --date 2026-08-01 --matlab-repo /app/matlab/calibration
    """
    if matlab_repo is None:
        raise ValueError(
            "A valid MATLAB repository path is required to generate Leinweber offsets."
        )
    matlab_repo_path = Path(matlab_repo)
    if not matlab_repo_path.is_dir():
        raise FileNotFoundError(f"MATLAB repository not found at {matlab_repo_path}.")
    matlab_script = matlab_repo_path / MATLAB_SCRIPT_RELATIVE_PATH
    if not matlab_script.is_file():
        raise FileNotFoundError(
            f"Expected MATLAB script not found at {matlab_script}; the acquired "
            "repository does not contain generate_raw_leinweber_offset."
        )

    app_settings = AppSettings()
    config = app_settings.calibrate.leinweber

    effective_duration = (
        duration_hours if duration_hours is not None else config.duration_hours
    )
    # calibration_matrix_version/input_json_file are shared with the scripted-L2
    # calibration, so they live on the parent calibrate config, not here.
    effective_matrix_version = (
        calibration_matrix_version
        if calibration_matrix_version is not None
        else app_settings.calibrate.scripted_l2_calibration_matrix_version
    )
    effective_input_json_file = (
        calibration_input_json_file
        if calibration_input_json_file is not None
        else app_settings.calibrate.scripted_l2_input_json_file
    )

    work_folder = app_settings.setup_work_folder_for_command(
        config,
        name_context={
            "date": date.strftime("%Y%m%d"),
            "duration": str(effective_duration),
        },
    )
    initialiseLoggingForCommand(
        work_folder
    )  # DO NOT log anything before this point (it won't be captured in the log file)

    # Recreate the output folder empty on each run so a previous
    # cleanup_temp_files_after_run=False run cannot leak stale outputs in. MATLAB
    # writes the produced CSVs into a "leinweber" subfolder of output_offsets_folder
    output_offsets_folder = work_folder / OUTPUT_SUBFOLDER_NAME
    output_dir = output_offsets_folder / LEINWEBER
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True)

    user_config_path = _write_user_config(
        app_settings.data_store, output_offsets_folder, work_folder
    )

    command = _build_matlab_command(
        date=date,
        duration_hours=effective_duration,
        sensors=sensors,
        calibration_matrix_version=effective_matrix_version,
        calibration_input_json_file=effective_input_json_file,
        file_path_config=user_config_path,
        verify=verify,
        configure_midpoint=configure_midpoint,
        allow_pivot_platform_extrapolation=allow_pivot_platform_extrapolation,
    )

    call_matlab(
        command,
        cwd=matlab_repo_path,
        include_project_paths=False,
        timeout=config.timeout_seconds_per_day,
    )

    produced = sorted(p for p in output_dir.glob("*.csv") if p.is_file())
    if not produced:
        raise FileNotFoundError(
            f"MATLAB Leinweber offset generation produced no output files in {output_dir}."
        )
    logger.info(
        f"MATLAB produced {len(produced)} Leinweber offset file(s) in {output_dir}."
    )

    outputManager = DatastoreFileManager.CreateByMode(
        app_settings, use_database=save_mode == SaveMode.LocalAndDatabase
    )

    saved: list[Path] = []
    for path in produced:
        handler = CalculatedOffsetsPathHandler.from_filename(path)
        if handler is None:
            raise ValueError(
                f"Could not parse a Leinweber offsets path handler from {path.name}."
            )
        destination, _, _ = outputManager.add_file(path, path_handler=handler)
        saved.append(destination)
        logger.info(f"Published Leinweber offsets to {destination}.")

    if cleanup_temp_files_after_run:
        user_config_path.unlink(missing_ok=True)
        shutil.rmtree(output_dir, ignore_errors=True)

    return saved


def _write_user_config(
    data_store: Path, output_offsets_folder: Path, work_folder: Path
) -> Path:
    """Write the MATLAB file-path config generate_raw_leinweber_offset needs.

    ``sharepoint_flight_data`` is read for science/spin-table data and the
    calibration matrices. ``output_offsets_folder`` is the dedicated work-folder
    output directory
    """
    config = {
        "sharepoint_flight_data": str(Path(data_store).resolve()),
        "output_offsets_folder": str(Path(output_offsets_folder).resolve()),
    }

    user_config_path = work_folder / USER_CONFIG_FILENAME
    with open(user_config_path, "w") as fid:
        json.dump(config, fid, indent=4)

    logger.info(f"Wrote MATLAB file-path config to {user_config_path}: {config}")
    return user_config_path

def _bool_to_matlab_str(value: bool) -> str:
    """Convert a Python boolean to a MATLAB boolean string."""
    return "true" if value else "false"

def _datetime_to_matlab_expr(dt: datetime) -> str:
    """Convert a Python datetime to a MATLAB datetime expression."""
    return (
        f"datetime({dt.year},{dt.month},{dt.day},"
        f"{dt.hour},{dt.minute},{dt.second})"
    )

def _build_matlab_command(
    date: datetime,
    duration_hours: int,
    sensors: list[Sensor],
    calibration_matrix_version: int,
    calibration_input_json_file: str,
    file_path_config: Path,
    verify: bool,
    configure_midpoint: ConfigureMidpoint | None = None,
    allow_pivot_platform_extrapolation: bool = False,
) -> str:
    """Build the ``generate_raw_leinweber_offset`` MATLAB command for one day.

    """
    date_expr = _datetime_to_matlab_expr(date)
    sensors_expr = "[" + ", ".join(f'"{s.value}"' for s in sensors) + "]"

    return (
        "calibration.tools.generate_raw_leinweber_offset("
        f"{date_expr}, {duration_hours}, "
        f"'sensors', {sensors_expr}, "
        f"'calibration_matrix_version', {calibration_matrix_version}, "
        f"'calibration_input', \"{calibration_input_json_file}\", "
        f"'file_path_config', \"{file_path_config.resolve()!s}\", "
        "'version', 1, "
        "'write', true, "
        f"'verify', {_bool_to_matlab_str(verify)}, "
        "'allow_pivot_platform_extrapolation', "
        f"{_bool_to_matlab_str(allow_pivot_platform_extrapolation)}"
        + (
            f",'configure_midpoint', \"{configure_midpoint.value}\""
            if configure_midpoint is not None
            else ""
        )
        + ")"
    )

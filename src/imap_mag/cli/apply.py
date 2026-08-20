import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Annotated

import typer

from imap_mag.cli.cliUtils import (
    fetch_file_for_work,
    initialiseLoggingForCommand,
)
from imap_mag.config import AppSettings, SaveMode
from imap_mag.io import DatastoreFileManager, FileFinder
from imap_mag.io.file import (
    AncillaryPathHandler,
    CalibrationLayerPathHandler,
    SciencePathHandler,
)
from imap_mag.io.file.VersionedPathHandler import VersionedPathHandler
from imap_mag.util import MAGSensor, ReferenceFrame, ScienceMode
from mag_toolkit.calibration import (
    CalibrationApplicator,
    CalibrationLayer,
    CalibrationMethod,
    FileType,
    ScienceLayer,
)

logger = logging.getLogger(__name__)


def _validate_offset_version_override(version: int | None) -> int | None:
    """Validate an integer offset file version override.

    Args:
        version: The version number to force (1-999), or None for auto-increment.

    Returns:
        The validated version, or None.

    Raises:
        ValueError: If the value is a bool, not an integer, or out of range.
    """
    if version is None:
        return None
    if isinstance(version, bool):
        raise ValueError("Offset version override must be an integer, not bool.")
    if not isinstance(version, int):
        raise ValueError(
            f"Offset version override must be an integer, got {type(version).__name__}."
        )
    if version < 1:
        raise ValueError(f"Offset version override must be at least 1, got {version}.")
    if version > 999:
        raise ValueError(f"Offset version override must be at most 999, got {version}.")
    logger.warning(
        f"Offset version override active: forcing offset file to v{version:03d}. "
        "Existing file at this version may be overwritten."
    )
    return version


def _validate_l2_version_override(
    override: tuple[int, int] | None,
) -> tuple[int, int] | None:
    """Validate a (major, minor) L2 science file version override tuple.

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
            raise ValueError(f"L2 version {name} must be an integer, not bool.")
        if not isinstance(val, int):
            raise ValueError(
                f"L2 version {name} must be an integer, got {type(val).__name__}."
            )
        if val < 0:
            raise ValueError(f"L2 version {name} must be non-negative, got {val}.")
        if val > max_val:
            raise ValueError(f"L2 version {name} must be at most {max_val}, got {val}.")

    logger.warning(
        f"L2 version override active: forcing L2-pre science files to v{major:03d}.{minor:04d}. "
        "Existing files at this version may be overwritten."
    )
    return (major, minor)


# TODO: REFACTOR - moving files to a work folder could be simplified/generalized?
def _prepare_layers_for_application(
    layers: list[str], datastore_finder: FileFinder, appSettings: AppSettings
) -> list[Path]:
    """
    Prepare the calibration layers for application by fetching the versioned files.
    """

    work_layers = []
    for layer in layers:
        cal_handler = CalibrationLayerPathHandler.from_filename(layer)
        if not cal_handler:
            logger.error(
                f"Could not parse metadata from calibration metadata file: {layer}"
            )
            raise ValueError(
                f"Could not parse metadata from calibration metadata file: {layer}"
            )

        versioned_layer_file: Path = datastore_finder.find_by_handler(
            path_handler=cal_handler,
            throw_if_not_found=True,
        )

        work_layers.append(
            fetch_file_for_work(
                versioned_layer_file,
                appSettings.work_folder,
                throw_if_not_found=True,
            )
        )

        # Get data file
        cal_layer = CalibrationLayer.from_file(versioned_layer_file)
        datafile = cal_layer.get_datafile_path()

        if datafile:
            fetch_file_for_work(
                datafile,
                appSettings.work_folder,
                throw_if_not_found=True,
            )

    return work_layers


def _prepare_rotation_layer_for_application(rotation, appSettings):
    """
    Prepare the rotation layer for application by fetching the versioned file.
    """
    if rotation:
        datastore_finder = FileFinder(appSettings.data_store)
        rotation_handler = AncillaryPathHandler.from_filename(rotation)
        if not rotation_handler:
            logger.error(f"Could not parse metadata from rotation file: {rotation}")
            raise ValueError(f"Could not parse metadata from rotation file: {rotation}")
        versioned_rotation_file = datastore_finder.find_by_handler(
            path_handler=rotation_handler,
            throw_if_not_found=True,
        )
        return fetch_file_for_work(
            versioned_rotation_file, appSettings.work_folder, throw_if_not_found=True
        )
    return None


def apply(
    layers: Annotated[
        list[str],
        typer.Option(
            help="Calibration layers (filenames or glob patterns like '*noop*') to apply"
        ),
    ],
    date: Annotated[
        datetime | None,
        typer.Option(
            "--date",
            help="Exact date of the input file data - cannot be combined with --start-date or --end-date",
        ),
    ] = None,
    start_date: Annotated[
        datetime | None,
        typer.Option("--start-date", help="Start date of the input file data"),
    ] = None,
    end_date: Annotated[
        datetime | None,
        typer.Option(
            "--end-date", help="End date for processing a date range (inclusive)"
        ),
    ] = None,
    mode: Annotated[
        ScienceMode | None,
        typer.Option(
            help="Science mode (norm or burst). Used to discover the input file when input is not provided."
        ),
    ] = None,
    offset_file_output_type: Annotated[
        str, typer.Option(help="Output type of the calibration file")
    ] = FileType.CDF.value,
    l2_output_type: Annotated[
        str, typer.Option(help="Output type of the L2 file")
    ] = FileType.CDF.value,
    rotation: Annotated[Path | None, typer.Option()] = None,
    input: Annotated[
        str | None,
        typer.Argument(
            help="The file name for the input file. If not provided, will be discovered using --mode and --date."
        ),
    ] = None,
    save_mode: Annotated[
        SaveMode,
        typer.Option(help="Whether to save locally only or to also save to database"),
    ] = SaveMode.LocalOnly,
    spice_metakernel: Annotated[
        Path | None,
        typer.Option(
            help="Path to spice metakernel file to be used. Will query database and generate one if none provided"
        ),
    ] = None,
    reference_frames: Annotated[
        list[ReferenceFrame],
        typer.Option(
            "--frames",
            help="Reference frames (SPICE) to generate L2 files in. Defaults to all frames.",
        ),
    ] = [
        ReferenceFrame.SRF,
        ReferenceFrame.GSE,
        ReferenceFrame.GSM,
        ReferenceFrame.RTN,
        ReferenceFrame.DSRF,
    ],
    offset_version_override: Annotated[
        int | None,
        typer.Option(
            "--offset-version-override",
            help="Force a specific version number (1-999) for the output offset file "
            "instead of auto-incrementing. Existing file at that version is overwritten.",
        ),
    ] = None,
    l2_version_override: Annotated[
        tuple[int, int] | None,
        typer.Option(
            "--l2-version-override",
            help="Force a specific (major, minor) version for output L2-pre science CDF files "
            "instead of auto-incrementing. Provide as two integers. Existing files at that "
            "version are overwritten.",
        ),
    ] = None,
):
    """
    Apply calibration rotation and layers to an input science file.

    Supports date ranges (--date to --end-date), glob patterns for layers,
    and automatic science file discovery by mode.

    imap-mag calibration apply --date [date] --layers [layers] [input]
    e.g. imap-mag calibration apply --date 2026-01-16 --layers '*noop*'
    e.g. imap-mag calibration apply --date 2026-01-16 --end-date 2026-01-20 --layers '*' --mode norm
    e.g. imap-mag -v calibration apply --layers "*manual*" --start-date 2026-01-16 --end-date 2026-01-16 --mode norm --rotation imap_mag_l2-calibration_20260101_v003.cdf --frames srf --frames gse --spice-metakernel tests/datastore/spice/mk/metakernel.txt
    """
    if (start_date is None and date is None) or (
        start_date is not None and date is not None
    ):
        raise typer.BadParameter(
            "A date must be provided via --date or --start-date, but not both."
        )

    current = start_date or date
    assert current is not None  # for mypy
    effective_end = end_date or current
    while current <= effective_end:
        _apply_for_date(
            layers=layers,
            date=current,
            mode=mode,
            input=input,
            offset_file_output_type=offset_file_output_type,
            l2_output_type=l2_output_type,
            rotation=rotation,
            save_mode=save_mode,
            spice_metakernel=spice_metakernel,
            reference_frames=reference_frames,
            offset_version_override=offset_version_override,
            l2_version_override=l2_version_override,
        )
        current += timedelta(days=1)


def _apply_for_date(
    layers: list[str],
    date: datetime,
    mode: ScienceMode | None,
    input: str | None,
    offset_file_output_type: str,
    l2_output_type: str,
    rotation: Path | None,
    save_mode: SaveMode,
    spice_metakernel: Path | None,
    reference_frames: list[ReferenceFrame],
    offset_version_override: int | None = None,
    l2_version_override: tuple[int, int] | None = None,
):
    """Apply calibration layers for a single date."""
    offset_version_override = _validate_offset_version_override(offset_version_override)
    l2_version_override = _validate_l2_version_override(l2_version_override)

    app_settings = AppSettings()  # type: ignore
    work_folder = app_settings.setup_work_folder_for_command(app_settings.apply)
    initialiseLoggingForCommand(
        work_folder
    )  # DO NOT log anything before this point (it won't be captured in the log file)

    if input is not None and mode is None:
        if ScienceMode.Burst.short_name in input:
            mode = ScienceMode.Burst
        else:
            mode = ScienceMode.Normal

    if mode is None:
        raise ValueError(
            "Burst/Normal mode could not be inferred from input filename. Please provide a mode (norm or burst) to assist with file discovery."
        )

    # Resolve layer patterns to actual filenames
    datastore_finder = FileFinder(app_settings.data_store, app_settings.work_folder)
    resolved_layers = (
        datastore_finder.find_layers_by_date_and_patterns(
            layers, start_date=date, end_date=date, mode=mode, throw_if_not_found=True
        )
        if layers
        else []  # ensure we throw if a layer is passed in but not found
    )

    # Discover science file if not provided
    if input is None:
        if mode is None:
            raise ValueError(
                "Either an input science file or a mode (norm/burst) must be provided "
                "so the science file can be discovered."
            )
        input = datastore_finder.find_latest_science_by_date(date, mode, MAGSensor.OBS)

    # Parse metadata from the filename regardless of where the file lives
    original_input_handler = SciencePathHandler.from_filename(Path(input).name)

    if not original_input_handler:
        logger.error(f"Could not parse metadata from input file: {input}")
        raise ValueError(f"Could not parse metadata from input file: {input}")

    if l2_output_type != FileType.CDF.value:
        raise NotImplementedError(f"Unsupported L2 output file type: {l2_output_type}")

    if not resolved_layers and not rotation:
        raise ValueError(
            "At least one of calibration layers or rotation file must be provided."
        )

    versioned_science_file = datastore_finder.find_by_name_or_path(
        input, throw_if_not_found=True
    )

    logger.info(f"Applying layers to input file {versioned_science_file}")

    if (
        spice_metakernel is not None
    ):  # not generating a metakernel, but one is provided, so need to resolve the path
        spice_metakernel = datastore_finder.find_by_name_or_path(
            spice_metakernel, throw_if_not_found=True
        )

    workScienceFile: Path = fetch_file_for_work(
        versioned_science_file, app_settings.work_folder, throw_if_not_found=True
    )

    workLayers = _prepare_layers_for_application(
        resolved_layers, datastore_finder, app_settings
    )
    workRotationFile = _prepare_rotation_layer_for_application(rotation, app_settings)

    offset_file_handler = AncillaryPathHandler(
        descriptor=f"l2-{original_input_handler.get_mode().short_name}-offsets",
        start_date=date,
        end_date=date,
        version=offset_version_override if offset_version_override is not None else 1,
        versioning_mode=VersionedPathHandler.VersionMode.USER_OVERRIDE
        if offset_version_override is not None
        else VersionedPathHandler.VersionMode.MAX_VERSION_PLUS_ONE,
        extension=offset_file_output_type,
    )

    offset_file_path = app_settings.work_folder / offset_file_handler.get_filename()

    applier = CalibrationApplicator(app_settings)
    rotateInfo = f"with rotation from {rotation}" if rotation else ""
    logger.info(f"Applying offsets from {resolved_layers} to {input} {rotateInfo}")

    outputManager = DatastoreFileManager.CreateByMode(
        app_settings, use_database=save_mode == SaveMode.LocalAndDatabase
    )

    if not workLayers:
        logger.info(
            "No calibration layers provided, proceeding with apply using only rotation. A temporary zero offset layer will be created."
        )
        workLayers = [
            _setup_zero_calibration_layer(
                work_folder, workScienceFile, date, app_settings
            )
        ]

    (L2_files, offset_file) = applier.apply(
        day_to_process=date,
        layer_files=workLayers,
        rotation=workRotationFile,
        dataFile=workScienceFile,
        outputOffsetsFile=offset_file_path,
        outputScienceFolder=app_settings.work_folder,
        spice_metakernel=spice_metakernel,
        reference_frames=reference_frames,
    )
    outputManager.add_file(offset_file, offset_file_handler)
    for L2_file in L2_files:
        l2_handler = SciencePathHandler.from_filename(L2_file.name)

        if not l2_handler:
            logger.warning(
                f"Could not parse metadata from output L2 file: {L2_file}, skipping saving to database"
            )
            continue

        l2_handler.level = "l2-pre"
        l2_handler.has_major_version = True
        if l2_version_override is not None:
            l2_handler.version_major = l2_version_override[0]
            l2_handler.version = l2_version_override[1]
            l2_handler.versioning_mode = VersionedPathHandler.VersionMode.USER_OVERRIDE
        else:
            l2_handler.version = 1
            l2_handler.version_major = app_settings.version_major
        outputManager.add_file(L2_file, l2_handler)

    cleanup_workfolder_after_apply(
        app_settings,
        workScienceFile,
        workLayers,
        workRotationFile,
        L2_files,
        offset_file,
    )

    logger.info(f"Apply complete for date {date}. Temporary files cleaned up.")


def cleanup_workfolder_after_apply(
    app_settings, workScienceFile, workLayers, workRotationFile, L2_files, offset_file
):
    files_to_cleanup: list[Path] = [
        offset_file,
        *L2_files,
        *workLayers,
        workScienceFile,
    ]
    if workRotationFile:
        files_to_cleanup.append(workRotationFile)

    # add the companion data file (CSV or Parquet) for each JSON layer to the cleanup list
    for layer_file in workLayers:
        handler = CalibrationLayerPathHandler.from_filename(layer_file)
        if handler is not None and handler.is_metadata_file():
            try:
                cal = CalibrationLayer.from_file(layer_file, load_contents=False)
                data_filename = cal.get_datafile_path()
            except Exception:
                data_filename = None

            if data_filename is not None:
                if data_filename.exists():
                    files_to_cleanup.append(data_filename)
            else:
                for ext in (".csv", ".parquet"):
                    companion = layer_file.with_suffix(ext)
                    if companion.exists():
                        files_to_cleanup.append(companion)

    work_folder_resolved = app_settings.work_folder.resolve()
    for temp_file in files_to_cleanup:
        temp_file_resolved = temp_file.resolve()
        if temp_file_resolved.exists() and (
            temp_file_resolved == work_folder_resolved
            or work_folder_resolved in temp_file_resolved.parents
        ):
            logger.info(f"Deleting temporary file {temp_file}")
            os.remove(temp_file)
        else:
            logger.warning(
                f"Skipping deletion of file outside work folder '{app_settings.work_folder}': {temp_file}"
            )


def _setup_zero_calibration_layer(
    work_folder: Path,
    workScienceFile: Path,
    content_date: datetime,
    app_settings: AppSettings,
) -> Path:
    logger.info(
        "No calibration layers provided, setting up a zero calibration layer for application."
    )

    calibration_handler = CalibrationLayerPathHandler.from_method(
        method=CalibrationMethod.NOOP, content_date=content_date, settings=app_settings
    )

    new_layer_file = work_folder / calibration_handler.get_filename()
    if new_layer_file.exists():
        logger.warning(
            f"Zero calibration layer file already exists and will be overwritten: {new_layer_file}"
        )
        new_layer_file.unlink()

    science_layer = ScienceLayer.from_file(workScienceFile, load_contents=True)
    zero_offset_layer = CalibrationLayer.create_zero_offset_layer_from_science(
        science_layer, app_settings
    )
    del science_layer

    zero_offset_layer.write_to_file(
        new_layer_file, False
    )  # json and also writes CSV for us automatically

    del zero_offset_layer
    return new_layer_file

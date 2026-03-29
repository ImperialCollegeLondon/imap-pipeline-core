import fnmatch
import logging
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Annotated

import typer

from imap_mag.cli.cliUtils import (
    fetch_file_for_work,
    initialiseLoggingForCommand,
)
from imap_mag.config import AppSettings, SaveMode
from imap_mag.io import DatastoreFileFinder, DatastoreFileManager
from imap_mag.io.file import (
    AncillaryPathHandler,
    CalibrationLayerPathHandler,
    SciencePathHandler,
)
from imap_mag.util import ReferenceFrame, ScienceMode
from mag_toolkit.calibration import (
    CalibrationApplicator,
    CalibrationLayer,
    CalibrationMethod,
    ScienceLayer,
)

logger = logging.getLogger(__name__)


class FileType(Enum):
    CSV = "csv"
    CDF = "cdf"
    JSON = "json"


def resolve_layer_patterns(
    layers: list[str], date: datetime, datastore: Path
) -> list[str]:
    """Resolve layer pattern strings to actual layer filenames.

    Each entry in layers can be:
    - An exact filename (e.g. "imap_mag_noop-layer_20260116_v001.json")
    - A glob pattern (e.g. "*noop*", "*") that matches layer filenames for the given date.

    Returns resolved filenames in the order the patterns were provided.
    """
    date_str = date.strftime("%Y%m%d")
    layer_dir = (
        datastore / "calibration" / "layers" / date.strftime("%Y") / date.strftime("%m")
    )

    resolved: list[str] = []
    for layer in layers:
        if "*" in layer or "?" in layer:
            if not layer_dir.exists():
                logger.warning(
                    f"Layer directory {layer_dir} does not exist, skipping pattern {layer}"
                )
                continue
            matched = []
            for f in sorted(layer_dir.iterdir()):
                if (
                    f.is_file()
                    and f.suffix == ".json"
                    and date_str in f.name
                    and fnmatch.fnmatch(f.name, layer)
                ):
                    matched.append(f.name)
            if not matched:
                logger.warning(
                    f"No layer files matched pattern '{layer}' for date {date_str} in {layer_dir}"
                )
            resolved.extend(matched)
        else:
            resolved.append(layer)

    return resolved


def find_science_file(date: datetime, mode: ScienceMode, datastore: Path) -> str:
    """Find the highest version science file in the datastore for a given date and mode.

    Scans the datastore directory structure to locate science files matching
    the date and mode, returning the filename of the highest version match.
    """
    date_str = date.strftime("%Y%m%d")
    science_dir = datastore / "science" / "mag"

    candidates: list[tuple[str, int]] = []

    if not science_dir.exists():
        raise FileNotFoundError(f"Science directory {science_dir} does not exist")

    for level_dir in sorted(science_dir.iterdir()):
        if not level_dir.is_dir():
            continue
        # Skip output levels (l2-pre, etc.)
        if "l2" in level_dir.name:
            continue

        date_dir = level_dir / date.strftime("%Y") / date.strftime("%m")
        if not date_dir.exists():
            continue

        for f in sorted(date_dir.iterdir()):
            if not f.is_file() or f.suffix != ".cdf":
                continue
            if date_str not in f.name:
                continue

            handler = SciencePathHandler.from_filename(f.name)
            if handler and handler.get_mode() == mode:
                candidates.append((f.name, handler.version))

    if not candidates:
        raise FileNotFoundError(
            f"No science file found for date {date.strftime('%Y-%m-%d')} and mode {mode.value}"
        )

    # Sort by version descending, return highest
    candidates.sort(key=lambda x: x[1], reverse=True)
    logger.info(
        f"Discovered science file {candidates[0][0]} for date {date.strftime('%Y-%m-%d')} mode {mode.value}"
    )
    return candidates[0][0]


# TODO: REFACTOR - moving files to a work folder could be simplified/generalized?
def prepare_layers_for_application(
    layers: list[str], appSettings: AppSettings
) -> list[Path]:
    """
    Prepare the calibration layers for application by fetching the versioned files.
    """
    datastore_finder = DatastoreFileFinder(appSettings.data_store)
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

        versioned_layer_file: Path = datastore_finder.find_matching_file(
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

        if cal_layer.metadata.data_filename:
            fetch_file_for_work(
                versioned_layer_file.parent / cal_layer.metadata.data_filename,
                appSettings.work_folder,
                throw_if_not_found=True,
            )

    return work_layers


def prepare_rotation_layer_for_application(rotation, appSettings):
    """
    Prepare the rotation layer for application by fetching the versioned file.
    """
    if rotation:
        datastore_finder = DatastoreFileFinder(appSettings.data_store)
        rotation_handler = AncillaryPathHandler.from_filename(rotation)
        if not rotation_handler:
            logger.error(f"Could not parse metadata from rotation file: {rotation}")
            raise ValueError(f"Could not parse metadata from rotation file: {rotation}")
        versioned_rotation_file = datastore_finder.find_matching_file(
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
        datetime,
        typer.Option("--date", help="Start date of the input file data"),
    ],
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
):
    """
    Apply calibration rotation and layers to an input science file.

    Supports date ranges (--date to --end-date), glob patterns for layers,
    and automatic science file discovery by mode.

    imap-mag calibration apply --date [date] --layers [layers] [input]
    e.g. imap-mag calibration apply --date 2026-01-16 --layers '*noop*'
    e.g. imap-mag calibration apply --date 2026-01-16 --end-date 2026-01-20 --layers '*' --mode norm
    """
    effective_end = end_date or date
    current = date
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
):
    """Apply calibration layers for a single date."""
    app_settings = AppSettings()  # type: ignore
    work_folder = app_settings.setup_work_folder_for_command(app_settings.calibration)
    initialiseLoggingForCommand(
        work_folder
    )  # DO NOT log anything before this point (it won't be captured in the log file)

    # Resolve layer patterns to actual filenames
    resolved_layers = resolve_layer_patterns(layers, date, app_settings.data_store)

    # Discover science file if not provided
    if input is None:
        if mode is None:
            raise ValueError(
                "Either an input science file or a mode (norm/burst) must be provided "
                "so the science file can be discovered."
            )
        input = find_science_file(date, mode, app_settings.data_store)

    original_input_handler = SciencePathHandler.from_filename(input)

    if not original_input_handler:
        logger.error(f"Could not parse metadata from input file: {input}")
        raise ValueError(f"Could not parse metadata from input file: {input}")

    if l2_output_type != FileType.CDF.value:
        raise NotImplementedError(f"Unsupported L2 output file type: {l2_output_type}")

    if not resolved_layers and not rotation:
        raise ValueError(
            "At least one of calibration layers or rotation file must be provided."
        )

    datastore_finder = DatastoreFileFinder(app_settings.data_store)
    versioned_science_file = datastore_finder.find_matching_file(
        path_handler=original_input_handler,
        throw_if_not_found=True,
    )

    logger.info(f"Applying layers to input file {versioned_science_file}")

    workScienceFile: Path = fetch_file_for_work(
        versioned_science_file, app_settings.work_folder, throw_if_not_found=True
    )

    workLayers = prepare_layers_for_application(resolved_layers, app_settings)
    workRotationFile = prepare_rotation_layer_for_application(rotation, app_settings)

    offset_file_handler = AncillaryPathHandler(
        descriptor=f"l2-{original_input_handler.get_mode().short_name}-offsets",
        start_date=date,
        end_date=date,
        version=0,
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
        workLayers = [_setup_zero_calibration_layer(work_folder, workScienceFile, date)]

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

        l2_handler.level = "l2-pre"  # set level to l2-pre for the output file naming, as it's pre-release l2
        l2_handler.version = 0  # set version to 0 for the output file naming
        outputManager.add_file(L2_file, l2_handler)


def _setup_zero_calibration_layer(
    work_folder: Path, workScienceFile: Path, content_date: datetime
) -> Path:
    logger.info(
        "No calibration layers provided, setting up a zero calibration layer for application."
    )

    calibration_handler = CalibrationLayerPathHandler(
        descriptor=CalibrationMethod.NOOP.short_name, content_date=content_date
    )
    new_layer_file = work_folder / calibration_handler.get_filename()
    if new_layer_file.exists():
        logger.warning(
            f"Zero calibration layer file already exists and will be overwritten: {new_layer_file}"
        )
        new_layer_file.unlink()

    science_layer = ScienceLayer.from_file(workScienceFile, load_contents=True)
    zero_offset_layer = CalibrationLayer.create_zero_offset_layer_from_science(
        science_layer
    )
    del science_layer

    zero_offset_layer.writeToFile(
        new_layer_file, False
    )  # json and also writes CSV for us automatically

    del zero_offset_layer
    return new_layer_file

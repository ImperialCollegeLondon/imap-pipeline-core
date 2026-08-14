import logging
from datetime import datetime
from pathlib import Path
from typing import Annotated

import numpy as np
import pandas as pd
import typer

from imap_mag.cli.apply import FileType
from imap_mag.cli.cliUtils import fetch_file_for_work, initialiseLoggingForCommand
from imap_mag.config import AppSettings, SaveMode
from imap_mag.db.Database import Database
from imap_mag.io import DatastoreFileManager, FileFinder
from imap_mag.io.file import CalibrationLayerPathHandler
from imap_mag.io.file.VersionedPathHandler import VersionedPathHandler
from imap_mag.util import ScienceMode
from mag_toolkit.calibration import CalibrationLayer, ConversionStrategy
from mag_toolkit.calibration.CalibrationDefinitions import CONSTANTS

app = typer.Typer()

logger = logging.getLogger(__name__)

_CONVERTIBLE_FORMATS = (FileType.CSV, FileType.PARQUET, FileType.CDF)


def convert(
    input_layers: Annotated[
        list[str],
        typer.Option(
            help="Calibration layer filenames or glob patterns (e.g. '*noop*') to convert"
        ),
    ],
    start_date: Annotated[
        datetime | None,
        typer.Option(
            "--start-date", help="Restrict conversion to layers on/after this date"
        ),
    ] = None,
    end_date: Annotated[
        datetime | None,
        typer.Option(
            "--end-date",
            help="Restrict conversion to layers on/before this date (inclusive)",
        ),
    ] = None,
    mode: Annotated[
        ScienceMode | None,
        typer.Option(help="Restrict conversion to layers for this science mode"),
    ] = None,
    output_layer_data_format: Annotated[
        FileType,
        typer.Option(help="Target format to convert the matched layers to"),
    ] = FileType.PARQUET,
    output_layer_versioning_strategy: Annotated[
        ConversionStrategy,
        typer.Option(
            help="Overwrite the existing version in place, or publish the "
            "converted layer as a new version and leave the original untouched"
        ),
    ] = ConversionStrategy.OVERWRITE,
    save_mode: Annotated[
        SaveMode,
        typer.Option(help="Whether to save locally only or to also save to database"),
    ] = SaveMode.LocalAndDatabase,
) -> list[Path]:
    """
    Convert existing calibration layer files between csv, parquet and cdf formats.

    Layers are found the same way as ``apply`` (exact filenames or glob patterns),
    optionally restricted to a date range and/or science mode.

    e.g. imap-mag convert --input-layers '*noop*' --output-layer-data-format csv
    """
    if output_layer_data_format not in _CONVERTIBLE_FORMATS:
        raise typer.BadParameter(
            f"output_layer_data_format must be one of {[f.value for f in _CONVERTIBLE_FORMATS]}."
        )

    app_settings = AppSettings()
    work_folder = app_settings.setup_work_folder_for_command(app_settings.convert)
    initialiseLoggingForCommand(
        work_folder
    )  # DO NOT log anything before this point (it won't be captured in the log file)

    datastore_finder = FileFinder(
        app_settings.data_store,
        work_folder,
        database=Database() if Database.get_environment_url() else None,
    )

    resolved_layers = datastore_finder.find_layers_by_patterns(
        input_layers,
        start_date=start_date,
        end_date=end_date,
        mode=mode,
        throw_if_not_found=True,
    )

    outputManager = (
        DatastoreFileManager.CreateByMode(
            app_settings, use_database=save_mode == SaveMode.LocalAndDatabase
        )
        if app_settings.convert.publish_to_data_store
        else None
    )

    converted: list[Path] = []
    for layer_name in resolved_layers:
        converted.extend(
            _convert_layer(
                layer_name=layer_name,
                datastore_finder=datastore_finder,
                work_folder=work_folder,
                output_layer_data_format=output_layer_data_format,
                output_layer_versioning_strategy=output_layer_versioning_strategy,
                outputManager=outputManager,
                app_settings=app_settings,
                save_mode=save_mode,
            )
        )

    return converted


def _convert_layer(
    layer_name: str,
    datastore_finder: FileFinder,
    work_folder: Path,
    output_layer_data_format: FileType,
    output_layer_versioning_strategy: ConversionStrategy,
    outputManager,
    app_settings: AppSettings,
    save_mode: SaveMode,
) -> list[Path]:
    source_handler = CalibrationLayerPathHandler.from_filename(layer_name)
    if source_handler is None:
        raise ValueError(f"Could not parse metadata from layer file: {layer_name}")

    versioned_source_path = datastore_finder.find_by_handler(
        source_handler, throw_if_not_found=True
    )
    work_source_path = fetch_file_for_work(
        versioned_source_path, work_folder, throw_if_not_found=True
    )

    old_datastore_paths: list[Path] = [versioned_source_path]
    work_companion_path: Path | None = None
    if source_handler.extension != FileType.CDF.value:
        source_peek = CalibrationLayer.from_file(work_source_path, load_contents=False)
        if source_peek.metadata.data_filename:
            companion_versioned_path = (
                versioned_source_path.parent / source_peek.metadata.data_filename.name
            )
            work_companion_path = fetch_file_for_work(
                companion_versioned_path, work_folder, throw_if_not_found=True
            )
            old_datastore_paths.append(companion_versioned_path)

    source_extension = (
        work_companion_path.suffix.lstrip(".")
        if work_companion_path is not None
        else source_handler.extension
    )

    if source_extension == output_layer_data_format.value:
        logger.info(
            f"Layer {layer_name} is already in {output_layer_data_format.value} "
            "format; skipping conversion."
        )
        return []

    layer = CalibrationLayer.from_file(work_source_path, load_contents=True)

    is_overwrite = output_layer_versioning_strategy == ConversionStrategy.OVERWRITE
    versioning_mode = (
        VersionedPathHandler.VersionMode.USER_OVERRIDE
        if is_overwrite
        else VersionedPathHandler.VersionMode.MAX_VERSION_PLUS_ONE
    )

    is_cdf_target = output_layer_data_format == FileType.CDF
    output_handler = CalibrationLayerPathHandler(
        descriptor=source_handler.descriptor,
        content_date=source_handler.content_date,
        version_major=source_handler.version_major,
        version=source_handler.version,
        _has_major_version=source_handler._has_major_version,
        versioning_mode=versioning_mode,
        extension="cdf" if is_cdf_target else "json",
        data_extension=output_layer_data_format.value,
        allow_overwrite=is_overwrite,
    )

    layer.version_major = output_handler.version_major
    layer.version = output_handler.version

    new_primary_path = work_folder / output_handler.get_filename()
    new_companion_path: Path | None = None

    if is_cdf_target:
        layer.metadata.data_filename = None
        layer.writeToFile(new_primary_path)
    else:
        data_handler = output_handler.get_equivalent_data_handler()
        layer.metadata.data_filename = Path(data_handler.get_filename())
        layer.metadata.data_hash = None
        new_companion_path = work_folder / data_handler.get_filename()
        layer.writeToFile(new_primary_path)

    _verify_matching_contents(
        original_path=work_source_path,
        converted_path=new_primary_path,
        involves_cdf=source_handler.extension == "cdf" or is_cdf_target,
    )

    published: list[Path] = []
    if outputManager is not None:
        destination, _, _ = outputManager.add_file(
            new_primary_path, path_handler=output_handler
        )
        published.append(destination)
        if new_companion_path is not None:
            companion_destination, _, _ = outputManager.add_file(
                new_companion_path,
                path_handler=output_handler.get_equivalent_data_handler(),
            )
            published.append(companion_destination)

        if is_overwrite:
            for old_path in old_datastore_paths:
                if old_path not in published and old_path.exists():
                    _delete_old_datastore_file(old_path, app_settings, save_mode)
    else:
        published = [new_primary_path] + (
            [new_companion_path] if new_companion_path is not None else []
        )

    return published


def _delete_old_datastore_file(
    path: Path, app_settings: AppSettings, save_mode: SaveMode
) -> None:
    """Remove a superseded layer file from the datastore (and soft-delete its DB record)."""
    relative_path = path.relative_to(app_settings.data_store).as_posix()
    logger.info(f"Deleting superseded layer file {relative_path} after conversion.")
    path.unlink()

    if save_mode == SaveMode.LocalAndDatabase and Database.get_environment_url():
        db = Database()
        for file_record in db.get_files_by_path(relative_path):
            if file_record.deletion_date is None:
                file_record.set_deleted()
                db.save(file_record)


def _verify_matching_contents(
    original_path: Path,
    converted_path: Path,
    involves_cdf: bool,
) -> None:
    """Read back the original and converted layer contents and confirm they match
    before the converted files are published and the originals removed.

    CDF stores offsets/timedelta as single-precision (CDF_FLOAT), so a tolerant
    comparison is used whenever CDF is on either side of the conversion; csv/parquet
    conversions must match exactly, since both are lossless for the values involved.
    """
    original = CalibrationLayer.from_file(original_path, load_contents=True)
    converted = CalibrationLayer.from_file(converted_path, load_contents=True)

    original_df = original._contents
    converted_df = converted._contents
    assert original_df is not None
    assert converted_df is not None

    if len(original_df) != len(converted_df):
        raise ValueError(
            f"Conversion verification failed for {original_path.name}: "
            f"original has {len(original_df)} rows, converted has {len(converted_df)}."
        )

    epoch = CONSTANTS.CSV_VARS.EPOCH
    if not pd.Series(
        pd.to_datetime(original_df[epoch]).values
        == pd.to_datetime(converted_df[epoch]).values
    ).all():
        raise ValueError(
            f"Conversion verification failed for {original_path.name}: epoch values differ."
        )

    numeric_cols = [
        CONSTANTS.CSV_VARS.OFFSET_X,
        CONSTANTS.CSV_VARS.OFFSET_Y,
        CONSTANTS.CSV_VARS.OFFSET_Z,
        CONSTANTS.CSV_VARS.TIMEDELTA,
    ]
    for col in numeric_cols:
        if col not in original_df.columns or col not in converted_df.columns:
            continue
        original_values = original_df[col].to_numpy(dtype=float)
        converted_values = converted_df[col].to_numpy(dtype=float)
        if involves_cdf:
            # CDF stores these as single-precision (CDF_FLOAT); allow round-trip error.
            matches = np.allclose(
                original_values.astype(np.float32),
                converted_values.astype(np.float32),
                rtol=1e-6,
                atol=1e-6,
                equal_nan=True,
            )
        else:
            matches = np.array_equal(original_values, converted_values, equal_nan=True)
        if not matches:
            raise ValueError(
                f"Conversion verification failed for {original_path.name}: "
                f"column '{col}' values differ after conversion."
            )

    for col in [CONSTANTS.CSV_VARS.QUALITY_FLAG, CONSTANTS.CSV_VARS.QUALITY_BITMASK]:
        if col not in original_df.columns or col not in converted_df.columns:
            continue
        if not np.array_equal(
            original_df[col].to_numpy(dtype=int), converted_df[col].to_numpy(dtype=int)
        ):
            raise ValueError(
                f"Conversion verification failed for {original_path.name}: "
                f"column '{col}' values differ after conversion."
            )

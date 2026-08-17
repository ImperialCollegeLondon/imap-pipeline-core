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
from mag_toolkit.calibration.CalibrationDefinitions import CONSTANTS, LayerDataFormat

app = typer.Typer()

logger = logging.getLogger(__name__)

_CONVERTIBLE_FORMATS = (FileType.CSV, FileType.PARQUET, FileType.CDF)


# e.g. imap-mag convert --input-layers '*noop*.parquet' --output-layer-data-format csv
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

    e.g. imap-mag convert --input-layers '*noop*.parquet' --output-layer-data-format csv
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

    logger.info(
        f"Converting layers matching {input_layers} to "
        f"{output_layer_data_format.value} ({output_layer_versioning_strategy.value})."
    )

    db = Database() if Database.get_environment_url() else None
    datastore_finder = FileFinder(
        app_settings.data_store,
        work_folder,
        database=db,
    )

    resolved_layers = datastore_finder.find_layers_by_patterns(
        input_layers,
        start_date=start_date,
        end_date=end_date,
        mode=mode,
        throw_if_not_found=True,
    )
    logger.info(f"Found {len(resolved_layers)} layer(s) to convert: {resolved_layers}")

    outputManager = (
        DatastoreFileManager.CreateByMode(
            app_settings, use_database=save_mode == SaveMode.LocalAndDatabase
        )
        if app_settings.convert.publish_to_data_store
        else None
    )
    if outputManager is None:
        logger.info(
            "publish_to_data_store is disabled; converted layers will only be "
            "written to the work folder."
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
                db=db,
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
    db: Database | None,
) -> list[Path]:
    logger.debug(f"Converting layer {layer_name} to {output_layer_data_format.value}.")

    source_handler = CalibrationLayerPathHandler.from_filename(layer_name)
    if source_handler is None:
        raise ValueError(f"Could not parse metadata from layer file: {layer_name}")

    versioned_source_path = datastore_finder.find_by_handler(
        source_handler, throw_if_not_found=True
    )
    work_source_path = fetch_file_for_work(
        versioned_source_path, work_folder, throw_if_not_found=True
    )

    # Load without contents first: a paired layer's companion must be fetched
    # into the work folder before its contents can be read, so the companion's
    # filename needs to come from the metadata before load_contents() below.
    layer = CalibrationLayer.from_file(work_source_path, load_contents=False)

    old_datastore_paths: list[Path] = [versioned_source_path]
    companion_versioned_path = layer.get_datafile_path()
    if companion_versioned_path is not None:
        fetch_file_for_work(
            companion_versioned_path, work_folder, throw_if_not_found=True
        )
        old_datastore_paths.append(companion_versioned_path)

    layer.load_contents()
    source_extension = layer.get_data_file_type().value
    if source_extension == output_layer_data_format.value:
        logger.info(
            f"Layer {layer_name} is already in {output_layer_data_format.value} "
            "format; skipping conversion."
        )
        return []

    is_overwrite = output_layer_versioning_strategy == ConversionStrategy.OVERWRITE
    versioning_mode = (
        VersionedPathHandler.VersionMode.USER_OVERRIDE
        if is_overwrite
        else VersionedPathHandler.VersionMode.MAX_VERSION_PLUS_ONE
    )

    output_is_self_contained = CalibrationLayer.is_self_contained_format(
        output_layer_data_format.value
    )
    output_handler = source_handler.with_new_primary_format(
        extension="cdf" if output_is_self_contained else "json",
        versioning_mode=versioning_mode,
        allow_overwrite=is_overwrite,
    )
    # FileType (csv/parquet/cdf, this CLI's output format option) and
    # LayerDataFormat (csv/parquet, what a companion data file handler needs)
    # share the same string values for csv/parquet.
    companion_format = (
        None
        if output_is_self_contained
        else LayerDataFormat(output_layer_data_format.value)
    )

    layer.version_major = output_handler.version_major
    layer.version = output_handler.version

    new_primary_path = work_folder / output_handler.get_filename()
    companion_filename = layer.prepare_metadata_for_output_format(
        output_handler, companion_format
    )
    new_companion_path = (
        work_folder / companion_filename if companion_filename else None
    )

    logger.debug(
        f"Writing converted layer {layer_name} to {new_primary_path.name}"
        + (f" (+ {new_companion_path.name})" if new_companion_path else "")
    )
    layer.write_to_file(new_primary_path)

    logger.debug(f"Verifying converted contents of {layer_name} match the original.")
    _verify_matching_contents(
        layer_name=layer_name,
        original=layer,
        converted_path=new_primary_path,
        involves_cdf=source_handler.extension == "cdf" or output_is_self_contained,
    )

    published: list[Path] = []
    if outputManager is not None:
        destination, _, _ = outputManager.add_file(
            new_primary_path, path_handler=output_handler
        )
        published.append(destination)
        logger.info(f"Published converted layer to {destination}.")
        if new_companion_path is not None:
            assert companion_format is not None
            companion_destination, _, _ = outputManager.add_file(
                new_companion_path,
                path_handler=output_handler.create_new_datafile_handler(
                    companion_format
                ),
            )
            published.append(companion_destination)
            logger.info(
                f"Published converted companion data to {companion_destination}."
            )

        if is_overwrite:
            for old_path in old_datastore_paths:
                if old_path not in published and old_path.exists():
                    _delete_old_datastore_file(old_path, app_settings, save_mode, db)
    else:
        logger.debug(
            f"Leaving converted layer for {layer_name} in the work folder only."
        )
        published = [new_primary_path] + (
            [new_companion_path] if new_companion_path is not None else []
        )

    return published


def _delete_old_datastore_file(
    path: Path, app_settings: AppSettings, save_mode: SaveMode, db: Database | None
) -> None:
    """Remove a superseded layer file from the datastore (and soft-delete its DB record)."""
    relative_path = path.relative_to(app_settings.data_store).as_posix()
    logger.info(f"Deleting superseded layer file {relative_path} after conversion.")
    path.unlink()

    if save_mode == SaveMode.LocalAndDatabase and db is not None:
        for file_record in db.get_files_by_path(relative_path):
            if file_record.deletion_date is None:
                file_record.set_deleted()
                db.save(file_record)


def _verify_matching_contents(
    layer_name: str,
    original: CalibrationLayer,
    converted_path: Path,
    involves_cdf: bool,
) -> None:
    """Read back the converted layer contents and confirm they match the already
    in-memory original contents, before the converted files are published and
    the originals removed.

    CDF stores offsets/timedelta as single-precision (CDF_FLOAT), so a tolerant
    comparison is used whenever CDF is on either side of the conversion; csv/parquet
    conversions must match exactly, since both are lossless for the values involved.
    """
    converted = CalibrationLayer.from_file(converted_path, load_contents=True)

    original_df = original._contents
    converted_df = converted._contents
    assert original_df is not None
    assert converted_df is not None

    if len(original_df) != len(converted_df):
        raise ValueError(
            f"Conversion verification failed for {layer_name}: "
            f"original has {len(original_df)} rows, converted has {len(converted_df)}."
        )

    epoch = CONSTANTS.CSV_VARS.EPOCH
    if not pd.Series(
        pd.to_datetime(original_df[epoch]).values
        == pd.to_datetime(converted_df[epoch]).values
    ).all():
        raise ValueError(
            f"Conversion verification failed for {layer_name}: epoch values differ."
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
                f"Conversion verification failed for {layer_name}: "
                f"column '{col}' values differ after conversion."
            )

    for col in [CONSTANTS.CSV_VARS.QUALITY_FLAG, CONSTANTS.CSV_VARS.QUALITY_BITMASK]:
        if col not in original_df.columns or col not in converted_df.columns:
            continue
        if not np.array_equal(
            original_df[col].to_numpy(dtype=int), converted_df[col].to_numpy(dtype=int)
        ):
            raise ValueError(
                f"Conversion verification failed for {layer_name}: "
                f"column '{col}' values differ after conversion."
            )

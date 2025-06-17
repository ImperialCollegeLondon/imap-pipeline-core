import logging
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Annotated

import typer

from imap_mag.api.apiUtils import (
    initialiseLoggingForCommand,
    prepareWorkFile,
)
from imap_mag.config import AppSettings
from imap_mag.io import (
    CalibrationLayerMetadataProvider,
    InputManager,
    OutputManager,
    StandardSPDFMetadataProvider,
)
from mag_toolkit.calibration import CalibrationApplicator

logger = logging.getLogger(__name__)


class FileType(Enum):
    CSV = "csv"
    CDF = "cdf"
    JSON = "json"


def prepare_layers_for_application(layers, appSettings):
    """
    Prepare the calibration layers for application by fetching the versioned files.
    """
    inputManager = InputManager(appSettings.data_store)
    workLayers = []
    for layer in layers:
        cal_layer_metadata = CalibrationLayerMetadataProvider.from_filename(layer)
        if not cal_layer_metadata:
            logger.error(f"Could not parse metadata from calibration layer: {layer}")
            raise ValueError(
                f"Could not parse metadata from calibration layer: {layer}"
            )
        versioned_cal_file = inputManager.get_versioned_file(
            metadata_provider=cal_layer_metadata, latest_version=False
        )
        workLayers.append(prepareWorkFile(versioned_cal_file, appSettings.work_folder))
    return workLayers


def prepare_rotation_layer_for_application(rotation, appSettings):
    """
    Prepare the rotation layer for application by fetching the versioned file.
    """
    if rotation:
        inputManager = InputManager(appSettings.data_store)
        rotation_metadata = StandardSPDFMetadataProvider.from_filename(rotation)
        if not rotation_metadata:
            logger.error(f"Could not parse metadata from rotation file: {rotation}")
            raise ValueError(f"Could not parse metadata from rotation file: {rotation}")
        versioned_rotation_file = inputManager.get_versioned_file(
            metadata_provider=rotation_metadata, latest_version=False
        )
        return prepareWorkFile(versioned_rotation_file, appSettings.work_folder)
    return None


# E.g., imap-mag apply --config calibration_application_config.yaml --calibration calibration.json imap_mag_l1a_norm-mago_20250502_v000.cdf
def apply(
    layers: Annotated[
        list[str],
        typer.Option(help="Calibration layers to apply to the input science file"),
    ],
    date: Annotated[
        datetime,
        typer.Option("--from", help="Date of the input file data"),
    ],
    calibration_output_type: Annotated[
        str, typer.Option(help="Output type of the calibration file")
    ] = FileType.CDF.value,
    l2_output_type: Annotated[
        str, typer.Option(help="Output type of the L2 file")
    ] = FileType.CDF.value,
    rotation: Annotated[Path | None, typer.Option()] = None,
    input: str = typer.Argument(help="The file name for the input file"),
):
    """
    Apply calibration rotation and layers to an input science file.

    imap-mag calibration apply --date [date] --rotation [rotation] [layers] [input]
    e.g. imap-mag calibration apply --date --rotation imap_mag_l2-calibration_20251017_v004.cdf imap_mag_noop-layer_20251017_v000.json imap_mag_l1b_norm-mago_20251017_v002.cdf
    """
    app_settings = AppSettings()  # type: ignore
    work_folder = app_settings.setup_work_folder_for_command(app_settings.fetch_science)
    initialiseLoggingForCommand(work_folder)

    original_input_metadata = StandardSPDFMetadataProvider.from_filename(input)  # type: ignore

    if not original_input_metadata:
        logger.error(f"Could not parse metadata from input file: {input}")
        raise ValueError(f"Could not parse metadata from input file: {input}")

    input_manager = InputManager(app_settings.data_store)
    versioned_file = input_manager.get_versioned_file(
        metadata_provider=original_input_metadata, latest_version=False
    )

    workDataFile = prepareWorkFile(versioned_file, app_settings.work_folder)

    if workDataFile is None:
        raise ValueError("Data file does not exist")

    workLayers = prepare_layers_for_application(layers, app_settings)
    workRotationFile = prepare_rotation_layer_for_application(rotation, app_settings)

    l2_metadata_provider = StandardSPDFMetadataProvider(
        level="l2",
        content_date=date,
        descriptor=original_input_metadata.descriptor,
        version=0,
        extension=l2_output_type,
    )
    norm_or_burst = (
        "burst"
        if original_input_metadata.descriptor
        and "burst" in original_input_metadata.descriptor
        else "norm"
    )
    cal_metadata_provider = StandardSPDFMetadataProvider(
        descriptor=f"l2-{norm_or_burst}-offsets",
        content_date=date,
        version=0,
        extension=calibration_output_type,
    )

    workCalFile = app_settings.work_folder / cal_metadata_provider.get_filename()
    workL2File = app_settings.work_folder / l2_metadata_provider.get_filename()

    applier = CalibrationApplicator()
    rotateInfo = f"with rotation from {rotation}" if rotation else ""
    logger.info(f"Applying offsets from {layers} to {input} {rotateInfo}")

    (L2_file, cal_file) = applier.apply(
        workLayers, workRotationFile, workDataFile, workCalFile, workL2File
    )

    outputManager = OutputManager(app_settings.data_store)

    outputManager.add_file(L2_file, l2_metadata_provider)
    outputManager.add_file(cal_file, cal_metadata_provider)


def publish():
    pass

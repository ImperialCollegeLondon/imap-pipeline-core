import logging
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Annotated

import typer

from imap_mag.api.apiUtils import (
    fetch_file_for_work,
    initialiseLoggingForCommand,
)
from imap_mag.config import AppSettings
from imap_mag.io import (
    AncillaryPathHandler,
    CalibrationLayerPathHandler,
    InputManager,
    OutputManager,
    SciencePathHandler,
)
from imap_mag.util import ScienceMode
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
        cal_layer_handler = CalibrationLayerPathHandler.from_filename(layer)
        if not cal_layer_handler:
            logger.error(f"Could not parse metadata from calibration layer: {layer}")
            raise ValueError(
                f"Could not parse metadata from calibration layer: {layer}"
            )
        versioned_cal_file = inputManager.get_versioned_file(
            path_handler=cal_layer_handler, latest_version=False
        )
        workLayers.append(
            fetch_file_for_work(versioned_cal_file, appSettings.work_folder)
        )
    return workLayers


def prepare_rotation_layer_for_application(rotation, appSettings):
    """
    Prepare the rotation layer for application by fetching the versioned file.
    """
    if rotation:
        inputManager = InputManager(appSettings.data_store)
        rotation_handler = AncillaryPathHandler.from_filename(rotation)
        if not rotation_handler:
            logger.error(f"Could not parse metadata from rotation file: {rotation}")
            raise ValueError(f"Could not parse metadata from rotation file: {rotation}")
        versioned_rotation_file = inputManager.get_versioned_file(
            path_handler=rotation_handler, latest_version=False
        )
        return fetch_file_for_work(versioned_rotation_file, appSettings.work_folder)
    return None


# E.g., imap-mag apply --calibration calibration.json imap_mag_l1a_norm-mago_20250502_v000.cdf
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
    initialiseLoggingForCommand(
        work_folder
    )  # DO NOT log anything before this point (it won't be captured in the log file)

    original_input_handler = SciencePathHandler.from_filename(input)  # type: ignore

    if not original_input_handler:
        logger.error(f"Could not parse metadata from input file: {input}")
        raise ValueError(f"Could not parse metadata from input file: {input}")

    input_manager = InputManager(app_settings.data_store)
    versioned_file = input_manager.get_versioned_file(
        path_handler=original_input_handler, latest_version=False
    )

    workDataFile = fetch_file_for_work(
        versioned_file, app_settings.work_folder, throw_if_not_found=True
    )

    workLayers = prepare_layers_for_application(layers, app_settings)
    workRotationFile = prepare_rotation_layer_for_application(rotation, app_settings)

    l2_path_handler = SciencePathHandler(
        level="l2-pre",
        content_date=date,
        descriptor=original_input_handler.descriptor,
        version=0,
        extension=l2_output_type,
    )
    norm_or_burst = (
        ScienceMode.Burst.short_name
        if original_input_handler.descriptor
        and ScienceMode.Burst.short_name in original_input_handler.descriptor
        else ScienceMode.Normal.short_name
    )
    cal_path_handler = AncillaryPathHandler(
        descriptor=f"l2-{norm_or_burst}-offsets",
        start_date=date,
        end_date=date,
        version=0,
        extension=calibration_output_type,
    )

    workCalFile = app_settings.work_folder / cal_path_handler.get_filename()
    workL2File = app_settings.work_folder / l2_path_handler.get_filename()

    applier = CalibrationApplicator()
    rotateInfo = f"with rotation from {rotation}" if rotation else ""
    logger.info(f"Applying offsets from {layers} to {input} {rotateInfo}")

    (L2_file, cal_file) = applier.apply(
        workLayers, workRotationFile, workDataFile, workCalFile, workL2File
    )

    outputManager = OutputManager(app_settings.data_store)

    outputManager.add_file(L2_file, l2_path_handler)
    outputManager.add_file(cal_file, cal_path_handler)


def publish():
    pass

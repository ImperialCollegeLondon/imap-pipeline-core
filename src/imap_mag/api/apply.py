import logging
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Annotated

import typer

from imap_mag import appConfig
from imap_mag.api.apiUtils import commandInit, prepareCalibrationFile, prepareWorkFile
from imap_mag.outputManager import OutputManager, StandardSPDFMetadataProvider
from mag_toolkit.calibration import CalibrationApplicator

logger = logging.getLogger(__name__)


class FileType(Enum):
    CSV = "csv"
    CDF = "cdf"
    JSON = "json"


# E.g., imap-mag apply --config calibration_application_config.yaml --calibration calibration.json imap_mag_l1a_norm-mago_20250502_v000.cdf
def apply(
    layers: Annotated[
        list[str],
        typer.Option(help="Calibration layers to apply to the input science file"),
    ],
    from_date: Annotated[
        datetime,
        typer.Option("--from", help="Date to apply calibration parameters from"),
    ],
    to_date: Annotated[
        datetime, typer.Option("--to", help="Date to apply calibration parameters to")
    ],
    config: Annotated[Path, typer.Option()] = Path(
        "calibration_application_config.yaml"
    ),
    calibration_output_type: Annotated[
        FileType, typer.Option(help="Output type of the calibration file")
    ] = FileType.CDF,
    l2_output_type: Annotated[
        FileType, typer.Option(help="Output type of the L2 file")
    ] = FileType.CDF,
    rotation: Annotated[Path | None, typer.Option()] = None,
    input: str = typer.Argument(
        help="The file name or pattern to match for the input file"
    ),
):
    """
    Apply calibration rotation and layers to an input science file.

    imap-mag calibration apply --from [date] --to [date] --rotation [rotation] [layers] [input]
    e.g. imap-mag calibration apply --from 2025-10-17 --to 2025-10-17 --rotation imap_mag_l2-calibration-matrices_20251017_v004.cdf 17-10-2025_17-10-2025_noop_v000.json imap_mag_l1b_norm-mago_20251017_v002.cdf
    """
    configFile: appConfig.AppConfig = commandInit(config)

    workDataFile = prepareWorkFile(input, configFile)

    if workDataFile is None:
        raise ValueError("Data file does not exist")

    workLayers = []
    for layer in layers:
        workLayers.append(prepareCalibrationFile(layer, configFile))

    workRotationFile = (
        prepareCalibrationFile(str(rotation), configFile) if rotation else None
    )

    workCalFile = (
        configFile.work_folder / f"calibration.{calibration_output_type.value}"
    )

    workL2File = configFile.work_folder / f"L2.{l2_output_type.value}"

    applier = CalibrationApplicator()

    rotateInfo = f"with rotation from {rotation}" if rotation else ""
    logger.info(f"Applying offsets from {layers} to {input} {rotateInfo}")

    (L2_file, cal_file) = applier.apply(
        workLayers, workRotationFile, workDataFile, workCalFile, workL2File
    )

    l2_metadata_provider = StandardSPDFMetadataProvider(
        level="l2",
        date=from_date,
        descriptor="norm-mago",
        version=0,
        extension=l2_output_type.value,
    )
    cal_metadata_provider = StandardSPDFMetadataProvider(
        level="l2",
        descriptor="norm-offsets",
        date=from_date,
        version=0,
        extension=calibration_output_type.value,
    )

    outputManager = OutputManager(configFile.destination.folder)

    logger.info(f"Writing offsets file to {cal_file}")
    logger.info(f"Writing L2 file to {L2_file}")
    outputManager.add_file(L2_file, l2_metadata_provider)
    outputManager.add_file(cal_file, cal_metadata_provider)


def publish():
    pass

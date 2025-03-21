from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer

from imap_mag import appConfig
from imap_mag.api.apiUtils import commandInit, prepareCalibrationFile, prepareWorkFile
from imap_mag.outputManager import OutputManager, StandardSPDFMetadataProvider
from mag_toolkit.calibration.CalibrationApplicator import CalibrationApplicator


# E.g., imap-mag apply --config calibration_application_config.yaml --calibration calibration.json imap_mag_l1a_norm-mago_20250502_v000.cdf
def apply(
    layers: list[str],
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
    input: str = typer.Argument(
        help="The file name or pattern to match for the input file"
    ),
):
    configFile: appConfig.AppConfig = commandInit(config)

    workDataFile = prepareWorkFile(input, configFile)

    if workDataFile is None:
        raise Exception("Data file does not exist")

    workLayers = []
    for layer in layers:
        workLayers.append(prepareCalibrationFile(layer, configFile))

    workCalFile = configFile.work_folder / "calibration.cdf"

    workL2File = configFile.work_folder / "L2.json"

    applier = CalibrationApplicator()

    (L2_file, cal_file) = applier.apply(
        workLayers, workDataFile, workCalFile, workL2File
    )

    l2_metadata_provider = StandardSPDFMetadataProvider(
        level="l2", date=from_date, descriptor="norm", version=1, extension="json"
    )
    cal_metadata_provider = StandardSPDFMetadataProvider(
        level="l2", descriptor="offsets", date=from_date, version=1, extension="cdf"
    )

    outputManager = OutputManager(configFile.destination.folder)

    outputManager.add_file(L2_file, l2_metadata_provider)
    outputManager.add_file(cal_file, cal_metadata_provider)


def publish():
    pass

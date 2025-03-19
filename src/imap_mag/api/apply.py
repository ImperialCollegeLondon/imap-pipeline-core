from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer

from imap_mag import appConfig, appUtils
from imap_mag.api.apiUtils import commandInit, prepareCalibrationFile, prepareWorkFile
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

    appUtils.copyFileToDestination(L2_file, configFile.destination)

    appUtils.copyFileToDestination(
        cal_file,
        appConfig.Destination(
            folder=configFile.destination.folder, filename="calibration.cdf"
        ),
    )


def publish():
    pass

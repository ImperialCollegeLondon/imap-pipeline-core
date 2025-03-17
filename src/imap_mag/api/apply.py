from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer

from imap_mag import appConfig, appUtils
from imap_mag.api.apiUtils import commandInit, prepareWorkFile
from mag_toolkit.calibration.CalibrationApplicator import CalibrationApplicator


# E.g., imap-mag apply --config calibration_application_config.yaml --calibration calibration.json imap_mag_l1a_norm-mago_20250502_v000.cdf
def apply(
    layers: list[Path],
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
        workLayers.append(prepareWorkFile(layer, configFile))

    workOutputFile = configFile.work_folder / "summed-layer.json"

    applier = CalibrationApplicator()

    L2_file = applier.apply(workLayers, workDataFile, workOutputFile)

    appUtils.copyFileToDestination(L2_file, configFile.destination)


def publish():
    pass

import os
from pathlib import Path
from typing import Annotated

import typer

from imap_mag import appConfig, appUtils
from imap_mag.api.apiUtils import commandInit, prepareWorkFile
from mag_toolkit.calibration.CalibrationApplicator import CalibrationApplicator


# E.g., imap-mag apply --config calibration_application_config.yaml --calibration calibration.json imap_mag_l1a_norm-mago_20250502_v000.cdf
def apply(
    config: Annotated[Path, typer.Option()] = Path(
        "calibration_application_config.yaml"
    ),
    calibration: Annotated[str, typer.Option()] = "calibration.json",
    input: str = typer.Argument(
        help="The file name or pattern to match for the input file"
    ),
):
    configFile: appConfig.CommandConfigBase = commandInit(config)

    workDataFile = prepareWorkFile(input, configFile)
    workCalibrationFile = prepareWorkFile(calibration, configFile)
    workOutputFile = os.path.join(configFile.work_folder, "l2_data.cdf")

    applier = CalibrationApplicator()

    L2_file = applier.apply(workCalibrationFile, workDataFile, workOutputFile)

    appUtils.copyFileToDestination(Path(L2_file), configFile.destination)

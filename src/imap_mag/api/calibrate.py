import logging
from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer

from imap_mag.api import apply
from imap_mag.api.apiUtils import initialiseLoggingForCommand, prepareWorkFile
from imap_mag.config import AppSettings
from imap_mag.io import (
    CalibrationLayerMetadataProvider,
    InputManager,
    OutputManager,
    StandardSPDFMetadataProvider,
)
from imap_mag.util import Level, ScienceMode
from mag_toolkit.calibration import (
    CalibrationMethod,
    EmptyCalibrator,
    ScienceLayer,
    Sensor,
)

app = typer.Typer()

logger = logging.getLogger(__name__)

app.command()(apply.apply)


# TODO: ?
def interpolate():
    pass


def publish():
    pass


# E.g., imap-mag calibrate --method SpinAxisCalibrator imap_mag_l1b_norm-mago_20250502_v000.cdf
def calibrate(
    date: Annotated[datetime, typer.Option("--date", help="Date to calibrate")],
    method: Annotated[
        CalibrationMethod, typer.Option(help="Calibration method")
    ] = CalibrationMethod.KEPKO,
    mode: Annotated[
        ScienceMode, typer.Option(help="Science mode")
    ] = ScienceMode.Normal,
    sensor: Annotated[
        Sensor, typer.Option(help="Sensor to calibrate, e.g., mago")
    ] = Sensor.MAGO,
):
    """
    Generate calibration parameters for a given input file.
    imap-mag calibrate --from [date] --to [date] --method [method] [input]

    e.g. imap-mag calibrate --date 2025-10-17 --mode norm --sensor mago --method noop imap_mag_l1b_norm-mago_20251017_v002.cdf

    """
    app_settings = AppSettings()  # type: ignore
    work_folder = app_settings.setup_work_folder_for_command(app_settings.fetch_science)
    initialiseLoggingForCommand(
        work_folder
    )  # DO NOT log anything before this point (it won't be captured in the log file)

    # TODO: Input manager for getting data of a given level?

    level = Level.level_1b if mode == ScienceMode.Burst else Level.level_1c
    metadata_provider = StandardSPDFMetadataProvider(
        level=level.value,
        content_date=date,
        descriptor=f"{mode.short_name}-{sensor.value.lower()}",
        extension="cdf",
    )

    input_manager = InputManager(app_settings.data_store)
    input_file = input_manager.get_versioned_file(metadata_provider)

    if not input_file:
        logging.critical(
            "Unable to find a file to process matching %s",
            metadata_provider.get_filename(),
        )
        raise FileNotFoundError(
            f"Unable to find a file to process matching {metadata_provider.get_filename()}"
        )

    workFile = prepareWorkFile(
        input_file, app_settings.work_folder, throw_if_not_found=True
    )

    scienceLayer = ScienceLayer.from_file(workFile)
    scienceLayerMetadata = CalibrationLayerMetadataProvider(
        calibration_descriptor="science", content_date=date
    )
    scienceLayerPath = app_settings.work_folder / scienceLayerMetadata.get_filename()
    scienceLayer.writeToFile(scienceLayerPath)

    match method:
        case CalibrationMethod.NOOP:
            calibrator = EmptyCalibrator()
        case _:
            raise ValueError("Calibration method is not implemented")

    calibrationLayerMetadata = CalibrationLayerMetadataProvider(
        calibration_descriptor=method.value, content_date=date
    )
    result: Path = calibrator.runCalibration(
        date,
        scienceLayerPath,
        Path(calibrationLayerMetadata.get_filename()),
        app_settings.data_store,
        None,
    )

    outputManager = OutputManager(app_settings.data_store)
    (output_calibration_path, _) = outputManager.add_file(
        result, metadata_provider=calibrationLayerMetadata
    )  # type: ignore

    return (output_calibration_path, input_file)

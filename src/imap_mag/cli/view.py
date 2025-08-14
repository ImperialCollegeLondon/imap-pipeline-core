import logging
from datetime import datetime
from typing import Annotated

import typer

from imap_mag.cli.cliUtils import (
    fetch_file_for_work,
    initialiseLoggingForCommand,
)
from imap_mag.config import AppSettings
from imap_mag.io import DatastoreFileFinder
from imap_mag.io.file import SciencePathHandler
from imap_mag.util import ScienceLevel, ScienceMode
from mag_toolkit.calibration import Sensor

logger = logging.getLogger(__name__)


def view(
    date: Annotated[datetime, typer.Option("--date", help="Date to calibrate")],
    mode: Annotated[
        ScienceMode, typer.Option(help="Science mode")
    ] = ScienceMode.Normal,
    sensor: Annotated[
        Sensor, typer.Option(help="Sensor to calibrate, e.g., mago")
    ] = Sensor.MAGO,
):
    app_settings = AppSettings()  # type: ignore
    work_folder = app_settings.setup_work_folder_for_command(app_settings.publish)

    initialiseLoggingForCommand(
        work_folder
    )  # DO NOT log anything before this point (it won't be captured in the log file)

    data_store = DatastoreFileFinder(app_settings.data_store)
    file_handler = SciencePathHandler(
        content_date=date,
        level=ScienceLevel.l1c,
        descriptor=f"{mode.value}-{sensor.value}",
    )
    latest_version = data_store.find_latest_version(file_handler)
    work_filepath = fetch_file_for_work(
        latest_version, app_settings.work_folder, throw_if_not_found=True
    )
    logging.info("Plotting file: %s", work_filepath)

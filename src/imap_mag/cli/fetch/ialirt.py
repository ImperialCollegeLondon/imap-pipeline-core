import logging
from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer

from imap_mag import appUtils
from imap_mag.cli.cliUtils import initialiseLoggingForCommand
from imap_mag.client.IALiRTApiClient import IALiRTApiClient
from imap_mag.config import AppSettings, FetchMode
from imap_mag.download.FetchIALiRT import FetchIALiRT
from imap_mag.io import DatastoreFileFinder
from imap_mag.io.file import IALiRTPathHandler

logger = logging.getLogger(__name__)


# E.g.,
# imap-mag fetch ialirt --start-date 2025-01-02 --end-date 2025-01-03
def fetch_ialirt(
    start_date: Annotated[datetime, typer.Option(help="Start date for the download")],
    end_date: Annotated[datetime, typer.Option(help="End date for the download")],
    fetch_mode: Annotated[
        FetchMode,
        typer.Option(
            case_sensitive=False,
            help="Whether to download only or download and update progress in database",
        ),
    ] = FetchMode.DownloadOnly,
) -> dict[Path, IALiRTPathHandler]:
    """Download binary data from I-ALiRT."""

    app_settings = AppSettings()  # type: ignore
    work_folder = app_settings.setup_work_folder_for_command(app_settings.fetch_ialirt)

    initialiseLoggingForCommand(
        work_folder
    )  # DO NOT log anything before this point (it won't be captured in the log file)

    logger.info(f"Downloading I-ALiRT from {start_date} to {end_date}.")

    data_access = IALiRTApiClient(
        app_settings.fetch_ialirt.api.auth_code,
        app_settings.fetch_ialirt.api.url_base,
    )
    datastore_finder = DatastoreFileFinder(app_settings.data_store)

    fetch_ialirt = FetchIALiRT(
        data_access, work_folder, datastore_finder, app_settings.packet_definition
    )
    downloaded_ialirt: dict[Path, IALiRTPathHandler] = (
        fetch_ialirt.download_ialirt_to_csv(
            start_date=start_date,
            end_date=end_date,
        )
    )

    if not downloaded_ialirt:
        logger.info(f"No I-ALiRT data downloaded from {start_date} to {end_date}.")
    else:
        logger.debug(
            f"Downloaded {len(downloaded_ialirt)} files:\n{', '.join(str(f) for f in downloaded_ialirt.keys())}"
        )

    ialirt_files_and_handlers: dict[Path, IALiRTPathHandler] = dict()

    if app_settings.fetch_ialirt.publish_to_data_store:
        output_manager = appUtils.getOutputManagerByMode(
            app_settings,
            use_database=(fetch_mode == FetchMode.DownloadAndUpdateProgress),
        )

        for file, path_handler in downloaded_ialirt.items():
            (output_file, output_handler) = output_manager.add_file(file, path_handler)
            ialirt_files_and_handlers[output_file] = output_handler
    else:
        ialirt_files_and_handlers = downloaded_ialirt
        logger.info("Files not published to data store based on config.")

    return ialirt_files_and_handlers

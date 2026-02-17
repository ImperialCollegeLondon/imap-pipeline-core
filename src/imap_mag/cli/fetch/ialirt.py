import logging
from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer

from imap_mag.cli.cliUtils import initialiseLoggingForCommand
from imap_mag.client.IALiRTApiClient import IALiRTApiClient
from imap_mag.config import AppSettings, FetchMode
from imap_mag.download.FetchIALiRT import FetchIALiRT
from imap_mag.io import DatastoreFileFinder, DatastoreFileManager
from imap_mag.io.file import IALiRTHKPathHandler, IALiRTPathHandler
from imap_mag.io.file.IFilePathHandler import IFilePathHandler

logger = logging.getLogger(__name__)


def _create_fetch_ialirt(app_settings: AppSettings) -> FetchIALiRT:
    """Create a FetchIALiRT instance with common configuration."""

    data_access = IALiRTApiClient(
        app_settings.fetch_ialirt.api.auth_code,
        app_settings.fetch_ialirt.api.url_base,
    )
    datastore_finder = DatastoreFileFinder(app_settings.data_store)
    work_folder = app_settings.setup_work_folder_for_command(app_settings.fetch_ialirt)

    initialiseLoggingForCommand(
        work_folder
    )  # DO NOT log anything before this point (it won't be captured in the log file)

    return FetchIALiRT(
        data_access, work_folder, datastore_finder, app_settings.packet_definition
    )


def _publish_files(
    app_settings: AppSettings,
    downloaded_files: dict[Path, IFilePathHandler],
    fetch_mode: FetchMode,
) -> dict[Path, IFilePathHandler]:
    """Publish downloaded files to data store."""

    if not app_settings.fetch_ialirt.publish_to_data_store:
        logger.info("Files not published to data store based on config.")
        return downloaded_files

    datastore_manager = DatastoreFileManager.CreateByMode(
        app_settings,
        use_database=(fetch_mode == FetchMode.DownloadAndUpdateProgress),
    )

    result: dict[Path, IFilePathHandler] = dict()
    for file, path_handler in downloaded_files.items():
        (output_file, output_handler) = datastore_manager.add_file(file, path_handler)
        result[output_file] = output_handler

    return result


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
    """Download I-ALiRT MAG data from SDC."""

    app_settings = AppSettings()  # type: ignore

    fetch = _create_fetch_ialirt(app_settings)

    logger.info(f"Downloading I-ALiRT MAG from {start_date} to {end_date}.")

    downloaded_ialirt: dict[Path, IALiRTPathHandler] = fetch.download_mag_to_csv(
        start_date=start_date,
        end_date=end_date,
    )

    if not downloaded_ialirt:
        logger.info(f"No I-ALiRT MAG data downloaded from {start_date} to {end_date}.")
    else:
        logger.debug(
            f"Downloaded {len(downloaded_ialirt)} files:\n{', '.join(str(f) for f in downloaded_ialirt.keys())}"
        )

    return _publish_files(app_settings, downloaded_ialirt, fetch_mode)


# E.g.,
# imap-mag fetch ialirt-hk --start-date 2025-01-02 --end-date 2025-01-03
def fetch_ialirt_hk(
    start_date: Annotated[datetime, typer.Option(help="Start date for the download")],
    end_date: Annotated[datetime, typer.Option(help="End date for the download")],
    fetch_mode: Annotated[
        FetchMode,
        typer.Option(
            case_sensitive=False,
            help="Whether to download only or download and update progress in database",
        ),
    ] = FetchMode.DownloadOnly,
) -> dict[Path, IALiRTHKPathHandler]:
    """Download I-ALiRT MAG HK data from SDC."""

    app_settings = AppSettings()  # type: ignore

    fetch = _create_fetch_ialirt(app_settings)

    logger.info(f"Downloading I-ALiRT MAG HK from {start_date} to {end_date}.")

    downloaded_hk: dict[Path, IALiRTHKPathHandler] = fetch.download_mag_hk_to_csv(
        start_date=start_date,
        end_date=end_date,
    )

    if not downloaded_hk:
        logger.info(
            f"No I-ALiRT MAG HK data downloaded from {start_date} to {end_date}."
        )
    else:
        logger.debug(
            f"Downloaded {len(downloaded_hk)} files:\n{', '.join(str(f) for f in downloaded_hk.keys())}"
        )

    return _publish_files(app_settings, downloaded_hk, fetch_mode)

import logging
from pathlib import Path
from typing import Annotated, Literal

import typer

from imap_mag.cli.cliUtils import initialiseLoggingForCommand
from imap_mag.client.NOAAApiClient import NOAARTSWApiClient
from imap_mag.config import AppSettings, FetchMode
from imap_mag.download.FetchNOAA import FetchNOAA
from imap_mag.io import DatastoreFileManager, FileFinder
from imap_mag.io.file.IFilePathHandler import IFilePathHandler

logger = logging.getLogger(__name__)


def _create_fetch_noaa(app_settings: AppSettings) -> FetchNOAA:
    """Create a FetchNOAA instance with common configuration."""

    data_access = NOAARTSWApiClient(
        app_settings.fetch_solar1_ace.api.url_base,
    )
    datastore_finder = FileFinder(app_settings.data_store)
    work_folder = app_settings.setup_work_folder_for_command(
        app_settings.fetch_solar1_ace
    )

    initialiseLoggingForCommand(
        work_folder
    )  # DO NOT log anything before this point (it won't be captured in the log file)

    return FetchNOAA(data_access, work_folder, datastore_finder)


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
# imap-mag fetch noaa --spacecraft SOLAR1 --instrument plasma
def fetch_noaa(
    spacecraft: Annotated[
        Literal["SOLAR1", "ACE"],
        typer.Option(
            help="Spacecraft to download data for. Must be 'SOLAR1' or 'ACE'",
        ),
    ],
    instrument: Annotated[
        Literal["mag", "plasma"],
        typer.Option(
            help="Instrument data to download. Must be 'mag' or 'palsma'",
        ),
    ],
    fetch_mode: Annotated[
        FetchMode,
        typer.Option(
            help="Whether to download only or download and update progress in database",
        ),
    ] = FetchMode.DownloadOnly,
) -> dict[Path, IFilePathHandler]:
    """Download SOLAR1 and ACE data from NOAA."""

    app_settings = AppSettings()  # type: ignore

    fetch = _create_fetch_noaa(app_settings)

    downloaded: dict[Path, IFilePathHandler] = fetch.download_csv(
        spacecraft=spacecraft, instrument=instrument
    )

    if not downloaded:
        logger.info(f"No '{instrument}' data downloaded for {spacecraft}.")
    else:
        logger.debug(
            f"Downloaded {len(downloaded)} files:\n{', '.join(str(f) for f in downloaded.keys())}"
        )

    return _publish_files(app_settings, downloaded, fetch_mode)

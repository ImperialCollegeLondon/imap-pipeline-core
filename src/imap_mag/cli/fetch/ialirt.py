import logging
from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer

from imap_mag.cli.cliUtils import initialiseLoggingForCommand
from imap_mag.client.IALiRTApiClient import IALiRTApiClient
from imap_mag.config import AppSettings, FetchMode
from imap_mag.download.FetchIALiRT import FetchIALiRT
from imap_mag.io import DatastoreFileManager, FileFinder
from imap_mag.io.file.IALiRTPathHandler import IALiRTPathHandler
from imap_mag.util.constants import (
    CONSTANTS,
    VALID_IALIRT_HK_INSTRUMENTS,
    VALID_IALIRT_INSTRUMENTS,
)

logger = logging.getLogger(__name__)


def _create_fetch_ialirt(app_settings: AppSettings) -> FetchIALiRT:
    """Create a FetchIALiRT instance with common configuration."""

    instrument_settings = app_settings.fetch_ialirt

    data_access = IALiRTApiClient(
        instrument_settings.api.auth_code,
        instrument_settings.api.url_base,
    )

    datastore_finder = FileFinder(app_settings.data_store)
    work_folder = app_settings.setup_work_folder_for_command(instrument_settings)

    initialiseLoggingForCommand(
        work_folder
    )  # DO NOT log anything before this point (it won't be captured in the log file)

    return FetchIALiRT(
        data_access, work_folder, datastore_finder, app_settings.packet_definition
    )


def _publish_files(
    app_settings: AppSettings,
    downloaded_files: dict[Path, IALiRTPathHandler],
    fetch_mode: FetchMode,
    instrument: str = "CONSTANTS.IALIRT_INSTRUMENTS.IALIRT_MAG",
) -> dict[Path, IALiRTPathHandler]:
    """Publish downloaded files to data store."""

    instrument_settings = getattr(
        app_settings,
        "fetch_ialirt_hk" if instrument.endswith("_hk") else "fetch_ialirt",
    )

    if not instrument_settings.publish_to_data_store:
        logger.info(
            f"Files for {instrument.upper()} not published to data store based on config."
        )
        return downloaded_files

    datastore_manager = DatastoreFileManager.CreateByMode(
        app_settings,
        use_database=(fetch_mode == FetchMode.DownloadAndUpdateProgress),
    )

    result: dict[Path, IALiRTPathHandler] = dict()
    for file, path_handler in downloaded_files.items():
        (output_file, output_handler) = datastore_manager.add_file(file, path_handler)
        result[output_file] = output_handler

    return result


# E.g.,
# imap-mag fetch ialirt --instrument mag --start-date 2025-01-02 --end-date 2025-01-03
def fetch_ialirt(
    start_date: Annotated[datetime, typer.Option(help="Start date for the download")],
    end_date: Annotated[datetime, typer.Option(help="End date for the download")],
    instrument: Annotated[
        str,
        typer.Option(help="Instrument to download data for (e.g., 'mag')"),
    ] = CONSTANTS.IALIRT_INSTRUMENTS.IALIRT_MAG,
    fetch_mode: Annotated[
        FetchMode,
        typer.Option(
            case_sensitive=False,
            help="Whether to download only or download and update progress in database",
        ),
    ] = FetchMode.DownloadOnly,
) -> dict[Path, IALiRTPathHandler]:  # type: ignore
    """Download I-ALiRT data from SDC for a specific instrument."""

    if instrument.lower() not in [v.lower() for v in VALID_IALIRT_INSTRUMENTS]:
        raise typer.BadParameter(
            f"'{instrument}' is not a valid instrument. Choose from: {', '.join(VALID_IALIRT_INSTRUMENTS)}"
        )

    app_settings = AppSettings()  # type: ignore

    fetch = _create_fetch_ialirt(app_settings)

    downloaded_ialirt: dict[Path, IALiRTPathHandler] = fetch.download_instrument_data(
        instrument=instrument,
        start_date=start_date,
        end_date=end_date,
    )  # type: ignore

    if not downloaded_ialirt:
        logger.info(
            f"No I-ALiRT {instrument.upper()} data downloaded from {start_date} to {end_date}."
        )
    else:
        logger.debug(
            f"Downloaded {len(downloaded_ialirt)} {instrument.upper()} files from {start_date} to {end_date}:\n{', '.join(str(f) for f in downloaded_ialirt.keys())}"
        )

    return _publish_files(app_settings, downloaded_ialirt, fetch_mode, instrument)


# E.g.,
# imap-mag fetch ialirt --instrument mag_hk --start-date 2025-01-02 --end-date 2025-01-03
def fetch_ialirt_hk(
    start_date: Annotated[datetime, typer.Option(help="Start date for the download")],
    end_date: Annotated[datetime, typer.Option(help="End date for the download")],
    instrument: Annotated[
        str,
        typer.Option(help="Instrument to download HK data for (e.g., 'mag')"),
    ] = CONSTANTS.IALIRT_INSTRUMENTS.IALIRT_MAG_HK,
    fetch_mode: Annotated[
        FetchMode,
        typer.Option(
            case_sensitive=False,
            help="Whether to download only or download and update progress in database",
        ),
    ] = FetchMode.DownloadOnly,
) -> dict[Path, IALiRTPathHandler]:
    """Download I-ALiRT MAG HK data from SDC."""

    if instrument.lower() not in [v.lower() for v in VALID_IALIRT_HK_INSTRUMENTS]:
        raise typer.BadParameter(
            f"'{instrument}' is not a valid instrument. Choose from: {', '.join(VALID_IALIRT_HK_INSTRUMENTS)}"
        )

    app_settings = AppSettings()  # type: ignore

    fetch = _create_fetch_ialirt(app_settings)

    logger.info(f"Downloading I-ALiRT MAG HK from {start_date} to {end_date}.")

    downloaded_hk: dict[Path, IALiRTPathHandler] = fetch.download_instrument_data(
        instrument=instrument,
        start_date=start_date,
        end_date=end_date,
        housekeeping=True,
    )  # type: ignore

    if not downloaded_hk:
        logger.info(f"No I-ALiRT HK data downloaded from {start_date} to {end_date}.")
    else:
        logger.debug(
            f"Downloaded {len(downloaded_hk)} {instrument.upper()} HK files from {start_date} to {end_date}:\n{', '.join(str(f) for f in downloaded_hk.keys())}"
        )

    return _publish_files(app_settings, downloaded_hk, fetch_mode)

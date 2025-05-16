import logging
from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer

from imap_mag import appUtils
from imap_mag.api.apiUtils import initialiseLoggingForCommand
from imap_mag.cli.fetchScience import (
    FetchScience,
    SDCMetadataProvider,
)
from imap_mag.client.sdcDataAccess import SDCDataAccess
from imap_mag.config.AppSettings import AppSettings
from imap_mag.config.FetchMode import FetchMode
from imap_mag.util import Level, MAGSensor, ScienceMode

logger = logging.getLogger(__name__)


# E.g., imap-mag fetch science --start-date 2025-05-02 --end-date 2025-05-03
# E.g., imap-mag fetch science --ingestion-date --start-date 2025-05-02 --end-date 2025-05-03
def fetch_science(
    auth_code: Annotated[
        str,
        typer.Option(
            envvar="SDC_AUTH_CODE",
            help="IMAP Science Data Centre API Key",
        ),
    ],
    start_date: Annotated[datetime, typer.Option(help="Start date for the download")],
    end_date: Annotated[datetime, typer.Option(help="End date for the download")],
    use_ingestion_date: Annotated[
        bool,
        typer.Option(
            "--ingestion-date",
            help="Use ingestion date into SDC database, rather than science measurement date",
        ),
    ] = False,
    level: Annotated[Level, typer.Option(help="Level to download")] = Level.level_2,
    modes: Annotated[
        list[ScienceMode],
        typer.Option(
            help="Science modes to download",
        ),
    ] = [
        "norm",  # type: ignore
        "burst",  # type: ignore
    ],  # for some reason Typer does not like these being enums
    sensors: Annotated[list[MAGSensor], typer.Option(help="Sensors to download")] = [
        MAGSensor.IBS,
        MAGSensor.OBS,
    ],
    fetch_mode: Annotated[
        FetchMode, typer.Option("--mode", case_sensitive=False)
    ] = FetchMode.DownloadOnly,
) -> dict[Path, SDCMetadataProvider]:
    """Download science data from the SDC."""

    if not auth_code:
        logger.critical("No SDC_AUTH_CODE API key provided")
        raise ValueError("No SDC_AUTH_CODE API key provided")

    settings_overrides = (
        {"fetch_science": {"api": {"auth_code": auth_code}}} if auth_code else {}
    )

    app_settings = AppSettings(**settings_overrides)
    work_folder = app_settings.setup_work_folder_for_command(app_settings.fetch_science)
    initialiseLoggingForCommand(work_folder)

    data_access = SDCDataAccess(
        data_dir=work_folder,
        sdc_url=app_settings.fetch_science.api.url_base,
    )

    fetch_science = FetchScience(data_access, modes=modes, sensors=sensors)
    downloaded_science: dict[Path, SDCMetadataProvider] = (
        fetch_science.download_latest_science(
            level=level.value,
            start_date=start_date,
            end_date=end_date,
            use_ingestion_date=use_ingestion_date,
        )
    )

    if not downloaded_science:
        logger.info(
            f"No data downloaded for packet {level.value} from {start_date} to {end_date}."
        )

    output_manager = appUtils.getOutputManagerByMode(
        app_settings.data_store, mode=fetch_mode
    )
    output_science: dict[Path, SDCMetadataProvider] = dict()

    for file, metadata_provider in downloaded_science.items():
        (output_file, output_metadata) = output_manager.add_file(
            file, metadata_provider
        )
        output_science[output_file] = output_metadata

    return output_science

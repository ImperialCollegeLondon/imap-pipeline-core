import logging
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Annotated

import typer

from imap_mag import appConfig, appUtils
from imap_mag.api.apiUtils import commandInit
from imap_mag.cli.fetchScience import FetchScience
from imap_mag.client.sdcDataAccess import SDCDataAccess
from imap_mag.outputManager import StandardSPDFMetadataProvider


class Level(str, Enum):
    level_1a = "l1a"
    level_1b = "l1b"
    level_1c = "l1c"
    level_2 = "l2"


# E.g., imap-mag fetch-science --start-date 2025-05-02 --end-date 2025-05-03
def fetch_science(
    auth_code: Annotated[
        str,
        typer.Option(
            envvar="SDC_AUTH_CODE",
            help="IMAP Science Data Centre API Key",
        ),
    ],
    start_date: Annotated[str, typer.Option(help="Start date for the download")],
    end_date: Annotated[str, typer.Option(help="End date for the download")],
    level: Annotated[Level, typer.Option(help="Level to download")] = Level.level_2,
    config: Annotated[Path, typer.Option()] = Path("config.yaml"),
):
    """DLevelcience data from the SDC."""

    configFile: appConfig.AppConfig = commandInit(config)

    if not auth_code:
        logging.critical("No SDC_AUTH_CODE API key provided")
        raise typer.Abort()

    start_datetime: datetime = appUtils.convertToDatetime(start_date)
    end_datetime: datetime = appUtils.convertToDatetime(end_date)

    logging.info(
        f"Downloading {level} science from {start_datetime} to {end_datetime}."
    )

    data_access = SDCDataAccess(
        data_dir=configFile.work_folder,
        sdc_url=configFile.api.sdc_url if configFile.api else None,
    )

    fetch_science = FetchScience(data_access)
    downloaded_science: dict[Path, StandardSPDFMetadataProvider] = (
        fetch_science.download_latest_science(
            level=level.value, start_date=start_datetime, end_date=end_datetime
        )
    )

    output_manager = appUtils.getOutputManager(configFile.destination)

    for file, metadata_provider in downloaded_science.items():
        output_manager.add_file(file, metadata_provider)

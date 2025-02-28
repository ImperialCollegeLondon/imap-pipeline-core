import logging
from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer

from imap_mag import appConfig, appUtils
from imap_mag.api.apiUtils import commandInit
from imap_mag.cli.fetchBinary import FetchBinary
from imap_mag.client.webPODA import WebPODA
from imap_mag.outputManager import StandardSPDFMetadataProvider


# E.g., imap-mag fetch binary --apid 1063 --start-date 2025-05-02 --end-date 2025-05-03
def fetch_binary(
    auth_code: Annotated[
        str,
        typer.Option(
            envvar="WEBPODA_AUTH_CODE",
            help="WebPODA authentication code",
        ),
    ],
    apid: Annotated[int, typer.Option(help="ApID to download")],
    start_date: Annotated[str, typer.Option(help="Start date for the download")],
    end_date: Annotated[str, typer.Option(help="End date for the download")],
    config: Annotated[Path, typer.Option()] = Path("config.yaml"),
):
    """Download binary data from WebPODA."""

    configFile: appConfig.AppConfig = commandInit(config)

    if not auth_code:
        logging.critical("No WebPODA authorization code provided")
        raise typer.Abort()

    packet: str = appUtils.getPacketFromApID(apid)
    start_datetime: datetime = appUtils.convertToDatetime(start_date)
    end_datetime: datetime = appUtils.convertToDatetime(end_date)

    logging.info(
        f"Downloading raw packet {packet} from {start_datetime} to {end_datetime}."
    )

    poda = WebPODA(
        auth_code,
        configFile.work_folder,
        configFile.api.webpoda_url if configFile.api else None,
    )

    fetch_binary = FetchBinary(poda)
    downloaded_binaries: dict[Path, StandardSPDFMetadataProvider] = (
        fetch_binary.download_binaries(
            packet=packet, start_date=start_datetime, end_date=end_datetime
        )
    )

    output_manager = appUtils.getOutputManager(configFile.destination)

    for file, metadata_provider in downloaded_binaries.items():
        output_manager.add_file(file, metadata_provider)

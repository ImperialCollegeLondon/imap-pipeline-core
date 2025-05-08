import logging
from datetime import datetime
from pathlib import Path
from typing import Annotated, Optional

import typer

from imap_mag import appUtils
from imap_mag.api.apiUtils import initialiseLoggingForCommand
from imap_mag.cli.fetchBinary import FetchBinary
from imap_mag.client.webPODA import WebPODA
from imap_mag.config.AppSettings import AppSettings
from imap_mag.config.FetchMode import FetchMode
from imap_mag.io import StandardSPDFMetadataProvider
from imap_mag.util import HKPacket

logger = logging.getLogger(__name__)


# E.g.,
# imap-mag fetch binary --apid 1063 --start-date 2025-01-02 --end-date 2025-01-03
# imap-mag fetch binary --packet SID3_PW --start-date 2025-01-02 --end-date 2025-01-03
def fetch_binary(
    auth_code: Annotated[
        str,
        typer.Option(
            envvar="WEBPODA_AUTH_CODE",
            help="WebPODA authentication code",
        ),
    ],
    start_date: Annotated[datetime, typer.Option(help="Start date for the download")],
    end_date: Annotated[datetime, typer.Option(help="End date for the download")],
    apid: Annotated[
        Optional[int],
        typer.Option("--apid", help="ApID to download"),
    ] = None,
    packet: Annotated[
        Optional[HKPacket],  # type: ignore
        typer.Option("--packet", help="Packet to download, e.g., SID1"),
    ] = None,
    fetch_mode: Annotated[
        FetchMode, typer.Option("--mode", case_sensitive=False)
    ] = FetchMode.DownloadOnly,
) -> dict[Path, StandardSPDFMetadataProvider]:
    """Download binary data from WebPODA."""

    # must provide a apid or a packet
    if (not apid and not packet) or (apid and packet):
        raise ValueError("Must provide either --apid or --packet, and not both")

    settings_overrides = (
        {"fetch_binary": {"webpoda": {"auth_code": auth_code}}} if auth_code else {}
    )

    app_settings = AppSettings(**settings_overrides)
    work_folder = app_settings.setup_work_folder_for_command(app_settings.fetch_binary)
    initialiseLoggingForCommand(work_folder)

    if apid is not None:
        packet_name: str = HKPacket.from_apid(apid).name
    elif packet is not None and isinstance(packet, str):
        packet_name: str = packet
    else:
        packet_name: str = packet.packet  # type: ignore

    if not auth_code:
        logger.critical("No WebPODA authorization code provided")
        raise ValueError("No SDC_AUTH_CODE API key provided")

    logger.info(
        f"Downloading raw packet {packet_name} from {start_date} to {end_date}."
    )

    poda = WebPODA(auth_code, work_folder, app_settings.fetch_binary.webpoda.url_base)

    fetch_binary = FetchBinary(poda)
    downloaded_binaries: dict[Path, StandardSPDFMetadataProvider] = (
        fetch_binary.download_binaries(
            packet=packet_name, start_date=start_date, end_date=end_date
        )
    )

    if not downloaded_binaries:
        logger.info(
            f"No data downloaded for packet {packet_name} from {start_date} to {end_date}."
        )

    if app_settings.fetch_binary.publish_to_data_store:
        output_manager = appUtils.getOutputManagerByMode(
            app_settings.data_store, mode=fetch_mode
        )
        output_binaries: dict[Path, StandardSPDFMetadataProvider] = dict()

        for file, metadata_provider in downloaded_binaries.items():
            (output_file, output_metadata) = output_manager.add_file(
                file, metadata_provider
            )
            output_binaries[output_file] = output_metadata
    else:
        output_binaries = dict()
        logger.info("Files not published to data store based on config.")

    return output_binaries

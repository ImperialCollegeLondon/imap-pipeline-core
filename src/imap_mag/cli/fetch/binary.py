import logging
from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer

from imap_mag.cli.cliUtils import initialiseLoggingForCommand
from imap_mag.client.WebPODA import WebPODA
from imap_mag.config import AppSettings, FetchMode
from imap_mag.download.FetchBinary import FetchBinary
from imap_mag.io import DatastoreFileManager
from imap_mag.io.file import HKBinaryPathHandler
from imap_mag.util import HKPacket

logger = logging.getLogger(__name__)


# E.g.,
# imap-mag fetch binary --apid 1063 --start-date 2025-01-02 --end-date 2025-01-03
# imap-mag fetch binary --packet SID3_PW --start-date 2025-01-02 --end-date 2025-01-03
def fetch_binary(
    start_date: Annotated[datetime, typer.Option(help="Start date for the download")],
    end_date: Annotated[datetime, typer.Option(help="End date for the download")],
    use_ert: Annotated[
        bool,
        typer.Option(
            "--ert",
            help="Use ERT (Earth Received Time), rather than HK measurement time",
        ),
    ] = False,
    apid: Annotated[
        int | None,
        typer.Option("--apid", help="ApID to download"),
    ] = None,
    packet: Annotated[
        HKPacket | None,
        typer.Option(case_sensitive=False, help="Packet to download, e.g., SID1"),
    ] = None,
    fetch_mode: Annotated[
        FetchMode,
        typer.Option(
            case_sensitive=False,
            help="Whether to download only or download and update progress in database",
        ),
    ] = FetchMode.DownloadOnly,
) -> dict[Path, HKBinaryPathHandler]:
    """Download binary data from WebPODA."""

    # Must provide a apid or a packet.
    if (not apid and not packet) or (apid and packet):
        raise ValueError("Must provide either --apid or --packet, and not both")

    app_settings = AppSettings()  # type: ignore
    work_folder = app_settings.setup_work_folder_for_command(app_settings.fetch_binary)

    initialiseLoggingForCommand(
        work_folder
    )  # DO NOT log anything before this point (it won't be captured in the log file)

    if apid is not None:
        packet = HKPacket.from_apid(apid)
    else:
        assert packet is not None

    packet_name = packet.packet_name

    logger.info(
        f"Downloading raw packet {packet_name} from {start_date} to {end_date}."
    )

    poda = WebPODA(
        app_settings.fetch_binary.api.auth_code,
        work_folder,
        app_settings.fetch_binary.api.url_base,
    )

    fetch_binary = FetchBinary(poda)
    downloaded_binaries: dict[Path, HKBinaryPathHandler] = (
        fetch_binary.download_binaries(
            packet=packet,
            start_date=start_date,
            end_date=end_date,
            use_ert=use_ert,
        )
    )

    if not downloaded_binaries:
        logger.info(
            f"No data downloaded for packet {packet_name} from {start_date} to {end_date}."
        )
    else:
        logger.debug(
            f"Downloaded {len(downloaded_binaries)} files:\n{', '.join(str(f) for f in downloaded_binaries.keys())}"
        )

    output_binaries: dict[Path, HKBinaryPathHandler] = dict()

    if app_settings.fetch_binary.publish_to_data_store:
        datastore_manager = DatastoreFileManager.CreateByMode(
            app_settings,
            use_database=(fetch_mode == FetchMode.DownloadAndUpdateProgress),
        )

        for file, path_handler in downloaded_binaries.items():
            (output_file, output_handler) = datastore_manager.add_file(
                file, path_handler
            )
            output_binaries[output_file] = output_handler
    else:
        logger.info("Files not published to data store based on config.")
        output_binaries = downloaded_binaries

    return output_binaries

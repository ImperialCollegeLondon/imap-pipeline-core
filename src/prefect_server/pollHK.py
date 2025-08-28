from datetime import datetime
from pathlib import Path
from typing import Annotated

from prefect import flow, get_run_logger
from prefect.runtime import flow_run
from pydantic import Field

from imap_mag.cli.fetch.binary import fetch_binary
from imap_mag.cli.fetch.DownloadDateManager import DownloadDateManager
from imap_mag.cli.process import process
from imap_mag.config import FetchMode, SaveMode
from imap_mag.db import Database, update_database_with_progress
from imap_mag.io.file import HKBinaryPathHandler
from imap_mag.util import CONSTANTS, DatetimeProvider, Environment, HKPacket
from prefect_server.constants import PREFECT_CONSTANTS
from prefect_server.prefectUtils import (
    get_secret_or_env_var,
)


def generate_flow_run_name() -> str:
    parameters = flow_run.parameters

    hk_packets: list[HKPacket] = parameters["hk_packets"]  # type: ignore
    start_date: str = (
        parameters["start_date"].strftime("%d-%m-%Y")
        if parameters["start_date"] is not None
        else "last-update"
    )
    end_date = parameters["end_date"] or DatetimeProvider.end_of_today()

    packet_names = [hk.name for hk in hk_packets]
    packet_text = (
        f"{','.join(packet_names)}-Packets"
        if packet_names != HKPacket.names()
        else "all-HK"
    )

    return (
        f"Download-{packet_text}-from-{start_date}-to-{end_date.strftime('%d-%m-%Y')}"
    )


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.POLL_HK,
    log_prints=True,
    flow_run_name=generate_flow_run_name,
)
async def poll_hk_flow(
    hk_packets: Annotated[
        list[HKPacket],
        Field(
            json_schema_extra={
                "title": "HK packets to download",
                "description": "List of HK packets to download from WebPODA. Default is all HK packets.",
            }
        ),
    ] = [hk for hk in HKPacket],  # type: ignore
    start_date: Annotated[
        datetime | None,
        Field(
            json_schema_extra={
                "title": "Start date",
                "description": "Start date for the download. Default is the last progress date for the packet (ERT).",
            }
        ),
    ] = None,
    end_date: Annotated[
        datetime | None,
        Field(
            json_schema_extra={
                "title": "End date",
                "description": "End date for the download. Default is the end of today (ERT).",
            }
        ),
    ] = None,
    force_ert: Annotated[
        bool,
        Field(
            json_schema_extra={
                "title": "Force input dates in ERT",
                "description": "If 'True' input dates are in Earth Received Time (ERT). Otherwise, input dates are in S/C clock time. Ignored if 'start_date' and 'end_date' are not provided.",
            }
        ),
    ] = False,
    force_database_update: Annotated[
        bool,
        Field(
            json_schema_extra={
                "title": "Force database update",
                "description": "Whether to force an update of the database with the downloaded packets. Ignored if 'start_date' and 'end_date' are not provided.",
            }
        ),
    ] = False,
):
    """
    Poll housekeeping data from WebPODA.
    """

    logger = get_run_logger()
    database = Database()

    auth_code = await get_secret_or_env_var(
        PREFECT_CONSTANTS.POLL_HK.WEBPODA_AUTH_CODE_SECRET_NAME,
        CONSTANTS.ENV_VAR_NAMES.WEBPODA_AUTH_CODE,
    )

    if force_database_update and not force_ert:
        logger.warning(
            "Database cannot be updated without forcing ERT. Database will not be updated."
        )

    # If this is an automated flow run, use the database to figure out what to download,
    # and use ERT to download data.
    automated_flow_run: bool = (start_date is None) and (end_date is None)
    use_database: bool = (force_database_update and force_ert) or automated_flow_run
    use_ert: bool = force_ert or automated_flow_run

    for packet in hk_packets:
        progress_item_id = packet.packet
        packet_start_timestamp = DatetimeProvider.now()

        logger.info(f"---------- Downloading Packet {progress_item_id} ----------")

        date_manager = DownloadDateManager(progress_item_id, database)

        packet_dates = date_manager.get_dates_for_download(
            original_start_date=start_date,
            original_end_date=end_date,
            validate_with_database=use_database,
        )

        if packet_dates is None:
            continue
        else:
            (packet_start_date, packet_end_date) = packet_dates

        # Download binary from WebPODA
        with Environment(CONSTANTS.ENV_VAR_NAMES.WEBPODA_AUTH_CODE, auth_code):
            downloaded_binaries: dict[Path, HKBinaryPathHandler] = fetch_binary(
                packet=packet,
                start_date=packet_start_date,
                end_date=packet_end_date,
                use_ert=use_ert,
                fetch_mode=FetchMode.DownloadAndUpdateProgress,
            )

        if downloaded_binaries:
            # Process binary data into CSV
            files = [file for file in downloaded_binaries.keys()]
            process(files, save_mode=SaveMode.LocalAndDatabase)
        else:
            logger.info(
                f"No data downloaded for {progress_item_id} from {packet_start_date} to {packet_end_date}."
            )

        # Update database with latest content date as progress (for HK)
        if use_database:
            ert_timestamps: list[datetime] = [
                metadata.ert
                for metadata in downloaded_binaries.values()
                if metadata.ert
            ]
            latest_ert_timestamp: datetime | None = (
                max(ert_timestamps) if ert_timestamps else None
            )

            update_database_with_progress(
                progress_item_id=progress_item_id,
                database=database,
                checked_timestamp=packet_start_timestamp,
                latest_timestamp=latest_ert_timestamp,
            )
        else:
            logger.info(f"Database not updated for {progress_item_id}.")

    logger.info("---------- Finished ----------")

from datetime import datetime
from pathlib import Path

from prefect import flow, get_run_logger
from prefect.runtime import flow_run

from imap_mag.api.fetch.binary import WebPODAMetadataProvider, fetch_binary
from imap_mag.api.process import process
from imap_mag.appConfig import manage_config
from imap_mag.config.FetchMode import FetchMode
from imap_mag.db import Database, update_database_with_progress
from imap_mag.util import DatetimeProvider, HKPacket, get_dates_for_download
from prefect_server.constants import CONSTANTS as PREFECT_CONSTANTS
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
        if packet_names != HKPacket.list()
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
    hk_packets: list[HKPacket] = [hk for hk in HKPacket],  # type: ignore
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    force_ert: bool = False,
    force_database_update: bool = False,
):
    """
    Poll housekeeping data from WebPODA.
    """

    logger = get_run_logger()
    database = Database()

    auth_code = await get_secret_or_env_var(
        PREFECT_CONSTANTS.POLL_HK.WEBPODA_AUTH_CODE_SECRET_NAME,
        PREFECT_CONSTANTS.ENV_VAR_NAMES.WEBPODA_AUTH_CODE,
    )

    # If this is an automated flow run, use the database to figure out what to download,
    # and use ERT to download data.
    automated_flow_run: bool = (start_date is None) and (end_date is None)
    use_database: bool = force_database_update or automated_flow_run
    use_ert: bool = force_ert or automated_flow_run

    for packet in hk_packets:
        packet_name = packet.packet

        logger.info(f"---------- Downloading Packet {packet_name} ----------")

        packet_dates = get_dates_for_download(
            packet_name=packet_name,
            database=database,
            original_start_date=start_date,
            original_end_date=end_date,
            check_and_update_database=use_database,
            logger=logger,
        )

        if packet_dates is None:
            continue
        else:
            (packet_start_date, packet_end_date) = packet_dates

        downloaded_binaries: dict[Path, WebPODAMetadataProvider] = fetch_binary(
            auth_code=auth_code,
            packet=packet,
            start_date=packet_start_date,
            end_date=packet_end_date,
            use_ert=use_ert,
            fetch_mode=FetchMode.DownloadAndUpdateProgress,
        )

        if not downloaded_binaries:
            logger.info(
                f"No data downloaded for packet {packet_name} from {packet_start_date} to {packet_end_date}. Database not updated."
            )
            continue

        # Process binary data into CSV
        for file, _ in downloaded_binaries.items():
            # TODO: get rid of all use of the dynamic config files
            with manage_config(
                source=file.parent, export_to_database=True
            ) as config_file:
                process(file=Path(file.name), config=config_file)

        # Update database with latest content date as progress (for HK)
        if use_database:
            update_database_with_progress(
                packet_name=packet_name,
                database=database,
                latest_timestamp=max(
                    metadata.ert for metadata in downloaded_binaries.values()
                ),
                logger=logger,
            )
        else:
            logger.info(f"Database not updated for {packet_name}.")

    logger.info("---------- Finished ----------")

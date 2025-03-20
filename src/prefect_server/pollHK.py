from datetime import datetime
from pathlib import Path

import pandas as pd
from prefect import flow, get_run_logger
from prefect.runtime import flow_run
from pydantic import SecretStr

from imap_mag.api.fetch.binary import fetch_binary
from imap_mag.api.process import process
from imap_mag.appConfig import manage_config
from imap_mag.appUtils import HK_PACKETS, HKPacket
from imap_mag.DB import Database
from imap_mag.outputManager import StandardSPDFMetadataProvider
from prefect_server.constants import CONSTANTS
from prefect_server.prefectUtils import (
    get_secret_block,
    get_start_and_end_dates_for_download,
)


def generate_flow_run_name() -> str:
    parameters = flow_run.parameters

    hk_packets: list[HKPacket] = parameters["hk_packets"]
    start_date: str = (
        parameters["start_date"].strftime("%d-%m-%Y")
        if parameters["start_date"] is not None
        else "last-update"
    )
    end_date: datetime = parameters["end_date"] or datetime.today().replace(
        hour=23, minute=59, second=59, microsecond=999999
    )

    packet_names = [hk.name for hk in hk_packets]
    packet_text = (
        f"{','.join(packet_names)}-Packets" if packet_names != HK_PACKETS else "all-HK"
    )

    return (
        f"Download-{packet_text}-from-{start_date}-to-{end_date.strftime('%d-%m-%Y')}"
    )


@flow(
    name=CONSTANTS.FLOW_NAMES.POLL_HK,
    log_prints=True,
    flow_run_name=generate_flow_run_name,
)
async def poll_hk_flow(
    hk_packets: list[HKPacket] = [hk for hk in HKPacket],
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    auth_code: SecretStr | None = None,
):
    """
    Poll housekeeping data from WebPODA.
    """

    logger = get_run_logger()

    if not auth_code:
        auth_code = SecretStr(
            await get_secret_block(CONSTANTS.POLL_HK.WEBPODA_AUTH_CODE_SECRET_NAME)
        )

    check_and_update_database = (start_date is None) and (end_date is None)

    database = Database()

    for packet in hk_packets:
        packet_name = packet.name
        logger.info(f"---------- Downloading Packet {packet_name} ----------")

        packet_dates = get_start_and_end_dates_for_download(
            packet_name=packet_name,
            database=database,
            original_start_date=start_date,
            original_end_date=end_date,
            check_and_update_database=check_and_update_database,
        )

        if packet_dates is None:
            continue
        else:
            (packet_start_date, packet_end_date) = packet_dates

        # Download binary from WebPODA
        with manage_config(export_to_database=True) as config_file:
            downloaded_binaries: dict[Path, StandardSPDFMetadataProvider] = (
                fetch_binary(
                    auth_code=auth_code.get_secret_value(),
                    apid_or_packet=packet,
                    start_date=packet_start_date,
                    end_date=packet_end_date,
                    config=config_file,
                )
            )

        if not downloaded_binaries:
            logger.info(
                f"No data downloaded for packet {packet_name} from {packet_start_date} to {packet_end_date}. Database not updated."
            )
            continue

        # Process binary data into CSV
        latest_timestamp: list[datetime] = []

        for file, _ in downloaded_binaries.items():
            with manage_config(
                source=file.parent, export_to_database=True
            ) as config_file:
                (processed_file, _) = process(file=Path(file.name), config=config_file)

            latest_timestamp.append(
                datetime.fromtimestamp(
                    pd.read_csv(processed_file).iloc[-1].epoch / 10**9
                    + datetime(2000, 1, 1, 11, 58, 55, 816000).timestamp()
                )
            )

        # Update database
        if check_and_update_database:
            download_progress = database.get_download_progress(packet_name)
            download_progress.record_successful_download(max(latest_timestamp))

            database.save(download_progress)
        else:
            logger.info(f"Database not updated for {packet_name}.")

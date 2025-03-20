from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from prefect import flow, get_run_logger
from prefect.runtime import flow_run
from pydantic import SecretStr

from imap_mag.api.fetch.binary import fetch_binary
from imap_mag.api.process import process
from imap_mag.appConfig import manage_config
from imap_mag.appUtils import HK_PACKETS, HKPacket, forceUTCTimeZone
from imap_mag.DB import Database
from imap_mag.outputManager import StandardSPDFMetadataProvider
from prefect_server.constants import CONSTANTS
from prefect_server.prefectUtils import get_secret_block


def generate_flow_run_name() -> str:
    parameters = flow_run.parameters

    hk_packets: list[HKPacket] = parameters["hk_packets"]
    start_date: datetime = parameters["start_date"] or datetime.today().replace(
        hour=0, minute=0, second=0, microsecond=0
    ) - timedelta(days=1)
    end_date: datetime = parameters["end_date"] or start_date + timedelta(days=1)

    packet_names = [hk.name for hk in hk_packets]
    packet_text = (
        f"{','.join(packet_names)}-Packets" if packet_names != HK_PACKETS else "all-HK"
    )

    return f"Download-{packet_text}-from-{start_date.strftime('%d-%m-%Y')}-to-{end_date.strftime('%d-%m-%Y')}"


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

    if start_date is None:
        start_date = datetime.today().replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - timedelta(days=1)

    if end_date is None:
        end_date = start_date + timedelta(days=1)

    (start_date, end_date) = forceUTCTimeZone(start_date, end_date)

    logger.info(
        f"Polling housekeeping data for packets {','.join([hk.name for hk in hk_packets])} from {start_date.strftime('%d-%m-%Y')} to {end_date.strftime('%d-%m-%Y')}."
    )

    database = Database()

    for packet in hk_packets:
        packet_name = packet.name
        logger.debug(f"Downloading packet {packet_name}.")

        download_progress = database.get_download_progress(packet_name)

        # Check what data actually needs downloading
        if check_and_update_database:
            last_updated_date = download_progress.progress_timestamp

            download_progress.record_checked_download(datetime.now())
            database.save(download_progress)

            logger.debug(
                f"Last update for packet {packet_name} is {last_updated_date}."
            )

            if (last_updated_date is None) or (last_updated_date <= start_date):
                logger.info(
                    f"Packet {packet_name} is not up to date. Downloading from {start_date}."
                )
                actual_start_date = start_date
            elif last_updated_date >= end_date:
                logger.info(
                    f"Packet {packet_name} is already up to date. Not downloading."
                )
                continue
            else:  # last_updated_date > start_date
                logger.info(
                    f"Packet {packet_name} is partially up to date. Downloading from {last_updated_date}."
                )
                actual_start_date = last_updated_date
        else:
            logger.info(
                f"Not checking database and forcing download from {start_date} to {end_date}."
            )
            actual_start_date = start_date

        # Download binary from WebPODA
        with manage_config(export_to_database=True) as config_file:
            downloaded_binaries: dict[Path, StandardSPDFMetadataProvider] = (
                fetch_binary(
                    auth_code=auth_code.get_secret_value(),
                    apid_or_packet=packet,
                    start_date=actual_start_date,
                    end_date=end_date,
                    config=config_file,
                )
            )

        if not downloaded_binaries:
            logger.info(
                f"No data downloaded for packet {packet_name} from {actual_start_date} to {end_date}. Database not updated."
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
            download_progress.record_successful_download(max(latest_timestamp))
            database.save(download_progress)
        else:
            logger.info(f"Database not updated for {packet_name}.")

from datetime import datetime
from pathlib import Path

from prefect import flow, get_run_logger
from prefect.runtime import flow_run
from pydantic import SecretStr
from spacepy import pycdf

from imap_mag.api.fetch.science import Level, MAGMode, fetch_science
from imap_mag.appConfig import manage_config
from imap_mag.DB import Database
from imap_mag.outputManager import StandardSPDFMetadataProvider
from prefect_server.constants import CONSTANTS
from prefect_server.prefectUtils import (
    get_secret_block,
    get_start_and_end_dates_for_download,
)


def convert_ints_to_string(apids: list[int]) -> str:
    return ",".join(str(apid) for apid in apids)


def generate_flow_run_name() -> str:
    parameters = flow_run.parameters

    level: Level = parameters["level"]
    modes: list[MAGMode] = parameters["modes"]
    start_date: str = (
        parameters["start_date"].strftime("%d-%m-%Y")
        if parameters["start_date"] is not None
        else "last-update"
    )
    end_date: datetime = parameters["end_date"] or datetime.today().replace(
        hour=23, minute=59, second=59, microsecond=999999
    )

    return f"Download-{','.join([m.value for m in modes])}-{level.value}-from-{start_date}-to-{end_date.strftime('%d-%m-%Y')}"


@flow(
    name=CONSTANTS.FLOW_NAMES.POLL_SCIENCE,
    log_prints=True,
    flow_run_name=generate_flow_run_name,
)
async def poll_science_flow(
    level: Level = Level.level_1c,
    modes: list[MAGMode] = [MAGMode.Normal, MAGMode.Burst],
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
            await get_secret_block(CONSTANTS.POLL_SCIENCE.SDC_AUTH_CODE_SECRET_NAME)
        )

    check_and_update_database = (start_date is None) and (end_date is None)
    database = Database()

    for mode in modes:
        if mode == MAGMode.Normal:
            packet_name = "MAG_SCI_NORM"
        else:
            packet_name = "MAG_SCI_BURST"

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

        # Download binary from SDC
        with manage_config(export_to_database=True) as config_file:
            downloaded_science: dict[Path, StandardSPDFMetadataProvider] = (
                fetch_science(
                    auth_code=auth_code.get_secret_value(),
                    level=level,
                    modes=[mode],
                    start_date=packet_start_date,
                    end_date=packet_end_date,
                    config=config_file,
                )
            )

        if not downloaded_science:
            logger.info(
                f"No data downloaded for packet {packet_name} from {packet_start_date} to {end_date}. Database not updated."
            )
            continue

        # Get latest science timestamp
        latest_timestamp: list[datetime] = []

        for file, _ in downloaded_science.items():
            latest_timestamp.append(pycdf.CDF(file.as_posix())["epoch"][-1])  # type: ignore

        # Update database
        if check_and_update_database:
            download_progress = database.get_download_progress(packet_name)
            download_progress.record_successful_download(max(latest_timestamp))

            database.save(download_progress)
        else:
            logger.info(f"Database not updated for {packet_name}.")

    logger.info("---------- Finished ----------")

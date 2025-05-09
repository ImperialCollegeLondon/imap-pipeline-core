from datetime import datetime
from pathlib import Path

from prefect import flow, get_run_logger
from prefect.runtime import flow_run

from imap_mag.api.fetch.science import (
    SDCMetadataProvider,
    fetch_science,
)
from imap_mag.appConfig import manage_config
from imap_mag.db import Database, update_database_with_progress
from imap_mag.util import DatetimeProvider, Level, ScienceMode, get_dates_for_download
from prefect_server.constants import CONSTANTS
from prefect_server.prefectUtils import get_secret_or_env_var


def convert_ints_to_string(apids: list[int]) -> str:
    return ",".join(str(apid) for apid in apids)


def generate_flow_run_name() -> str:
    parameters = flow_run.parameters

    level: Level = parameters["level"]
    modes: list[ScienceMode] = parameters["modes"]
    start_date: str = (
        parameters["start_date"].strftime("%d-%m-%Y")
        if parameters["start_date"] is not None
        else "last-update"
    )
    end_date = parameters["end_date"] or DatetimeProvider.end_of_today()

    return f"Download-{','.join([m.short_name for m in modes])}-{level.value}-from-{start_date}-to-{end_date.strftime('%d-%m-%Y')}"


@flow(
    name=CONSTANTS.FLOW_NAMES.POLL_SCIENCE,
    log_prints=True,
    flow_run_name=generate_flow_run_name,
)
async def poll_science_flow(
    level: Level = Level.level_1c,
    modes: list[ScienceMode] = [ScienceMode.Normal, ScienceMode.Burst],
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    force_ingestion_date: bool = False,
    force_database_update: bool = False,
):
    """
    Poll housekeeping data from WebPODA.
    """

    logger = get_run_logger()

    auth_code = await get_secret_or_env_var(
        CONSTANTS.POLL_SCIENCE.SDC_AUTH_CODE_SECRET_NAME,
        CONSTANTS.ENV_VAR_NAMES.SDC_AUTH_CODE,
    )

    # If this is an automated flow run, use the database to figure out what to download,
    # and use the ingestion date to download data; otherwise use the file start date.
    use_database_and_ingestion_date = (start_date is None) and (end_date is None)
    database = Database()

    for mode in modes:
        packet_name = mode.packet

        logger.info(f"---------- Downloading Packet {packet_name} ----------")

        packet_dates = get_dates_for_download(
            packet_name=packet_name,
            database=database,
            original_start_date=start_date,
            original_end_date=end_date,
            check_and_update_database=use_database_and_ingestion_date,
            logger=logger,
        )

        if packet_dates is None:
            continue
        else:
            (packet_start_date, packet_end_date) = packet_dates

        # Download binary from SDC
        with manage_config(export_to_database=True) as config_file:
            downloaded_science: dict[Path, SDCMetadataProvider] = fetch_science(
                auth_code=auth_code,
                level=level,
                modes=[mode],
                start_date=packet_start_date,
                end_date=packet_end_date,
                use_ingestion_date=(
                    use_database_and_ingestion_date or force_ingestion_date
                ),
                config=config_file,
            )

        if not downloaded_science:
            logger.info(
                f"No data downloaded for packet {packet_name} from {packet_start_date} to {packet_end_date}. Database not updated."
            )
            continue

        # Update database with latest ingestion date as progress (for science)
        if use_database_and_ingestion_date or force_database_update:
            update_database_with_progress(
                packet_name=packet_name,
                database=database,
                latest_timestamp=max(
                    metadata.ingestion_date for metadata in downloaded_science.values()
                ),
                logger=logger,
            )
        else:
            logger.info(f"Database not updated for {packet_name}.")

    logger.info("---------- Finished ----------")

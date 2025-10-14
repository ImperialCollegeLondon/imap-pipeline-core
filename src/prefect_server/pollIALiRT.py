import time
from datetime import datetime
from pathlib import Path
from typing import Annotated

from prefect import flow, get_run_logger
from prefect.runtime import flow_run
from pydantic import Field

from imap_mag.cli.fetch.DownloadDateManager import DownloadDateManager
from imap_mag.cli.fetch.ialirt import fetch_ialirt
from imap_mag.config.FetchMode import FetchMode
from imap_mag.db import Database, update_database_with_progress
from imap_mag.io.file import IALiRTPathHandler
from imap_mag.util import CONSTANTS, DatetimeProvider, Environment
from prefect_server.constants import PREFECT_CONSTANTS
from prefect_server.prefectUtils import get_secret_or_env_var


def convert_ints_to_string(apids: list[int]) -> str:
    return ",".join(str(apid) for apid in apids)


def generate_flow_run_name() -> str:
    parameters = flow_run.parameters

    start_date: datetime = (
        parameters["start_date"]
        if parameters["start_date"]
        else DatetimeProvider.start_of_hour()
    )
    end_date: datetime = (
        parameters["end_date"]
        if parameters["end_date"]
        else DatetimeProvider.end_of_hour()
    )

    return f"Poll-IALiRT-from-{start_date.strftime('%d-%m-%YT%H:%M:%S')}-to-{end_date.strftime('%d-%m-%YT%H:%M:%S')}"


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.POLL_IALIRT,
    log_prints=True,
    flow_run_name=generate_flow_run_name,
)
async def poll_ialirt_flow(
    start_date: Annotated[
        datetime | None,
        Field(
            json_schema_extra={
                "title": "Start date",
                "description": "Start date for the download. Default is the start of the hour.",
            }
        ),
    ] = None,
    end_date: Annotated[
        datetime | None,
        Field(
            json_schema_extra={
                "title": "End date",
                "description": "End date for the download. Default is the end of the hour.",
            }
        ),
    ] = None,
) -> None:
    """
    Poll I-ALiRT data from SDC.
    """

    logger = get_run_logger()
    database = Database()

    auth_code = await get_secret_or_env_var(
        PREFECT_CONSTANTS.POLL_IALIRT.IALIRT_AUTH_CODE_SECRET_NAME,
        CONSTANTS.ENV_VAR_NAMES.IALIRT_AUTH_CODE,
    )

    logger.info("---------- Start I-ALiRT Poll ----------")

    # With I-ALiRT we poll for 1 hour, every hour.
    start_date = (
        start_date or None  # use None to force download from last progress date
    )
    end_date = end_date or DatetimeProvider.end_of_hour()

    while (end_date - DatetimeProvider.now()).total_seconds() > 30:
        logger.info("--- Waiting 30 seconds before polling for new data ---")
        time.sleep(30)

        start_timestamp = DatetimeProvider.now()
        progress_item_id = "MAG_IALIRT"

        date_manager = DownloadDateManager(
            progress_item_id, database, earliest_date=DatetimeProvider.yesterday()
        )

        packet_dates = date_manager.get_dates_for_download(
            original_start_date=start_date,
            original_end_date=end_date,
            validate_with_database=True,
        )

        if packet_dates is None:
            logger.info("No dates for download - skipping")
            continue
        else:
            (packet_start_date, packet_end_date) = packet_dates

        # Download files from SDC
        with Environment(CONSTANTS.ENV_VAR_NAMES.IALIRT_AUTH_CODE, auth_code):
            downloaded_ialirt: dict[Path, IALiRTPathHandler] = fetch_ialirt(
                start_date=packet_start_date,
                end_date=packet_end_date,
                fetch_mode=FetchMode.DownloadAndUpdateProgress,
            )

        if not downloaded_ialirt:
            logger.info(
                f"No I-ALiRT data downloaded from {packet_start_date} to {packet_end_date}."
            )
            continue

        # Update database with latest downloaded date as progress (for I-ALiRT)
        content_dates: list[datetime] = [
            metadata.content_date
            for metadata in downloaded_ialirt.values()
            if metadata.content_date
        ]
        latest_date: datetime | None = max(content_dates) if content_dates else None

        update_database_with_progress(
            progress_item_id=progress_item_id,
            database=database,
            checked_timestamp=start_timestamp,
            latest_timestamp=latest_date,
        )

    logger.info("---------- End I-ALiRT Poll ----------")

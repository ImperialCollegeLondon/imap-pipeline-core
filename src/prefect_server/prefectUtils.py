import logging
import os
from datetime import datetime

from prefect import get_run_logger
from prefect.blocks.system import Secret

from imap_mag.appUtils import DatetimeProvider, forceUTCTimeZone
from imap_mag.DB import Database


def get_cron_from_env(env_var_name: str, default: str | None = None) -> str | None:
    cron = os.getenv(env_var_name, default)

    if cron is None or cron == "":
        return None
    else:
        cron = cron.strip(" '\"")
        print(f"Using cron schedule: {env_var_name}={cron}")
        return cron


# TODO: This is copied from so-pipeline-core
async def get_secret_block(secret_name: str) -> str:
    logger = get_run_logger()

    logger.info(f"Retrieving secret block {secret_name}.")

    try:
        secret: Secret = await Secret.aload(secret_name)
    except ValueError as e:
        logger.error(f"Block {secret_name} does not exist.")
        raise e

    value = secret.get()

    if not value:
        logger.error(f"Block {secret_name} is empty.")
        raise ValueError(f"Block {secret_name} is empty.")

    logger.debug(f"Block {secret_name} retrieved successfully.")

    return value


async def get_secret_or_env_var(secret_name: str, env_var_name: str) -> str:
    logger = get_run_logger()

    auth_code: str | None = None

    try:
        auth_code = await get_secret_block(secret_name)
    except ValueError:
        logger.info(
            f"{secret_name} not found or empty. Using environment variable {env_var_name}."
        )

    if not auth_code:
        auth_code = os.getenv(env_var_name)

    if not auth_code:
        logger.error(
            f"Environment variable {env_var_name} and secret {secret_name} are both undefined."
        )
        raise ValueError(
            f"Environment variable {env_var_name} and secret {secret_name} are both undefined."
        )

    return auth_code


def get_start_and_end_dates_for_download(
    *,
    packet_name: str,
    database: Database,
    original_start_date: datetime | None,
    original_end_date: datetime | None,
    check_and_update_database: bool,
    logger: logging.Logger | logging.LoggerAdapter,
) -> tuple[datetime, datetime] | None:
    """
    Check database for last update date and return start and end dates for download,
    based on what data has already been downloaded so far.
    """

    # Check end date
    if original_end_date is None:
        logger.info(
            f"End date not provided. Using end of today as default download date for {packet_name}."
        )
        packet_end_date = DatetimeProvider.end_of_today()
    else:
        logger.info(f"Using provided end date {original_end_date} for {packet_name}.")
        packet_end_date = original_end_date

    # Get last updated date from database
    download_progress = database.get_download_progress(packet_name)
    last_updated_date = download_progress.get_progress_timestamp()

    logger.debug(f"Last update for packet {packet_name} is {last_updated_date}.")

    if check_and_update_database:
        download_progress.record_checked_download(DatetimeProvider.now())
        database.save(download_progress)

    # Check start date
    if (original_start_date is None) and (last_updated_date is None):
        logger.info(
            f"Start date not provided. Using yesterday as default download date for {packet_name}."
        )
        packet_start_date = DatetimeProvider.yesterday()
    elif original_start_date is None:
        logger.info(
            f"Start date not provided. Using last updated date {last_updated_date} for {packet_name} from database."
        )
        packet_start_date = last_updated_date
    else:
        logger.info(
            f"Using provided start date {original_start_date} for {packet_name}."
        )
        packet_start_date = original_start_date

        # Check what data actually needs downloading
        if check_and_update_database:
            if (last_updated_date is None) or (last_updated_date <= packet_start_date):
                logger.info(
                    f"Packet {packet_name} is not up to date. Downloading from {packet_start_date}."
                )
            elif last_updated_date >= packet_end_date:
                logger.info(
                    f"Packet {packet_name} is already up to date. Not downloading."
                )
                return None
            else:  # last_updated_date > packet_start_date
                logger.info(
                    f"Packet {packet_name} is partially up to date. Downloading from {last_updated_date}."
                )
                packet_start_date = last_updated_date
        else:
            logger.info(
                f"Not checking database and forcing download from {packet_start_date} to {packet_end_date}."
            )

    # Remove any timezone information
    (packet_start_date, packet_end_date) = forceUTCTimeZone(
        packet_start_date, packet_end_date
    )

    return (packet_start_date, packet_end_date)

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated

import pytz
from prefect import flow
from prefect.blocks.notifications import MicrosoftTeamsWebhook
from prefect.events import Event, emit_event
from prefect.runtime import flow_run
from pydantic import Field

from imap_mag.cli.fetch.DownloadDateManager import DownloadDateManager
from imap_mag.cli.fetch.ialirt import fetch_ialirt, fetch_ialirt_hk
from imap_mag.config.FetchMode import FetchMode
from imap_mag.db import Database, update_database_with_progress
from imap_mag.io.file.IFilePathHandler import IFilePathHandler
from imap_mag.util import CONSTANTS, DatetimeProvider, Environment
from prefect_server.constants import PREFECT_CONSTANTS
from prefect_server.prefectUtils import get_secret_or_env_var, try_get_prefect_logger
from prefect_server.quicklookIALiRT import quicklook_ialirt_flow


def generate_flow_run_name() -> str:
    parameters = flow_run.parameters

    start_date: str = (
        parameters["start_date"].strftime("%d-%m-%YT%H:%M:%S")
        if parameters["start_date"]
        else "last-update"
    )
    end_date: datetime = (
        parameters["end_date"]
        if parameters["end_date"]
        else DatetimeProvider.end_of_hour()
    )

    return f"Poll-IALiRT-from-{start_date}-to-{end_date.strftime('%d-%m-%YT%H:%M:%S')}"


def generate_hk_flow_run_name() -> str:
    parameters = flow_run.parameters

    start_date: str = (
        parameters["start_date"].strftime("%d-%m-%YT%H:%M:%S")
        if parameters["start_date"]
        else "last-update"
    )
    end_date: datetime = (
        parameters["end_date"]
        if parameters["end_date"]
        else DatetimeProvider.end_of_hour()
    )

    return (
        f"Poll-IALiRT-HK-from-{start_date}-to-{end_date.strftime('%d-%m-%YT%H:%M:%S')}"
    )


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.POLL_IALIRT,
    log_prints=True,
    flow_run_name=generate_flow_run_name,
    retries=1,
)
async def poll_ialirt_flow(
    start_date: Annotated[
        datetime | None,
        Field(
            json_schema_extra={
                "title": "Start date",
                "description": "Start date for the download. Default is the last update.",
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
    force_download: Annotated[
        bool,
        Field(
            json_schema_extra={
                "title": "Force download",
                "description": "If true, the flow will download data even if it has already been downloaded before.",
            }
        ),
    ] = False,
    wait_for_new_data_to_arrive: Annotated[
        bool,
        Field(
            json_schema_extra={
                "title": "Wait for new data to arrive",
                "description": "If true, the flow will poll for new data until the end date is reached.",
            }
        ),
    ] = True,
    timeout: Annotated[
        int,
        Field(
            json_schema_extra={
                "title": "Timeout",
                "description": "Time in seconds to wait between polling for new data. Only used when waiting for new data.",
            }
        ),
    ] = 5 * 60,  # 5 minutes
    plot_last_3_days: Annotated[
        bool,
        Field(
            json_schema_extra={
                "title": "Plot last 3 days",
                "description": "If true, the flow will generate a quicklook plot of the downloaded data over the last 3 days.",
            }
        ),
    ] = True,
    imap_notification_webhook_name: Annotated[
        str,
        Field(
            default=None,
            json_schema_extra={
                "title": "IMAP Webhook Name",
                "description": "Name of the notification webhook to use for IMAP alerts.",
            },
        ),
    ] = PREFECT_CONSTANTS.IMAP_WEBHOOK_BLOCK_NAME,
) -> None:
    """
    Poll I-ALiRT MAG data from SDC.
    """

    logger = try_get_prefect_logger(__name__)
    database = Database()

    auth_code = await get_secret_or_env_var(
        PREFECT_CONSTANTS.POLL_IALIRT.IALIRT_AUTH_CODE_SECRET_NAME,
        CONSTANTS.ENV_VAR_NAMES.IALIRT_AUTH_CODE,
    )
    end_date = end_date or DatetimeProvider.end_of_hour()
    updated_files: list[Path] = []

    logger.info("---------- Start I-ALiRT MAG Poll ----------")

    if wait_for_new_data_to_arrive:
        while (end_date - DatetimeProvider.now()).total_seconds() > timeout:
            files = do_poll_ialirt(
                database, auth_code, start_date, end_date, force_download, logger
            )
            updated_files.extend(files)

            logger.info(
                f"--- Waiting {timeout} seconds before polling for new data ---"
            )
            await asyncio.sleep(timeout)
    else:
        files = do_poll_ialirt(
            database, auth_code, start_date, end_date, force_download, logger
        )
        updated_files.extend(files)

    logger.info("---------- End I-ALiRT MAG Poll ----------")

    if plot_last_3_days:
        await quicklook_ialirt_flow(
            start_date=DatetimeProvider.today() - timedelta(days=2),
            end_date=DatetimeProvider.now(),
            combined_plot=True,
        )

        # If this is the 6 AM (UK time) polling job, send the latest figure to Teams
        uk_end_time = end_date.replace(tzinfo=UTC).astimezone(
            pytz.timezone("Europe/London")
        )

        if wait_for_new_data_to_arrive and (uk_end_time.hour == 6):
            imap_webhook_block = await MicrosoftTeamsWebhook.aload(
                imap_notification_webhook_name
            )

            latest_ialirt_date = database.get_workflow_progress(
                CONSTANTS.DATABASE.IALIRT_PROGRESS_ID
            )
            message_body: str = (
                f"View the [latest I-ALiRT data on Sharepoint]({PREFECT_CONSTANTS.POLL_IALIRT.IALIRT_QUICKLOOK_SHAREPOINT_URL}).\n\n"
                f"Updated to {latest_ialirt_date.progress_timestamp} UTC."
            )

            imap_webhook_block.notify_type = "info"
            await imap_webhook_block.notify(
                body=message_body,
                subject="I-ALiRT Latest Quicklook",
            )  # type: ignore


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.POLL_IALIRT_HK,
    log_prints=True,
    flow_run_name=generate_hk_flow_run_name,
    retries=1,
)
async def poll_ialirt_hk_flow(
    start_date: Annotated[
        datetime | None,
        Field(
            json_schema_extra={
                "title": "Start date",
                "description": "Start date for the download. Default is the last update.",
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
    force_download: Annotated[
        bool,
        Field(
            json_schema_extra={
                "title": "Force download",
                "description": "If true, the flow will download data even if it has already been downloaded before.",
            }
        ),
    ] = False,
    wait_for_new_data_to_arrive: Annotated[
        bool,
        Field(
            json_schema_extra={
                "title": "Wait for new data to arrive",
                "description": "If true, the flow will poll for new data until the end date is reached.",
            }
        ),
    ] = True,
    timeout: Annotated[
        int,
        Field(
            json_schema_extra={
                "title": "Timeout",
                "description": "Time in seconds to wait between polling for new data. Only used when waiting for new data.",
            }
        ),
    ] = 5 * 60,  # 5 minutes
) -> None:
    """
    Poll I-ALiRT MAG HK data from SDC.
    """

    logger = try_get_prefect_logger(__name__)
    database = Database()

    auth_code = await get_secret_or_env_var(
        PREFECT_CONSTANTS.POLL_IALIRT.IALIRT_AUTH_CODE_SECRET_NAME,
        CONSTANTS.ENV_VAR_NAMES.IALIRT_AUTH_CODE,
    )
    end_date = end_date or DatetimeProvider.end_of_hour()
    updated_files: list[Path] = []

    logger.info("---------- Start I-ALiRT MAG HK Poll ----------")

    if wait_for_new_data_to_arrive:
        while (end_date - DatetimeProvider.now()).total_seconds() > timeout:
            files = do_poll_ialirt_hk(
                database, auth_code, start_date, end_date, force_download, logger
            )
            updated_files.extend(files)

            logger.info(
                f"--- Waiting {timeout} seconds before polling for new data ---"
            )
            await asyncio.sleep(timeout)
    else:
        files = do_poll_ialirt_hk(
            database, auth_code, start_date, end_date, force_download, logger
        )
        updated_files.extend(files)

    logger.info("---------- End I-ALiRT MAG HK Poll ----------")


def do_poll_ialirt(
    database: Database,
    auth_code: str,
    start_date: datetime | None,
    end_date: datetime | None,
    force_download: bool,
    logger,
) -> list[Path]:
    return _do_poll(
        database=database,
        auth_code=auth_code,
        start_date=start_date,
        end_date=end_date,
        force_download=force_download,
        logger=logger,
        progress_item_id=CONSTANTS.DATABASE.IALIRT_PROGRESS_ID,
        fetch_fn=fetch_ialirt,
        event_type=PREFECT_CONSTANTS.EVENT.IALIRT_UPDATED,
    )


def do_poll_ialirt_hk(
    database: Database,
    auth_code: str,
    start_date: datetime | None,
    end_date: datetime | None,
    force_download: bool,
    logger,
) -> list[Path]:
    return _do_poll(
        database=database,
        auth_code=auth_code,
        start_date=start_date,
        end_date=end_date,
        force_download=force_download,
        logger=logger,
        progress_item_id=CONSTANTS.DATABASE.IALIRT_HK_PROGRESS_ID,
        fetch_fn=fetch_ialirt_hk,
        event_type=PREFECT_CONSTANTS.EVENT.IALIRT_HK_UPDATED,
    )


def _do_poll(
    database: Database,
    auth_code: str,
    start_date: datetime | None,
    end_date: datetime | None,
    force_download: bool,
    logger,
    progress_item_id: str,
    fetch_fn,
    event_type: str,
) -> list[Path]:
    start_timestamp = DatetimeProvider.now()

    date_manager = DownloadDateManager(
        progress_item_id,
        database,
        earliest_date=DatetimeProvider.yesterday(),
        progress_time_buffer=timedelta(
            seconds=1  # do not download the last packet again
        ),
    )

    packet_dates = date_manager.get_dates_for_download(
        original_start_date=start_date,
        original_end_date=end_date,
        validate_with_database=(not force_download),
    )

    if packet_dates is None:
        logger.info("No dates to download - skipping")
        return []
    else:
        (packet_start_date, packet_end_date) = packet_dates

    # Download files from SDC
    with Environment(CONSTANTS.ENV_VAR_NAMES.IALIRT_AUTH_CODE, auth_code):
        downloaded: dict[Path, IFilePathHandler] = fetch_fn(
            start_date=packet_start_date,
            end_date=packet_end_date,
            fetch_mode=FetchMode.DownloadAndUpdateProgress,
        )

    if not downloaded:
        logger.info(
            f"No I-ALiRT data downloaded from {packet_start_date} to {packet_end_date}."
        )
        return []

    # Update database with latest downloaded date as progress
    content_dates: list[datetime] = [
        metadata.content_date
        for metadata in downloaded.values()
        if metadata.content_date
    ]
    latest_date: datetime | None = max(content_dates) if content_dates else None

    update_database_with_progress(
        progress_item_id=progress_item_id,
        database=database,
        checked_timestamp=start_timestamp,
        latest_timestamp=latest_date,
    )

    # Trigger event to notify updated I-ALiRT data
    logger.debug(f"Emitting {event_type} event")

    event: Event | None = emit_event(
        event=event_type,
        resource={
            "prefect.resource.id": f"prefect.flow-run.{flow_run.id}",
            "prefect.resource.name": flow_run.name,
            "prefect.resource.role": "flow-run",
        },
        payload={"files": list(downloaded.keys())},
    )

    if event is None:
        logger.warning(f"Failed to emit {event_type} event")

    return list(downloaded.keys())

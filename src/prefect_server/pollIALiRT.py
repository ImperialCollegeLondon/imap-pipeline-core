import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated

import pytz
from prefect import flow, task
from prefect.blocks.notifications import MicrosoftTeamsWebhook
from prefect.events import Event, emit_event
from prefect.runtime import flow_run
from pydantic import Field

from imap_mag.config.AppSettings import AppSettings
from imap_mag.data_pipelines import FetchByDatesRunParameters
from imap_mag.data_pipelines.IALiRTInstrumentPipeline import IALiRTInstrumentPipeline
from imap_mag.data_pipelines.Result import Result
from imap_mag.db import Database
from imap_mag.util import CONSTANTS, DatetimeProvider, Environment
from prefect_server.constants import PREFECT_CONSTANTS
from prefect_server.prefectUtils import get_secret_or_env_var, try_get_prefect_logger
from prefect_server.quicklookIALiRT import quicklook_ialirt_flow

_ALL_INSTRUMENTS = list(CONSTANTS.DATABASE.IALIRT_INSTRUMENT_PROGRESS_IDS.keys())


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

    return (
        f"Poll-IALiRT-All-from-{start_date}-to-{end_date.strftime('%d-%m-%YT%H:%M:%S')}"
    )


@task(name="poll-ialirt-instrument")
async def poll_instrument_task(
    instrument: str,
    start_date: datetime | None,
    end_date: datetime | None,
    force_download: bool,
    auth_code: str,
) -> Result:
    """Download the latest data for a single I-ALiRT instrument."""

    with Environment(CONSTANTS.ENV_VAR_NAMES.IALIRT_AUTH_CODE, auth_code):
        settings = AppSettings()  # type: ignore
        database = Database()

        pipeline = IALiRTInstrumentPipeline(
            instrument=instrument,
            database=database,
            settings=settings,
        )

        run_params = FetchByDatesRunParameters(
            start_date=start_date,  # None = use DB progress as start
            end_date=end_date,
            force_redownload=force_download,
        )

        pipeline.build(run_params)
        await pipeline.run()
        return pipeline.get_results()


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
                "description": "If true, the flow will generate a quicklook plot of the downloaded MAG data over the last 3 days.",
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
    Poll all I-ALiRT instruments from the I-ALiRT API.

    Runs one Prefect task per instrument per polling tick so each instrument's
    progress and logs are tracked independently.
    """

    logger = try_get_prefect_logger(__name__)

    auth_code = await get_secret_or_env_var(
        PREFECT_CONSTANTS.POLL_IALIRT.IALIRT_AUTH_CODE_SECRET_NAME,
        CONSTANTS.ENV_VAR_NAMES.IALIRT_AUTH_CODE,
    )
    end = end_date or DatetimeProvider.end_of_hour()

    logger.info(
        f"---------- Start I-ALiRT All Instruments Poll (instruments: {_ALL_INSTRUMENTS}) ----------"
    )

    # Accumulate mag_hk file paths across all ticks for event emission
    mag_hk_downloaded_files: list[Path] = []

    async def run_one_tick() -> None:
        results: list[Result] = await asyncio.gather(
            *[
                poll_instrument_task(
                    instrument=instrument,
                    start_date=start_date,
                    end_date=end,
                    force_download=force_download,
                    auth_code=auth_code,
                )
                for instrument in _ALL_INSTRUMENTS
            ],
            return_exceptions=True,
        )

        for instrument, result in zip(_ALL_INSTRUMENTS, results):
            if isinstance(result, Exception):
                logger.error(f"Task for instrument '{instrument}' failed: {result}")
                continue

            if instrument == "mag_hk" and result.success and result.data_items:
                mag_hk_downloaded_files.extend(
                    item.file_path
                    for item in result.data_items
                    if hasattr(item, "file_path")
                )

    if wait_for_new_data_to_arrive:
        while (end - DatetimeProvider.now()).total_seconds() > timeout:
            await run_one_tick()
            logger.info(
                f"--- Waiting {timeout} seconds before polling for new data ---"
            )
            await asyncio.sleep(timeout)
    else:
        await run_one_tick()

    logger.info("---------- End I-ALiRT All Instruments Poll ----------")

    # Emit event so downstream flows (e.g. check_ialirt) know HK data was updated
    if mag_hk_downloaded_files:
        logger.debug(f"Emitting {PREFECT_CONSTANTS.EVENT.IALIRT_HK_UPDATED} event")
        event: Event | None = emit_event(
            event=PREFECT_CONSTANTS.EVENT.IALIRT_HK_UPDATED,
            resource={
                "prefect.resource.id": f"prefect.flow-run.{flow_run.id}",
                "prefect.resource.name": flow_run.name,
                "prefect.resource.role": "flow-run",
            },
            payload={"files": [str(f) for f in mag_hk_downloaded_files]},
        )
        if event is None:
            logger.error(
                f"Failed to emit {PREFECT_CONSTANTS.EVENT.IALIRT_HK_UPDATED} event"
            )

    if plot_last_3_days:
        await quicklook_ialirt_flow(
            start_date=DatetimeProvider.today() - timedelta(days=2),
            end_date=DatetimeProvider.now(),
            combined_plot=True,
        )

        # If this is the 6 AM (UK time) polling job, send the latest figure to Teams
        uk_end_time = end.replace(tzinfo=UTC).astimezone(pytz.timezone("Europe/London"))

        if wait_for_new_data_to_arrive and (uk_end_time.hour == 6):
            imap_webhook_block = await MicrosoftTeamsWebhook.aload(
                imap_notification_webhook_name
            )

            database = Database()
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

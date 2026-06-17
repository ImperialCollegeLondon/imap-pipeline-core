import asyncio
from datetime import UTC, datetime, timedelta
from typing import Annotated

import pytz
from prefect import flow, task
from prefect.blocks.notifications import MicrosoftTeamsWebhook
from prefect.events import Event, emit_event
from prefect.runtime import flow_run
from prefect.states import Completed
from pydantic import Field, SecretStr

from imap_mag.config.AppSettings import AppSettings
from imap_mag.data_pipelines import AutomaticRunParameters, FetchByDatesRunParameters
from imap_mag.data_pipelines.IALiRTInstrumentPipeline import IALiRTPipeline
from imap_mag.db import Database
from imap_mag.util import DatetimeProvider
from imap_mag.util.constants import (
    CONSTANTS,
    VALID_IALIRT_HK_INSTRUMENTS,
    VALID_IALIRT_INSTRUMENTS,
)
from prefect_server.constants import PREFECT_CONSTANTS
from prefect_server.prefectUtils import (
    get_secret_or_env_var,
    try_get_prefect_logger,
)
from prefect_server.quicklookIALiRT import quicklook_ialirt_flow


def generate_flow_run_name() -> str:
    parameters = flow_run.parameters

    start_date: str = (
        parameters["start_date"].strftime("%d-%m-%YT%H:%M:%S")
        if parameters.get("start_date")
        else "last-update"
    )
    end_date: datetime = (
        parameters["end_date"]
        if parameters.get("end_date")
        else DatetimeProvider.end_of_hour()
    )

    return f"Poll-IALiRT-from-{start_date}-to-{end_date.strftime('%d-%m-%YT%H:%M:%S')}"


@task(name="Execute I-ALiRT Pipeline", retries=2, retry_delay_seconds=30)
async def run_ialirt_polling_pipeline_task(
    instrument: str,
    run_parameters: AutomaticRunParameters | FetchByDatesRunParameters,
    database: Database | None,
    settings: AppSettings,
):
    """Wrap IALiRTPipeline in a Prefect task."""
    logger = try_get_prefect_logger(__name__)

    pipeline = IALiRTPipeline(
        instrument=instrument, database=database, settings=settings
    )

    logger.info(f"Building and running pipeline for {instrument.upper()}...")
    pipeline.build(run_parameters)
    await pipeline.run()

    result = pipeline.get_results()

    if not result.success:
        raise RuntimeError(f"I-ALiRT Pipeline failed for {instrument}: {result}")

    logger = try_get_prefect_logger(__name__)

    if instrument.endswith("_hk"):
        logger.debug(
            f"Emitting {PREFECT_CONSTANTS.EVENT.IALIRT_HK_UPDATED} event for {instrument}"
        )

        event: Event | None = emit_event(
            event=PREFECT_CONSTANTS.EVENT.IALIRT_HK_UPDATED,
            resource={
                "prefect.resource.id": f"prefect.flow-run.{flow_run.id}",
                "prefect.resource.name": flow_run.name,
                "prefect.resource.role": "flow-run",
            },
            payload={"instrument": instrument, "status": "completed"},
        )
        if event is None:
            logger.error(
                f"Failed to emit {PREFECT_CONSTANTS.EVENT.IALIRT_HK_UPDATED} event"
            )
    else:
        logger.debug(
            f"Emitting {PREFECT_CONSTANTS.EVENT.IALIRT_UPDATED} event for {instrument}"
        )

        event: Event | None = emit_event(
            event=PREFECT_CONSTANTS.EVENT.IALIRT_UPDATED,
            resource={
                "prefect.resource.id": f"prefect.flow-run.{flow_run.id}",
                "prefect.resource.name": flow_run.name,
                "prefect.resource.role": "flow-run",
            },
            payload={"instrument": instrument, "status": "completed"},
        )
        if event is None:
            logger.error(
                f"Failed to emit {PREFECT_CONSTANTS.EVENT.IALIRT_UPDATED} event"
            )
    return result


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.POLL_IALIRT,
    flow_run_name=generate_flow_run_name,
    log_prints=True,
    validate_parameters=False,  # Avoid Prefect trying to validate the complex types
)
async def poll_ialirt_flow(
    run_parameters: Annotated[
        AutomaticRunParameters | FetchByDatesRunParameters,
        Field(
            default_factory=AutomaticRunParameters,
            json_schema_extra={
                "title": "Run parameters",
                "description": "Select 'Automatic' to use database trackers, or 'Fetch By Dates' to backfill specific time windows.",
            },
        ),
    ] = AutomaticRunParameters(),
    wait_for_new_data_to_arrive: Annotated[
        bool,
        Field(
            json_schema_extra={
                "title": "Wait for new data to arrive",
                "description": "If true, the flow will poll for new data until the end date is reached.",
            }
        ),
    ] = True,
    timeout_seconds: Annotated[
        int,
        Field(
            json_schema_extra={
                "title": "Timeout",
                "description": "Time in seconds to wait between polling for new data. Only used when waiting for new data.",
            }
        ),
    ] = 300,
    plot_last_3_days: Annotated[
        bool,
        Field(
            json_schema_extra={
                "title": "Plot last 3 days",
                "description": "If true, the flow will generate a quicklook plot of the downloaded data over the last 3 days.",
            }
        ),
    ] = True,
    start_date=None,
    end_date=None,
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
    use_database: Annotated[
        bool,
        Field(
            json_schema_extra={
                "title": "Use Database",
                "description": "If true, the flow will use the database to track progress.",
            }
        ),
    ] = True,
):
    """
    Runs continuously for one hour, sequentially polling the API
    for all 8 instruments every 5 minutes.
    """
    logger = try_get_prefect_logger(__name__)

    database = Database() if use_database else None
    settings = AppSettings()  # type: ignore

    auth_code = await get_secret_or_env_var(
        PREFECT_CONSTANTS.POLL_IALIRT.IALIRT_AUTH_CODE_SECRET_NAME,
        CONSTANTS.ENV_VAR_NAMES.IALIRT_AUTH_CODE,
    )
    settings.fetch_webtcad.api.auth_code = SecretStr(auth_code)

    end_date = getattr(run_parameters, "end_date", None)
    start_date = getattr(run_parameters, "start_date", None)

    if not end_date:
        end_date = DatetimeProvider.now().replace(minute=59, second=50, microsecond=0)
    if not start_date:
        start_date = DatetimeProvider.now().replace(minute=0, second=10, microsecond=0)
    logger.info(f"Starting IALirt Polling: {start_date} - {end_date}")

    combined_instruments = VALID_IALIRT_INSTRUMENTS + VALID_IALIRT_HK_INSTRUMENTS
    logger.info(f"All instruments: {combined_instruments}")
    if wait_for_new_data_to_arrive:
        iteration = 1
        while True:
            current_time = DatetimeProvider.now()

            if (end_date - current_time).total_seconds() < timeout_seconds:
                break

            logger.info(f"Starting 5-Minute I-ALiRT Polling Batch #{iteration}")

            for inst in combined_instruments:
                logger.info(f"Polling for {inst.upper()}")
                try:
                    await run_ialirt_polling_pipeline_task(
                        instrument=inst,
                        run_parameters=run_parameters,
                        database=database,
                        settings=settings,
                    )
                except Exception as e:
                    logger.error(f"Download failed for {inst.upper()}: {e}.")

            # Calculate how long the downloads took
            batch_duration = (DatetimeProvider.now() - current_time).total_seconds()

            # Sleep only for the remainder of the 5 minutes
            time_to_sleep = max(0, timeout_seconds - batch_duration)

            logger.info(
                f"Batch #{iteration} complete in {batch_duration:.1f}s. Sleeping for {time_to_sleep:.1f}s"
            )

            await asyncio.sleep(time_to_sleep)
            iteration += 1
    else:
        for inst in combined_instruments:
            logger.info(f"Polling for {inst.upper()}")
            await run_ialirt_polling_pipeline_task(
                instrument=inst,
                run_parameters=run_parameters,
                database=database,
                settings=settings,
            )

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

            latest_ialirt_date = database.get_workflow_progress(  # type: ignore
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

    return Completed(message="Hourly I-ALiRT Polling completed successfully.")

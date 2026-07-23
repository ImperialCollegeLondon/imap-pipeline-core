import asyncio
from datetime import UTC, datetime, timedelta
from typing import Annotated

import pytz
from prefect import flow, task
from prefect.blocks.notifications import MicrosoftTeamsWebhook
from prefect.cache_policies import NO_CACHE
from prefect.events import Event, emit_event
from prefect.runtime import flow_run
from prefect.states import Completed, Failed
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


def generate_flow_run_name(
    datetime_provider: DatetimeProvider = DatetimeProvider(),
) -> str:
    parameters = flow_run.parameters

    start_date: str = (
        parameters["start_date"].strftime("%d-%m-%YT%H:%M:%S")
        if parameters.get("start_date")
        else "last-update"
    )
    end_date: datetime = (
        parameters["end_date"]
        if parameters.get("end_date")
        else datetime_provider.end_of_hour()
    )

    return f"Poll-IALiRT-from-{start_date}-to-{end_date.strftime('%d-%m-%YT%H:%M:%S')}"


@task(
    name="Download I-ALiRT",
    task_run_name="Download-I-ALiRT-{instrument}",
    retries=1,
    retry_delay_seconds=30,
    cache_policy=NO_CACHE,
)
async def run_ialirt_polling_pipeline_task(
    instrument: str,
    database: Database | None,
    settings: AppSettings,
    run_parameters: AutomaticRunParameters | FetchByDatesRunParameters,
    datetime_provider: DatetimeProvider = DatetimeProvider(),
):
    """Wrap IALiRTPipeline in a Prefect task."""
    logger = try_get_prefect_logger(__name__)

    pipeline = IALiRTPipeline(
        instrument=instrument,
        database=database,
        settings=settings,
        datetime_provider=datetime_provider,
    )

    pipeline.build(run_parameters)
    await pipeline.run()

    result = pipeline.get_results()

    if not result.success:
        raise RuntimeError(f"I-ALiRT Pipeline failed for {instrument}: {result}")

    logger = try_get_prefect_logger(__name__)

    if instrument.endswith("_hk"):
        event_type = PREFECT_CONSTANTS.EVENT.IALIRT_HK_UPDATED
    else:
        event_type = PREFECT_CONSTANTS.EVENT.IALIRT_UPDATED

    logger.debug(f"Emitting {event_type} event for {instrument}")

    event: Event | None = emit_event(
        event=event_type,
        resource={
            "prefect.resource.id": f"prefect.flow-run.{flow_run.id}",
            "prefect.resource.name": flow_run.name,
            "prefect.resource.role": "flow-run",
        },
        payload={"instrument": instrument, "status": "completed"},
    )
    if event is None:
        logger.error(f"Failed to emit {event_type} event")

    return result


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.POLL_IALIRT,
    flow_run_name=generate_flow_run_name,
    log_prints=True,
)
async def poll_ialirt_flow(
    run_parameters: Annotated[
        AutomaticRunParameters | FetchByDatesRunParameters,
        Field(
            json_schema_extra={
                "title": "Run parameters",
                "description": "Parameters for the pipeline run. If 'start_date' and 'end_date' are not provided, the pipeline will automatically determine the date range based on the last workflow progress.",
            }
        ),
    ] = AutomaticRunParameters(),
    wait_for_new_data_to_arrive_up_to_an_hour: Annotated[
        bool,
        Field(
            json_schema_extra={
                "title": "Wait for new data to arrive",
                "description": "If true, the flow will poll for new data until the end date is reached.",
            }
        ),
    ] = True,
    polling_interval_seconds: Annotated[
        int,
        Field(
            json_schema_extra={
                "title": "Polling Interval",
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
    force_send_notification: bool = False,
    # Used for automated testing only, to override the default datetime provider with a test one
    datetime_provider: Annotated[
        None | DatetimeProvider,
        Field(exclude=True, frozen=True, json_schema_extra={"title": "(Do not use)"}),
    ] = None,
):
    """
    Runs continuously for one hour, sequentially polling the SDC API
    for all 8 instruments every 5 minutes.
    """
    logger = try_get_prefect_logger(__name__)

    if (
        wait_for_new_data_to_arrive_up_to_an_hour
        and type(run_parameters) is not AutomaticRunParameters
    ):
        raise ValueError(
            "When waiting for new data to arrive, run_parameters must be of type Automatic Run"
        )

    database = Database() if use_database else None
    settings = AppSettings()  # type: ignore
    datetime_provider = (
        DatetimeProvider() if datetime_provider is None else datetime_provider
    )

    auth_code = await get_secret_or_env_var(
        PREFECT_CONSTANTS.POLL_IALIRT.IALIRT_AUTH_CODE_SECRET_NAME,
        CONSTANTS.ENV_VAR_NAMES.IALIRT_AUTH_CODE,
    )
    settings.fetch_ialirt.api.auth_code = SecretStr(auth_code)

    combined_instruments = VALID_IALIRT_INSTRUMENTS + VALID_IALIRT_HK_INSTRUMENTS

    polling_window_end_date = datetime_provider.end_of_hour() - timedelta(
        seconds=polling_interval_seconds
    )

    iteration = 1
    while True:
        tasks = [
            run_ialirt_polling_pipeline_task(
                instrument=inst,
                database=database,
                settings=settings,
                datetime_provider=datetime_provider,
                run_parameters=run_parameters,
            )
            for inst in combined_instruments
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # error handling
        exception_count = sum(isinstance(r, Exception) for r in results)
        for inst, result in zip(combined_instruments, results):
            if isinstance(result, Exception):
                logger.error(f"Download failed for {inst.upper()}", exc_info=result)

        if exception_count == len(combined_instruments):
            return Failed(message="All I-ALiRT downloads failed")

        if not wait_for_new_data_to_arrive_up_to_an_hour:
            break

        if wait_for_new_data_to_arrive_up_to_an_hour:
            if (
                datetime_provider.now() + timedelta(seconds=polling_interval_seconds)
                >= polling_window_end_date
            ):
                logger.info(
                    f"Will be end of hour soon - stopping after {iteration} iterations"
                )
                break

        logger.info(
            f"Batch #{iteration} complete. Sleeping for {polling_interval_seconds:.1f}s"
        )
        await asyncio.sleep(polling_interval_seconds)
        iteration += 1

    if plot_last_3_days or force_send_notification:
        await quicklook_ialirt_flow(
            start_date=datetime_provider.today() - timedelta(days=2),
            end_date=datetime_provider.now(),
            combined_plot=True,
        )

        # If this is the 6 AM (UK time) polling job, send the latest figure to Teams
        uk_end_time = polling_window_end_date.replace(tzinfo=UTC).astimezone(
            pytz.timezone("Europe/London")
        )

        if (
            wait_for_new_data_to_arrive_up_to_an_hour and (uk_end_time.hour == 6)
        ) or force_send_notification:
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

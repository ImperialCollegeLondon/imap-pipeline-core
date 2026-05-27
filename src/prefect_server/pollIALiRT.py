import asyncio
from datetime import datetime
from typing import Annotated

from prefect import flow, task
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

    if instrument.endswith("_hk"):
        logger = try_get_prefect_logger(__name__)
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
            default_factory=AutomaticRunParameters,
            json_schema_extra={
                "title": "Run parameters",
                "description": "Select 'Automatic' to use database trackers, or 'Fetch By Dates' to backfill specific time windows.",
            },
        ),
    ],
    timeout_seconds: Annotated[
        int,
        Field(
            description="The polling interval in seconds. Defaults to 300 (5 minutes)."
        ),
    ] = PREFECT_CONSTANTS.POLL_IALIRT.DEFAULT_TIMEOUT_SECONDS,
    use_database: bool = True,
):
    """
    Runs continuously for one hour, sequentially polling the SDC API
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

    return Completed(message="Hourly I-ALiRT Polling completed successfully.")

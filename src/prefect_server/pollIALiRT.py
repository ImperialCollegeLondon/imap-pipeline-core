from datetime import datetime

from prefect import task
from prefect.events import Event, emit_event
from prefect.runtime import flow_run
from pydantic import SecretStr

from imap_mag.config.AppSettings import AppSettings
from imap_mag.data_pipelines import AutomaticRunParameters
from imap_mag.data_pipelines.IALiRTInstrumentPipeline import IALiRTPipeline
from imap_mag.db import Database
from imap_mag.util import DatetimeProvider
from prefect_server.constants import PREFECT_CONSTANTS
from prefect_server.prefectUtils import try_get_prefect_logger


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


@task(name="Execute I-ALiRT Pipeline", retries=2, retry_delay_seconds=30)
async def run_ialirt_pipeline_task(
    instrument: str, auth_code: str, use_database: bool = True
):
    """Wrap IALiRTPipeline in a Prefect task."""
    logger = try_get_prefect_logger(__name__)
    database = Database() if use_database else None
    settings = AppSettings()  # type: ignore

    settings.fetch_ialirt.api.auth_code = SecretStr(auth_code)

    pipeline = IALiRTPipeline(
        instrument=instrument, database=database, settings=settings
    )

    run_params = AutomaticRunParameters()

    logger.info(f"Building and running pipeline for {instrument.upper()}...")
    pipeline.build(run_params)
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

from typing import Annotated

from prefect import flow
from prefect.runtime import flow_run
from pydantic import Field

from imap_mag.config.AppSettings import AppSettings
from imap_mag.data_pipelines import AutomaticRunParameters
from imap_mag.data_pipelines.NOAAPipeline import NOAAPipeline
from imap_mag.db import Database
from imap_mag.util import DatetimeProvider
from prefect_server.constants import PREFECT_CONSTANTS


def generate_flow_run_name() -> str:
    parameters = flow_run.parameters

    instrument = parameters["instrument"]
    spacecraft = parameters["spacecraft"]
    now = DatetimeProvider().now()

    return f"Download-NOAA-{spacecraft}-{instrument}-at-{now.strftime('%d-%m-%Y-%H-%M-%S')}"


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.POLL_NOAA,
    log_prints=True,
    flow_run_name=lambda: generate_flow_run_name(),
)
async def poll_noaa_flow(
    spacecraft: Annotated[
        str,
        Field(
            json_schema_extra={
                "title": "Spacecraft",
                "description": "Spacecraft to download data for. Must be 'SOLAR1' or 'ACE'",
            }
        ),
    ],
    instrument: Annotated[
        str,
        Field(
            json_schema_extra={
                "title": "Instrument",
                "description": "Instrument data to download. Must be 'mag' for the magnetic field instrument or 'wind' for the plasma instrument",
            }
        ),
    ],
    run_parameters: Annotated[
        AutomaticRunParameters,
        Field(
            json_schema_extra={
                "title": "Run parameters",
                "description": "Parameters for the pipeline run",
            }
        ),
    ] = AutomaticRunParameters(),
    use_database: bool = True,
):
    """Poll small forces files from SDC API."""

    database = Database() if use_database else None
    settings = AppSettings()  # type: ignore

    pipeline = NOAAPipeline(
        spacecraft=spacecraft,
        instrument=instrument,
        database=database,
        settings=settings,
    )
    pipeline.build(run_parameters)
    await pipeline.run()
    result = pipeline.get_results()

    if not result.success:
        raise RuntimeError(f"Pipeline failed: {result}")

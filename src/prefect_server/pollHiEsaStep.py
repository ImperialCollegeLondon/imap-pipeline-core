from typing import Annotated

from prefect import flow
from pydantic import Field

from imap_mag.client.WebTCADLaTiS import HKWebTCADItems
from imap_mag.data_pipelines import AutomaticRunParameters, FetchByDatesRunParameters
from prefect_server.constants import PREFECT_CONSTANTS
from prefect_server.webTCADFlowHelpers import make_flow_run_name, run_webtcad_pipeline

_RUN_PARAMETERS_FIELD = Field(
    json_schema_extra={
        "title": "Run parameters",
        "description": "Parameters for the pipeline run. If 'start_date' and 'end_date' are not provided, the pipeline will automatically determine the date range based on the last workflow progress.",
    }
)


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.POLL_HI45_ESA_STEP,
    log_prints=True,
    flow_run_name=make_flow_run_name("HI45-ESA-STEP"),
)
async def poll_hi45_esa_step_flow(
    run_parameters: Annotated[
        AutomaticRunParameters | FetchByDatesRunParameters,
        _RUN_PARAMETERS_FIELD,
    ],
    use_database: bool = True,
):
    """Poll IMAP-Hi 45 instrument ESA STEP housekeeping data from WebTCAD LaTiS API."""

    await run_webtcad_pipeline(
        item=HKWebTCADItems.HI45_ESA_STEP,
        run_parameters=run_parameters,
        use_database=use_database,
    )


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.POLL_HI90_ESA_STEP,
    log_prints=True,
    flow_run_name=make_flow_run_name("HI90-ESA-STEP"),
)
async def poll_hi90_esa_step_flow(
    run_parameters: Annotated[
        AutomaticRunParameters | FetchByDatesRunParameters,
        _RUN_PARAMETERS_FIELD,
    ],
    use_database: bool = True,
):
    """Poll IMAP-Hi 90 instrument ESA STEP housekeeping data from WebTCAD LaTiS API."""

    await run_webtcad_pipeline(
        item=HKWebTCADItems.HI90_ESA_STEP,
        run_parameters=run_parameters,
        use_database=use_database,
    )

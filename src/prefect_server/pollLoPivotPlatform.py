from typing import Annotated

from prefect import flow
from pydantic import Field

from imap_mag.client.WebTCADLaTiS import HKWebTCADItems
from imap_mag.data_pipelines import AutomaticRunParameters, FetchByDatesRunParameters
from prefect_server.constants import PREFECT_CONSTANTS
from prefect_server.webTCADFlowHelpers import make_flow_run_name, run_webtcad_pipeline


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.POLL_LO_PIVOT_PLATFORM,
    log_prints=True,
    flow_run_name=make_flow_run_name("LO-PivotAngle"),
)
async def poll_lo_pivot_platform_flow(
    run_parameters: Annotated[
        AutomaticRunParameters | FetchByDatesRunParameters,
        Field(
            json_schema_extra={
                "title": "Run parameters",
                "description": "Parameters for the pipeline run. If 'start_date' and 'end_date' are not provided, the pipeline will automatically determine the date range based on the last workflow progress.",
            }
        ),
    ],
    use_database: bool = True,
):
    """Poll low pivot platform angle data from WebTCAD LaTiS API."""

    await run_webtcad_pipeline(
        item=HKWebTCADItems.LO_PIVOT_PLATFORM_ANGLE,
        run_parameters=run_parameters,
        use_database=use_database,
    )
